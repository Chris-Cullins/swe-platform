//go:build windows

package server

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var errInterruptUnsupported = errors.New("interrupt control is unsupported on Windows")

type processDomain struct {
	cmd     *exec.Cmd
	mu      sync.Mutex
	job     windows.Handle
	process *os.Process
}

func newProcessDomain(cmd *exec.Cmd) *processDomain { return &processDomain{cmd: cmd} }

func (d *processDomain) start() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		windows.CloseHandle(job)
		return err
	}

	// This follows the MIT-licensed microsoft/hcsshim job-list launch pattern:
	// put the Job and the exact inherited handles in the startup attribute list.
	const procThreadAttributeJobList = 0x0002000D
	attrs, err := windows.NewProcThreadAttributeList(2)
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	defer attrs.Delete()
	if err = attrs.Update(procThreadAttributeJobList, unsafe.Pointer(&job), unsafe.Sizeof(job)); err != nil {
		windows.CloseHandle(job)
		return err
	}
	files := []*os.File{d.cmd.Stdin.(*os.File), d.cmd.Stdout.(*os.File), d.cmd.Stderr.(*os.File)}
	handles := make([]windows.Handle, len(files))
	for i, f := range files {
		handles[i] = windows.Handle(f.Fd())
		if err = windows.SetHandleInformation(handles[i], windows.HANDLE_FLAG_INHERIT, windows.HANDLE_FLAG_INHERIT); err != nil {
			windows.CloseHandle(job)
			return err
		}
		defer windows.SetHandleInformation(handles[i], windows.HANDLE_FLAG_INHERIT, 0)
	}
	if err = attrs.Update(windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST, unsafe.Pointer(&handles[0]), uintptr(len(handles))*unsafe.Sizeof(handles[0])); err != nil {
		windows.CloseHandle(job)
		return err
	}
	path, err := exec.LookPath(d.cmd.Path)
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	path, err = windows.FullPath(path)
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	app, err := windows.UTF16PtrFromString(path)
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	line, err := windows.UTF16PtrFromString(windows.ComposeCommandLine(d.cmd.Args))
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	var cwd *uint16
	if d.cmd.Dir != "" {
		cwd, err = windows.UTF16PtrFromString(d.cmd.Dir)
		if err != nil {
			windows.CloseHandle(job)
			return err
		}
	}
	env := make([]uint16, 0)
	for _, entry := range d.cmd.Env {
		encoded, encodeErr := windows.UTF16FromString(entry)
		if encodeErr != nil {
			windows.CloseHandle(job)
			return encodeErr
		}
		env = append(env, encoded...)
	}
	if len(env) == 0 {
		env = append(env, 0)
	}
	env = append(env, 0)
	si := windows.StartupInfoEx{ProcThreadAttributeList: attrs.List()}
	si.StartupInfo.Cb = uint32(unsafe.Sizeof(si))
	si.StartupInfo.Flags = windows.STARTF_USESTDHANDLES
	si.StartupInfo.StdInput, si.StartupInfo.StdOutput, si.StartupInfo.StdErr = handles[0], handles[1], handles[2]
	var pi windows.ProcessInformation
	err = windows.CreateProcess(app, line, nil, nil, true, windows.EXTENDED_STARTUPINFO_PRESENT|windows.CREATE_UNICODE_ENVIRONMENT, &env[0], cwd, &si.StartupInfo, &pi)
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	windows.CloseHandle(pi.Thread)
	process, findErr := os.FindProcess(int(pi.ProcessId))
	windows.CloseHandle(pi.Process)
	if findErr != nil {
		windows.TerminateJobObject(job, 1)
		windows.CloseHandle(job)
		return findErr
	}
	d.job = job
	d.process = process
	return nil
}

func (d *processDomain) wait() error {
	state, err := d.process.Wait()
	if err == nil && !state.Success() {
		return &exec.ExitError{ProcessState: state}
	}
	return err
}
func (d *processDomain) interrupt() error { return errInterruptUnsupported }
func (d *processDomain) terminate() error { return d.force() }

func (d *processDomain) force() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.job == 0 {
		return nil
	}
	if err := windows.TerminateJobObject(d.job, 1); err != nil {
		return err
	}
	return d.waitEmptyLocked(10 * time.Second)
}

type jobBasicAccounting struct {
	TotalUserTime, TotalKernelTime                                                 int64
	ThisPeriodTotalUserTime, ThisPeriodTotalKernelTime                             int64
	TotalPageFaultCount, TotalProcesses, ActiveProcesses, TotalTerminatedProcesses uint32
}

func (d *processDomain) activeProcessesLocked() (uint32, error) {
	var info jobBasicAccounting
	err := windows.QueryInformationJobObject(d.job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), nil)
	return info.ActiveProcesses, err
}
func (d *processDomain) waitEmptyLocked(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		n, err := d.activeProcessesLocked()
		if err != nil || n == 0 {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for job to empty (%d active)", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (d *processDomain) close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.job == 0 {
		return nil
	}
	if err := windows.TerminateJobObject(d.job, 1); err != nil {
		return err
	}
	if err := d.waitEmptyLocked(10 * time.Second); err != nil {
		return err
	}
	err := windows.CloseHandle(d.job)
	d.job = 0
	return err
}
