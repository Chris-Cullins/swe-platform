package main

import (
	"bytes"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestListenerScriptAcceptsTCPConnections(t *testing.T) {
	if strings.Contains(listenerScript, `\"`) {
		t.Fatal("listener script contains escaped quotes that Node receives literally")
	}
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("node", "-e", listenerScript, strconv.Itoa(port))
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var exitErr error
	go func() { exitErr = command.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		<-done
	})
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	// The root suite starts many packages concurrently on CI. Allow bounded
	// process startup, but report an early Node exit immediately with diagnostics.
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		select {
		case <-done:
			t.Fatalf("Node listener exited before accepting TCP: %v\n%s", exitErr, output.String())
		default:
		}
		connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = command.Process.Kill()
	<-done
	t.Fatalf("Node listener did not accept a TCP connection within 10 seconds: %v\n%s", exitErr, output.String())
}
