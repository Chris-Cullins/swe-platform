package main

import (
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
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("Node listener did not accept a TCP connection")
}
