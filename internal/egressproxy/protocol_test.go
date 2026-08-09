package egressproxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestParseCONNECT(t *testing.T) {
	valid := "CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\n\r\n"
	bad := []string{
		"GET api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\n\r\n", "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
		"CONNECT https://api.example.com:443/ HTTP/1.1\r\nHost: https://api.example.com:443/\r\n\r\n",
		"CONNECT u@api.example.com:443 HTTP/1.1\r\nHost: u@api.example.com:443\r\n\r\n",
		"CONNECT api%2eexample.com:443 HTTP/1.1\r\nHost: api%2eexample.com:443\r\n\r\n",
		"CONNECT [2001:4860::1]:443 HTTP/1.1\r\nHost: [2001:4860::1]:443\r\n\r\n",
		"CONNECT 127.0.0.1:443 HTTP/1.1\r\nHost: 127.0.0.1:443\r\n\r\n",
		"CONNECT 0x7f.0.0.1:443 HTTP/1.1\r\nHost: 0x7f.0.0.1:443\r\n\r\n",
		"CONNECT 0177.0.0.1:443 HTTP/1.1\r\nHost: 0177.0.0.1:443\r\n\r\n",
		"CONNECT 2130706433:443 HTTP/1.1\r\nHost: 2130706433:443\r\n\r\n",
		"CONNECT API.example.com:443 HTTP/1.1\r\nHost: API.example.com:443\r\n\r\n",
		"CONNECT xn--x.example:443 HTTP/1.1\r\nHost: xn--x.example:443\r\n\r\n",
		"CONNECT localhost:443 HTTP/1.1\r\nHost: localhost:443\r\n\r\n",
		"CONNECT  api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\n\r\n",
		"CONNECT api.example.com:443 HTTP/1.1\nHost: api.example.com:443\n\n",
		"CONNECT api.example.com:443 HTTP/1.1\r\n Host: api.example.com:443\r\n\r\n",
		"CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\nTransfer-Encoding: chunked\r\n\r\n",
		"CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\nContent-Length: 1\r\n\r\n",
		"CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
		"CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\nProxy-Authorization: x\r\n\r\n",
		"CONNECT api.example.com:443 HTTP/1.1\r\n\r\n", valid[:len(valid)-2] + "Host: api.example.com:443\r\n\r\n",
		"CONNECT api.example.com:443 HTTP/1.1\r\nHost: other.example.com:443\r\n\r\n", valid + "x",
		"CONNECT api.example.com:443 HTTP/1.1\r\nBad Name: x\r\nHost: api.example.com:443\r\n\r\n",
	}
	if got, err := parseCONNECT(bufio.NewReader(strings.NewReader(valid))); err != nil || got != "api.example.com" {
		t.Fatalf("valid: %q %v", got, err)
	}
	for i, in := range bad {
		if _, err := parseCONNECT(bufio.NewReader(strings.NewReader(in))); err == nil {
			t.Errorf("bad[%d] accepted", i)
		}
	}
	headers := "CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\n" + strings.Repeat("X: y\r\n", 64) + "\r\n"
	if _, err := parseCONNECT(bufio.NewReader(strings.NewReader(headers))); err == nil {
		t.Error(">64 headers accepted")
	}
	if _, err := parseCONNECT(bufio.NewReader(strings.NewReader(valid[:len(valid)-2] + "X: " + strings.Repeat("a", 8192) + "\r\n\r\n"))); err == nil {
		t.Error(">8KiB accepted")
	}
}

func TestFrames(t *testing.T) {
	var b bytes.Buffer
	if err := writeRequest(&b, "api.example.com"); err != nil {
		t.Fatal(err)
	}
	frame := append([]byte(nil), b.Bytes()...)
	r, err := readRequest(&b)
	if err != nil || r.Name != "api.example.com" {
		t.Fatal(r, err)
	}
	if binary.BigEndian.Uint16(frame[6:8]) != 443 {
		t.Fatal("request does not explicitly encode port 443")
	}
	for _, mutate := range []func([]byte){func(p []byte) { p[0] = 'X' }, func(p []byte) { p[4] = 2 }, func(p []byte) { p[5] = 1 }, func(p []byte) { binary.BigEndian.PutUint16(p[6:8], 80) }, func(p []byte) { binary.BigEndian.PutUint16(p[8:10], 0) }} {
		b.Reset()
		_ = writeRequest(&b, "api.example.com")
		p := append([]byte(nil), b.Bytes()...)
		mutate(p)
		if _, e := readRequest(bytes.NewReader(p)); e == nil {
			t.Error("malformed accepted")
		}
	}
	for _, n := range []string{"API.example.com", "xn--x.example", "localhost"} {
		var x bytes.Buffer
		p := []byte(n)
		x.Write([]byte{'S', 'W', 'E', 'E', 1, 0, 1, 187, byte(len(p) >> 8), byte(len(p))})
		x.Write(p)
		if _, e := readRequest(&x); e == nil {
			t.Errorf("name %q accepted", n)
		}
	}
	for _, ok := range []bool{true, false} {
		b.Reset()
		_ = writeStatus(&b, ok)
		e := readStatus(&b)
		if (e == nil) != ok {
			t.Errorf("status %v: %v", ok, e)
		}
	}
	for i := 0; i < len(frame); i++ {
		if _, err := readRequest(bytes.NewReader(frame[:i])); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	for _, statusLen := range []int{0, 1, 2, 3, 4, 5} {
		if err := readStatus(bytes.NewReader([]byte{'S', 'W', 'E', 'E', 1, 0}[:statusLen])); err == nil {
			t.Fatalf("accepted status truncation at %d", statusLen)
		}
	}
	short := &shortWriter{limit: 1}
	if err := writeRequest(short, "api.example.com"); err != nil {
		t.Fatal(err)
	}
	if _, err := readRequest(bytes.NewReader(short.b.Bytes())); err != nil {
		t.Fatal(err)
	}
	short = &shortWriter{limit: 1}
	if err := writeStatus(short, true); err != nil || readStatus(bytes.NewReader(short.b.Bytes())) != nil {
		t.Fatal("short status write", err)
	}
}

type shortWriter struct {
	b     bytes.Buffer
	limit int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		p = p[:w.limit]
	}
	return w.b.Write(p)
}

func FuzzParseCONNECT(f *testing.F) {
	f.Add([]byte("CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\n\r\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 16384 {
			return
		}
		_, _ = parseCONNECT(bufio.NewReader(bytes.NewReader(b)))
	})
}
func FuzzReadRequest(f *testing.F) {
	var b bytes.Buffer
	_ = writeRequest(&b, "api.example.com")
	f.Add(b.Bytes())
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1024 {
			return
		}
		_, _ = readRequest(bytes.NewReader(b))
	})
}
