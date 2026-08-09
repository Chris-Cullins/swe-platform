package egressproxy

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	protocolVersion  = 1
	maxName          = 253
	requestHeaderLen = 10
	approvedPort     = 443
)

var protocolMagic = [4]byte{'S', 'W', 'E', 'E'}

type forwardRequest struct{ Name string }

func writeRequest(w io.Writer, name string) error {
	if canonical, err := canonicalTarget(name + ":443"); err != nil || canonical != name {
		return errors.New("invalid request name")
	}
	b := []byte(name)
	if len(b) == 0 || len(b) > maxName {
		return errors.New("invalid name length")
	}
	h := []byte{protocolMagic[0], protocolMagic[1], protocolMagic[2], protocolMagic[3], protocolVersion, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint16(h[6:8], approvedPort)
	binary.BigEndian.PutUint16(h[8:10], uint16(len(b)))
	if err := writeFull(w, h); err != nil {
		return err
	}
	return writeFull(w, b)
}

func readRequest(r io.Reader) (forwardRequest, error) {
	h := make([]byte, requestHeaderLen)
	if _, err := io.ReadFull(r, h); err != nil {
		return forwardRequest{}, err
	}
	if string(h[:4]) != string(protocolMagic[:]) || h[4] != protocolVersion || h[5] != 0 || binary.BigEndian.Uint16(h[6:8]) != approvedPort {
		return forwardRequest{}, errors.New("invalid protocol header")
	}
	n := int(binary.BigEndian.Uint16(h[8:10]))
	if n == 0 || n > maxName {
		return forwardRequest{}, errors.New("invalid protocol length")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return forwardRequest{}, err
	}
	name := string(b)
	if canonical, err := canonicalTarget(name + ":443"); err != nil || canonical != name {
		return forwardRequest{}, errors.New("invalid request name")
	}
	return forwardRequest{Name: name}, nil
}

func writeStatus(w io.Writer, ok bool) error {
	var s byte
	if !ok {
		s = 1
	}
	return writeFull(w, []byte{protocolMagic[0], protocolMagic[1], protocolMagic[2], protocolMagic[3], protocolVersion, s})
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) != 0 {
		n, err := w.Write(b)
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
func readStatus(r io.Reader) error {
	b := make([]byte, 6)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	if string(b[:4]) != string(protocolMagic[:]) || b[4] != protocolVersion || b[5] > 1 {
		return errors.New("invalid status")
	}
	if b[5] != 0 {
		return errors.New("forward denied")
	}
	return nil
}

func parseCONNECT(input io.Reader) (string, error) {
	const max = 8192
	r := bufio.NewReaderSize(io.LimitReader(input, max+1), max+1)
	var total int
	lines := make([]string, 0, 16)
	terminated := false
	for len(lines) < 66 {
		line, err := r.ReadString('\n')
		total += len(line)
		if total > max {
			return "", errors.New("headers too large")
		}
		if err != nil {
			return "", err
		}
		if len(line) < 2 || line[len(line)-2:] != "\r\n" {
			return "", errors.New("invalid line ending")
		}
		line = line[:len(line)-2]
		if line == "" {
			terminated = true
			break
		}
		lines = append(lines, line)
	}
	if !terminated || len(lines) == 0 || len(lines)-1 > 64 {
		return "", errors.New("invalid header count")
	}
	var method, target, version string
	if n, _ := fmt.Sscanf(lines[0], "%s %s %s", &method, &target, &version); n != 3 || method != "CONNECT" || version != "HTTP/1.1" {
		return "", errors.New("CONNECT HTTP/1.1 required")
	}
	if lines[0] != method+" "+target+" "+version {
		return "", errors.New("invalid request line")
	}
	hosts, contentLengths := 0, 0
	for _, l := range lines[1:] {
		if len(l) == 0 || l[0] == ' ' || l[0] == '\t' {
			return "", errors.New("folded header")
		}
		i := -1
		for j := range l {
			if l[j] == ':' {
				i = j
				break
			}
		}
		if i < 1 {
			return "", errors.New("invalid header")
		}
		for _, c := range []byte(l[:i]) {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))) {
				return "", errors.New("invalid header name")
			}
		}
		k, v := asciiLower(l[:i]), l[i+1:]
		for len(v) > 0 && (v[0] == ' ' || v[0] == '\t') {
			v = v[1:]
		}
		switch k {
		case "host":
			hosts++
			if v != target {
				return "", errors.New("Host mismatch")
			}
		case "content-length":
			contentLengths++
			if contentLengths != 1 || v != "0" {
				return "", errors.New("body forbidden")
			}
		case "transfer-encoding", "proxy-authorization":
			return "", errors.New("forbidden header")
		}
		for _, c := range []byte(l) {
			if c < 32 || c == 127 {
				return "", errors.New("control byte")
			}
		}
	}
	if hosts != 1 {
		return "", errors.New("exactly one Host required")
	}
	if r.Buffered() != 0 {
		return "", errors.New("pipelining forbidden")
	}
	return canonicalTarget(target)
}

func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}
