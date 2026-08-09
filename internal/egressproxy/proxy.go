package egressproxy

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"
)

type Identity struct {
	Fingerprint [32]byte
	Subject     string
}
type Authorization struct {
	ExecutionKey, ProjectKey string
	DeniedPrefixes           []netip.Prefix
}
type Authorizer interface {
	Authorize(context.Context, Identity, string) (Authorization, error)
}
type DisabledAuthorizer struct{}

func (DisabledAuthorizer) Authorize(context.Context, Identity, string) (Authorization, error) {
	return Authorization{}, errors.New("egress proxy is disabled")
}

type Server struct {
	TLSConfig  *tls.Config
	Authorizer Authorizer
	Resolver   DNSResolver
	Quotas     *QuotaManager
	Dialer     func(context.Context, string, string) (net.Conn, error)
	// MaxPreAuth bounds accepted connections that have not completed authorization.
	// Zero selects the default of 2048.
	MaxPreAuth int
}

func (s *Server) Serve(l net.Listener) error {
	if s.Authorizer == nil || s.Resolver == nil {
		return errors.New("authorizer and resolver required")
	}
	if s.TLSConfig == nil || len(s.TLSConfig.Certificates) == 0 || s.TLSConfig.MinVersion != tls.VersionTLS13 || s.TLSConfig.MaxVersion != tls.VersionTLS13 || s.TLSConfig.ClientAuth != tls.RequireAnyClientCert || !s.TLSConfig.SessionTicketsDisabled {
		return errors.New("TLS 1.3 client certificates required")
	}
	if s.Quotas == nil {
		s.Quotas = NewQuotaManager()
	}
	tl := tls.NewListener(l, s.TLSConfig)
	max := s.MaxPreAuth
	if max == 0 {
		max = 2048
	}
	if max < 0 {
		return errors.New("invalid pre-auth connection limit")
	}
	preAuth := make(chan struct{}, max)
	for {
		c, e := tl.Accept()
		if e != nil {
			return e
		}
		select {
		case preAuth <- struct{}{}:
			go func() {
				release := sync.OnceFunc(func() { <-preAuth })
				defer release()
				s.handleWithPreAuth(c, release)
			}()
		default:
			_ = c.Close()
		}
	}
}

func (s *Server) handle(c net.Conn) {
	s.handleWithPreAuth(c, func() {})
}

func (s *Server) handleWithPreAuth(c net.Conn, releasePreAuth func()) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	tc, ok := c.(*tls.Conn)
	if !ok || tc.Handshake() != nil {
		return
	}
	st := tc.ConnectionState()
	if len(st.PeerCertificates) < 1 {
		return
	}
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	req, e := readRequest(c)
	if e != nil {
		return
	}
	name, e := canonicalTarget(req.Name + ":443")
	if e != nil {
		s.deny(c)
		return
	}
	// The self-signed leaf is untrusted lookup material. The Authorizer is the
	// authority and must pin this SHA-256 fingerprint to the execution.
	id := Identity{Fingerprint: sha256.Sum256(st.PeerCertificates[0].Raw), Subject: st.PeerCertificates[0].Subject.String()}
	authCtx, authCancel := context.WithTimeout(context.Background(), 5*time.Second)
	a, e := s.Authorizer.Authorize(authCtx, id, name)
	authCancel()
	releasePreAuth()
	if e != nil {
		s.deny(c)
		return
	}
	reservation, e := s.quota().Reserve(a.ExecutionKey, a.ProjectKey)
	if e != nil {
		s.deny(c)
		return
	}
	defer reservation.Release()
	ips, e := Resolve(context.Background(), s.Resolver, name, a.DeniedPrefixes)
	if e != nil {
		s.deny(c)
		return
	}
	var out net.Conn
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	for _, ip := range ips {
		ctx := dialCtx
		out, e = s.dialer()(ctx, "tcp", net.JoinHostPort(ip.String(), "443"))
		if e == nil {
			break
		}
	}
	dialCancel()
	if out == nil {
		s.deny(c)
		return
	}
	defer out.Close()
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if writeStatus(c, true) != nil {
		return
	}
	hello, e := readClientHello(c, name)
	if e != nil {
		return
	}
	if e = reservation.Activate(); e != nil {
		return
	}
	_ = out.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, e = out.Write(hello); e != nil {
		return
	}
	_ = c.SetDeadline(time.Time{})
	_ = out.SetDeadline(time.Time{})
	relay(c, out, 5*time.Minute, time.Hour)
}
func (s *Server) deny(c net.Conn) {
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_ = writeStatus(c, false)
}
func (s *Server) dialer() func(context.Context, string, string) (net.Conn, error) {
	if s.Dialer != nil {
		return s.Dialer
	}
	d := net.Dialer{}
	return d.DialContext
}
func (s *Server) quota() *QuotaManager {
	return s.Quotas
}

type Forwarder struct {
	TLSConfig     *tls.Config
	ServerAddress string
	// MaxConnections bounds all accepted connections for their entire lifetime.
	// Zero selects the default of 32.
	MaxConnections int
	DialTLS        func(context.Context, string, string, *tls.Config) (net.Conn, error)
}

func (f *Forwarder) Serve(l net.Listener) error {
	if f.TLSConfig == nil || f.TLSConfig.MinVersion != tls.VersionTLS13 || f.TLSConfig.MaxVersion != tls.VersionTLS13 || f.TLSConfig.RootCAs == nil || f.TLSConfig.ServerName == "" || len(f.TLSConfig.Certificates) == 0 {
		return errors.New("complete TLS 1.3 configuration required")
	}
	if ip := net.ParseIP(hostOnly(l.Addr().String())); ip == nil || !ip.IsLoopback() {
		return errors.New("forward listener must be loopback")
	}
	max := f.MaxConnections
	if max == 0 {
		max = 32
	}
	if max < 0 {
		return errors.New("invalid connection limit")
	}
	connections := make(chan struct{}, max)
	for {
		c, e := l.Accept()
		if e != nil {
			return e
		}
		select {
		case connections <- struct{}{}:
			go func() {
				defer func() { <-connections }()
				f.handle(c)
			}()
		default:
			_ = c.Close()
		}
	}
}
func hostOnly(s string) string {
	h, _, e := net.SplitHostPort(s)
	if e != nil {
		return ""
	}
	return h
}
func (f *Forwarder) handle(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	name, e := parseCONNECT(c)
	if e != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	up, e := f.dialTLS(ctx, "tcp", f.ServerAddress)
	if e != nil {
		return
	}
	defer up.Close()
	_ = up.SetDeadline(time.Now().Add(5 * time.Second))
	if writeRequest(up, name) != nil || readStatus(up) != nil {
		return
	}
	if _, e = io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); e != nil {
		return
	}
	_ = c.SetDeadline(time.Time{})
	_ = up.SetDeadline(time.Time{})
	relay(c, up, 5*time.Minute, time.Hour)
}

func (f *Forwarder) dialTLS(ctx context.Context, network, address string) (net.Conn, error) {
	if f.DialTLS != nil {
		return f.DialTLS(ctx, network, address, f.TLSConfig)
	}
	d := tls.Dialer{Config: f.TLSConfig}
	return d.DialContext(ctx, network, address)
}

func readClientHello(r io.Reader, want string) ([]byte, error) {
	deadline, ok := r.(interface{ SetReadDeadline(time.Time) error })
	if ok {
		_ = deadline.SetReadDeadline(time.Now().Add(5 * time.Second))
	}
	var all, hs []byte
	for records := 0; records < 64 && len(all) < 65536; records++ {
		h := make([]byte, 5)
		if _, e := io.ReadFull(r, h); e != nil {
			return nil, e
		}
		if h[0] != 22 || h[1] != 3 || h[2] > 4 {
			return nil, errors.New("non-TLS ClientHello")
		}
		n := int(binary.BigEndian.Uint16(h[3:]))
		if n == 0 || len(all)+5+n > 65536 {
			return nil, errors.New("ClientHello oversized")
		}
		b := make([]byte, n)
		if _, e := io.ReadFull(r, b); e != nil {
			return nil, e
		}
		all = append(all, h...)
		all = append(all, b...)
		hs = append(hs, b...)
		if len(hs) >= 4 {
			z := int(hs[1])<<16 | int(hs[2])<<8 | int(hs[3])
			if z+4 > 65536 {
				return nil, errors.New("handshake oversized")
			}
			if len(hs) >= z+4 {
				if len(hs) != z+4 {
					return nil, errors.New("bytes after ClientHello")
				}
				sni, e := clientHelloSNI(hs[:z+4])
				if e != nil || sni != want {
					return nil, errors.New("SNI mismatch")
				}
				return all, nil
			}
		}
	}
	return nil, errors.New("ClientHello incomplete")
}
func clientHelloSNI(b []byte) (string, error) {
	if len(b) < 4 || b[0] != 1 {
		return "", errors.New("not ClientHello")
	}
	if len(b) < 6 || b[4] != 3 || b[5] < 3 {
		return "", errors.New("legacy TLS version too old")
	}
	p := 4 + 2 + 32
	if p >= len(b) {
		return "", io.ErrUnexpectedEOF
	}
	sid := int(b[p])
	p += 1 + sid
	if p+2 > len(b) {
		return "", io.ErrUnexpectedEOF
	}
	cs := int(binary.BigEndian.Uint16(b[p:]))
	if cs == 0 || cs%2 != 0 {
		return "", errors.New("bad cipher suites")
	}
	p += 2 + cs
	if p >= len(b) {
		return "", io.ErrUnexpectedEOF
	}
	cm := int(b[p])
	if cm == 0 || p+1+cm > len(b) || !bytesContains(b[p+1:p+1+cm], 0) {
		return "", errors.New("null compression required")
	}
	p += 1 + cm
	if p+2 > len(b) {
		return "", io.ErrUnexpectedEOF
	}
	el := int(binary.BigEndian.Uint16(b[p:]))
	p += 2
	if p+el != len(b) {
		return "", errors.New("bad extensions")
	}
	var sni string
	seenExtensions := make(map[uint16]bool)
	for p < len(b) {
		if p+4 > len(b) {
			return "", io.ErrUnexpectedEOF
		}
		typ, n := binary.BigEndian.Uint16(b[p:]), int(binary.BigEndian.Uint16(b[p+2:]))
		p += 4
		if p+n > len(b) {
			return "", io.ErrUnexpectedEOF
		}
		v := b[p : p+n]
		p += n
		if seenExtensions[typ] {
			return "", errors.New("duplicate extension")
		}
		seenExtensions[typ] = true
		if typ == 0xfe0d || typ == 0xffce {
			return "", errors.New("ECH forbidden")
		}
		if typ == 0 {
			if sni != "" || len(v) < 5 {
				return "", errors.New("bad SNI")
			}
			ln := int(binary.BigEndian.Uint16(v))
			if ln+2 != len(v) || v[2] != 0 {
				return "", errors.New("bad SNI")
			}
			nn := int(binary.BigEndian.Uint16(v[3:]))
			if nn+5 != len(v) {
				return "", errors.New("bad SNI")
			}
			sni = string(v[5:])
		}
	}
	if sni == "" {
		return "", errors.New("SNI required")
	}
	if _, e := canonicalTarget(sni + ":443"); e != nil {
		return "", e
	}
	return sni, nil
}

func bytesContains(b []byte, want byte) bool {
	for _, v := range b {
		if v == want {
			return true
		}
	}
	return false
}

func relay(a, b net.Conn, idle, absolute time.Duration) {
	end := time.Now().Add(absolute)
	var wg sync.WaitGroup
	closeBoth := sync.OnceFunc(func() {
		_ = a.Close()
		_ = b.Close()
	})
	cp := func(dst, src net.Conn) {
		defer wg.Done()
		defer closeBoth()
		buf := make([]byte, 32<<10)
		for {
			d := time.Now().Add(idle)
			if d.After(end) {
				d = end
			}
			_ = src.SetReadDeadline(d)
			n, e := src.Read(buf)
			if n > 0 {
				_ = dst.SetWriteDeadline(d)
				if _, w := dst.Write(buf[:n]); w != nil {
					return
				}
			}
			if e != nil {
				return
			}
		}
	}
	wg.Add(2)
	go cp(a, b)
	go cp(b, a)
	wg.Wait()
}
