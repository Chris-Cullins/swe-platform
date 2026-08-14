package egressproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

type authorizerFunc func(context.Context, Identity, string) (Authorization, error)

func (f authorizerFunc) Authorize(ctx context.Context, id Identity, name string) (Authorization, error) {
	return f(ctx, id, name)
}

type panicResolver struct{}

func (panicResolver) Query(context.Context, dnsmessage.Name, dnsmessage.Type) ([]dnsmessage.Resource, error) {
	panic("resolver invoked after denial")
}

type releaseCheckingResolver struct {
	released <-chan struct{}
	t        *testing.T
}

func (r releaseCheckingResolver) Query(context.Context, dnsmessage.Name, dnsmessage.Type) ([]dnsmessage.Resource, error) {
	select {
	case <-r.released:
	default:
		r.t.Error("pre-authorization slot was retained after authorization")
	}
	return nil, errors.New("stop after authorization")
}

func hello(name string, extra ...uint16) []byte {
	ext := func(typ uint16, v []byte) []byte {
		b := make([]byte, 4+len(v))
		binary.BigEndian.PutUint16(b, typ)
		binary.BigEndian.PutUint16(b[2:], uint16(len(v)))
		copy(b[4:], v)
		return b
	}
	s := make([]byte, 5+len(name))
	binary.BigEndian.PutUint16(s, uint16(3+len(name)))
	s[2] = 0
	binary.BigEndian.PutUint16(s[3:], uint16(len(name)))
	copy(s[5:], name)
	exts := ext(0, s)
	for _, x := range extra {
		exts = append(exts, ext(x, nil)...)
	}
	body := append([]byte{3, 3}, make([]byte, 32)...)
	body = append(body, 0, 0, 2, 0x13, 1, 1, 0)
	z := make([]byte, 2)
	binary.BigEndian.PutUint16(z, uint16(len(exts)))
	body = append(body, z...)
	body = append(body, exts...)
	hs := append([]byte{1, byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}, body...)
	rec := []byte{22, 3, 3, byte(len(hs) >> 8), byte(len(hs))}
	return append(rec, hs...)
}
func TestClientHello(t *testing.T) {
	b := hello("api.example.com")
	got, e := readClientHello(bytes.NewReader(b), "api.example.com")
	if e != nil || !bytes.Equal(got, b) {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		name  string
		extra []uint16
	}{{"other.example.com", nil}, {"API.example.com", nil}, {"api.example.com", []uint16{0xfe0d}}, {"api.example.com", []uint16{0xffce}}} {
		if _, e := readClientHello(bytes.NewReader(hello(tc.name, tc.extra...)), "api.example.com"); e == nil {
			t.Errorf("accepted %+v", tc)
		}
	}
	if _, e := readClientHello(bytes.NewReader(hello("api.example.com", 0)), "api.example.com"); e == nil {
		t.Error("duplicate SNI accepted")
	}
	for _, b := range [][]byte{{1, 2, 3}, hello("api.example.com")[:10]} {
		if _, e := readClientHello(bytes.NewReader(b), "api.example.com"); e == nil {
			t.Error("malformed accepted")
		}
	}
	// Split one handshake over two records and ensure exact wire bytes survive.
	hs := b[5:]
	cut := len(hs) / 2
	fragmented := append([]byte{22, 3, 3, byte(cut >> 8), byte(cut)}, hs[:cut]...)
	n := len(hs) - cut
	fragmented = append(fragmented, 22, 3, 3, byte(n>>8), byte(n))
	fragmented = append(fragmented, hs[cut:]...)
	got, e = readClientHello(bytes.NewReader(fragmented), "api.example.com")
	if e != nil || !bytes.Equal(got, fragmented) {
		t.Fatal("fragment", e)
	}
}
func TestDisabledAndLoopback(t *testing.T) {
	if _, err := (DisabledAuthorizer{}).Authorize(context.Background(), Identity{}, "api.example.com"); err == nil {
		t.Error("disabled authorizer allowed")
	}
	if got := hostOnly(net.JoinHostPort("127.0.0.1", "443")); got != "127.0.0.1" {
		t.Fatalf("hostOnly=%q", got)
	}
}

func testCurrentAuthorization(release func()) Authorization {
	return Authorization{ExecutionKey: "execution", ProjectKey: "project", Currentness: make(chan struct{}), ReleaseCurrentness: release}
}

func TestForwarderMaxConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cert := testCertificate(t, "forwarder")
	forwarder := &Forwarder{
		TLSConfig:      &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: x509.NewCertPool(), ServerName: "proxy.example.com", MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13},
		MaxConnections: 1,
	}
	var dialCount atomic.Int32
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	forwarder.DialTLS = func(context.Context, string, string, *tls.Config) (net.Conn, error) {
		switch count := dialCount.Add(1); count {
		case 1:
			close(firstEntered)
			<-release
		case 2:
			close(secondEntered)
		default:
			t.Errorf("DialTLS called %d times", count)
		}
		return nil, errors.New("test dial")
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- forwarder.Serve(listener) }()

	first, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := io.WriteString(first, "CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first DialTLS did not start")
	}

	second, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(2 * time.Second))
	one := make([]byte, 1)
	if _, err := second.Read(one); err == nil {
		t.Fatal("excess connection was not closed")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("excess connection remained open until timeout: %v", err)
	} else if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("excess connection read returned %v, want EOF or connection reset", err)
	}
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("DialTLS count while first connection blocked = %d, want 1", got)
	}

	close(release)
	_ = first.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := first.Read(one); err == nil {
		t.Fatal("first handler did not close its connection")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("first handler did not end before timeout: %v", err)
	}

	// Observing the first connection's FIN does not guarantee the serving
	// goroutine has run its immediately-following deferred slot release. Retry
	// bounded connection attempts until one observes that released slot.
	deadline := time.Now().Add(2 * time.Second)
	acquired := false
	for !acquired {
		third, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := io.WriteString(third, "CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\n\r\n")
		select {
		case <-secondEntered:
			acquired = true
			_ = third.Close()
		case <-time.After(50 * time.Millisecond):
			_ = third.Close()
		}
		if !acquired && writeErr != nil && time.Now().After(deadline) {
			t.Fatalf("connection attempts failed after slot release: %v", writeErr)
		}
		if !acquired && time.Now().After(deadline) {
			t.Fatal("no connection acquired the released slot")
		}
	}
	if got := dialCount.Load(); got != 2 {
		t.Fatalf("DialTLS count = %d, want 2", got)
	}

	_ = listener.Close()
	select {
	case <-serveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop")
	}
}

func TestRelayClosesBothDirectionsWhenOneSideEnds(t *testing.T) {
	client, relayClient := net.Pipe()
	relayUpstream, upstream := net.Pipe()
	done := make(chan struct{})
	go func() {
		relay(relayClient, relayUpstream, time.Minute, time.Hour)
		close(done)
	}()

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	_ = upstream.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := upstream.Read(make([]byte, 1)); err == nil {
		t.Fatal("opposite relay direction remained open")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("opposite relay direction blocked until deadline: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not release after one direction ended")
	}
}

func TestSelfSignedClientFingerprintAuthorizerDenial(t *testing.T) {
	serverCert := testCertificate(t, "server")
	clientCert := testCertificate(t, "execution")
	want := sha256.Sum256(clientCert.Certificate[0])
	called := false
	s := &Server{
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, SessionTicketsDisabled: true},
		Resolver:  panicResolver{},
		Authorizer: authorizerFunc(func(_ context.Context, id Identity, name string) (Authorization, error) {
			called = true
			if id.Fingerprint != want || name != "api.example.com" {
				t.Errorf("identity/name mismatch: %x %q", id.Fingerprint, name)
			}
			return Authorization{}, errors.New("deny")
		}),
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			t.Error("dial invoked after denial")
			return nil, errors.New("unexpected")
		},
	}
	serverSide, clientSide := net.Pipe()
	done := make(chan struct{})
	go func() { s.handle(tls.Server(serverSide, s.TLSConfig)); close(done) }()
	c := tls.Client(clientSide, &tls.Config{Certificates: []tls.Certificate{clientCert}, InsecureSkipVerify: true, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13})
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := writeRequest(c, "api.example.com"); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(c); err == nil {
		t.Fatal("denial status accepted")
	}
	_ = c.Close()
	<-done
	if !called {
		t.Fatal("authorizer not called")
	}
}

func TestServerReleasesPreAuthSlotBeforeResolution(t *testing.T) {
	serverCert := testCertificate(t, "server")
	clientCert := testCertificate(t, "execution")
	released := make(chan struct{})
	authorizationReleased := make(chan struct{})
	s := &Server{
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, SessionTicketsDisabled: true},
		Resolver:  releaseCheckingResolver{released: released, t: t},
		Quotas:    NewQuotaManager(),
		Authorizer: authorizerFunc(func(context.Context, Identity, string) (Authorization, error) {
			return testCurrentAuthorization(sync.OnceFunc(func() { close(authorizationReleased) })), nil
		}),
	}
	serverSide, clientSide := net.Pipe()
	done := make(chan struct{})
	go func() {
		s.handleWithPreAuth(tls.Server(serverSide, s.TLSConfig), func() { close(released) })
		close(done)
	}()
	c := tls.Client(clientSide, &tls.Config{Certificates: []tls.Certificate{clientCert}, InsecureSkipVerify: true, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13})
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := writeRequest(c, "api.example.com"); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(c); err == nil {
		t.Fatal("resolution failure status accepted")
	}
	_ = c.Close()
	<-done
	select {
	case <-authorizationReleased:
	default:
		t.Fatal("handler did not release currentness after resolution failure")
	}
}

func TestServerClosesEstablishedTunnelAndReleasesOnCurrentnessRevocation(t *testing.T) {
	serverCert := testCertificate(t, "server")
	clientCert := testCertificate(t, "execution")
	currentness := make(chan struct{})
	released := make(chan struct{})
	upstreamServer, upstreamPeer := net.Pipe()
	defer upstreamPeer.Close()
	s := &Server{
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, SessionTicketsDisabled: true},
		Resolver: &fakeResolver{answers: map[string][]dnsmessage.Resource{
			"api.example.com./TypeA": {a("api.example.com.", [4]byte{8, 8, 8, 8})},
		}},
		Quotas: NewQuotaManager(),
		Authorizer: authorizerFunc(func(context.Context, Identity, string) (Authorization, error) {
			return Authorization{ExecutionKey: "execution", ProjectKey: "project", Currentness: currentness, ReleaseCurrentness: sync.OnceFunc(func() { close(released) })}, nil
		}),
		Dialer: func(context.Context, string, string) (net.Conn, error) { return upstreamServer, nil },
	}
	serverSide, clientSide := net.Pipe()
	handlerDone := make(chan struct{})
	go func() {
		s.handle(tls.Server(serverSide, s.TLSConfig))
		close(handlerDone)
	}()
	client := tls.Client(clientSide, &tls.Config{Certificates: []tls.Certificate{clientCert}, InsecureSkipVerify: true, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13})
	if err := client.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := writeRequest(client, "api.example.com"); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(client); err != nil {
		t.Fatal(err)
	}
	clientHello := hello("api.example.com")
	upstreamReceived := make(chan error, 1)
	go func() {
		got := make([]byte, len(clientHello))
		_, err := io.ReadFull(upstreamPeer, got)
		if err == nil && !bytes.Equal(got, clientHello) {
			err = errors.New("upstream ClientHello changed")
		}
		upstreamReceived <- err
	}()
	if _, err := client.Write(clientHello); err != nil {
		t.Fatal(err)
	}
	if err := <-upstreamReceived; err != nil {
		t.Fatal(err)
	}
	close(currentness)
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("revoked tunnel remained open")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("revoked tunnel remained open until timeout: %v", err)
	}
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("revoked handler did not stop")
	}
	select {
	case <-released:
	default:
		t.Fatal("revoked handler did not release final currentness lease")
	}
}

func testCertificate(t *testing.T, cn string) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
func FuzzClientHello(f *testing.F) {
	f.Add(hello("api.example.com"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 70000 {
			return
		}
		_, _ = readClientHello(bytes.NewReader(b), "api.example.com")
	})
}
