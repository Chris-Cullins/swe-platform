// egress-proxy is operationally disabled: serve mode always denies authorization.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/Chris-Cullins/swe-platform/internal/egressproxy"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
func run(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: egress-proxy serve|forward")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	addr := fs.String("address", "", "listen address")
	cert := fs.String("cert", "", "certificate PEM")
	key := fs.String("key", "", "private key PEM")
	ca := fs.String("ca", "", "peer CA PEM")
	server := fs.String("server", "", "serve address (forward mode)")
	serverName := fs.String("server-name", "", "TLS server name")
	var resolvers stringList
	fs.Var(&resolvers, "resolver", "DNS resolver IP/address (repeatable; port defaults to 53)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *addr == "" || *cert == "" || *key == "" {
		return errors.New("--address, --cert and --key are required")
	}
	pair, err := tls.LoadX509KeyPair(*cert, *key)
	if err != nil {
		return err
	}
	switch args[0] {
	case "serve":
		if *ca != "" || *server != "" || *serverName != "" {
			return errors.New("serve does not accept --ca, --server, or --server-name")
		}
		if len(resolvers) == 0 {
			return errors.New("at least one --resolver is required")
		}
		l, e := net.Listen("tcp", *addr)
		if e != nil {
			return e
		}
		defer l.Close()
		resolver, e := egressproxy.NewDNSClient(resolvers)
		if e != nil {
			return e
		}
		return (&egressproxy.Server{TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, SessionTicketsDisabled: true}, Authorizer: egressproxy.DisabledAuthorizer{}, Resolver: resolver}).Serve(l)
	case "forward":
		if *server == "" || *serverName == "" || *ca == "" || len(resolvers) != 0 {
			return errors.New("forward requires --server, --server-name and --ca, and does not accept --resolver")
		}
		pool, err := loadPool(*ca)
		if err != nil {
			return err
		}
		l, e := net.Listen("tcp", *addr)
		if e != nil {
			return e
		}
		defer l.Close()
		return (&egressproxy.Forwarder{ServerAddress: *server, TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: pool, ServerName: *serverName, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, SessionTicketsDisabled: true}}).Serve(l)
	default:
		return errors.New("usage: egress-proxy serve|forward")
	}
}

type stringList []string

func (s *stringList) String() string { return fmt.Sprint([]string(*s)) }
func (s *stringList) Set(v string) error {
	if v == "" {
		return errors.New("empty resolver")
	}
	*s = append(*s, v)
	return nil
}
func loadPool(path string) (*x509.CertPool, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(b) {
		return nil, errors.New("CA contains no certificates")
	}
	return p, nil
}
