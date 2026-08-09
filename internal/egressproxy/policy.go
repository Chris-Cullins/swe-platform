package egressproxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/egresspolicy"
	"golang.org/x/net/dns/dnsmessage"
)

func canonicalTarget(s string) (string, error) {
	if !strings.HasSuffix(s, ":443") {
		return "", errors.New("invalid CONNECT target")
	}
	hostname, err := egresspolicy.ParseHostname(strings.TrimSuffix(s, ":443"))
	if err != nil {
		return "", err
	}
	return string(hostname), nil
}

type ForbiddenPrefixEntry struct {
	Prefix netip.Prefix
	Name   string
	Source string
}

func forbidden(prefix, name, source string) ForbiddenPrefixEntry {
	return ForbiddenPrefixEntry{Prefix: netip.MustParsePrefix(prefix), Name: name, Source: source}
}

// ForbiddenPrefixSnapshot is the complete Address Block snapshot from the IANA IPv4 and IPv6
// Special-Purpose Address Registries, both Last Updated 2025-10-09, retrieved 2026-08-09.
// Sources: https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry-1.csv
// and https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry-1.csv.
var ForbiddenPrefixSnapshot = []ForbiddenPrefixEntry{
	forbidden("0.0.0.0/8", "This network", "IANA IPv4"), forbidden("0.0.0.0/32", "This host on this network", "IANA IPv4"), forbidden("10.0.0.0/8", "Private-Use", "IANA IPv4"), forbidden("100.64.0.0/10", "Shared Address Space", "IANA IPv4"), forbidden("127.0.0.0/8", "Loopback", "IANA IPv4"), forbidden("169.254.0.0/16", "Link Local", "IANA IPv4"), forbidden("172.16.0.0/12", "Private-Use", "IANA IPv4"),
	forbidden("192.0.0.0/24", "IETF Protocol Assignments", "IANA IPv4"), forbidden("192.0.0.0/29", "IPv4 Service Continuity Prefix", "IANA IPv4"), forbidden("192.0.0.8/32", "IPv4 dummy address", "IANA IPv4"), forbidden("192.0.0.9/32", "Port Control Protocol Anycast", "IANA IPv4"), forbidden("192.0.0.10/32", "TURN Anycast", "IANA IPv4"), forbidden("192.0.0.170/32", "NAT64/DNS64 Discovery", "IANA IPv4"), forbidden("192.0.0.171/32", "NAT64/DNS64 Discovery", "IANA IPv4"),
	forbidden("192.0.2.0/24", "Documentation (TEST-NET-1)", "IANA IPv4"), forbidden("192.31.196.0/24", "AS112-v4", "IANA IPv4"), forbidden("192.52.193.0/24", "AMT", "IANA IPv4"), forbidden("192.88.99.0/24", "Deprecated (6to4 Relay Anycast)", "IANA IPv4"), forbidden("192.88.99.2/32", "6a44-relay anycast address", "IANA IPv4"), forbidden("192.168.0.0/16", "Private-Use", "IANA IPv4"), forbidden("192.175.48.0/24", "Direct Delegation AS112 Service", "IANA IPv4"), forbidden("198.18.0.0/15", "Benchmarking", "IANA IPv4"), forbidden("198.51.100.0/24", "Documentation (TEST-NET-2)", "IANA IPv4"), forbidden("203.0.113.0/24", "Documentation (TEST-NET-3)", "IANA IPv4"), forbidden("240.0.0.0/4", "Reserved", "IANA IPv4"), forbidden("255.255.255.255/32", "Limited Broadcast", "IANA IPv4"),
	forbidden("::1/128", "Loopback Address", "IANA IPv6"), forbidden("::/128", "Unspecified Address", "IANA IPv6"), forbidden("::ffff:0:0/96", "IPv4-mapped Address", "IANA IPv6"), forbidden("64:ff9b::/96", "IPv4-IPv6 Translation", "IANA IPv6"), forbidden("64:ff9b:1::/48", "IPv4-IPv6 Translation", "IANA IPv6"), forbidden("100::/64", "Discard-Only Address Block", "IANA IPv6"), forbidden("100:0:0:1::/64", "Dummy IPv6 Prefix", "IANA IPv6"), forbidden("2001::/23", "IETF Protocol Assignments", "IANA IPv6"), forbidden("2001::/32", "TEREDO", "IANA IPv6"), forbidden("2001:1::1/128", "Port Control Protocol Anycast", "IANA IPv6"), forbidden("2001:1::2/128", "TURN Anycast", "IANA IPv6"), forbidden("2001:1::3/128", "DNS-SD Service Registration Anycast", "IANA IPv6"), forbidden("2001:2::/48", "Benchmarking", "IANA IPv6"), forbidden("2001:3::/32", "AMT", "IANA IPv6"), forbidden("2001:4:112::/48", "AS112-v6", "IANA IPv6"), forbidden("2001:10::/28", "Deprecated ORCHID", "IANA IPv6"), forbidden("2001:20::/28", "ORCHIDv2", "IANA IPv6"), forbidden("2001:30::/28", "Drone Remote ID DETs", "IANA IPv6"), forbidden("2001:db8::/32", "Documentation", "IANA IPv6"), forbidden("2002::/16", "6to4", "IANA IPv6"), forbidden("2620:4f:8000::/48", "Direct Delegation AS112 Service", "IANA IPv6"), forbidden("3fff::/20", "Documentation", "IANA IPv6"), forbidden("5f00::/16", "Segment Routing SIDs", "IANA IPv6"), forbidden("fc00::/7", "Unique-Local", "IANA IPv6"), forbidden("fe80::/10", "Link-Local Unicast", "IANA IPv6"),
	// Protocol-reserved additions: multicast is outside the two special-purpose registries.
	forbidden("224.0.0.0/4", "IPv4 Multicast", "protocol-reserved"), forbidden("ff00::/8", "IPv6 Multicast", "protocol-reserved"),
}

var ForbiddenPrefixes = func() []netip.Prefix {
	result := make([]netip.Prefix, len(ForbiddenPrefixSnapshot))
	for i, entry := range ForbiddenPrefixSnapshot {
		result[i] = entry.Prefix
	}
	return result
}()

func ValidateAddress(a netip.Addr, extra []netip.Prefix) error {
	if !a.IsValid() || a.Zone() != "" {
		return errors.New("invalid address")
	}
	a = a.Unmap()
	if !a.IsGlobalUnicast() {
		return errors.New("non-global address")
	}
	for _, p := range append(ForbiddenPrefixes, extra...) {
		if p.Contains(a) {
			return errors.New("forbidden address")
		}
	}
	return nil
}

// DNSResolver exposes raw answer records; implementations must not synthesize or recursively follow names.
type DNSResolver interface {
	Query(context.Context, dnsmessage.Name, dnsmessage.Type) ([]dnsmessage.Resource, error)
}

// DNSClient sends bounded DNS queries only to explicitly configured resolver endpoints.
type DNSClient struct {
	Resolvers   []string
	next        atomic.Uint32
	once        sync.Once
	addresses   []string
	validateErr error
}

func NewDNSClient(resolvers []string) (*DNSClient, error) {
	d := &DNSClient{Resolvers: append([]string(nil), resolvers...)}
	d.validate()
	return d, d.validateErr
}

func (d *DNSClient) validate() {
	d.once.Do(func() {
		if len(d.Resolvers) == 0 {
			d.validateErr = errors.New("DNS resolver address required")
			return
		}
		for _, raw := range d.Resolvers {
			a := raw
			if _, _, err := net.SplitHostPort(a); err != nil {
				a = net.JoinHostPort(a, "53")
			}
			host, port, err := net.SplitHostPort(a)
			if err != nil || port == "" {
				d.validateErr = errors.New("invalid DNS resolver address")
				return
			}
			ip, err := netip.ParseAddr(host)
			if err != nil || ip.Zone() != "" {
				d.validateErr = errors.New("DNS resolver must be a literal IP address")
				return
			}
			d.addresses = append(d.addresses, net.JoinHostPort(ip.String(), port))
		}
	})
}

func (d *DNSClient) Query(ctx context.Context, name dnsmessage.Name, typ dnsmessage.Type) ([]dnsmessage.Resource, error) {
	d.validate()
	if d.validateErr != nil {
		return nil, d.validateErr
	}
	var last error
	for i := 0; i < len(d.addresses); i++ {
		a := d.addresses[(int(d.next.Add(1)-1)+i)%len(d.addresses)]
		ans, e := dnsExchange(ctx, a, name, typ)
		if e == nil {
			return ans, nil
		}
		last = e
	}
	return nil, last
}

var dnsID atomic.Uint32

func dnsExchange(ctx context.Context, address string, name dnsmessage.Name, typ dnsmessage.Type) ([]dnsmessage.Resource, error) {
	id := uint16(dnsID.Add(1))
	m := dnsmessage.Message{Header: dnsmessage.Header{ID: id, RecursionDesired: true}, Questions: []dnsmessage.Question{{Name: name, Type: typ, Class: dnsmessage.ClassINET}}}
	wire, e := m.Pack()
	if e != nil {
		return nil, e
	}
	exchange := func(network string) (dnsmessage.Message, error) {
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return dnsmessage.Message{}, e
		}
		defer c.Close()
		deadline, _ := ctx.Deadline()
		_ = c.SetDeadline(deadline)
		if network == "tcp" {
			p := make([]byte, 2+len(wire))
			binary.BigEndian.PutUint16(p, uint16(len(wire)))
			copy(p[2:], wire)
			_, e = c.Write(p)
			if e != nil {
				return dnsmessage.Message{}, e
			}
			h := make([]byte, 2)
			if _, e = io.ReadFull(c, h); e != nil {
				return dnsmessage.Message{}, e
			}
			n := int(binary.BigEndian.Uint16(h))
			if n > 65535 {
				return dnsmessage.Message{}, errors.New("DNS response too large")
			}
			p = make([]byte, n)
			_, e = io.ReadFull(c, p)
			if e != nil {
				return dnsmessage.Message{}, e
			}
			var x dnsmessage.Message
			e = x.Unpack(p)
			return x, e
		}
		if _, e = c.Write(wire); e != nil {
			return dnsmessage.Message{}, e
		}
		p := make([]byte, 4096)
		n, e := c.Read(p)
		if e != nil {
			return dnsmessage.Message{}, e
		}
		var x dnsmessage.Message
		e = x.Unpack(p[:n])
		return x, e
	}
	x, e := exchange("udp")
	if e != nil {
		return nil, e
	}
	validate := func(response dnsmessage.Message) error {
		if response.Header.ID != id || !response.Header.Response || response.Header.OpCode != 0 || len(response.Questions) != 1 || response.Questions[0].Name != name || response.Questions[0].Type != typ || response.Questions[0].Class != dnsmessage.ClassINET {
			return errors.New("invalid DNS response")
		}
		return nil
	}
	if e = validate(x); e != nil {
		return nil, e
	}
	if x.Header.Truncated {
		x, e = exchange("tcp")
		if e != nil {
			return nil, e
		}
		if e = validate(x); e != nil {
			return nil, e
		}
	}
	if x.Header.Truncated {
		return nil, errors.New("truncated final DNS response")
	}
	if x.Header.RCode != dnsmessage.RCodeSuccess {
		return nil, fmt.Errorf("DNS rcode %v", x.Header.RCode)
	}
	return x.Answers, nil
}

const (
	maxDNSQueries      = 18 // Nine names (original plus an eight-hop chain), each queried for A and AAAA.
	maxDNSQueriedNames = 9
)

func Resolve(ctx context.Context, r DNSResolver, name string, extra []netip.Prefix) ([]netip.Addr, error) {
	if r == nil {
		return nil, errors.New("resolver required")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	original, cur := ensureRoot(name), ensureRoot(name)
	edges := map[string]string{}
	addresses := map[string][]netip.Addr{}
	queried := map[string]bool{}
	queries := 0
	records := 0
	for {
		queriedName := cur
		if queried[cur] {
			return nil, errors.New("DNS resolution made no progress")
		}
		if len(queried) >= maxDNSQueriedNames {
			return nil, errors.New("too many DNS names")
		}
		queried[cur] = true
		dn, e := dnsmessage.NewName(cur)
		if e != nil {
			return nil, e
		}
		for _, typ := range []dnsmessage.Type{dnsmessage.TypeA, dnsmessage.TypeAAAA} {
			if queries >= maxDNSQueries {
				return nil, errors.New("too many DNS queries")
			}
			queries++
			ans, e := r.Query(ctx, dn, typ)
			if e != nil {
				return nil, e
			}
			records += len(ans)
			if records > 64 {
				return nil, errors.New("too many DNS answers")
			}
			responseEdges := map[string]string{}
			responseOwners := map[string]bool{}
			for _, rr := range ans {
				owner := rr.Header.Name.String()
				responseOwners[owner] = true
				switch b := rr.Body.(type) {
				case *dnsmessage.CNAMEResource:
					n := b.CNAME.String()
					if _, exists := responseEdges[owner]; exists {
						return nil, errors.New("duplicate CNAME")
					}
					responseEdges[owner] = n
					if old, exists := edges[owner]; exists && old != n {
						return nil, errors.New("conflicting CNAME")
					}
				case *dnsmessage.AResource:
					ip := netip.AddrFrom4(b.A)
					if e = ValidateAddress(ip, extra); e != nil {
						return nil, e
					}
					addresses[owner] = append(addresses[owner], ip)
				case *dnsmessage.AAAAResource:
					ip := netip.AddrFrom16(b.AAAA).Unmap()
					if e = ValidateAddress(ip, extra); e != nil {
						return nil, e
					}
					addresses[owner] = append(addresses[owner], ip)
				default:
					return nil, errors.New("unexpected DNS answer")
				}
			}
			for owner, to := range responseEdges {
				edges[owner] = to
			}
			for owner := range responseOwners {
				x := cur
				reachable := false
				for i := 0; i <= 8; i++ {
					if x == owner {
						reachable = true
						break
					}
					next, ok := edges[x]
					if !ok {
						break
					}
					x = next
				}
				if !reachable {
					return nil, errors.New("unrelated DNS answer")
				}
			}
		}
		x, seen := original, map[string]bool{}
		for hops := 0; ; hops++ {
			if seen[x] {
				return nil, errors.New("CNAME loop")
			}
			seen[x] = true
			if len(addresses[x]) != 0 {
				if _, ok := edges[x]; ok {
					return nil, errors.New("CNAME and address conflict")
				}
				return addresses[x], nil
			}
			next, ok := edges[x]
			if !ok {
				cur = x
				break
			}
			if hops >= 8 {
				return nil, errors.New("CNAME chain too long")
			}
			trim := strings.TrimSuffix(next, ".")
			if _, e = canonicalTarget(trim + ":443"); e != nil {
				return nil, e
			}
			x = next
		}
		if cur == queriedName && len(addresses[cur]) == 0 {
			return nil, errors.New("no addresses")
		}
	}
}
func ensureRoot(s string) string {
	if !strings.HasSuffix(s, ".") {
		return s + "."
	}
	return s
}
