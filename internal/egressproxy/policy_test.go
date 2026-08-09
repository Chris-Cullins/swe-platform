package egressproxy

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/egresspolicy"
	"golang.org/x/net/dns/dnsmessage"
)

type fakeResolver struct {
	answers map[string][]dnsmessage.Resource
	calls   []string
}

func (f *fakeResolver) Query(_ context.Context, n dnsmessage.Name, t dnsmessage.Type) ([]dnsmessage.Resource, error) {
	k := n.String() + "/" + t.String()
	f.calls = append(f.calls, k)
	return f.answers[k], nil
}
func header(n string, t dnsmessage.Type) dnsmessage.ResourceHeader {
	x, _ := dnsmessage.NewName(n)
	return dnsmessage.ResourceHeader{Name: x, Type: t, Class: dnsmessage.ClassINET}
}
func a(n string, ip [4]byte) dnsmessage.Resource {
	return dnsmessage.Resource{Header: header(n, dnsmessage.TypeA), Body: &dnsmessage.AResource{A: ip}}
}
func aaaa(n string, ip [16]byte) dnsmessage.Resource {
	return dnsmessage.Resource{Header: header(n, dnsmessage.TypeAAAA), Body: &dnsmessage.AAAAResource{AAAA: ip}}
}
func cname(n, to string) dnsmessage.Resource {
	x, _ := dnsmessage.NewName(to)
	return dnsmessage.Resource{Header: header(n, dnsmessage.TypeCNAME), Body: &dnsmessage.CNAMEResource{CNAME: x}}
}
func TestResolveRawAnswers(t *testing.T) {
	f := &fakeResolver{answers: map[string][]dnsmessage.Resource{"a.example./TypeA": {cname("a.example.", "b.example.")}, "a.example./TypeAAAA": {cname("a.example.", "b.example.")}, "b.example./TypeA": {a("b.example.", [4]byte{8, 8, 8, 8})}}}
	got, e := Resolve(context.Background(), f, "a.example", nil)
	if e != nil || len(got) != 1 || got[0].String() != "8.8.8.8" {
		t.Fatal(got, e)
	}
	for _, c := range f.calls {
		if c[0] == '.' {
			t.Fatal(c)
		}
	}
	for _, ip := range []string{"192.88.99.1", "64:ff9b:1::1", "100:0:0:1::1", "2001::1", "2002::1", "3fff::1", "5f00::1", "fe80::1%eth0"} {
		if ValidateAddress(netip.MustParseAddr(ip), nil) == nil {
			t.Errorf("accepted %s", ip)
		}
	}
	mixed := &fakeResolver{answers: map[string][]dnsmessage.Resource{"x.example./TypeA": {a("x.example.", [4]byte{8, 8, 8, 8}), a("x.example.", [4]byte{127, 0, 0, 1})}}}
	if _, e = Resolve(context.Background(), mixed, "x.example", nil); e == nil {
		t.Error("mixed answer accepted")
	}
}
func TestResolveRecursiveResponseAndUnrelatedAnswers(t *testing.T) {
	chain := []dnsmessage.Resource{
		cname("a.example.", "b.example."),
		cname("b.example.", "c.example."),
		a("c.example.", [4]byte{8, 8, 4, 4}),
	}
	f := &fakeResolver{answers: map[string][]dnsmessage.Resource{"a.example./TypeA": chain}}
	got, err := Resolve(context.Background(), f, "a.example", nil)
	if err != nil || len(got) != 1 || got[0].String() != "8.8.4.4" {
		t.Fatalf("recursive response: %v %v", got, err)
	}
	for _, answers := range [][]dnsmessage.Resource{
		{cname("a.example.", "b.example."), a("unrelated.example.", [4]byte{8, 8, 8, 8})},
		{cname("a.example.", "b.example."), a("unrelated.example.", [4]byte{127, 0, 0, 1})},
		{cname("a.example.", "b.example."), a("b.example.", [4]byte{8, 8, 8, 8}), cname("b.example.", "c.example.")},
	} {
		bad := &fakeResolver{answers: map[string][]dnsmessage.Resource{"a.example./TypeA": answers}}
		if _, err := Resolve(context.Background(), bad, "a.example", nil); err == nil {
			t.Error("accepted unrelated, forbidden, or mixed CNAME/address response")
		}
	}
}

func TestDNSClientRejectsHostnameResolver(t *testing.T) {
	if _, err := NewDNSClient([]string{"resolver.example:53"}); err == nil {
		t.Error("constructor accepted hostname resolver")
	}
	d := &DNSClient{Resolvers: []string{"resolver.example"}}
	n, _ := dnsmessage.NewName("a.example.")
	if _, err := d.Query(context.Background(), n, dnsmessage.TypeA); err == nil {
		t.Error("Query accepted hostname resolver")
	}
}
func TestResolveBounds(t *testing.T) {
	f := &fakeResolver{answers: map[string][]dnsmessage.Resource{}}
	for i := 0; i < 9; i++ {
		n := string(rune('a'+i)) + ".example."
		to := string(rune('b'+i)) + ".example."
		f.answers[n+"/TypeA"] = []dnsmessage.Resource{cname(n, to)}
		f.answers[n+"/TypeAAAA"] = []dnsmessage.Resource{cname(n, to)}
	}
	if _, e := Resolve(context.Background(), f, "a.example", nil); e == nil {
		t.Error("over 8 hops accepted")
	}
	many := &fakeResolver{answers: map[string][]dnsmessage.Resource{}}
	for i := 0; i < 65; i++ {
		many.answers["x.example./TypeA"] = append(many.answers["x.example./TypeA"], a("x.example.", [4]byte{8, 8, 8, 8}))
	}
	if _, e := Resolve(context.Background(), many, "x.example", nil); e == nil {
		t.Error("over 64 records accepted")
	}
}

func TestCanonicalTargetMatchesPolicyGrammar(t *testing.T) {
	label63 := strings.Repeat("a", 63)
	values := []string{"example.com", "api-2.example123.com", label63 + "." + label63 + "." + label63 + "." + strings.Repeat("a", 61), "", "localhost", "Example.com", "example.com.", "example..com", "-api.example.com", "api-.example.com", strings.Repeat("a", 64) + ".com", label63 + "." + label63 + "." + label63 + "." + label63, "xn--bcher-kva.example", "www.xn--bcher-kva.example", "bücher.example", "*.example.com", "https://example.com", "example.com:443", "example.com/path", "example.com?x=y", "example.com#x", "user@example.com", "example%2ecom", "example .com", "example.com\t", "example.com\n", "example.com\x00", "127.0.0.1", "127.1", "0177.0.0.1", "0x7f.0x0.0x0.0x1", "0x7f.01", "0x.0x", "0x.1", "0x.0x.0x.1", "2130706433", "09.0.0.1", "2001:db8::1", "::ffff:127.0.0.1"}
	for _, value := range values {
		_, policyErr := egresspolicy.ParseHostname(value)
		got, targetErr := canonicalTarget(value + ":443")
		if (policyErr == nil) != (targetErr == nil) {
			t.Errorf("%q: ParseHostname err=%v, canonicalTarget err=%v", value, policyErr, targetErr)
		} else if targetErr == nil && got != value {
			t.Errorf("%q canonicalized to %q", value, got)
		}
	}
	for _, target := range []string{"example.com", "example.com:80", "example.com:443:443", ":443"} {
		if _, err := canonicalTarget(target); err == nil {
			t.Errorf("accepted target %q", target)
		}
	}
}

func TestForbiddenPrefixSnapshot(t *testing.T) {
	expected := []string{
		"0.0.0.0/8", "0.0.0.0/32", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
		"192.0.0.0/24", "192.0.0.0/29", "192.0.0.8/32", "192.0.0.9/32", "192.0.0.10/32", "192.0.0.170/32", "192.0.0.171/32",
		"192.0.2.0/24", "192.31.196.0/24", "192.52.193.0/24", "192.88.99.0/24", "192.88.99.2/32", "192.168.0.0/16", "192.175.48.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "255.255.255.255/32",
		"::1/128", "::/128", "::ffff:0.0.0.0/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "100:0:0:1::/64", "2001::/23", "2001::/32", "2001:1::1/128", "2001:1::2/128", "2001:1::3/128", "2001:2::/48", "2001:3::/32", "2001:4:112::/48", "2001:10::/28", "2001:20::/28", "2001:30::/28", "2001:db8::/32", "2002::/16", "2620:4f:8000::/48", "3fff::/20", "5f00::/16", "fc00::/7", "fe80::/10",
		"224.0.0.0/4", "ff00::/8",
	}
	if len(expected) != 53 {
		t.Fatalf("expected snapshot contains %d prefixes, want 53", len(expected))
	}
	actual := make(map[string]int, len(ForbiddenPrefixSnapshot))
	for _, entry := range ForbiddenPrefixSnapshot {
		actual[entry.Prefix.String()]++
	}
	want := make(map[string]int, len(expected))
	for _, prefix := range expected {
		want[prefix]++
	}
	for prefix, count := range want {
		if count != 1 {
			t.Errorf("expected snapshot duplicates %s %d times", prefix, count)
		}
		if actual[prefix] == 0 {
			t.Errorf("production snapshot is missing %s", prefix)
		} else if actual[prefix] != 1 {
			t.Errorf("production snapshot duplicates %s %d times", prefix, actual[prefix])
		}
	}
	for prefix, count := range actual {
		if want[prefix] == 0 {
			t.Errorf("production snapshot has extra prefix %s (%d entries)", prefix, count)
		}
	}
	if len(ForbiddenPrefixSnapshot) != len(expected) {
		t.Errorf("production snapshot contains %d prefixes, want %d", len(ForbiddenPrefixSnapshot), len(expected))
	}
	for _, value := range expected {
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			t.Fatalf("invalid expected prefix %q: %v", value, err)
		}
		t.Run(value, func(t *testing.T) {
			if err := ValidateAddress(prefix.Addr(), nil); err == nil {
				t.Fatalf("accepted first address of %s", prefix)
			}
		})
	}
	for _, value := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if err := ValidateAddress(netip.MustParseAddr(value), nil); err != nil {
			t.Errorf("public address %s rejected: %v", value, err)
		}
	}
}

func TestResolveTerminalNODATAExactCalls(t *testing.T) {
	f := &fakeResolver{answers: map[string][]dnsmessage.Resource{
		"a.example./TypeA": {cname("a.example.", "b.example.")},
	}}
	if _, err := Resolve(context.Background(), f, "a.example", nil); err == nil {
		t.Fatal("terminal NODATA accepted")
	}
	if len(f.calls) != 4 {
		t.Fatalf("calls = %d (%v), want 4", len(f.calls), f.calls)
	}
}

func TestResolveMaximumQueryBound(t *testing.T) {
	f := &fakeResolver{answers: map[string][]dnsmessage.Resource{}}
	for i := 0; i < 8; i++ {
		from := string(rune('a'+i)) + ".example."
		to := string(rune('b'+i)) + ".example."
		f.answers[from+"/TypeA"] = []dnsmessage.Resource{cname(from, to)}
	}
	f.answers["i.example./TypeA"] = []dnsmessage.Resource{a("i.example.", [4]byte{8, 8, 8, 8})}
	if _, err := Resolve(context.Background(), f, "a.example", nil); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != maxDNSQueries {
		t.Fatalf("calls = %d, want %d", len(f.calls), maxDNSQueries)
	}
}

func TestDNSExchangeRejectsTruncatedTCPResponse(t *testing.T) {
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	udp, err := net.ListenPacket("udp", tcp.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	respond := func(query []byte) []byte {
		var request dnsmessage.Message
		if err := request.Unpack(query); err != nil {
			t.Error(err)
			return nil
		}
		response := dnsmessage.Message{Header: dnsmessage.Header{ID: request.ID, Response: true, Truncated: true}, Questions: request.Questions, Answers: []dnsmessage.Resource{a("a.example.", [4]byte{8, 8, 8, 8})}}
		wire, err := response.Pack()
		if err != nil {
			t.Error(err)
		}
		return wire
	}
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		b := make([]byte, 2048)
		n, addr, e := udp.ReadFrom(b)
		if e == nil {
			_, _ = udp.WriteTo(respond(b[:n]), addr)
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		c, e := tcp.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		h := make([]byte, 2)
		if _, e = io.ReadFull(c, h); e != nil {
			return
		}
		b := make([]byte, binary.BigEndian.Uint16(h))
		if _, e = io.ReadFull(c, b); e != nil {
			return
		}
		wire := respond(b)
		binary.BigEndian.PutUint16(h, uint16(len(wire)))
		_, _ = c.Write(append(h, wire...))
	}()
	name, _ := dnsmessage.NewName("a.example.")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := dnsExchange(ctx, tcp.Addr().String(), name, dnsmessage.TypeA); err == nil {
		t.Fatal("accepted truncated TCP response")
	}
	<-done
	<-done
}
