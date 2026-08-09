package egresspolicy

import (
	"strings"
	"testing"
)

func FuzzParseHostname(f *testing.F) {
	for _, seed := range []string{
		"example.com", "api-2.example.com", "Example.com", "example.com.",
		"xn--bcher-kva.example", "bücher.example", "*.example.com", "https://example.com",
		"example.com:443", "user@example.com", "example%2ecom", "example.com\x00",
		"127.0.0.1", "127.1", "0177.0.0.1", "0x7f.0x0.0x0.0x1", "0x.0x", "0x.1", "0x.0x.0x.1", "::ffff:127.0.0.1",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		hostname, err := ParseHostname(value)
		if err != nil {
			return
		}
		if string(hostname) != value {
			t.Fatalf("parser normalized %q to %q", value, hostname)
		}
		if len(value) == 0 || len(value) > MaxHostnameBytes || strings.ToLower(value) != value || strings.Count(value, ".") < 1 {
			t.Fatalf("accepted value violates canonical bounds: %q", value)
		}
		for i := 0; i < len(value); i++ {
			if value[i] > 0x7f {
				t.Fatalf("accepted non-ASCII value %q", value)
			}
		}
		if reparsed, reparseErr := ParseHostname(string(hostname)); reparseErr != nil || reparsed != hostname {
			t.Fatalf("accepted hostname is not stable: %q, %v", reparsed, reparseErr)
		}
	})
}
