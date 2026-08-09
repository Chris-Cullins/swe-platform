package egresspolicy

import (
	"crypto/sha256"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func TestParseHostname(t *testing.T) {
	label63 := strings.Repeat("a", 63)
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{name: "two labels", value: "example.com", valid: true},
		{name: "digits and hyphens", value: "api-2.example123.com", valid: true},
		{name: "maximum labels", value: label63 + "." + label63 + "." + label63 + "." + strings.Repeat("a", 61), valid: true},
		{name: "empty"},
		{name: "one label", value: "localhost"},
		{name: "uppercase", value: "Example.com"},
		{name: "trailing dot", value: "example.com."},
		{name: "empty label", value: "example..com"},
		{name: "leading hyphen", value: "-api.example.com"},
		{name: "trailing hyphen", value: "api-.example.com"},
		{name: "long label", value: strings.Repeat("a", 64) + ".com"},
		{name: "long name", value: label63 + "." + label63 + "." + label63 + "." + label63},
		{name: "idna", value: "xn--bcher-kva.example"},
		{name: "idna inner label", value: "www.xn--bcher-kva.example"},
		{name: "unicode", value: "bücher.example"},
		{name: "wildcard", value: "*.example.com"},
		{name: "scheme", value: "https://example.com"},
		{name: "port", value: "example.com:443"},
		{name: "path", value: "example.com/path"},
		{name: "query", value: "example.com?x=y"},
		{name: "fragment", value: "example.com#x"},
		{name: "userinfo", value: "user@example.com"},
		{name: "percent encoding", value: "example%2ecom"},
		{name: "space", value: "example .com"},
		{name: "tab", value: "example.com\t"},
		{name: "newline", value: "example.com\n"},
		{name: "nul", value: "example.com\x00"},
		{name: "IPv4", value: "127.0.0.1"},
		{name: "IPv4 short", value: "127.1"},
		{name: "IPv4 octal", value: "0177.0.0.1"},
		{name: "IPv4 hex", value: "0x7f.0x0.0x0.0x1"},
		{name: "IPv4 mixed", value: "0x7f.01"},
		{name: "IPv4 empty hex components", value: "0x.0x"},
		{name: "IPv4 empty hex then decimal", value: "0x.1"},
		{name: "IPv4 empty hex components and decimal", value: "0x.0x.0x.1"},
		{name: "IPv4 integer", value: "2130706433"},
		{name: "invalid octal remains hostname", value: "09.0.0.1", valid: true},
		{name: "IPv6", value: "2001:db8::1"},
		{name: "mapped IPv6", value: "::ffff:127.0.0.1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseHostname(test.value)
			if test.valid && (err != nil || string(got) != test.value) {
				t.Fatalf("ParseHostname(%q) = %q, %v", test.value, got, err)
			}
			if !test.valid && err == nil {
				t.Fatalf("ParseHostname(%q) unexpectedly accepted", test.value)
			}
		})
	}
}

func TestEvaluate(t *testing.T) {
	tests := []struct {
		name      string
		ceiling   []string
		baseline  []string
		selection []string
		want      []Hostname
		wantErr   bool
	}{
		{name: "deny all", want: []Hostname{}},
		{name: "baseline union selection", ceiling: []string{"z.example.com", "api.example.com", "git.example.com"}, baseline: []string{"git.example.com"}, selection: []string{"z.example.com", "git.example.com"}, want: []Hostname{"git.example.com", "z.example.com"}},
		{name: "empty selection gets baseline", ceiling: []string{"git.example.com"}, baseline: []string{"git.example.com"}, want: []Hostname{"git.example.com"}},
		{name: "baseline outside ceiling", ceiling: []string{"api.example.com"}, baseline: []string{"git.example.com"}, wantErr: true},
		{name: "selection outside ceiling", ceiling: []string{"api.example.com"}, selection: []string{"git.example.com"}, wantErr: true},
		{name: "invalid ceiling", ceiling: []string{"API.example.com"}, wantErr: true},
		{name: "invalid baseline", ceiling: []string{"api.example.com"}, baseline: []string{"API.example.com"}, wantErr: true},
		{name: "invalid selection", ceiling: []string{"api.example.com"}, selection: []string{"API.example.com"}, wantErr: true},
		{name: "duplicate ceiling", ceiling: []string{"api.example.com", "api.example.com"}, wantErr: true},
		{name: "duplicate baseline", ceiling: []string{"api.example.com"}, baseline: []string{"api.example.com", "api.example.com"}, wantErr: true},
		{name: "duplicate selection", ceiling: []string{"api.example.com"}, selection: []string{"api.example.com", "api.example.com"}, wantErr: true},
		{name: "ceiling bound", ceiling: repeatedHosts(MaxCeilingEntries + 1), wantErr: true},
		{name: "baseline bound", ceiling: repeatedHosts(MaxBaselineEntries + 1), baseline: repeatedHosts(MaxBaselineEntries + 1), wantErr: true},
		{name: "selection bound", ceiling: repeatedHosts(MaxProjectEntries + 1), selection: repeatedHosts(MaxProjectEntries + 1), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Evaluate(test.ceiling, test.baseline, test.selection)
			if test.wantErr {
				if err == nil {
					t.Fatalf("Evaluate unexpectedly succeeded: %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(hostnameStrings(got.Effective), ",") != strings.Join(hostnameStrings(test.want), ",") {
				t.Fatalf("effective = %#v, want %#v", got.Effective, test.want)
			}
		})
	}
}

func TestRuntimeRevisionCanonicalAndStable(t *testing.T) {
	configHash := sha256.Sum256([]byte("immutable config"))
	first := RuntimeRevisionInputs{
		InstallationUID: "installation-uid", ConfigMapUID: "config-uid", ConfigMapContentSHA256: configHash, ProjectUID: "project-uid",
		Ceiling: []string{"z.example.com", "api.example.com", "git.example.com"}, Baseline: []string{"git.example.com"}, ProjectSelection: []string{"z.example.com", "api.example.com"},
	}
	second := first
	second.Ceiling = []string{"git.example.com", "z.example.com", "api.example.com"}
	second.ProjectSelection = []string{"api.example.com", "z.example.com"}
	firstBytes, err := first.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := second.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(firstBytes) != string(secondBytes) {
		t.Fatalf("ordering changed canonical bytes:\n%s\n%s", firstBytes, secondBytes)
	}
	wantBytes := `{"domain":"swe.dev/egress-policy/runtime-revision/v1","installationUID":"installation-uid","configMapUID":"config-uid","configMapContentSHA256":"1d7d78aa9b2d7295da53b1a23b6abdfcd1957523da38db1deb6a9eba909bd2d6","ceiling":["api.example.com","git.example.com","z.example.com"],"baseline":["git.example.com"],"projectUID":"project-uid","projectSelection":["api.example.com","z.example.com"]}`
	if string(firstBytes) != wantBytes {
		t.Fatalf("canonical bytes changed:\n got %s\nwant %s", firstBytes, wantBytes)
	}
	revision, err := first.Revision()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := revision.String(), "b2905abe69e3622311928373d2ece0fe6e6edcd578594f5eef983e742bfb131d"; got != want {
		t.Fatalf("revision = %s, want %s", got, want)
	}

	mutations := []struct {
		name string
		edit func(*RuntimeRevisionInputs)
	}{
		{name: "installation UID", edit: func(in *RuntimeRevisionInputs) { in.InstallationUID = types.UID("other-installation") }},
		{name: "ConfigMap UID", edit: func(in *RuntimeRevisionInputs) { in.ConfigMapUID = types.UID("other-config") }},
		{name: "ConfigMap content", edit: func(in *RuntimeRevisionInputs) { in.ConfigMapContentSHA256 = sha256.Sum256([]byte("other config")) }},
		{name: "ceiling", edit: func(in *RuntimeRevisionInputs) { in.Ceiling = append(in.Ceiling, "other.example.com") }},
		{name: "baseline", edit: func(in *RuntimeRevisionInputs) { in.Baseline = append(in.Baseline, "api.example.com") }},
		{name: "Project UID", edit: func(in *RuntimeRevisionInputs) { in.ProjectUID = types.UID("other-project") }},
		{name: "Project selection", edit: func(in *RuntimeRevisionInputs) { in.ProjectSelection = []string{"z.example.com"} }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			changed := first
			changed.Ceiling = append([]string(nil), first.Ceiling...)
			changed.Baseline = append([]string(nil), first.Baseline...)
			changed.ProjectSelection = append([]string(nil), first.ProjectSelection...)
			mutation.edit(&changed)
			got, err := changed.Revision()
			if err != nil {
				t.Fatal(err)
			}
			if got == revision {
				t.Fatal("revision did not change")
			}
		})
	}
}

func TestRuntimeRevisionRequiresAuthoritativeInputs(t *testing.T) {
	valid := RuntimeRevisionInputs{InstallationUID: "i", ConfigMapUID: "c", ConfigMapContentSHA256: sha256.Sum256([]byte("config")), ProjectUID: "p"}
	for _, test := range []struct {
		name string
		edit func(*RuntimeRevisionInputs)
	}{
		{name: "installation", edit: func(in *RuntimeRevisionInputs) { in.InstallationUID = "" }},
		{name: "ConfigMap", edit: func(in *RuntimeRevisionInputs) { in.ConfigMapUID = "" }},
		{name: "ConfigMap content hash", edit: func(in *RuntimeRevisionInputs) { in.ConfigMapContentSHA256 = [sha256.Size]byte{} }},
		{name: "Project", edit: func(in *RuntimeRevisionInputs) { in.ProjectUID = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			test.edit(&input)
			if _, err := input.Revision(); err == nil {
				t.Fatal("missing identity accepted")
			}
		})
	}
}

func repeatedHosts(count int) []string {
	result := make([]string, count)
	for i := range result {
		result[i] = "host-" + strconv.Itoa(i) + ".example.com"
	}
	return result
}

func hostnameStrings(values []Hostname) []string {
	result := make([]string, len(values))
	for i := range values {
		result[i] = string(values[i])
	}
	return result
}
