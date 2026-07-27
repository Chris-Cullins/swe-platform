package serviceconfig

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestParseValidAndSorted(t *testing.T) {
	input := []byte(`version: 1
services:
  worker:
    command: ["npm", "run", "worker"]
  web-1:
    command: [npm, run, dev]
    port: 3000
`)
	got, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	port := int32(3000)
	want := []Declaration{
		{Name: "web-1", Argv: []string{"npm", "run", "dev"}, Port: &port},
		{Name: "worker", Argv: []string{"npm", "run", "worker"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Parse() = %#v, want %#v", got, want)
	}

	for _, empty := range [][]byte{nil, {}} {
		got, err := Parse(empty)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("Parse(empty) = %#v, %v", got, err)
		}
	}
	got, err = Parse([]byte("version: 1\nservices: {}\n"))
	if err != nil || len(got) != 0 {
		t.Fatalf("Parse(empty services) = %#v, %v", got, err)
	}
}

func TestParseRejectsInvalidSchema(t *testing.T) {
	longName := strings.Repeat("a", 33)
	tests := map[string]string{
		"whitespace only":       " \n",
		"missing version":       "services: {}\n",
		"wrong version":         "version: 2\nservices: {}\n",
		"string version":        "version: '1'\nservices: {}\n",
		"noncanonical version":  "version: 01\nservices: {}\n",
		"missing services":      "version: 1\n",
		"null services":         "version: 1\nservices:\n",
		"sequence services":     "version: 1\nservices: []\n",
		"unknown root field":    "version: 1\nservices: {}\nother: true\n",
		"non-string root key":   "version: 1\nservices: {}\n1: x\n",
		"invalid service name":  serviceFile("Not_DNS", "command: [run]"),
		"long service name":     serviceFile(longName, "command: [run]"),
		"typed service name":    "version: 1\nservices:\n  true: {command: [run]}\n",
		"scalar service":        serviceFile("web", "run"),
		"unknown service field": serviceFile("web", "command: [run]\n    extra: x"),
		"typed service field":   serviceFile("web", "command: [run]\n    1: x"),
		"missing command":       serviceFile("web", "port: 80"),
		"scalar command":        serviceFile("web", "command: run"),
		"empty command":         serviceFile("web", "command: []"),
		"empty executable":      serviceFile("web", "command: ['', x]"),
		"empty argument":        serviceFile("web", "command: [run, '']"),
		"typed argument":        serviceFile("web", "command: [run, 1]"),
		"boolean argument":      serviceFile("web", "command: [true]"),
		"NUL argument":          serviceFile("web", `command: ["run\0bad"]`),
		"string port":           serviceFile("web", "command: [run]\n    port: '80'"),
		"float port":            serviceFile("web", "command: [run]\n    port: 80.0"),
		"zero port":             serviceFile("web", "command: [run]\n    port: 0"),
		"negative port":         serviceFile("web", "command: [run]\n    port: -1"),
		"large port":            serviceFile("web", "command: [run]\n    port: 65536"),
		"reserved port":         serviceFile("web", "command: [run]\n    port: 50051"),
		"leading-zero port":     serviceFile("web", "command: [run]\n    port: 080"),
		"plus port":             serviceFile("web", "command: [run]\n    port: +80"),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if got, err := Parse([]byte(input)); err == nil {
				t.Fatalf("Parse() accepted invalid input: %#v", got)
			}
		})
	}
}

func TestParseRejectsUnsafeYAML(t *testing.T) {
	tests := map[string]string{
		"duplicate root":     "version: 1\nversion: 1\nservices: {}\n",
		"duplicate service":  "version: 1\nservices:\n  web: {command: [a]}\n  web: {command: [b]}\n",
		"duplicate field":    serviceFile("web", "command: [a]\n    command: [b]"),
		"anchor":             "version: &v 1\nservices: {}\n",
		"alias":              "version: &v 1\nservices: {web: {command: [*v]}}\n",
		"merge":              "version: 1\nservices:\n  web:\n    <<: {command: [run]}\n",
		"multiple documents": "version: 1\nservices: {}\n---\nversion: 1\nservices: {}\n",
		"malformed":          "version: [\n",
		"complex key":        "version: 1\nservices:\n  ? [web]\n  : {command: [run]}\n",
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(input)); err == nil {
				t.Fatal("Parse() accepted unsafe YAML")
			}
		})
	}
	if _, err := Parse([]byte{0xff}); err == nil {
		t.Fatal("Parse() accepted invalid UTF-8")
	}
}

func TestParseBounds(t *testing.T) {
	validName := strings.Repeat("a", 32)
	if _, err := Parse([]byte(serviceFile(validName, "command: [run]\n    port: 65535"))); err != nil {
		t.Fatalf("boundary declaration rejected: %v", err)
	}

	args64 := make([]string, MaxArgs)
	for i := range args64 {
		args64[i] = "a"
	}
	if _, err := Parse([]byte(serviceFile("web", "command: ["+strings.Join(args64, ",")+"]"))); err != nil {
		t.Fatalf("max args rejected: %v", err)
	}
	args65 := append(args64, "a")
	assertRejected(t, serviceFile("web", "command: ["+strings.Join(args65, ",")+"]"))

	maxArg := "'" + strings.Repeat("a", MaxArgBytes) + "'"
	if _, err := Parse([]byte(serviceFile("web", "command: ["+maxArg+"]"))); err != nil {
		t.Fatalf("max argument rejected: %v", err)
	}
	assertRejected(t, serviceFile("web", fmt.Sprintf("command: [run, '%s']", strings.Repeat("a", MaxArgBytes+1))))
	aggregate := make([]string, 4)
	for i := range aggregate {
		aggregate[i] = maxArg
	}
	if _, err := Parse([]byte(serviceFile("web", "command: ["+strings.Join(aggregate, ",")+"]"))); err != nil {
		t.Fatalf("max aggregate argv rejected: %v", err)
	}
	aggregate = append(aggregate, "a")
	assertRejected(t, serviceFile("web", "command: ["+strings.Join(aggregate, ",")+"]"))

	assertRejected(t, string(make([]byte, MaxInputBytes+1)))

	var tooMany strings.Builder
	tooMany.WriteString("version: 1\nservices:\n")
	for i := 0; i < 33; i++ {
		fmt.Fprintf(&tooMany, "  service-%d: {command: [run]}\n", i)
	}
	assertRejected(t, tooMany.String())
}

func serviceFile(name, body string) string {
	return "version: 1\nservices:\n  " + name + ":\n    " + body + "\n"
}

func assertRejected(t *testing.T, input string) {
	t.Helper()
	if _, err := Parse([]byte(input)); err == nil {
		t.Fatal("Parse() accepted out-of-bounds input")
	}
}
