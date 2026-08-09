package egressidentity

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"reflect"
	"strings"
	"testing"
	"time"
)

func validClaims() Claims {
	return Claims{InstallationNamespace: "swe-system", InstallationName: "main", InstallationUID: "i-uid",
		ProjectNamespace: "project-a", ProjectName: "project", ProjectUID: "p-uid",
		EnvironmentNamespace: "project-a", EnvironmentName: "environment", EnvironmentUID: "e-uid",
		PodName: "env-environment", PodUID: "pod-uid", ExecutionGeneration: 7,
		RuntimePolicyRevision: strings.Repeat("a", 64), ForwarderRevision: "1",
		CertificateFingerprint: strings.Repeat("b", 64)}
}

func TestClaimsExactCanonicalRoundTrip(t *testing.T) {
	c := validClaims()
	b, err := c.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"domain":"swe.dev/egress-identity/v1","installationNamespace":"swe-system","installationName":"main","installationUID":"i-uid","projectNamespace":"project-a","projectName":"project","projectUID":"p-uid","environmentNamespace":"project-a","environmentName":"environment","environmentUID":"e-uid","podName":"env-environment","podUID":"pod-uid","executionGeneration":7,"runtimePolicyRevision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","forwarderSecurityRevision":"1","certificateSHA256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`
	if string(b) != want {
		t.Fatalf("canonical bytes:\n got %s\nwant %s", b, want)
	}
	got, err := Parse(b)
	if err != nil || !reflect.DeepEqual(got, c) {
		t.Fatalf("Parse = %#v, %v", got, err)
	}
	for _, bad := range [][]byte{append([]byte(" "), b...), append(b, '\n'), []byte(strings.Replace(string(b), `"podUID":"pod-uid"`, `"podUID":"pod-uid","extra":1`, 1))} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("noncanonical identity accepted: %q", bad)
		}
	}
}

func TestClaimsRejectUnboundAndStaleInputs(t *testing.T) {
	base := validClaims()
	tests := map[string]func(*Claims){
		"Pod UID": func(c *Claims) { c.PodUID = "" }, "generation": func(c *Claims) { c.ExecutionGeneration = 0 },
		"fingerprint":        func(c *Claims) { c.CertificateFingerprint = strings.Repeat("A", 64) },
		"runtime revision":   func(c *Claims) { c.RuntimePolicyRevision = "" },
		"forwarder revision": func(c *Claims) { c.ForwarderRevision = strings.Repeat("x", MaxRevisionBytes+1) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			c := base
			mutate(&c)
			if _, err := c.CanonicalBytes(); err == nil {
				t.Fatal("invalid claims accepted")
			}
		})
	}
	first, _ := base.CanonicalBytes()
	base.PodUID = "replacement"
	second, _ := base.CanonicalBytes()
	if string(first) == string(second) {
		t.Fatal("stale Pod identity serialized identically")
	}
}

func TestIssueClientAuthMaterialAndClear(t *testing.T) {
	input := validClaims()
	input.CertificateFingerprint = ""
	claims, m, err := IssueForClaims(time.Unix(1000, 0), input)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(m.Certificate)
	if block == nil {
		t.Fatal("certificate is not PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cert.ExtKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}) || cert.KeyUsage != x509.KeyUsageDigitalSignature {
		t.Fatalf("certificate usages = %v, %v", cert.ExtKeyUsage, cert.KeyUsage)
	}
	if got := sha256.Sum256(cert.Raw); got != m.Fingerprint {
		t.Fatalf("fingerprint = %s, want %s", hex.EncodeToString(m.Fingerprint[:]), hex.EncodeToString(got[:]))
	}
	if claims.CertificateFingerprint != hex.EncodeToString(m.Fingerprint[:]) {
		t.Fatalf("claims fingerprint = %q", claims.CertificateFingerprint)
	}
	if string(m.CA) != string(m.Certificate) {
		t.Fatal("self-signed client trust material differs from certificate")
	}
	certBytes, keyBytes, caBytes := m.Certificate, m.PrivateKey, m.CA
	m.Clear()
	for name, value := range map[string][]byte{"certificate": certBytes, "key": keyBytes, "CA": caBytes, "fingerprint": m.Fingerprint[:]} {
		for _, b := range value {
			if b != 0 {
				t.Fatalf("%s was not zeroized", name)
			}
		}
	}
}
