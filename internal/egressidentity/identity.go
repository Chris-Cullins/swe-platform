// Package egressidentity defines the inert v1 per-execution identity contract
// shared by future restricted-egress publishers and proxy authorizers.
package egressidentity

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	Domain            = "swe.dev/egress-identity/v1"
	MaxCanonicalBytes = 4096
	MaxRevisionBytes  = 128

	ClientCertificateKey = "client.crt"
	ClientPrivateKeyKey  = "client.key"
	ClientTrustKey       = "client-trust.crt"
	CanonicalClaimsKey   = "identity.json"
)

// Claims is the canonical, exact authority lookup key. Certificate subject
// fields are deliberately excluded: they are untrusted lookup hints only.
type Claims struct {
	InstallationNamespace  string    `json:"installationNamespace"`
	InstallationName       string    `json:"installationName"`
	InstallationUID        types.UID `json:"installationUID"`
	ProjectNamespace       string    `json:"projectNamespace"`
	ProjectName            string    `json:"projectName"`
	ProjectUID             types.UID `json:"projectUID"`
	EnvironmentNamespace   string    `json:"environmentNamespace"`
	EnvironmentName        string    `json:"environmentName"`
	EnvironmentUID         types.UID `json:"environmentUID"`
	PodName                string    `json:"podName"`
	PodUID                 types.UID `json:"podUID"`
	ExecutionGeneration    int64     `json:"executionGeneration"`
	RuntimePolicyRevision  string    `json:"runtimePolicyRevision"`
	ForwarderRevision      string    `json:"forwarderSecurityRevision"`
	CertificateFingerprint string    `json:"certificateSHA256"`
}

// CertificateHint is all the disabled proxy can learn from a presented leaf.
// Neither field is authority; an authorizer must use the fingerprint to look
// up and then revalidate canonical Claims against live authoritative objects.
type CertificateHint struct {
	Fingerprint [sha256.Size]byte
	Subject     string
}

type canonicalClaims struct {
	Domain                 string    `json:"domain"`
	InstallationNamespace  string    `json:"installationNamespace"`
	InstallationName       string    `json:"installationName"`
	InstallationUID        types.UID `json:"installationUID"`
	ProjectNamespace       string    `json:"projectNamespace"`
	ProjectName            string    `json:"projectName"`
	ProjectUID             types.UID `json:"projectUID"`
	EnvironmentNamespace   string    `json:"environmentNamespace"`
	EnvironmentName        string    `json:"environmentName"`
	EnvironmentUID         types.UID `json:"environmentUID"`
	PodName                string    `json:"podName"`
	PodUID                 types.UID `json:"podUID"`
	ExecutionGeneration    int64     `json:"executionGeneration"`
	RuntimePolicyRevision  string    `json:"runtimePolicyRevision"`
	ForwarderRevision      string    `json:"forwarderSecurityRevision"`
	CertificateFingerprint string    `json:"certificateSHA256"`
}

func (c Claims) canonical() (canonicalClaims, error) {
	for label, value := range map[string]string{
		"Installation namespace": c.InstallationNamespace, "Project namespace": c.ProjectNamespace,
		"Environment namespace": c.EnvironmentNamespace,
	} {
		if len(validation.IsDNS1123Label(value)) != 0 {
			return canonicalClaims{}, fmt.Errorf("%s is invalid", label)
		}
	}
	for label, value := range map[string]string{
		"Installation name": c.InstallationName, "Project name": c.ProjectName,
		"Environment name": c.EnvironmentName, "Pod name": c.PodName,
	} {
		if len(validation.IsDNS1123Subdomain(value)) != 0 {
			return canonicalClaims{}, fmt.Errorf("%s is invalid", label)
		}
	}
	if c.InstallationUID == "" || c.ProjectUID == "" || c.EnvironmentUID == "" || c.PodUID == "" {
		return canonicalClaims{}, errors.New("Installation, Project, Environment, and Pod UIDs are required")
	}
	for label, value := range map[string]types.UID{"Installation": c.InstallationUID, "Project": c.ProjectUID, "Environment": c.EnvironmentUID, "Pod": c.PodUID} {
		if len(value) > 128 || strings.IndexFunc(string(value), func(r rune) bool { return r < 0x21 || r > 0x7e }) >= 0 {
			return canonicalClaims{}, fmt.Errorf("%s UID is invalid", label)
		}
	}
	if c.ExecutionGeneration < 1 {
		return canonicalClaims{}, errors.New("execution generation must be positive")
	}
	if !validRevision(c.RuntimePolicyRevision) || !validRevision(c.ForwarderRevision) {
		return canonicalClaims{}, errors.New("runtime policy and forwarder security revisions must be bounded printable ASCII")
	}
	if len(c.CertificateFingerprint) != sha256.Size*2 {
		return canonicalClaims{}, errors.New("certificate fingerprint must be a SHA-256 hex digest")
	}
	fingerprint, err := hex.DecodeString(c.CertificateFingerprint)
	if err != nil || hex.EncodeToString(fingerprint) != c.CertificateFingerprint {
		return canonicalClaims{}, errors.New("certificate fingerprint must be lowercase SHA-256 hex")
	}
	return canonicalClaims{Domain, c.InstallationNamespace, c.InstallationName, c.InstallationUID,
		c.ProjectNamespace, c.ProjectName, c.ProjectUID, c.EnvironmentNamespace, c.EnvironmentName,
		c.EnvironmentUID, c.PodName, c.PodUID, c.ExecutionGeneration, c.RuntimePolicyRevision,
		c.ForwarderRevision, c.CertificateFingerprint}, nil
}

func validRevision(value string) bool {
	return len(value) > 0 && len(value) <= MaxRevisionBytes && strings.IndexFunc(value, func(r rune) bool { return r < 0x21 || r > 0x7e }) < 0
}

func (c Claims) CanonicalBytes() ([]byte, error) {
	canonical, err := c.canonical()
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxCanonicalBytes {
		return nil, errors.New("canonical egress identity is too large")
	}
	return b, nil
}

func Parse(value []byte) (Claims, error) {
	if len(value) == 0 || len(value) > MaxCanonicalBytes {
		return Claims{}, errors.New("invalid egress identity size")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var canonical canonicalClaims
	if err := decoder.Decode(&canonical); err != nil {
		return Claims{}, fmt.Errorf("decode egress identity: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Claims{}, errors.New("egress identity has trailing content")
	}
	if canonical.Domain != Domain {
		return Claims{}, errors.New("unsupported egress identity domain")
	}
	c := Claims{canonical.InstallationNamespace, canonical.InstallationName, canonical.InstallationUID,
		canonical.ProjectNamespace, canonical.ProjectName, canonical.ProjectUID, canonical.EnvironmentNamespace,
		canonical.EnvironmentName, canonical.EnvironmentUID, canonical.PodName, canonical.PodUID,
		canonical.ExecutionGeneration, canonical.RuntimePolicyRevision, canonical.ForwarderRevision,
		canonical.CertificateFingerprint}
	reencoded, err := c.CanonicalBytes()
	if err != nil {
		return Claims{}, err
	}
	if !bytes.Equal(value, reencoded) {
		return Claims{}, errors.New("egress identity is not canonical")
	}
	return c, nil
}

// Material is a separate ClientAuth credential; it must never be reused as
// sandboxd's ServerAuth keypair. Clear destroys all returned byte slices.
type Material struct {
	Certificate []byte
	PrivateKey  []byte
	CA          []byte
	Fingerprint [sha256.Size]byte
}

func (m *Material) Clear() {
	if m == nil {
		return
	}
	clear(m.Certificate)
	clear(m.PrivateKey)
	clear(m.CA)
	clear(m.Fingerprint[:])
}

// IssueForClaims issues a client identity and returns claims bound to its exact
// DER fingerprint. The input fingerprint must be empty so callers cannot
// accidentally combine independently produced claims and material.
func IssueForClaims(now time.Time, claims Claims) (Claims, *Material, error) {
	if claims.CertificateFingerprint != "" {
		return Claims{}, nil, errors.New("certificate fingerprint is output-only")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Claims{}, nil, err
	}
	defer key.D.SetInt64(0)
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return Claims{}, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "egress-client.swe.dev"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return Claims{}, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return Claims{}, nil, err
	}
	defer clear(keyDER)
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privateKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	fingerprint := sha256.Sum256(der)
	claims.CertificateFingerprint = hex.EncodeToString(fingerprint[:])
	if _, err := claims.CanonicalBytes(); err != nil {
		clear(certificate)
		clear(privateKey)
		return Claims{}, nil, err
	}
	return claims, &Material{Certificate: certificate, PrivateKey: privateKey, CA: append([]byte(nil), certificate...), Fingerprint: fingerprint}, nil
}

// FingerprintCertificate returns the SHA-256 digest of one PEM certificate.
func FingerprintCertificate(certificatePEM []byte) ([sha256.Size]byte, error) {
	block, rest := pem.Decode(certificatePEM)
	if block == nil || block.Type != "CERTIFICATE" || len(rest) != 0 {
		return [sha256.Size]byte{}, errors.New("invalid canonical certificate PEM")
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(block.Bytes), nil
}
