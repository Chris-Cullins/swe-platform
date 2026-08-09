// Package egresspolicy is the sole authority for the v1 egress destination
// grammar, policy composition, and runtime policy revision.
package egresspolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

const (
	MaxProjectEntries  = 64
	MaxCeilingEntries  = 256
	MaxBaselineEntries = 64
	MaxHostnameBytes   = 253
	MaxLabelBytes      = 63

	// RuntimeRevisionDomain changes whenever canonical revision semantics or a
	// security-relevant policy input changes.
	RuntimeRevisionDomain = "swe.dev/egress-policy/runtime-revision/v1"
)

// Hostname is an exact lowercase ASCII FQDN accepted by ParseHostname.
// Its implicit and only v1 destination is TLS-over-CONNECT on TCP port 443.
type Hostname string

// Policy is a validated, sorted v1 policy. Each slice is an independent copy.
type Policy struct {
	Ceiling          []Hostname
	Baseline         []Hostname
	ProjectSelection []Hostname
	Effective        []Hostname
}

// RuntimeRevisionInputs are the authoritative immutable identities and policy
// content needed by later operator and proxy integrations. ConfigMapContentSHA256
// is the digest of the immutable administrator-owned ConfigMap content.
type RuntimeRevisionInputs struct {
	InstallationUID        types.UID
	ConfigMapUID           types.UID
	ConfigMapContentSHA256 [sha256.Size]byte
	ProjectUID             types.UID
	Ceiling                []string
	Baseline               []string
	ProjectSelection       []string
}

// RuntimeRevision is the SHA-256 digest of canonical runtime policy inputs.
type RuntimeRevision [sha256.Size]byte

func (r RuntimeRevision) String() string { return hex.EncodeToString(r[:]) }

// ParseHostname validates an exact v1 destination. It never normalizes input.
func ParseHostname(value string) (Hostname, error) {
	if len(value) == 0 || len(value) > MaxHostnameBytes {
		return "", fmt.Errorf("hostname length must be 1-%d bytes", MaxHostnameBytes)
	}
	for i := 0; i < len(value); i++ {
		if value[i] > 0x7f {
			return "", errors.New("hostname must contain lowercase ASCII only")
		}
	}

	labels := strings.Split(value, ".")
	if len(labels) < 2 {
		return "", errors.New("hostname must contain at least two labels")
	}
	for _, label := range labels {
		if err := validateLabel(label); err != nil {
			return "", fmt.Errorf("invalid hostname %q: %w", value, err)
		}
	}
	if isIPLiteral(value, labels) {
		return "", errors.New("IP literals are not egress hostnames")
	}
	return Hostname(value), nil
}

func validateLabel(label string) error {
	if len(label) == 0 || len(label) > MaxLabelBytes {
		return fmt.Errorf("label length must be 1-%d bytes", MaxLabelBytes)
	}
	if strings.HasPrefix(label, "xn--") {
		return errors.New("IDNA A-labels are not supported")
	}
	if label[0] == '-' || label[len(label)-1] == '-' {
		return errors.New("label must start and end with a letter or digit")
	}
	for i := 0; i < len(label); i++ {
		c := label[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return errors.New("label must match [a-z0-9]([a-z0-9-]*[a-z0-9])?")
		}
	}
	return nil
}

func isIPLiteral(value string, labels []string) bool {
	if _, err := netip.ParseAddr(value); err == nil {
		return true
	}
	if len(labels) > 4 {
		return false
	}
	parts := make([]uint64, len(labels))
	for i, label := range labels {
		part, ok := parseIPv4Number(label)
		if !ok {
			return false
		}
		parts[i] = part
	}
	switch len(parts) {
	case 1:
		return parts[0] <= 0xffffffff
	case 2:
		return parts[0] <= 0xff && parts[1] <= 0xffffff
	case 3:
		return parts[0] <= 0xff && parts[1] <= 0xff && parts[2] <= 0xffff
	case 4:
		return parts[0] <= 0xff && parts[1] <= 0xff && parts[2] <= 0xff && parts[3] <= 0xff
	default:
		return false
	}
}

func parseIPv4Number(value string) (uint64, bool) {
	base, digits := 10, value
	if value == "0x" {
		return 0, true
	}
	if len(value) > 2 && value[0:2] == "0x" {
		base, digits = 16, value[2:]
	} else if len(value) > 1 && value[0] == '0' {
		base = 8
	}
	if digits == "" {
		return 0, false
	}
	result, err := strconv.ParseUint(digits, base, 32)
	return result, err == nil
}

// Evaluate validates the administrator ceiling and baseline, validates the
// Project selection as a subset of that ceiling, and returns baseline union
// selection. Invalid entries and out-of-ceiling selections fail rather than
// being normalized or silently intersected.
func Evaluate(ceiling, baseline, projectSelection []string) (Policy, error) {
	canonicalCeiling, err := parseSet("ceiling", ceiling, MaxCeilingEntries)
	if err != nil {
		return Policy{}, err
	}
	canonicalBaseline, err := parseSet("baseline", baseline, MaxBaselineEntries)
	if err != nil {
		return Policy{}, err
	}
	canonicalSelection, err := parseSet("project selection", projectSelection, MaxProjectEntries)
	if err != nil {
		return Policy{}, err
	}
	ceilingSet := hostnameSet(canonicalCeiling)
	if err := requireSubset("baseline", canonicalBaseline, ceilingSet); err != nil {
		return Policy{}, err
	}
	if err := requireSubset("project selection", canonicalSelection, ceilingSet); err != nil {
		return Policy{}, err
	}
	effectiveSet := hostnameSet(canonicalBaseline)
	for _, hostname := range canonicalSelection {
		effectiveSet[hostname] = struct{}{}
	}
	effective := make([]Hostname, 0, len(effectiveSet))
	for hostname := range effectiveSet {
		effective = append(effective, hostname)
	}
	sort.Slice(effective, func(i, j int) bool { return effective[i] < effective[j] })
	return Policy{
		Ceiling:          canonicalCeiling,
		Baseline:         canonicalBaseline,
		ProjectSelection: canonicalSelection,
		Effective:        effective,
	}, nil
}

func parseSet(name string, values []string, maximum int) ([]Hostname, error) {
	if len(values) > maximum {
		return nil, fmt.Errorf("%s has %d entries, maximum is %d", name, len(values), maximum)
	}
	result := make([]Hostname, 0, len(values))
	seen := make(map[Hostname]struct{}, len(values))
	for i, value := range values {
		hostname, err := ParseHostname(value)
		if err != nil {
			return nil, fmt.Errorf("%s entry %d: %w", name, i, err)
		}
		if _, exists := seen[hostname]; exists {
			return nil, fmt.Errorf("%s contains duplicate hostname %q", name, hostname)
		}
		seen[hostname] = struct{}{}
		result = append(result, hostname)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func hostnameSet(values []Hostname) map[Hostname]struct{} {
	result := make(map[Hostname]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func requireSubset(name string, values []Hostname, ceiling map[Hostname]struct{}) error {
	for _, value := range values {
		if _, allowed := ceiling[value]; !allowed {
			return fmt.Errorf("%s hostname %q is outside the administrator ceiling", name, value)
		}
	}
	return nil
}

// CanonicalBytes validates and serializes revision inputs deterministically.
func (in RuntimeRevisionInputs) CanonicalBytes() ([]byte, error) {
	if in.InstallationUID == "" || in.ConfigMapUID == "" || in.ProjectUID == "" {
		return nil, errors.New("installation, ConfigMap, and Project UIDs are required")
	}
	if in.ConfigMapContentSHA256 == ([sha256.Size]byte{}) {
		return nil, errors.New("ConfigMap content SHA-256 is required")
	}
	policy, err := Evaluate(in.Ceiling, in.Baseline, in.ProjectSelection)
	if err != nil {
		return nil, err
	}
	type canonicalInputs struct {
		Domain                 string     `json:"domain"`
		InstallationUID        types.UID  `json:"installationUID"`
		ConfigMapUID           types.UID  `json:"configMapUID"`
		ConfigMapContentSHA256 string     `json:"configMapContentSHA256"`
		Ceiling                []Hostname `json:"ceiling"`
		Baseline               []Hostname `json:"baseline"`
		ProjectUID             types.UID  `json:"projectUID"`
		ProjectSelection       []Hostname `json:"projectSelection"`
	}
	return json.Marshal(canonicalInputs{
		Domain:                 RuntimeRevisionDomain,
		InstallationUID:        in.InstallationUID,
		ConfigMapUID:           in.ConfigMapUID,
		ConfigMapContentSHA256: hex.EncodeToString(in.ConfigMapContentSHA256[:]),
		Ceiling:                policy.Ceiling,
		Baseline:               policy.Baseline,
		ProjectUID:             in.ProjectUID,
		ProjectSelection:       policy.ProjectSelection,
	})
}

// Revision returns SHA-256 over CanonicalBytes.
func (in RuntimeRevisionInputs) Revision() (RuntimeRevision, error) {
	canonical, err := in.CanonicalBytes()
	if err != nil {
		return RuntimeRevision{}, err
	}
	return sha256.Sum256(canonical), nil
}
