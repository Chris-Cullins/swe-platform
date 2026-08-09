package egresspolicy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"sort"

	"github.com/distribution/reference"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	ConfigAPIVersion              = "swe.dev/egress-policy/v1"
	ConfigDataKey                 = "policy.json"
	ConfigContentSHA256Annotation = "swe.dev/egress-policy-content-sha256"
	RestrictedProfileCalicoV1     = "calico-v3.32.1"
)

const (
	ModeUnrestricted = "unrestricted"
	ModeRestricted   = "restricted"
)

// RestrictedProfile is the bounded Calico v3.32.1-only network authority
// input. Generic Kubernetes NetworkPolicy profiles are deliberately absent.
type RestrictedProfile struct {
	Name                  string   `json:"name"`
	ResolverIPs           []string `json:"resolverIPs"`
	APIServerCIDRs        []string `json:"apiServerCIDRs"`
	PodCIDRs              []string `json:"podCIDRs"`
	ServiceCIDRs          []string `json:"serviceCIDRs"`
	NodeCIDRs             []string `json:"nodeCIDRs"`
	ControlPlaneCIDRs     []string `json:"controlPlaneCIDRs"`
	AdditionalDeniedCIDRs []string `json:"additionalDeniedCIDRs"`
}

// Config is the canonical administrator-owned system egress policy document.
// ProxyImage is an immutable repository@sha256 identity, not a mutable tag.
type Config struct {
	APIVersion        string             `json:"apiVersion"`
	Mode              string             `json:"mode"`
	Ceiling           []Hostname         `json:"ceiling"`
	Baseline          []Hostname         `json:"baseline"`
	RestrictedProfile *RestrictedProfile `json:"restrictedProfile,omitempty"`
	TLSSecretName     string             `json:"tlsSecretName"`
	ProxyImage        string             `json:"proxyImage"`
}

// ParsedConfig binds validated canonical policy content to the exact live
// immutable ConfigMap incarnation that carried it.
type ParsedConfig struct {
	Config
	ContentSHA256 [sha256.Size]byte
}

type configWire struct {
	APIVersion        string             `json:"apiVersion"`
	Mode              string             `json:"mode"`
	Ceiling           []string           `json:"ceiling"`
	Baseline          []string           `json:"baseline"`
	RestrictedProfile *RestrictedProfile `json:"restrictedProfile,omitempty"`
	TLSSecretName     string             `json:"tlsSecretName"`
	ProxyImage        string             `json:"proxyImage"`
}

// ParseConfigMap strictly validates immutable, content-addressed authority.
// The only accepted data is canonical policy.json; Helm-rendered values are
// never treated as authority.
func ParseConfigMap(configMap *corev1.ConfigMap) (ParsedConfig, error) {
	if configMap == nil || configMap.UID == "" || !configMap.DeletionTimestamp.IsZero() {
		return ParsedConfig{}, errors.New("stable live policy ConfigMap is required")
	}
	if configMap.Immutable == nil || !*configMap.Immutable {
		return ParsedConfig{}, errors.New("policy ConfigMap must be immutable")
	}
	if len(configMap.Data) != 1 || len(configMap.BinaryData) != 0 {
		return ParsedConfig{}, errors.New("policy ConfigMap must contain only policy.json")
	}
	raw, ok := configMap.Data[ConfigDataKey]
	if !ok || raw == "" {
		return ParsedConfig{}, errors.New("policy ConfigMap has no policy.json")
	}
	parsed, canonical, err := parseConfig([]byte(raw))
	if err != nil {
		return ParsedConfig{}, err
	}
	if !bytes.Equal([]byte(raw), canonical) {
		return ParsedConfig{}, errors.New("policy.json is not canonical")
	}
	digest := sha256.Sum256(canonical)
	wantDigest := hex.EncodeToString(digest[:])
	if configMap.Annotations[ConfigContentSHA256Annotation] != wantDigest {
		return ParsedConfig{}, errors.New("policy ConfigMap content address does not match canonical content")
	}
	return ParsedConfig{Config: parsed, ContentSHA256: digest}, nil
}

func parseConfig(raw []byte) (Config, []byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire configWire
	if err := decoder.Decode(&wire); err != nil {
		return Config{}, nil, fmt.Errorf("decode policy.json: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Config{}, nil, errors.New("policy.json has trailing content")
	}
	if wire.APIVersion != ConfigAPIVersion {
		return Config{}, nil, errors.New("unsupported egress policy apiVersion")
	}
	policy, err := Evaluate(wire.Ceiling, wire.Baseline, nil)
	if err != nil {
		return Config{}, nil, err
	}
	if len(validation.IsDNS1123Subdomain(wire.TLSSecretName)) != 0 {
		return Config{}, nil, errors.New("TLS Secret name must be a DNS-1123 subdomain")
	}
	image, err := reference.ParseNormalizedNamed(wire.ProxyImage)
	digested, isDigested := image.(reference.Digested)
	_, isTagged := image.(reference.Tagged)
	if err != nil || !isDigested || isTagged || digested.Digest().Algorithm().String() != "sha256" || reference.FamiliarString(image) != wire.ProxyImage {
		return Config{}, nil, errors.New("proxy image must be an immutable repository@sha256 identity")
	}
	config := Config{APIVersion: wire.APIVersion, Mode: wire.Mode, Ceiling: policy.Ceiling,
		Baseline: policy.Baseline, RestrictedProfile: wire.RestrictedProfile,
		TLSSecretName: wire.TLSSecretName, ProxyImage: wire.ProxyImage}
	switch wire.Mode {
	case ModeUnrestricted:
		if wire.RestrictedProfile != nil {
			return Config{}, nil, errors.New("unrestricted mode must not define a restricted profile")
		}
	case ModeRestricted:
		if wire.RestrictedProfile == nil {
			return Config{}, nil, errors.New("restricted mode requires a profile")
		}
		profile, err := validateRestrictedProfile(*wire.RestrictedProfile)
		if err != nil {
			return Config{}, nil, err
		}
		config.RestrictedProfile = &profile
	default:
		return Config{}, nil, errors.New("mode must be unrestricted or restricted")
	}
	canonical, err := json.Marshal(config)
	if err != nil {
		return Config{}, nil, err
	}
	return config, canonical, nil
}

func validateRestrictedProfile(profile RestrictedProfile) (RestrictedProfile, error) {
	if profile.Name != RestrictedProfileCalicoV1 {
		return RestrictedProfile{}, errors.New("restricted profile must be calico-v3.32.1")
	}
	var err error
	if profile.ResolverIPs, err = canonicalIPs("resolverIPs", profile.ResolverIPs, 1, 4); err != nil {
		return RestrictedProfile{}, err
	}
	for _, set := range []struct {
		name             string
		values           *[]string
		minimum, maximum int
	}{
		{"apiServerCIDRs", &profile.APIServerCIDRs, 1, 8},
		{"podCIDRs", &profile.PodCIDRs, 1, 16},
		{"serviceCIDRs", &profile.ServiceCIDRs, 1, 16},
		{"nodeCIDRs", &profile.NodeCIDRs, 1, 32},
		{"controlPlaneCIDRs", &profile.ControlPlaneCIDRs, 1, 16},
		{"additionalDeniedCIDRs", &profile.AdditionalDeniedCIDRs, 0, 32},
	} {
		*set.values, err = canonicalCIDRs(set.name, *set.values, set.minimum, set.maximum)
		if err != nil {
			return RestrictedProfile{}, err
		}
	}
	return profile, nil
}

func canonicalIPs(name string, values []string, minimum, maximum int) ([]string, error) {
	if len(values) < minimum || len(values) > maximum {
		return nil, fmt.Errorf("%s must contain %d-%d entries", name, minimum, maximum)
	}
	result := make([]string, len(values))
	copy(result, values)
	seen := make(map[netip.Addr]struct{}, len(values))
	for _, value := range result {
		address, err := netip.ParseAddr(value)
		if err != nil || address.Zone() != "" || address.Is4In6() || address.String() != value {
			return nil, fmt.Errorf("%s contains a non-canonical IP literal", name)
		}
		if _, exists := seen[address]; exists {
			return nil, fmt.Errorf("%s contains a duplicate", name)
		}
		seen[address] = struct{}{}
	}
	sort.Strings(result)
	return result, nil
}

func canonicalCIDRs(name string, values []string, minimum, maximum int) ([]string, error) {
	if len(values) < minimum || len(values) > maximum {
		return nil, fmt.Errorf("%s must contain %d-%d entries", name, minimum, maximum)
	}
	result := make([]string, len(values))
	copy(result, values)
	seen := make(map[netip.Prefix]struct{}, len(values))
	for _, value := range result {
		prefix, err := netip.ParsePrefix(value)
		if err != nil || prefix.Addr().Zone() != "" || prefix.Addr().Is4In6() || prefix != prefix.Masked() || prefix.String() != value {
			return nil, fmt.Errorf("%s contains a non-canonical CIDR", name)
		}
		if _, exists := seen[prefix]; exists {
			return nil, fmt.Errorf("%s contains a duplicate", name)
		}
		seen[prefix] = struct{}{}
	}
	sort.Strings(result)
	return result, nil
}
