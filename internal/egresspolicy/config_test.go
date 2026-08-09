package egresspolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func validConfig() Config {
	return Config{
		APIVersion: ConfigAPIVersion, Mode: ModeRestricted,
		Ceiling: []Hostname{"api.example.com", "git.example.com"}, Baseline: []Hostname{"git.example.com"},
		RestrictedProfile: &RestrictedProfile{
			Name: RestrictedProfileCalicoV1, ResolverIPs: []string{"10.96.0.10"},
			APIServerCIDRs: []string{"10.0.0.1/32"}, PodCIDRs: []string{"10.244.0.0/16"},
			ServiceCIDRs: []string{"10.96.0.0/12"}, NodeCIDRs: []string{"192.168.0.0/16"},
			ControlPlaneCIDRs: []string{"172.16.0.0/16"}, AdditionalDeniedCIDRs: []string{},
		},
		TLSSecretName: "egress-proxy-tls", ProxyImage: "registry.example.com/swe/egress-proxy@sha256:" + strings.Repeat("a", 64),
	}
}

func configMapFor(t *testing.T, config Config) *corev1.ConfigMap {
	t.Helper()
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: "system", UID: "config-uid", Annotations: map[string]string{
		ConfigContentSHA256Annotation: hex.EncodeToString(digest[:]),
	}}, Immutable: ptr.To(true), Data: map[string]string{ConfigDataKey: string(raw)}}
}

func TestParseConfigMapCanonicalAuthority(t *testing.T) {
	configMap := configMapFor(t, validConfig())
	got, err := ParseConfigMap(configMap)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != ModeRestricted || got.RestrictedProfile == nil || got.RestrictedProfile.Name != RestrictedProfileCalicoV1 || len(got.Ceiling) != 2 {
		t.Fatalf("unexpected parsed config: %#v", got)
	}

	tests := map[string]func(*corev1.ConfigMap){
		"mutable":              func(c *corev1.ConfigMap) { c.Immutable = ptr.To(false) },
		"missing UID":          func(c *corev1.ConfigMap) { c.UID = "" },
		"deleting":             func(c *corev1.ConfigMap) { now := metav1.Now(); c.DeletionTimestamp = &now },
		"extra data":           func(c *corev1.ConfigMap) { c.Data["other"] = "value" },
		"binary data":          func(c *corev1.ConfigMap) { c.BinaryData = map[string][]byte{"other": {1}} },
		"content address":      func(c *corev1.ConfigMap) { c.Annotations[ConfigContentSHA256Annotation] = strings.Repeat("0", 64) },
		"noncanonical content": func(c *corev1.ConfigMap) { c.Data[ConfigDataKey] += "\n" },
		"unknown field": func(c *corev1.ConfigMap) {
			c.Data[ConfigDataKey] = strings.TrimSuffix(c.Data[ConfigDataKey], "}") + `,"extra":true}`
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			configMap := configMapFor(t, validConfig())
			mutate(configMap)
			if _, err := ParseConfigMap(configMap); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestParseConfigMapStrictSchemaAndBounds(t *testing.T) {
	tests := map[string]func(*Config){
		"version":          func(c *Config) { c.APIVersion = "v2" },
		"mode":             func(c *Config) { c.Mode = "production" },
		"profile":          func(c *Config) { c.RestrictedProfile.Name = "generic-knp" },
		"resolver minimum": func(c *Config) { c.RestrictedProfile.ResolverIPs = nil },
		"resolver maximum": func(c *Config) {
			c.RestrictedProfile.ResolverIPs = []string{"1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5"}
		},
		"noncanonical resolver":    func(c *Config) { c.RestrictedProfile.ResolverIPs = []string{"2001:0db8::1"} },
		"API CIDR minimum":         func(c *Config) { c.RestrictedProfile.APIServerCIDRs = nil },
		"unmasked CIDR":            func(c *Config) { c.RestrictedProfile.PodCIDRs = []string{"10.244.1.1/16"} },
		"baseline outside ceiling": func(c *Config) { c.Baseline = []Hostname{"other.example.com"} },
		"ceiling maximum":          func(c *Config) { c.Ceiling = make([]Hostname, MaxCeilingEntries+1) },
		"TLS Secret":               func(c *Config) { c.TLSSecretName = "Bad" },
		"mutable image":            func(c *Config) { c.ProxyImage = "proxy:latest" },
		"tagged digest image":      func(c *Config) { c.ProxyImage = "registry.example.com/proxy:latest@sha256:" + strings.Repeat("a", 64) },
		"noncanonical image":       func(c *Config) { c.ProxyImage = "docker.io/library/proxy@sha256:" + strings.Repeat("a", 64) },
		"invalid image path":       func(c *Config) { c.ProxyImage = "registry.example.com/Bad/proxy@sha256:" + strings.Repeat("a", 64) },
		"image control byte":       func(c *Config) { c.ProxyImage = "registry.example.com/proxy\n@sha256:" + strings.Repeat("a", 64) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := validConfig()
			mutate(&config)
			if _, err := ParseConfigMap(configMapFor(t, config)); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}

	unrestricted := validConfig()
	unrestricted.Mode = ModeUnrestricted
	unrestricted.RestrictedProfile = nil
	if _, err := ParseConfigMap(configMapFor(t, unrestricted)); err != nil {
		t.Fatalf("valid unrestricted config rejected: %v", err)
	}
	unrestricted.RestrictedProfile = validConfig().RestrictedProfile
	if _, err := ParseConfigMap(configMapFor(t, unrestricted)); err == nil {
		t.Fatal("unrestricted config accepted a restricted profile")
	}
}
