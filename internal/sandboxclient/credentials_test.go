package sandboxclient

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
)

func TestDialOptionsRejectsIncompletePodCredentialBundle(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		sandboxdauth.IdentityAnnotation: "current.sandboxd.swe.dev",
		sandboxdauth.TokenAnnotation:    "terminal-token",
	}}}

	_, err := DialOptions(pod)
	if err == nil || !strings.Contains(err.Error(), "trust bundle") {
		t.Fatalf("DialOptions() error = %v, want missing trust bundle", err)
	}
}
