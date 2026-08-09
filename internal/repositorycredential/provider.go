// Package repositorycredential defines the provider-neutral short-lived repository credential boundary.
package repositorycredential

import (
	"context"
	"errors"
	"time"
)

const MaxTokenBytes = 16 << 10

const (
	DefaultRetryDelay = 10 * time.Second
	MinimumValidity   = 5 * time.Minute
	RefreshMargin     = 10 * time.Minute
)

type Credential struct {
	Token          []byte
	Repository     string
	InstallationID int64
	ExpiresAt      time.Time
}

type Provider interface {
	CanonicalRepository(string) (string, error)
	// Issue returns both a credential and an error only when the credential is
	// cleanup-capable but must not be delivered.
	Issue(context.Context, string) (*Credential, error)
	Revoke(context.Context, *Credential) error
}

// Canonicalizer is the stateless provider identity operation needed even when
// issuance and revocation are disabled.
type Canonicalizer interface {
	CanonicalRepository(string) (string, error)
}

type Error struct {
	Retryable  bool
	Operation  string
	Reason     string
	RetryAfter time.Duration
	StatusCode int
}

func (e *Error) Error() string { return "repository credential provider " + e.Operation + " failed" }
func IsRetryable(err error) bool {
	var target *Error
	return errors.As(err, &target) && target.Retryable
}

func Reason(err error) string {
	var target *Error
	if errors.As(err, &target) && target.Reason != "" {
		return target.Reason
	}
	return "ProviderError"
}

func RetryDelay(err error) time.Duration {
	var target *Error
	if errors.As(err, &target) && target.RetryAfter > 0 {
		return target.RetryAfter
	}
	return DefaultRetryDelay
}
