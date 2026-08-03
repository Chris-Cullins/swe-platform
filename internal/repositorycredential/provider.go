// Package repositorycredential defines the provider-neutral short-lived repository credential boundary.
package repositorycredential

import (
	"context"
	"errors"
	"time"
)

const MaxTokenBytes = 16 << 10

type Credential struct {
	Token          []byte
	Repository     string
	InstallationID int64
	ExpiresAt      time.Time
}

type Provider interface {
	Issue(context.Context, string) (*Credential, error)
	Revoke(context.Context, *Credential) error
}

type Error struct {
	Retryable bool
	Operation string
}

func (e *Error) Error() string { return "repository credential provider " + e.Operation + " failed" }
func IsRetryable(err error) bool {
	var target *Error
	return errors.As(err, &target) && target.Retryable
}
