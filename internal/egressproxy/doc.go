// Package egressproxy contains the INERT and DISABLED transport plus an
// unconstructed Kubernetes currentness-authorizer foundation for the future
// egress proxy. It does not enforce network policy. The shipped command
// deliberately installs an authorizer which denies every request; only tests
// construct the uncached currentness proof.
// This inert slice intentionally emits no request-derived logs until bounded,
// non-sensitive observability is integrated.
package egressproxy
