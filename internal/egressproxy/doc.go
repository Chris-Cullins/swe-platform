// Package egressproxy contains the INERT, DISABLED and non-authoritative
// transport slice of the future egress proxy. It does not consult Kubernetes,
// authorize live Environments, or enforce network policy. The shipped command
// deliberately installs an authorizer which denies every request. Tests and a
// future authoritative integration may inject an Authorizer.
// This inert slice intentionally emits no request-derived logs until bounded,
// non-sensitive observability is integrated.
package egressproxy
