package controlplane

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
)

const (
	portalNotFoundBody              = "not found\n"
	portalSessionHandoffCodeLength  = 32
	portalSessionHandoffBodyLimit   = int64(len("code=") + portalSessionHandoffCodeLength)
	portalSessionHandoffReadLimit   = 8
	portalSessionHandoffReadTimeout = 2 * time.Second
)
const (
	maxPortalResponseBytes   int64 = 64 << 20
	portalAdmissionTimeout         = 2 * time.Minute
	portalAdmissionHeartbeat       = time.Second
	portalLeasePollInterval        = 2 * time.Second
)

// PortalResolver is the narrow durable route authority used by the gateway.
type PortalResolver interface {
	client.Client
}

type PortalEnvironmentEnumerator interface {
	ListPortalEnvironments(context.Context) ([]platformv1alpha1.Environment, error)
}

type PortalDialer interface {
	DialPortal(context.Context, context.Context, lifecycle.ExecutionFence, sandboxclient.ServiceDeclarationSnapshot, string) (net.Conn, error)
}

type portalGateway struct {
	server             *Server
	resolver           PortalResolver
	enumerator         PortalEnvironmentEnumerator
	dialer             PortalDialer
	suffix, scheme     string
	requests           chan struct{}
	admissionHeartbeat time.Duration
	leasePoll          time.Duration
	handoffReadTimeout time.Duration
	handoffReads       chan struct{}
	handoffsMu         sync.Mutex
	handoffs           map[string]portalSessionHandoff
}

type portalSessionHandoff struct {
	sessionID string
	locator   string
	origin    string
	expiresAt time.Time
}

type PortalRoute struct {
	URL                   string `json:"url"`
	Disabled              bool   `json:"disabled,omitempty"`
	EnvironmentUID        string `json:"environmentUID"`
	Service               string `json:"service"`
	Revision              int64  `json:"revision"`
	DeclarationInstanceID string `json:"declarationInstanceID"`
	RouteGeneration       int64  `json:"routeGeneration"`
}

func newPortalGateway(s *Server, o ServerOptions) *portalGateway {
	suffix := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(o.PortalSuffix)), ".")
	scheme := strings.ToLower(strings.TrimSpace(o.PortalScheme))
	if o.PortalResolver == nil {
		return nil
	}
	if suffix == "" {
		return &portalGateway{server: s, resolver: o.PortalResolver, suffix: suffix, scheme: scheme, requests: make(chan struct{}, 64), handoffReads: make(chan struct{}, portalSessionHandoffReadLimit), handoffs: make(map[string]portalSessionHandoff)}
	}
	if o.PortalDialer == nil {
		return nil
	}
	if scheme != "https" && scheme != "http" {
		return nil
	}
	if strings.ContainsAny(suffix, ":/ ") || strings.HasPrefix(suffix, ".") {
		return nil
	}
	if o.PortalEnvironmentEnumerator == nil {
		return nil
	}
	return &portalGateway{server: s, resolver: o.PortalResolver, enumerator: o.PortalEnvironmentEnumerator, dialer: o.PortalDialer, suffix: suffix, scheme: scheme, requests: make(chan struct{}, 64), handoffReads: make(chan struct{}, portalSessionHandoffReadLimit), handoffs: make(map[string]portalSessionHandoff)}
}

func (g *portalGateway) enabled() bool { return g != nil && g.suffix != "" }

// ValidatePortalConfiguration rejects partially configured or malformed
// production gateway settings. Empty settings intentionally disable portals.
func ValidatePortalConfiguration(suffix, scheme string) error {
	suffix = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(suffix)), ".")
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if suffix == "" {
		return nil
	}
	if scheme != "https" && scheme != "http" {
		return errors.New("portal scheme must be http or https")
	}
	if strings.ContainsAny(suffix, ":/ ") || strings.HasPrefix(suffix, ".") || strings.Contains(suffix, "..") {
		return errors.New("portal suffix is invalid")
	}
	return nil
}

func portalDiscoveryPath(path string) (string, string, string, bool) {
	remainder := strings.TrimPrefix(path, namespacedPathPrefix)
	if remainder == path {
		return "", "", "", false
	}
	p := strings.Split(remainder, "/")
	if len(p) != 5 || p[1] != "environments" || p[3] != "portal" {
		return "", "", "", false
	}
	vals := make([]string, 3)
	for i, j := range []int{0, 2, 4} {
		v, e := url.PathUnescape(p[j])
		if e != nil || v == "" || strings.Contains(v, "/") {
			return "", "", "", false
		}
		vals[i] = v
	}
	return vals[0], vals[1], vals[2], true
}

func (g *portalGateway) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		locator, ok := g.locatorForRequest(r)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/.swe/session-handoff" && locator != "" {
			g.consumeSessionHandoff(w, r, locator)
			return
		}
		if r.URL.Path == "/api/v1/session" && locator != "" {
			g.server.handleSession(w, r)
			return
		}
		g.serveLocator(w, r, locator)
	})
}

func (g *portalGateway) locatorForRequest(r *http.Request) (string, bool) {
	host := r.Host
	proto := "http"
	if r.TLS != nil {
		proto = "https"
	}
	if g.server.trustProxy {
		if h := r.Header.Values("X-Forwarded-Host"); len(h) == 1 && !strings.Contains(h[0], ",") {
			host = h[0]
		} else if len(h) > 0 {
			return "", true
		}
		if p := r.Header.Values("X-Forwarded-Proto"); len(p) == 1 && !strings.Contains(p[0], ",") {
			proto = strings.ToLower(p[0])
		} else if len(p) > 0 {
			return "", true
		}
	}
	host = strings.TrimSpace(host)
	hostname := host
	if strings.Contains(host, ":") {
		i := strings.LastIndexByte(host, ':')
		hostname = host[:i]
		port := host[i+1:]
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			hostname = strings.ToLower(hostname)
			if strings.HasSuffix(strings.TrimSuffix(hostname, "."), "."+g.suffix) {
				return "", true
			}
			return "", false
		}
	}
	host = strings.ToLower(hostname)
	suffix := "." + g.suffix
	underSuffix := strings.HasSuffix(strings.TrimSuffix(host, "."), suffix)
	if !underSuffix {
		return "", false
	}
	if proto != g.scheme || strings.HasSuffix(host, ".") {
		return "", true
	}
	locator := strings.TrimSuffix(host, suffix)
	if locator == "" || strings.Contains(locator, ".") || !validLocator(locator) {
		return "", true
	}
	return locator, true
}

func validLocator(s string) bool {
	if len(s) < 20 || len(s) > 63 {
		return false
	}
	for _, c := range s {
		if c < 'a' || c > 'z' {
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}
func portal404(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-store")
	w.WriteHeader(404)
	_, _ = io.WriteString(w, portalNotFoundBody)
}

func (g *portalGateway) discover(w http.ResponseWriter, r *http.Request, ns, name, service string) {
	if err := g.authorize(r, ns, name, service, nil); err != nil {
		writeRESTAccessError(w, err)
		return
	}
	key := types.NamespacedName{Namespace: ns, Name: name}
	for attempt := 0; attempt < 5; attempt++ {
		var env platformv1alpha1.Environment
		if err := g.resolver.Get(r.Context(), key, &env); err != nil {
			g.server.writeResourceError(w, "get portal environment", ns, name, err)
			return
		}
		if err := g.authorize(r, ns, name, service, &env); err != nil {
			writeRESTAccessError(w, err)
			return
		}
		decl, ok := exactPortalDeclaration(&env, service)
		if !ok {
			http.NotFound(w, r)
			return
		}
		route, changed, err := g.routeForDeclaration(&env, decl)
		if err != nil {
			g.server.writeResourceError(w, "generate portal route", ns, name, err)
			return
		}
		if changed {
			if err := g.resolver.Status().Update(r.Context(), &env); err != nil {
				if apierrors.IsConflict(err) {
					continue
				}
				g.server.writeResourceError(w, "update portal route", ns, name, err)
				return
			}
		}
		writeJSON(w, http.StatusOK, g.portalRoute(&env, decl, route))
		return
	}
	writeProblem(w, 409, "conflict", "Conflict", "portal route changed concurrently")
}

func (g *portalGateway) routeForDeclaration(env *platformv1alpha1.Environment, decl platformv1alpha1.EnvironmentServiceDeclaration) (platformv1alpha1.EnvironmentPortalRoute, bool, error) {
	if !g.enabled() {
		changed, route, err := reconcileDisabledPortalRoute(env, decl.Name, decl.InstanceID, decl.Revision, g.presentationID())
		return route, changed, err
	}
	changed, route, err := reconcilePortalRoutes(env, decl.Name, decl.InstanceID, decl.Revision, g.presentationID())
	return route, changed, err
}

func (g *portalGateway) portalRoute(env *platformv1alpha1.Environment, decl platformv1alpha1.EnvironmentServiceDeclaration, route platformv1alpha1.EnvironmentPortalRoute) PortalRoute {
	result := PortalRoute{Disabled: !g.enabled(), EnvironmentUID: string(env.UID), Service: decl.Name, Revision: decl.Revision, DeclarationInstanceID: decl.InstanceID, RouteGeneration: route.Generation}
	if g.enabled() {
		result.URL = fmt.Sprintf("%s://%s.%s", g.scheme, route.Locator, g.suffix)
	}
	return result
}

func (g *portalGateway) listRunPortals(w http.ResponseWriter, r *http.Request, namespace string, association RunTerminalAssociation) {
	key := types.NamespacedName{Namespace: namespace, Name: association.EnvironmentName}
	for attempt := 0; attempt < 5; attempt++ {
		var env platformv1alpha1.Environment
		if err := g.resolver.Get(r.Context(), key, &env); err != nil {
			g.server.writeResourceError(w, "get portal environment", namespace, association.EnvironmentName, err)
			return
		}
		if string(env.UID) != association.EnvironmentUID {
			writeRunTerminalAssociationConflict(w)
			return
		}
		if err := validateRunEnvironmentAssociation(r.Context(), g.resolver, namespace, &association, &env); err != nil {
			if errors.Is(err, errRunUIDConflict) || errors.Is(err, errRunTerminalAssociation) {
				writeRunTerminalAssociationConflict(w)
				return
			}
			g.server.writeResourceError(w, "validate Run portal association", namespace, association.RunName, err)
			return
		}
		declarations := append([]platformv1alpha1.EnvironmentServiceDeclaration(nil), env.Spec.Services...)
		sort.Slice(declarations, func(i, j int) bool { return declarations[i].Name < declarations[j].Name })
		result := RunPortalServiceList{Items: make([]RunPortalService, 0, len(declarations))}
		changed := false
		if g.enabled() {
			cleaned, _, err := reconcilePortalRoutes(&env, "", "", 0, g.presentationID())
			if err != nil {
				g.server.writeResourceError(w, "clean portal routes", namespace, env.Name, err)
				return
			}
			changed = cleaned
		}
		for _, declaration := range declarations {
			decl, eligible := exactPortalDeclaration(&env, declaration.Name)
			if !eligible {
				continue
			}
			if err := g.authorize(r, namespace, env.Name, decl.Name, &env); err != nil {
				if errors.Is(err, errForbidden) {
					continue
				}
				writeRESTAccessError(w, err)
				return
			}
			state, reason := portalServiceState(&env, decl, time.Now())
			item := RunPortalService{Name: decl.Name, TargetPort: decl.TargetPort, Status: state, Reason: reason}
			if g.enabled() {
				route, routeChanged, err := g.routeForDeclaration(&env, decl)
				if err != nil {
					g.server.writeResourceError(w, "generate portal route", namespace, env.Name, err)
					return
				}
				changed = changed || routeChanged
				discovered := g.portalRoute(&env, decl, route)
				item.URL = discovered.URL
				item.OpenURL = fmt.Sprintf("/api/v1/namespaces/%s/runs/%s/portals/%s/%s/%s/open", url.PathEscape(namespace), url.PathEscape(association.RunName), url.PathEscape(association.RunUID), url.PathEscape(association.EnvironmentUID), url.PathEscape(decl.Name))
			}
			result.Items = append(result.Items, item)
		}
		if changed {
			if err := g.resolver.Status().Update(r.Context(), &env); err != nil {
				if apierrors.IsConflict(err) {
					continue
				}
				g.server.writeResourceError(w, "update portal routes", namespace, env.Name, err)
				return
			}
		}
		if err := validateRunEnvironmentAssociation(r.Context(), g.resolver, namespace, &association, nil); err != nil {
			if errors.Is(err, errRunUIDConflict) || errors.Is(err, errRunTerminalAssociation) {
				writeRunTerminalAssociationConflict(w)
				return
			}
			g.server.writeResourceError(w, "revalidate Run portal association", namespace, association.RunName, err)
			return
		}
		w.Header().Set("Cache-Control", "private, no-store")
		writeJSON(w, http.StatusOK, result)
		return
	}
	writeProblem(w, http.StatusConflict, "conflict", "Conflict", "portal routes changed concurrently")
}

func portalServiceState(env *platformv1alpha1.Environment, declaration platformv1alpha1.EnvironmentServiceDeclaration, now time.Time) (string, string) {
	suspended := env.Spec.Paused || env.Status.Lifecycle.Suspended || env.Spec.Lifecycle.Hold != nil && env.Spec.Lifecycle.Hold.Enabled
	idleWakeable := env.DeletionTimestamp.IsZero() && !env.Spec.Paused && (env.Spec.Lifecycle.Hold == nil || !env.Spec.Lifecycle.Hold.Enabled) && env.Status.Lifecycle.Suspended && env.Status.Lifecycle.SuspensionReason == platformv1alpha1.EnvironmentSuspensionReasonIdle
	if !env.DeletionTimestamp.IsZero() || env.Spec.Paused || env.Spec.Lifecycle.Hold != nil && env.Spec.Lifecycle.Hold.Enabled || env.Status.Lifecycle.Suspended && !idleWakeable {
		return "Unavailable", "Environment policy prevents portal access"
	}
	if env.Status.ServiceObservations == nil {
		if idleWakeable {
			return "Paused", "Opening the portal wakes this idle Environment"
		}
		return "Stale", "No current service observation"
	}
	observations := env.Status.ServiceObservations
	for _, observation := range observations.Records {
		if observation.Name != declaration.Name {
			continue
		}
		classificationCurrent := false
		switch observation.State {
		case platformv1alpha1.EnvironmentServiceObservationPending:
			classificationCurrent = observations.ExecutionGeneration == nil && !suspended && !platformv1alpha1.IsEnvironmentReady(env)
		case platformv1alpha1.EnvironmentServiceObservationUnavailable:
			classificationCurrent = observations.ExecutionGeneration == nil && suspended
		default:
			classificationCurrent = observations.ExecutionGeneration != nil && *observations.ExecutionGeneration == env.Status.ExecutionGeneration && platformv1alpha1.IsEnvironmentReady(env) && !suspended
		}
		age := now.Sub(observations.ObservedAt.Time)
		current := env.DeletionTimestamp.IsZero() && observations.ObservedGeneration == env.Generation && observation.DeclarationRevision == declaration.Revision && observations.LifecycleEpoch == env.Status.Lifecycle.Epoch && observations.HoldRevision == lifecycle.HoldPolicyRevision(env) && classificationCurrent && age >= 0 && age <= 15*time.Second
		if !current {
			return "Stale", "Service observation is no longer current"
		}
		switch observation.State {
		case platformv1alpha1.EnvironmentServiceObservationHealthy:
			return "Ready", ""
		case platformv1alpha1.EnvironmentServiceObservationPending:
			return "Waking", "Environment is becoming ready"
		case platformv1alpha1.EnvironmentServiceObservationUnavailable:
			if idleWakeable {
				return "Paused", "Opening the portal wakes this idle Environment"
			}
			return "Unavailable", "Environment is suspended"
		case platformv1alpha1.EnvironmentServiceObservationUnhealthy:
			return "Unavailable", "Declared port is not accepting connections"
		default:
			return "Failed", "Service observation failed"
		}
	}
	return "Stale", "No observation for the current declaration"
}

func (g *portalGateway) openRunPortal(w http.ResponseWriter, r *http.Request, namespace string, association RunTerminalAssociation, service string) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" {
		writeProblem(w, http.StatusBadRequest, "browser-session-required", "Browser session required", "opening a portal from the console requires its current browser session")
		return
	}
	var env platformv1alpha1.Environment
	if err := g.resolver.Get(r.Context(), types.NamespacedName{Namespace: namespace, Name: association.EnvironmentName}, &env); err != nil {
		g.server.writeResourceError(w, "get portal environment", namespace, association.EnvironmentName, err)
		return
	}
	if err := validateRunEnvironmentAssociation(r.Context(), g.resolver, namespace, &association, &env); err != nil {
		writeRunTerminalAssociationConflict(w)
		return
	}
	decl, ok := exactPortalDeclaration(&env, service)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if err := g.authorize(r, namespace, env.Name, service, &env); err != nil {
		writeRESTAccessError(w, err)
		return
	}
	route, changed, err := g.routeForDeclaration(&env, decl)
	if err != nil || !g.enabled() {
		writeProblem(w, http.StatusServiceUnavailable, "portal-unavailable", "Portal unavailable", "the portal gateway is not configured")
		return
	}
	if changed {
		if err := g.resolver.Status().Update(r.Context(), &env); err != nil {
			g.server.writeResourceError(w, "update portal route", namespace, env.Name, err)
			return
		}
	}
	var current platformv1alpha1.Environment
	if err := g.resolver.Get(r.Context(), types.NamespacedName{Namespace: namespace, Name: association.EnvironmentName}, &current); err != nil {
		g.server.writeResourceError(w, "refresh portal environment", namespace, association.EnvironmentName, err)
		return
	}
	currentDeclaration, currentDeclarationOK := exactPortalDeclaration(&current, service)
	if err := validateRunEnvironmentAssociation(r.Context(), g.resolver, namespace, &association, &current); err != nil || !currentDeclarationOK || currentDeclaration.InstanceID != decl.InstanceID || currentDeclaration.Revision != decl.Revision || !activeExactRoute(&current, route) || g.authorize(r, namespace, current.Name, service, &current) != nil {
		writeRunTerminalAssociationConflict(w)
		return
	}
	code, err := randomLocator()
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "portal-handoff-unavailable", "Portal handoff unavailable", "a one-time browser handoff could not be created")
		return
	}
	g.handoffsMu.Lock()
	now := time.Now()
	if g.handoffs == nil {
		g.handoffs = make(map[string]portalSessionHandoff)
	}
	for key, handoff := range g.handoffs {
		if !now.Before(handoff.expiresAt) {
			delete(g.handoffs, key)
		}
	}
	if len(g.handoffs) >= 256 {
		g.handoffsMu.Unlock()
		writeProblem(w, http.StatusServiceUnavailable, "portal-handoff-capacity", "Portal handoff unavailable", "too many browser portal handoffs are active")
		return
	}
	g.handoffs[code] = portalSessionHandoff{sessionID: cookie.Value, locator: route.Locator, origin: r.Header.Get("Origin"), expiresAt: now.Add(30 * time.Second)}
	g.handoffsMu.Unlock()
	action := fmt.Sprintf("%s://%s.%s/.swe/session-handoff", g.scheme, route.Locator, g.suffix)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; form-action "+action+"; frame-ancestors 'none'")
	fmt.Fprintf(w, `<!doctype html><title>Opening portal</title><form method="post" action="%s"><input type="hidden" name="code" value="%s"><button>Continue to portal</button></form><script>document.forms[0].submit()</script>`, html.EscapeString(action), html.EscapeString(code))
}

func (g *portalGateway) consumeSessionHandoff(w http.ResponseWriter, r *http.Request, locator string) {
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery {
		rejectUnreadHandoff(w, r)
		return
	}
	mediaType, _, contentTypeErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if contentTypeErr != nil || mediaType != "application/x-www-form-urlencoded" {
		rejectUnreadHandoff(w, r)
		return
	}
	select {
	case g.handoffReads <- struct{}{}:
		defer func() { <-g.handoffReads }()
	default:
		rejectUnreadHandoff(w, r)
		return
	}
	readTimeout := g.handoffReadTimeout
	if readTimeout <= 0 {
		readTimeout = portalSessionHandoffReadTimeout
	}
	controller := http.NewResponseController(w)
	if err := controller.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		panic(http.ErrAbortHandler)
	}
	defer func() { _ = controller.SetReadDeadline(time.Time{}) }()
	r.Body = http.MaxBytesReader(w, r.Body, portalSessionHandoffBodyLimit)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		portal404(w)
		return
	}
	form, err := url.ParseQuery(string(body))
	if err != nil || len(form) != 1 || len(form["code"]) != 1 {
		portal404(w)
		return
	}
	code := form["code"][0]
	if len(code) != portalSessionHandoffCodeLength || !validLocator(code) || string(body) != "code="+code {
		portal404(w)
		return
	}
	g.handoffsMu.Lock()
	handoff, ok := g.handoffs[code]
	delete(g.handoffs, code)
	g.handoffsMu.Unlock()
	origins := r.Header.Values("Origin")
	if !ok || handoff.locator != locator || !time.Now().Before(handoff.expiresAt) || len(origins) != 1 || origins[0] != handoff.origin {
		portal404(w)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Value: handoff.sessionID, Path: "/", HttpOnly: true, Secure: !g.server.allowInsecureSessions, SameSite: http.SameSiteStrictMode})
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; frame-ancestors 'none'")
	_, _ = io.WriteString(w, `<!doctype html><title>Portal ready</title><p><a href="/">Continue to portal</a></p><script>location.replace("/")</script>`)
}

func rejectUnreadHandoff(w http.ResponseWriter, r *http.Request) {
	// Prevent net/http from draining an unread HTTP/1 request body before
	// writing the rejection, and bound its post-handler close attempt.
	if r.ProtoMajor == 1 {
		w.Header().Set("Connection", "close")
	}
	if err := http.NewResponseController(w).SetReadDeadline(time.Now()); err != nil {
		panic(http.ErrAbortHandler)
	}
	portal404(w)
}

func reconcileDisabledPortalRoute(env *platformv1alpha1.Environment, want, instanceID string, revision int64, presentationID string) (bool, platformv1alpha1.EnvironmentPortalRoute, error) {
	changed := false
	for i := range env.Status.PortalRoutes {
		route := &env.Status.PortalRoutes[i]
		if route.Active {
			route.Active = false
			changed = true
		}
	}
	if !changed {
		var denial platformv1alpha1.EnvironmentPortalRoute
		latestGeneration := int64(0)
		for i := range env.Status.PortalRoutes {
			route := &env.Status.PortalRoutes[i]
			if route.Name != want {
				continue
			}
			if route.Generation > latestGeneration {
				latestGeneration = route.Generation
			}
			if !route.Active && route.PresentationID == presentationID && route.DeclarationInstanceID == instanceID && route.DeclarationRevision == revision && route.Generation > denial.Generation {
				denial = *route
			}
		}
		if denial.Generation > 0 && denial.Generation >= latestGeneration {
			return false, denial, nil
		}
	}
	locator, err := randomLocator()
	if err != nil {
		return false, platformv1alpha1.EnvironmentPortalRoute{}, err
	}
	env.Status.NextPortalRouteGeneration++
	route := platformv1alpha1.EnvironmentPortalRoute{Name: want, PresentationID: presentationID, DeclarationInstanceID: instanceID, DeclarationRevision: revision, Locator: locator, Generation: env.Status.NextPortalRouteGeneration, Active: false}
	env.Status.PortalRoutes = append(env.Status.PortalRoutes, route)
	for len(env.Status.PortalRoutes) > 64 {
		idx := -1
		for i := range env.Status.PortalRoutes[:len(env.Status.PortalRoutes)-1] {
			if !env.Status.PortalRoutes[i].Active {
				idx = i
				break
			}
		}
		if idx < 0 {
			break
		}
		env.Status.PortalRoutes = append(env.Status.PortalRoutes[:idx], env.Status.PortalRoutes[idx+1:]...)
	}
	return true, route, nil
}

func exactPortalDeclaration(env *platformv1alpha1.Environment, name string) (platformv1alpha1.EnvironmentServiceDeclaration, bool) {
	var d *platformv1alpha1.EnvironmentServiceDeclaration
	for i := range env.Spec.Services {
		x := &env.Spec.Services[i]
		if x.Name == name {
			if d != nil {
				return platformv1alpha1.EnvironmentServiceDeclaration{}, false
			}
			d = x
		}
	}
	returnValue := platformv1alpha1.EnvironmentServiceDeclaration{}
	if d == nil || !validLocator(d.InstanceID) || d.Protocol != platformv1alpha1.EnvironmentServiceProtocolHTTP || d.Visibility != platformv1alpha1.EnvironmentServiceVisibilityProject {
		return returnValue, false
	}
	return *d, true
}

func reconcilePortalRoutes(env *platformv1alpha1.Environment, want, instanceID string, revision int64, presentationID string) (bool, platformv1alpha1.EnvironmentPortalRoute, error) {
	changed := false
	var current *platformv1alpha1.EnvironmentPortalRoute
	declarations := map[string]platformv1alpha1.EnvironmentServiceDeclaration{}
	for _, d := range env.Spec.Services {
		if d.Protocol == platformv1alpha1.EnvironmentServiceProtocolHTTP && d.Visibility == platformv1alpha1.EnvironmentServiceVisibilityProject {
			declarations[d.Name] = d
		}
	}
	for i := range env.Status.PortalRoutes {
		r := &env.Status.PortalRoutes[i]
		d, exists := declarations[r.Name]
		if r.Active && (!exists || d.InstanceID != r.DeclarationInstanceID || d.Revision != r.DeclarationRevision || r.PresentationID != presentationID) {
			r.Active = false
			changed = true
		}
		if r.Active && r.Name == want && r.DeclarationInstanceID == instanceID && r.DeclarationRevision == revision {
			current = r
		}
	}
	if current != nil {
		return changed, *current, nil
	}
	if want == "" {
		return changed, platformv1alpha1.EnvironmentPortalRoute{}, nil
	}
	loc, err := randomLocator()
	if err != nil {
		return false, platformv1alpha1.EnvironmentPortalRoute{}, err
	}
	env.Status.NextPortalRouteGeneration++
	route := platformv1alpha1.EnvironmentPortalRoute{Name: want, PresentationID: presentationID, DeclarationInstanceID: instanceID, DeclarationRevision: revision, Locator: loc, Generation: env.Status.NextPortalRouteGeneration, Active: true}
	env.Status.PortalRoutes = append(env.Status.PortalRoutes, route)
	changed = true
	for len(env.Status.PortalRoutes) > 64 {
		idx := -1
		for i, r := range env.Status.PortalRoutes {
			if !r.Active {
				idx = i
				break
			}
		}
		if idx < 0 {
			break
		}
		env.Status.PortalRoutes = append(env.Status.PortalRoutes[:idx], env.Status.PortalRoutes[idx+1:]...)
	}
	return changed, route, nil
}
func (g *portalGateway) presentationID() string {
	sum := sha256.Sum256([]byte(g.scheme + "\x00" + g.suffix))
	return fmt.Sprintf("%x", sum[:8])
}
func randomLocator() (string, error) {
	b := make([]byte, 20)
	if _, e := rand.Read(b); e != nil {
		return "", fmt.Errorf("generate portal locator: %w", e)
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)), nil
}

func (g *portalGateway) authorize(r *http.Request, ns, env, service string, environment *platformv1alpha1.Environment) error {
	pa, ok := g.server.access.(principalAccessController)
	if !ok {
		return fmt.Errorf("portal principal authorization unavailable")
	}
	principal, err := pa.AuthorizePrincipal(r, ResourceAccess{Namespace: ns, Verb: "get", Resource: "environments", Subresource: "portal", Name: env}, true)
	if err != nil {
		return err
	}
	if principal == "bootstrap" {
		return errForbidden
	}
	_, err = pa.AuthorizePrincipal(r, ResourceAccess{Namespace: ns, Verb: "get", Resource: "environmentservices", Subresource: "portal", Name: env + "." + service}, true)
	if err != nil {
		return err
	}
	if environment == nil {
		return nil
	}
	return g.authorizeAssociation(r, environment, pa)
}

func (g *portalGateway) authorizeAssociation(r *http.Request, env *platformv1alpha1.Environment, pa principalAccessController) error {
	var ref *platformv1alpha1.RunReference
	if env.Status.ClaimedBy != nil {
		ref = env.Status.ClaimedBy
	} else {
		for _, owner := range env.OwnerReferences {
			if owner.APIVersion == platformv1alpha1.GroupVersion.String() && owner.Kind == "Run" && owner.Controller != nil && *owner.Controller {
				if ref != nil {
					return errForbidden
				}
				ref = &platformv1alpha1.RunReference{Name: owner.Name, UID: owner.UID}
			}
		}
	}
	if ref != nil {
		var run platformv1alpha1.Run
		if ref.Name == "" || ref.UID == "" || g.resolver.Get(r.Context(), types.NamespacedName{Namespace: env.Namespace, Name: ref.Name}, &run) != nil || run.UID != ref.UID || terminalRunState(run.Status.State) || !runOwnsOrClaimsEnvironment(&run, env) {
			return errForbidden
		}
		_, err := pa.AuthorizePrincipal(r, ResourceAccess{Namespace: env.Namespace, Verb: "get", Resource: "runs", Name: run.Name}, true)
		return err
	}
	if env.Spec.ProjectRef == "" {
		return errForbidden
	}
	var project platformv1alpha1.Project
	if err := g.resolver.Get(r.Context(), types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.ProjectRef}, &project); err != nil {
		return errForbidden
	}
	_, err := pa.AuthorizePrincipal(r, ResourceAccess{Namespace: env.Namespace, Verb: "get", Resource: "projects", Name: env.Spec.ProjectRef}, true)
	return err
}
func terminalRunState(s platformv1alpha1.RunState) bool {
	return s == platformv1alpha1.RunStateSucceeded || s == platformv1alpha1.RunStateFailed || s == platformv1alpha1.RunStateCancelled
}

func (g *portalGateway) serveLocator(w http.ResponseWriter, r *http.Request, locator string) {
	authenticator, ok := g.server.access.(principalAuthenticator)
	if !ok {
		portal404(w)
		return
	}
	principal, err := authenticator.AuthenticatePrincipal(r, true)
	if err != nil || principal == "bootstrap" {
		portal404(w)
		return
	}
	select {
	case g.requests <- struct{}{}:
		defer func() { <-g.requests }()
	default:
		portal404(w)
		return
	}
	r, cancelStream := g.server.withStreamLifecycle(r)
	defer cancelStream()
	ctx, cancel := context.WithTimeout(r.Context(), portalAdmissionTimeout)
	defer cancel()
	lifetimeCtx := r.Context()
	authRequest := r.Clone(ctx)
	items, err := g.enumerator.ListPortalEnvironments(ctx)
	if err != nil {
		portal404(w)
		return
	}
	var found *platformv1alpha1.Environment
	var route platformv1alpha1.EnvironmentPortalRoute
	for i := range items {
		env := &items[i]
		for _, x := range env.Status.PortalRoutes {
			if x.Active && x.Locator == locator {
				if found != nil {
					portal404(w)
					return
				}
				found = env.DeepCopy()
				route = x
			}
		}
	}
	if found == nil {
		portal404(w)
		return
	}
	var env platformv1alpha1.Environment
	if g.resolver.Get(ctx, client.ObjectKeyFromObject(found), &env) != nil || env.UID != found.UID {
		portal404(w)
		return
	}
	decl, ok := exactPortalDeclaration(&env, route.Name)
	if !ok || decl.InstanceID != route.DeclarationInstanceID || decl.Revision != route.DeclarationRevision || route.PresentationID != g.presentationID() {
		g.tombstoneStaleRoute(ctx, client.ObjectKeyFromObject(&env), env.UID, route)
		portal404(w)
		return
	}
	if g.authorize(authRequest, env.Namespace, env.Name, route.Name, &env) != nil || portalPolicyError(&env) != nil {
		portal404(w)
		return
	}
	if r.Header.Get("Authorization") == "" && (r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions || strings.EqualFold(r.Header.Get("Upgrade"), "websocket")) && !g.server.sameOrigin(r) {
		portal404(w)
		return
	}
	nextActivity := time.Time{}
	lastActivityFence := lifecycle.ExecutionFence{}
	recordActivity := func() error {
		return g.recordAdmissionActivity(ctx, &env, &nextActivity, &lastActivityFence)
	}
	if recordActivity() != nil {
		portal404(w)
		return
	}
	woke := false
	if env.Status.Lifecycle.Suspended {
		woke = true
		requestID := fmt.Sprintf("portal/wake/%d", time.Now().UnixNano())
		if err := lifecycle.RequestWake(ctx, g.resolver, client.ObjectKeyFromObject(&env), env.UID, lifecycle.HoldPolicyRevision(&env), requestID); err != nil {
			portal404(w)
			return
		}
		for {
			select {
			case <-ctx.Done():
				portal404(w)
				return
			case <-time.After(250 * time.Millisecond):
			}
			if g.resolver.Get(ctx, client.ObjectKeyFromObject(&env), &env) != nil || portalPolicyError(&env) != nil {
				portal404(w)
				return
			}
			if err := recordActivity(); err != nil {
				if errors.Is(err, lifecycle.ErrExecutionFenceChanged) {
					continue
				}
				portal404(w)
				return
			}
			if platformv1alpha1.IsEnvironmentReady(&env) {
				break
			}
		}
	}
	if !platformv1alpha1.IsEnvironmentReady(&env) {
		portal404(w)
		return
	}
	if err := recordActivity(); err != nil {
		portal404(w)
		return
	}
	// Re-fetch and reauthorize immediately before every proof-bearing dial.
	// A wake may make the Environment Ready before its declared listener has
	// restarted, so keep the bounded request queued until a fresh exact probe
	// and tunnel proof succeeds. Active-environment misses fail immediately.
	var (
		conn     net.Conn
		fence    lifecycle.ExecutionFence
		snapshot sandboxclient.ServiceDeclarationSnapshot
	)
	for {
		if g.resolver.Get(ctx, client.ObjectKeyFromObject(&env), &env) != nil || g.authorize(authRequest, env.Namespace, env.Name, route.Name, &env) != nil || !platformv1alpha1.IsEnvironmentReady(&env) || portalPolicyError(&env) != nil {
			portal404(w)
			return
		}
		if err := recordActivity(); err != nil {
			if woke && errors.Is(err, lifecycle.ErrExecutionFenceChanged) {
				select {
				case <-ctx.Done():
					portal404(w)
					return
				case <-time.After(250 * time.Millisecond):
				}
				continue
			}
			portal404(w)
			return
		}
		decl, ok = exactPortalDeclaration(&env, route.Name)
		if !ok || decl.InstanceID != route.DeclarationInstanceID || decl.Revision != route.DeclarationRevision || !activeExactRoute(&env, route) {
			portal404(w)
			return
		}
		fence, snapshot = lifecycle.CaptureExecutionFence(&env), sandboxclient.CaptureServiceDeclarationSnapshot(&env)
		conn, err = g.dialer.DialPortal(ctx, lifetimeCtx, fence, snapshot, route.Name)
		if err == nil {
			break
		}
		if !woke {
			portal404(w)
			return
		}
		select {
		case <-ctx.Done():
			portal404(w)
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
	var post platformv1alpha1.Environment
	postDecl, postRouteOK := platformv1alpha1.EnvironmentServiceDeclaration{}, false
	if g.resolver.Get(ctx, client.ObjectKeyFromObject(&env), &post) == nil {
		postDecl, postRouteOK = exactPortalDeclaration(&post, route.Name)
	}
	if fence.Validate(&post) != nil || !snapshot.Matches(&post) || !platformv1alpha1.IsEnvironmentReady(&post) || portalPolicyError(&post) != nil || !post.DeletionTimestamp.IsZero() || !postRouteOK || postDecl.InstanceID != route.DeclarationInstanceID || postDecl.Revision != route.DeclarationRevision || !activeExactRoute(&post, route) || g.authorize(authRequest, post.Namespace, post.Name, route.Name, &post) != nil {
		_ = conn.Close()
		portal404(w)
		return
	}
	defer conn.Close()
	leaseCtx, cancelLease := context.WithCancel(lifetimeCtx)
	defer cancelLease()
	leaseRequest := r.Clone(lifetimeCtx)
	proxyRequest := r.Clone(lifetimeCtx)
	go g.revalidatePortalLease(leaseCtx, leaseRequest, conn, client.ObjectKeyFromObject(&post), fence, snapshot, route)
	g.proxy(w, proxyRequest, conn)
}

// tombstoneStaleRoute performs gateway-owned denial cleanup after a locator
// resolves to a declaration that no longer exists or no longer matches. The
// request remains a uniform 404 regardless of cleanup success.
func (g *portalGateway) tombstoneStaleRoute(ctx context.Context, key types.NamespacedName, uid types.UID, stale platformv1alpha1.EnvironmentPortalRoute) {
	for attempt := 0; attempt < 5; attempt++ {
		var current platformv1alpha1.Environment
		if g.resolver.Get(ctx, key, &current) != nil || current.UID != uid {
			return
		}
		found := false
		for i := range current.Status.PortalRoutes {
			route := &current.Status.PortalRoutes[i]
			if route.Active && route.Generation == stale.Generation && route.Locator == stale.Locator {
				route.Active = false
				found = true
				break
			}
		}
		if !found {
			return
		}
		if err := g.resolver.Status().Update(ctx, &current); apierrors.IsConflict(err) {
			continue
		}
		return
	}
}

func (g *portalGateway) recordAdmissionActivity(ctx context.Context, env *platformv1alpha1.Environment, next *time.Time, previousFence *lifecycle.ExecutionFence) error {
	now := time.Now()
	fence := lifecycle.CaptureExecutionFence(env)
	if fence != *previousFence {
		*next = time.Time{}
	}
	if now.Before(*next) {
		return nil
	}
	if err := lifecycle.RecordActivity(ctx, g.resolver, fence, platformv1alpha1.EnvironmentActivitySourcePortal, fmt.Sprintf("portal/admission/%d", now.UnixNano())); err != nil {
		*next = time.Time{}
		return err
	}
	interval := g.admissionHeartbeat
	if interval <= 0 {
		interval = portalAdmissionHeartbeat
	}
	*previousFence = fence
	*next = now.Add(interval)
	return nil
}

func (g *portalGateway) revalidatePortalLease(ctx context.Context, request *http.Request, conn net.Conn, key types.NamespacedName, fence lifecycle.ExecutionFence, snapshot sandboxclient.ServiceDeclarationSnapshot, route platformv1alpha1.EnvironmentPortalRoute) {
	interval := g.leasePoll
	if interval <= 0 {
		interval = portalLeasePollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var env platformv1alpha1.Environment
			authRequest := request.Clone(ctx)
			if g.resolver.Get(ctx, key, &env) != nil || fence.Validate(&env) != nil || !snapshot.Matches(&env) || portalPolicyError(&env) != nil || !env.DeletionTimestamp.IsZero() || !activeExactRoute(&env, route) || g.authorize(authRequest, env.Namespace, env.Name, route.Name, &env) != nil || lifecycle.RecordActivity(ctx, g.resolver, fence, platformv1alpha1.EnvironmentActivitySourcePortal, fmt.Sprintf("portal/lease/%d", time.Now().UnixNano())) != nil {
				_ = conn.Close()
				return
			}
		}
	}
}

func activeExactRoute(env *platformv1alpha1.Environment, want platformv1alpha1.EnvironmentPortalRoute) bool {
	for _, route := range env.Status.PortalRoutes {
		if route == want && route.Active {
			return true
		}
	}
	return false
}
func portalPolicyError(env *platformv1alpha1.Environment) error {
	if env.Spec.Paused || env.Spec.Lifecycle.Hold != nil && env.Spec.Lifecycle.Hold.Enabled {
		return errors.New("held")
	}
	if env.Status.Lifecycle.Suspended && env.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonIdle {
		return errors.New("suspended")
	}
	return nil
}

func (g *portalGateway) proxy(w http.ResponseWriter, r *http.Request, conn net.Conn) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
	_, externalHost, originOK := effectiveRequestOrigin(r, g.server.trustProxy)
	if !originOK {
		_ = conn.Close()
		portal404(w)
		return
	}
	upgrade := r.Header.Get("Upgrade")
	stripHopHeaders(r.Header)
	if upgrade != "" {
		r.Header.Set("Connection", "Upgrade")
		r.Header.Set("Upgrade", upgrade)
	}
	r.Header.Del("Authorization")
	for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Forwarded-Port"} {
		r.Header.Del(name)
	}
	filterRequestCookies(r)
	r.Header.Set("X-Forwarded-Host", externalHost)
	r.Header.Set("X-Forwarded-Proto", g.scheme)
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		r.Header.Set("X-Forwarded-For", ip)
	}
	used := false
	tr := &http.Transport{DisableKeepAlives: true, ResponseHeaderTimeout: 30 * time.Second, DialContext: func(context.Context, string, string) (net.Conn, error) {
		if used {
			return nil, errors.New("portal connection already used")
		}
		used = true
		return conn, nil
	}}
	p := &httputil.ReverseProxy{Director: func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = "portal.invalid"
		req.Host = "portal.invalid"
	}, Transport: tr, FlushInterval: -1, ModifyResponse: func(resp *http.Response) error {
		upgrade := resp.Header.Get("Upgrade")
		connectionUpgrade := headerHasToken(resp.Header, "Connection", "upgrade")
		stripHopHeaders(resp.Header)
		if resp.StatusCode == http.StatusSwitchingProtocols && connectionUpgrade && strings.EqualFold(upgrade, r.Header.Get("Upgrade")) {
			resp.Header.Set("Connection", "Upgrade")
			resp.Header.Set("Upgrade", upgrade)
		}
		filterResponseCookies(resp.Header)
		if resp.StatusCode != http.StatusSwitchingProtocols {
			if resp.ContentLength > maxPortalResponseBytes {
				return errors.New("portal response body too large")
			}
			resp.Body = &boundedPortalBody{ReadCloser: resp.Body, remaining: maxPortalResponseBytes}
		}
		return nil
	}, ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) { portal404(w) }}
	p.ServeHTTP(w, r)
}

type boundedPortalBody struct {
	io.ReadCloser
	remaining int64
}

func (b *boundedPortalBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		var one [1]byte
		n, err := b.ReadCloser.Read(one[:])
		if n != 0 {
			return 0, errors.New("portal response body too large")
		}
		return 0, err
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}
func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, x := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(x), token) {
				return true
			}
		}
	}
	return false
}
func filterRequestCookies(r *http.Request) {
	cookies := r.Cookies()
	r.Header.Del("Cookie")
	for _, c := range cookies {
		if c.Name != sessionCookieName {
			r.AddCookie(c)
		}
	}
}
func filterResponseCookies(h http.Header) {
	values := h.Values("Set-Cookie")
	h.Del("Set-Cookie")
	for _, value := range values {
		c, err := http.ParseSetCookie(value)
		if err == nil && c.Name != sessionCookieName && c.Domain == "" {
			h.Add("Set-Cookie", value)
		}
	}
}
func stripHopHeaders(h http.Header) {
	for _, v := range h.Values("Connection") {
		for _, n := range strings.Split(v, ",") {
			h.Del(strings.TrimSpace(n))
		}
	}
	for _, n := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding"} {
		h.Del(n)
	}
}
