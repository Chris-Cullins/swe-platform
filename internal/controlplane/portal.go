package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
)

const portalNotFoundBody = "not found\n"
const (
	maxPortalResponseBytes  int64 = 64 << 20
	portalAdmissionTimeout        = 2 * time.Minute
	portalLeasePollInterval       = 2 * time.Second
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
	server         *Server
	resolver       PortalResolver
	enumerator     PortalEnvironmentEnumerator
	dialer         PortalDialer
	suffix, scheme string
	requests       chan struct{}
	leasePoll      time.Duration
}

type PortalRoute struct {
	URL            string `json:"url"`
	EnvironmentUID string `json:"environmentUID"`
	Service        string `json:"service"`
	Revision       int64  `json:"revision"`
}

func newPortalGateway(s *Server, o ServerOptions) *portalGateway {
	suffix := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(o.PortalSuffix)), ".")
	scheme := strings.ToLower(strings.TrimSpace(o.PortalScheme))
	if suffix == "" || o.PortalResolver == nil || o.PortalDialer == nil {
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
	return &portalGateway{server: s, resolver: o.PortalResolver, enumerator: o.PortalEnvironmentEnumerator, dialer: o.PortalDialer, suffix: suffix, scheme: scheme, requests: make(chan struct{}, 64)}
}

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
		changed, route, err := reconcilePortalRoutes(&env, service, decl.InstanceID, decl.Revision)
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
		writeJSON(w, http.StatusOK, PortalRoute{URL: fmt.Sprintf("%s://%s.%s", g.scheme, route.Locator, g.suffix), EnvironmentUID: string(env.UID), Service: service, Revision: decl.Revision})
		return
	}
	writeProblem(w, 409, "conflict", "Conflict", "portal route changed concurrently")
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
	if d == nil || d.Protocol != platformv1alpha1.EnvironmentServiceProtocolHTTP || d.Visibility != platformv1alpha1.EnvironmentServiceVisibilityProject {
		return returnValue, false
	}
	return *d, true
}

func reconcilePortalRoutes(env *platformv1alpha1.Environment, want, instanceID string, revision int64) (bool, platformv1alpha1.EnvironmentPortalRoute, error) {
	changed := false
	declarations := map[string]platformv1alpha1.EnvironmentServiceDeclaration{}
	for _, d := range env.Spec.Services {
		if d.Protocol == platformv1alpha1.EnvironmentServiceProtocolHTTP && d.Visibility == platformv1alpha1.EnvironmentServiceVisibilityProject {
			declarations[d.Name] = d
		}
	}
	for i := range env.Status.PortalRoutes {
		r := &env.Status.PortalRoutes[i]
		d, exists := declarations[r.Name]
		if r.Active && (!exists || d.InstanceID != r.DeclarationInstanceID || d.Revision != r.DeclarationRevision) {
			r.Active = false
			changed = true
		}
		if r.Active && r.Name == want && r.DeclarationInstanceID == instanceID && r.DeclarationRevision == revision {
			return changed, *r, nil
		}
	}
	if want == "" {
		return changed, platformv1alpha1.EnvironmentPortalRoute{}, nil
	}
	loc, err := randomLocator()
	if err != nil {
		return false, platformv1alpha1.EnvironmentPortalRoute{}, err
	}
	env.Status.NextPortalRouteGeneration++
	route := platformv1alpha1.EnvironmentPortalRoute{Name: want, DeclarationInstanceID: instanceID, DeclarationRevision: revision, Locator: loc, Generation: env.Status.NextPortalRouteGeneration, Active: true}
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
	select {
	case g.requests <- struct{}{}:
		defer func() { <-g.requests }()
	default:
		portal404(w)
		return
	}
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
	if !ok || decl.InstanceID != route.DeclarationInstanceID || decl.Revision != route.DeclarationRevision || g.authorize(authRequest, env.Namespace, env.Name, route.Name, &env) != nil || portalPolicyError(&env) != nil {
		portal404(w)
		return
	}
	if r.Header.Get("Authorization") == "" && (r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions || strings.EqualFold(r.Header.Get("Upgrade"), "websocket")) && !g.server.sameOrigin(r) {
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
		for !platformv1alpha1.IsEnvironmentReady(&env) {
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
		}
	}
	if !platformv1alpha1.IsEnvironmentReady(&env) {
		portal404(w)
		return
	}
	if err := lifecycle.RecordActivity(ctx, g.resolver, lifecycle.CaptureExecutionFence(&env), platformv1alpha1.EnvironmentActivitySourcePortal, fmt.Sprintf("portal/%d", time.Now().UnixNano())); err != nil {
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
	leaseCtx, cancelLease := context.WithCancel(lifetimeCtx)
	defer cancelLease()
	go g.revalidatePortalLease(leaseCtx, r, conn, client.ObjectKeyFromObject(&post), fence, snapshot, route)
	g.proxy(w, r.WithContext(lifetimeCtx), conn)
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
