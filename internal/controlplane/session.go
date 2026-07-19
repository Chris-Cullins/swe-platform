package controlplane

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	if s.sessions == nil {
		writeProblem(w, http.StatusServiceUnavailable, "session-unavailable", "Session service unavailable", "browser session exchange is not configured")
		return
	}
	switch r.Method {
	case http.MethodPost:
		s.createSession(w, r)
	case http.MethodGet:
		s.getSession(w, r)
	case http.MethodDelete:
		s.deleteSession(w, r)
	default:
		w.Header().Set("Allow", "POST, GET, DELETE")
		writeProblem(w, http.StatusMethodNotAllowed, "method-not-allowed", "Method not allowed", "session supports POST, GET, and DELETE")
	}
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	scheme, _, originOK := effectiveRequestOrigin(r, s.trustProxy)
	if !originOK {
		writeProblem(w, http.StatusBadRequest, "invalid-forwarded-origin", "Invalid forwarded origin", "trusted forwarded host and protocol headers must be single-valued and valid")
		return
	}
	if !s.allowInsecureSessions && scheme != "https" {
		writeProblem(w, http.StatusBadRequest, "https-required", "HTTPS required", "browser sessions may only be created over HTTPS")
		return
	}
	session, sessionID, err := s.sessions.CreateSession(r)
	if err != nil {
		writeRESTAccessError(w, err)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		Secure:   !s.allowInsecureSessions,
		SameSite: http.SameSiteStrictMode,
	})
	writeJSON(w, http.StatusOK, session)
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	session, err := s.sessions.CurrentSession(r)
	if err != nil {
		writeRESTAccessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) {
	if _, err := r.Cookie(sessionCookieName); err == nil && !s.sameOrigin(r) {
		writeProblem(w, http.StatusForbidden, "invalid-origin", "Invalid origin", "cookie-authenticated mutations require an exact same-origin Origin header")
		return
	}
	s.sessions.DeleteSession(r)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   !s.allowInsecureSessions,
		SameSite: http.SameSiteStrictMode,
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) requireMutationOrigin(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Authorization") != "" {
		return true
	}
	if s.sameOrigin(r) {
		return true
	}
	writeProblem(w, http.StatusForbidden, "invalid-origin", "Invalid origin", "cookie-authenticated mutations require an exact same-origin Origin header")
	return false
}

func (s *Server) sameOrigin(r *http.Request) bool {
	origins := r.Header.Values("Origin")
	if len(origins) != 1 || strings.Contains(origins[0], ",") {
		return false
	}
	origin := origins[0]
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	scheme, host, ok := effectiveRequestOrigin(r, s.trustProxy)
	return ok && parsed.Scheme == scheme && strings.EqualFold(parsed.Host, host)
}

func effectiveRequestOrigin(r *http.Request, trustProxy bool) (scheme, host string, ok bool) {
	scheme = "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host = r.Host
	if trustProxy {
		forwardedHosts := r.Header.Values("X-Forwarded-Host")
		forwardedProtos := r.Header.Values("X-Forwarded-Proto")
		if len(forwardedHosts) != 0 || len(forwardedProtos) != 0 {
			if len(forwardedHosts) != 1 || len(forwardedProtos) != 1 || strings.Contains(forwardedHosts[0], ",") || strings.Contains(forwardedProtos[0], ",") {
				return "", "", false
			}
			host = strings.TrimSpace(forwardedHosts[0])
			scheme = strings.ToLower(strings.TrimSpace(forwardedProtos[0]))
			if host == "" || (scheme != "http" && scheme != "https") {
				return "", "", false
			}
		}
	}
	return scheme, host, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeProblem(w http.ResponseWriter, status int, code, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Problem{
		Type:   "https://swe-platform.dev/problems/" + code,
		Title:  title,
		Status: status,
		Detail: detail,
	})
}
