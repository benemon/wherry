package view

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed static
var static embed.FS

// ServeOptions configure Serve.
type ServeOptions struct {
	// Port is the loopback port, 0 for a free one.
	Port int
	// ExternalURL is the origin a reverse proxy serves the view at, whose Host and Origin are then accepted and whose
	// scheme decides the cookie's Secure; empty for none. A URL with a path, query or fragment is refused.
	ExternalURL string
	Logger      *log.Logger
	// WriteDeadline bounds each write to the stream; a client that does not read within it is dropped. KeepAlive is
	// the interval of the stream's comments. The defaults are 10 and 15 seconds.
	WriteDeadline, KeepAlive time.Duration
	// SessionRate is how many POST /session a minute are answered; the default is 10.
	SessionRate int
}

// Server serves a view on 127.0.0.1 and nothing else: the static page, one bounded POST /session that exchanges the
// printed token for a session cookie, read-only JSON and one Server-Sent Events stream.
type Server struct {
	v        *View
	opts     ServeOptions
	listener net.Listener
	server   *http.Server
	port     int
	token    string
	cookie   string
	external *url.URL

	mu       sync.Mutex
	sessions map[string]time.Time
	// bucket is the session endpoint's token bucket.
	bucket   float64
	refilled time.Time
	done     chan struct{}
	err      error
}

// The fixed error texts; nothing of a request reaches a response.
const (
	textForbidden   = "forbidden"
	textNotFound    = "not found"
	textBadRequest  = "bad request"
	textTooMany     = "too many requests"
	textBusy        = "busy"
	textUnavailable = "unavailable"
	textTooLarge    = "response over its bound"
)

// csp is the page's Content-Security-Policy: assets from the embedded files alone, no inline script.
const csp = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'"

// sessionBodyLimit bounds a POST /session body.
const sessionBodyLimit = 1024

// runIDPattern is the grammar of a run ID in a path.
var runIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// Serve listens on 127.0.0.1 at opts.Port and serves v until Close. It returns once it listens; the token is in URL.
func Serve(v *View, opts ServeOptions) (*Server, error) {
	if opts.Logger == nil {
		opts.Logger = log.New(io.Discard, "", 0)
	}
	if opts.WriteDeadline <= 0 {
		opts.WriteDeadline = 10 * time.Second
	}
	if opts.KeepAlive <= 0 {
		opts.KeepAlive = 15 * time.Second
	}
	if opts.SessionRate <= 0 {
		opts.SessionRate = 10
	}
	s := &Server{v: v, opts: opts, sessions: map[string]time.Time{}, bucket: float64(opts.SessionRate), refilled: time.Now(), done: make(chan struct{})}
	if opts.ExternalURL != "" {
		external, err := url.Parse(opts.ExternalURL)
		if err != nil || external.Scheme != "http" && external.Scheme != "https" || external.Host == "" || external.Path != "" && external.Path != "/" || external.RawQuery != "" || external.Fragment != "" || external.User != nil {
			return nil, errors.New("--external-url must be an http or https origin with no path, query or fragment")
		}
		external.Path = ""
		s.external = external
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	s.token = hex.EncodeToString(token)
	// The address is fixed: no flag names another.
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(opts.Port)))
	if err != nil {
		return nil, err
	}
	s.listener = listener
	s.port = listener.Addr().(*net.TCPAddr).Port
	s.cookie = "wherry-view-" + strconv.Itoa(s.port)
	mux := http.NewServeMux()
	mux.Handle("GET /{$}", s.guard(s.static("index.html", "text/html; charset=utf-8"), false))
	mux.Handle("GET /app.js", s.guard(s.static("app.js", "text/javascript; charset=utf-8"), false))
	mux.Handle("GET /app.css", s.guard(s.static("app.css", "text/css; charset=utf-8"), false))
	mux.Handle("POST /session", s.guard(http.HandlerFunc(s.session), false))
	mux.Handle("GET /api/model", s.guard(http.HandlerFunc(s.model), true))
	mux.Handle("GET /api/runs/{id}", s.guard(http.HandlerFunc(s.run), true))
	mux.Handle("GET /api/runs/{id}/trace", s.guard(http.HandlerFunc(s.trace), true))
	mux.Handle("GET /api/stream", s.guard(http.HandlerFunc(s.stream), true))
	mux.Handle("/", s.guard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.fail(w, http.StatusNotFound, textNotFound)
		s.log(r, http.StatusNotFound)
	}), false))
	// WriteTimeout stays 0 for the stream, whose writes carry their own deadline.
	s.server = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, ErrorLog: log.New(io.Discard, "", 0)}
	go func() {
		s.err = s.server.Serve(listener)
		close(s.done)
	}()
	return s, nil
}

// Port is the port the server listens on.
func (s *Server) Port() int { return s.port }

// Addr is the listener's address.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// URL is the address to open, with the token in the fragment, which browsers never send.
func (s *Server) URL() string {
	if s.external != nil {
		return s.external.String() + "/#t=" + s.token
	}
	return "http://" + s.Addr() + "/#t=" + s.token
}

// Close stops serving within ctx.
func (s *Server) Close(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		s.server.Close()
	}
	return err
}

// Wait returns when the server stops.
func (s *Server) Wait() error {
	<-s.done
	if errors.Is(s.err, http.ErrServerClosed) {
		return nil
	}
	return s.err
}

// allowedHost reports whether host, a request's Host, is the bound address, localhost on its port, or the external
// URL's authority.
func (s *Server) allowedHost(host string) bool {
	port := strconv.Itoa(s.port)
	if host == "127.0.0.1:"+port || host == "localhost:"+port {
		return true
	}
	return s.external != nil && host == s.external.Host
}

// allowedOrigin reports whether origin, a request's Origin, is exactly one of the server's origins.
func (s *Server) allowedOrigin(origin string) bool {
	port := strconv.Itoa(s.port)
	if origin == "http://127.0.0.1:"+port || origin == "http://localhost:"+port {
		return true
	}
	return s.external != nil && origin == s.external.Scheme+"://"+s.external.Host
}

// guard checks Host, then Origin when present, sets the fixed headers and, with cookie, requires a session cookie.
// Forwarded headers are ignored. Nothing of the request is logged but its method, route and status.
func (s *Server) guard(next http.Handler, cookie bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Cache-Control", "no-store")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		if !s.allowedHost(r.Host) {
			s.fail(w, http.StatusForbidden, textForbidden)
			s.log(r, http.StatusForbidden)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && !s.allowedOrigin(origin) {
			s.fail(w, http.StatusForbidden, textForbidden)
			s.log(r, http.StatusForbidden)
			return
		}
		if cookie && !s.authenticated(r) {
			s.fail(w, http.StatusForbidden, textForbidden)
			s.log(r, http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authenticated reports whether r carries the session cookie with an issued session ID.
func (s *Server) authenticated(r *http.Request) bool {
	c, err := r.Cookie(s.cookie)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.sessions[c.Value]
	return ok
}

// routes are the fixed identifiers the log names each matched pattern by; a request matching none is "other".
var routes = map[string]string{
	"GET /{$}": "page", "GET /app.js": "asset", "GET /app.css": "asset", "POST /session": "session",
	"GET /api/model": "model", "GET /api/runs/{id}": "run", "GET /api/runs/{id}/trace": "trace", "GET /api/stream": "stream",
}

// log writes the method, the matched route's fixed identifier and the status: nothing of the path's free components,
// which a request can fill with anything.
func (s *Server) log(r *http.Request, status int) {
	route := routes[r.Pattern]
	if route == "" {
		route = "other"
	}
	s.opts.Logger.Printf("view: %s %s %d", r.Method, route, status)
}

func (s *Server) fail(w http.ResponseWriter, status int, text string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	io.WriteString(w, text+"\n")
}

func (s *Server) static(name, contentType string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content, err := static.ReadFile("static/" + name)
		if err != nil {
			s.fail(w, http.StatusNotFound, textNotFound)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Write(content)
	})
}

// session exchanges the token, posted once by the page, for a cookie holding a server-issued session ID; the
// comparison is constant-time and the endpoint rate-limited. The body is never logged.
func (s *Server) session(w http.ResponseWriter, r *http.Request) {
	if !s.take() {
		s.fail(w, http.StatusTooManyRequests, textTooMany)
		s.log(r, http.StatusTooManyRequests)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, sessionBodyLimit)
	token := ""
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		var body struct {
			Token string `json:"token"`
		}
		if json.UnmarshalRead(r.Body, &body) != nil {
			s.fail(w, http.StatusBadRequest, textBadRequest)
			s.log(r, http.StatusBadRequest)
			return
		}
		token = body.Token
	} else {
		if err := r.ParseForm(); err != nil {
			s.fail(w, http.StatusBadRequest, textBadRequest)
			s.log(r, http.StatusBadRequest)
			return
		}
		token = r.PostForm.Get("token")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) != 1 {
		s.fail(w, http.StatusForbidden, textForbidden)
		s.log(r, http.StatusForbidden)
		return
	}
	id := make([]byte, 32)
	if _, err := rand.Read(id); err != nil {
		s.fail(w, http.StatusInternalServerError, textUnavailable)
		return
	}
	session := hex.EncodeToString(id)
	s.mu.Lock()
	s.sessions[session] = time.Now()
	s.mu.Unlock()
	// Host-only (no Domain), named with the port since cookies are not port-isolated on loopback.
	http.SetCookie(w, &http.Cookie{Name: s.cookie, Value: session, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: s.external != nil && s.external.Scheme == "https"})
	w.WriteHeader(http.StatusNoContent)
	s.log(r, http.StatusNoContent)
}

// take spends one of the session endpoint's tokens, refilled at the rate per minute.
func (s *Server) take() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.bucket = min(float64(s.opts.SessionRate), s.bucket+now.Sub(s.refilled).Minutes()*float64(s.opts.SessionRate))
	s.refilled = now
	if s.bucket < 1 {
		return false
	}
	s.bucket--
	return true
}

// boundedBuffer collects an encoding up to its limit and refuses the write that would pass it, so no encoding
// larger than the bound is built.
type boundedBuffer struct {
	buf   bytes.Buffer
	limit int
	over  bool
}

var errOverBound = errors.New("encoding over its bound")

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.buf.Len()+len(p) > b.limit {
		b.over = true
		return 0, errOverBound
	}
	return b.buf.Write(p)
}

// marshalBounded encodes value within limit bytes; fits is false when the encoding passed the limit and was stopped.
func marshalBounded(value any, limit int) (encoded []byte, fits bool, err error) {
	b := &boundedBuffer{limit: limit}
	err = json.MarshalWrite(b, value)
	if b.over {
		return nil, false, nil
	}
	return b.buf.Bytes(), err == nil, err
}

// tooLarge counts the response bound hit and answers the fixed over-bound text.
func (s *Server) tooLarge(w http.ResponseWriter, r *http.Request) {
	s.v.mu.Lock()
	s.v.m.count(capResponse, 1)
	s.v.mu.Unlock()
	s.fail(w, http.StatusInsufficientStorage, textTooLarge)
	s.log(r, http.StatusInsufficientStorage)
}

// writeJSON writes value within the response bound, or the fixed over-bound text.
func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, value any) bool {
	encoded, fits, err := marshalBounded(value, s.v.opts.Caps.ResponseBytes)
	switch {
	case err != nil:
		s.fail(w, http.StatusInternalServerError, textUnavailable)
		s.log(r, http.StatusInternalServerError)
		return false
	case !fits:
		s.tooLarge(w, r)
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(encoded)
	s.log(r, http.StatusOK)
	return true
}

// model serves the snapshot, its runs list cut by halves until the response fits its bound.
func (s *Server) model(w http.ResponseWriter, r *http.Request) {
	maxRuns := 0
	for {
		snapshot := s.v.Snapshot(maxRuns)
		encoded, fits, err := marshalBounded(snapshot, s.v.opts.Caps.ResponseBytes)
		if err != nil {
			s.fail(w, http.StatusInternalServerError, textUnavailable)
			s.log(r, http.StatusInternalServerError)
			return
		}
		if fits {
			w.Header().Set("Content-Type", "application/json")
			w.Write(encoded)
			s.log(r, http.StatusOK)
			return
		}
		s.v.mu.Lock()
		s.v.m.count(capResponse, 1)
		s.v.mu.Unlock()
		if maxRuns == 1 || len(snapshot.Runs) == 0 {
			s.fail(w, http.StatusInsufficientStorage, textTooLarge)
			s.log(r, http.StatusInsufficientStorage)
			return
		}
		if maxRuns == 0 {
			maxRuns = len(snapshot.Runs)
		}
		maxRuns = max(maxRuns/2, 1)
	}
}

func (s *Server) run(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !runIDPattern.MatchString(id) {
		s.fail(w, http.StatusNotFound, textNotFound)
		s.log(r, http.StatusNotFound)
		return
	}
	detail, ok := s.v.RunDetail(id)
	if !ok {
		s.fail(w, http.StatusNotFound, textNotFound)
		s.log(r, http.StatusNotFound)
		return
	}
	// A detail whose least possible encoding is over the bound is refused before any of it is encoded.
	if detail.atLeast() > s.v.opts.Caps.ResponseBytes {
		s.tooLarge(w, r)
		return
	}
	s.writeJSON(w, r, detail)
}

func (s *Server) trace(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !runIDPattern.MatchString(id) {
		s.fail(w, http.StatusNotFound, textNotFound)
		s.log(r, http.StatusNotFound)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.v.opts.Caps.TraceScan+10*time.Second)
	defer cancel()
	forest, err := s.v.traces.Trace(ctx, id)
	switch {
	case errors.Is(err, errNotFound):
		s.fail(w, http.StatusNotFound, textNotFound)
		s.log(r, http.StatusNotFound)
		return
	case errors.Is(err, errBusy):
		s.fail(w, http.StatusServiceUnavailable, textBusy)
		s.log(r, http.StatusServiceUnavailable)
		return
	case err != nil:
		// The error's text can name a record; the response says only that the read failed.
		s.fail(w, http.StatusBadGateway, textUnavailable)
		s.log(r, http.StatusBadGateway)
		return
	}
	s.writeJSON(w, r, forest)
}

// StreamEvent is one stream event: the version and the run count, so a page re-fetches the model.
type StreamEvent struct {
	Version uint64           `json:"version"`
	Now     time.Time        `json:"now"`
	Rebuilt bool             `json:"rebuilt"`
	Runs    int              `json:"runs"`
	Counts  map[string]int64 `json:"counts,omitzero"`
}

// stream is the Server-Sent Events stream: one event per version, a comment every keep-alive interval, each write
// under its deadline; a client that does not read is dropped, never buffered.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	controller := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	s.log(r, http.StatusOK)
	versions, unsubscribe := s.v.Subscribe()
	defer unsubscribe()
	write := func(text string) bool {
		if err := controller.SetWriteDeadline(time.Now().Add(s.opts.WriteDeadline)); err != nil {
			return false
		}
		if _, err := io.WriteString(w, text); err != nil {
			return false
		}
		return controller.Flush() == nil
	}
	if !write(s.event(s.v.Version())) {
		return
	}
	keepAlive := time.NewTicker(s.opts.KeepAlive)
	defer keepAlive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.done:
			return
		case version := <-versions:
			if !write(s.event(version)) {
				return
			}
		case <-keepAlive.C:
			if !write(": keep-alive\n\n") {
				return
			}
		}
	}
}

// event is the stream event for version within the event bound, or the version alone over it.
func (s *Server) event(version uint64) string {
	s.v.mu.RLock()
	e := StreamEvent{Version: version, Now: s.v.opts.Now(), Rebuilt: s.v.rebuilt, Runs: len(s.v.m.runs)}
	s.v.mu.RUnlock()
	encoded, _ := json.Marshal(e)
	if len(encoded) > s.v.opts.Caps.EventBytes {
		s.v.mu.Lock()
		s.v.m.count(capEvent, 1)
		s.v.mu.Unlock()
		encoded = []byte(fmt.Sprintf(`{"version":%d}`, version))
	}
	return "event: model\ndata: " + string(encoded) + "\n\n"
}
