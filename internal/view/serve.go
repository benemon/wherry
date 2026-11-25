package view

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The page as make ui builds it from internal/view/web. THIRD_PARTY_NOTICES beside it ships in the release archive,
// not the binary.
//
//go:embed static/index.html static/assets
var static embed.FS

// assets is the hashed files the page references, served by exact name alone.
var assets, _ = fs.Sub(static, "static/assets")

// noncePlaceholder is what index.html carries where each response's nonce goes.
const noncePlaceholder = "__CSP_NONCE__"

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
	textForbidden        = "forbidden"
	textNotFound         = "not found"
	textBadRequest       = "bad request"
	textMethodNotAllowed = "method not allowed"
	textTooMany          = "too many requests"
	textBusy             = "busy"
	textUnavailable      = "unavailable"
	textTooLarge         = "response over its bound"
)

// csp is the Content-Security-Policy of every response: assets from the embedded files alone, no inline script, data:
// for the images EUI's styles carry. The page's own policy adds a per-response nonce to style-src, since EUI injects
// its styles at runtime; nothing else differs, and script-src never carries a nonce.
const csp = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"

func pageCSP(nonce string) string {
	return strings.Replace(csp, "style-src 'self'", "style-src 'self' 'nonce-"+nonce+"'", 1)
}

// assetTypes are the content types of the files the build emits: scripts, one stylesheet and the fonts; anything else
// is served as bytes.
var assetTypes = map[string]string{".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".woff2": "font/woff2"}

// sessionBodyLimit bounds a POST /session body.
const sessionBodyLimit = 1024

// maxSessions bounds the sessions issued; the oldest is dropped beyond it.
const maxSessions = 1024

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
	mux.Handle("GET /{$}", http.HandlerFunc(s.page))
	mux.Handle("GET /assets/{file}", http.HandlerFunc(s.asset))
	mux.Handle("POST /session", http.HandlerFunc(s.session))
	mux.Handle("GET /api/model", http.HandlerFunc(s.model))
	mux.Handle("GET /api/runs/{id}", http.HandlerFunc(s.run))
	mux.Handle("GET /api/runs/{id}/trace", http.HandlerFunc(s.trace))
	mux.Handle("GET /api/runs/{id}/transcript", http.HandlerFunc(s.transcript))
	mux.Handle("GET /api/stream", http.HandlerFunc(s.stream))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.fail(w, http.StatusNotFound, textNotFound)
	}))
	// WriteTimeout stays 0 for the stream, whose writes carry their own deadline. OPTIONS * reaches the guard like any
	// other request. The server's own 400, 408 and 431 answers to a request it cannot read pass through no handler.
	s.server = &http.Server{Handler: s.guard(mux), DisableGeneralOptionsHandler: true, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, ErrorLog: log.New(io.Discard, "", 0)}
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
// URL's authority, each compared as sent: a proxy sends the bare Host or the authority exactly.
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

// guard wraps the whole mux, so no request reaches it, its redirects included, before the checks: Host, then Origin
// when present, then the path, then the session cookie on the routes that need one. The fixed headers go on every
// response it writes, and each request is logged once, by method, route and status. Forwarded headers are ignored.
func (s *Server) guard(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Cache-Control", "no-store")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		rec := &recorder{ResponseWriter: w}
		defer func() { s.log(r, rec.status) }()
		switch {
		case !s.allowedHost(r.Host):
			s.fail(rec, http.StatusForbidden, textForbidden)
		case r.Header.Get("Origin") != "" && !s.allowedOrigin(r.Header.Get("Origin")):
			s.fail(rec, http.StatusForbidden, textForbidden)
		case r.Method == http.MethodConnect:
			s.fail(rec, http.StatusMethodNotAllowed, textMethodNotAllowed)
		case r.RequestURI != "*" && !cleanPath(r.URL.EscapedPath()):
			s.fail(rec, http.StatusNotFound, textNotFound)
		case s.needsCookie(mux, r) && !s.authenticated(r):
			s.fail(rec, http.StatusForbidden, textForbidden)
		default:
			mux.ServeHTTP(rec, r)
		}
	})
}

// needsCookie reports whether the route r matches needs a session cookie, by the pattern the mux would match: the path
// is clean by now, so the mux names the pattern itself, never a redirect. The pattern is kept on r for the log, as the
// mux sets it. OPTIONS * matches nothing.
func (s *Server) needsCookie(mux *http.ServeMux, r *http.Request) bool {
	if r.RequestURI == "*" {
		return false
	}
	_, r.Pattern = mux.Handler(r)
	return routes[r.Pattern].cookie
}

// cleanPath reports whether path, a request's escaped path, is one the mux matches as sent rather than redirects: it
// starts with a slash, and no segment is empty, a dot, two dots or escapes a dot or a slash. The root alone ends in a
// slash.
func cleanPath(path string) bool {
	if path == "/" {
		return true
	}
	if !strings.HasPrefix(path, "/") || strings.Contains(strings.ToLower(path), "%2e") || strings.Contains(strings.ToLower(path), "%2f") {
		return false
	}
	for _, segment := range strings.Split(path[1:], "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

// recorder keeps the status a handler wrote for the log. Unwrap hands the server's writer to a ResponseController, so
// the stream's write deadlines and flushes reach the connection.
type recorder struct {
	http.ResponseWriter
	status int
}

func (w *recorder) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *recorder) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *recorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

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

// routes are the fixed identifiers the log names each matched pattern by, and whether the route needs the session
// cookie; a request matching none is "other".
var routes = map[string]struct {
	name   string
	cookie bool
}{
	"GET /{$}": {"page", false}, "GET /assets/{file}": {"asset", false}, "POST /session": {"session", false},
	"GET /api/model": {"model", true}, "GET /api/runs/{id}": {"run", true}, "GET /api/runs/{id}/trace": {"trace", true}, "GET /api/runs/{id}/transcript": {"transcript", true}, "GET /api/stream": {"stream", true},
}

// log writes the method as one of the fixed set, the matched route's fixed identifier and the status: nothing of the
// request line's free text, which a request can fill with anything. A request refused before the route was matched
// is "other".
func (s *Server) log(r *http.Request, status int) {
	route := routes[r.Pattern].name
	if route == "" {
		route = "other"
	}
	s.opts.Logger.Printf("view: %s %s %d", fixed(httpMethods, r.Method), route, status)
}

func (s *Server) fail(w http.ResponseWriter, status int, text string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	io.WriteString(w, text+"\n")
}

// page serves index.html with a fresh nonce in the policy and in the meta tag the app reads.
func (s *Server) page(w http.ResponseWriter, r *http.Request) {
	index, err := static.ReadFile("static/index.html")
	if err != nil {
		s.fail(w, http.StatusNotFound, textNotFound)
		return
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		s.fail(w, http.StatusInternalServerError, textUnavailable)
		return
	}
	nonce := base64.StdEncoding.EncodeToString(raw)
	w.Header().Set("Content-Security-Policy", pageCSP(nonce))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(bytes.Replace(index, []byte(noncePlaceholder), []byte(nonce), 1))
}

// asset serves one file of the build's assets directory by its exact name; any other name, a directory included,
// gets the fixed 404. The names are content-hashed, so the response is cacheable for good.
func (s *Server) asset(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("file")
	content, err := fs.ReadFile(assets, name)
	if err != nil {
		s.fail(w, http.StatusNotFound, textNotFound)
		return
	}
	contentType := assetTypes[path.Ext(name)]
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Write(content)
}

// session exchanges the token, posted once by the page, for a cookie holding a server-issued session ID; the
// comparison is constant-time and the endpoint rate-limited. The body is never logged.
func (s *Server) session(w http.ResponseWriter, r *http.Request) {
	if !s.take() {
		s.fail(w, http.StatusTooManyRequests, textTooMany)
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
			return
		}
		token = body.Token
	} else {
		if err := r.ParseForm(); err != nil {
			s.fail(w, http.StatusBadRequest, textBadRequest)
			return
		}
		token = r.PostForm.Get("token")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) != 1 {
		s.fail(w, http.StatusForbidden, textForbidden)
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
	dropped := 0
	for len(s.sessions) > maxSessions {
		oldest := ""
		for other, issued := range s.sessions {
			if oldest == "" || issued.Before(s.sessions[oldest]) {
				oldest = other
			}
		}
		delete(s.sessions, oldest)
		dropped++
	}
	s.mu.Unlock()
	if dropped > 0 {
		s.v.mu.Lock()
		s.v.m.count(capSessions, int64(dropped))
		s.v.mu.Unlock()
	}
	// Host-only (no Domain), named with the port since cookies are not port-isolated on loopback.
	http.SetCookie(w, &http.Cookie{Name: s.cookie, Value: session, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: s.external != nil && s.external.Scheme == "https"})
	w.WriteHeader(http.StatusNoContent)
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
}

// writeJSON writes value within the response bound, or the fixed over-bound text.
func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, value any) bool {
	encoded, fits, err := marshalBounded(value, s.v.opts.Caps.ResponseBytes)
	switch {
	case err != nil:
		s.fail(w, http.StatusInternalServerError, textUnavailable)
		return false
	case !fits:
		s.tooLarge(w, r)
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(encoded)
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
			return
		}
		if fits {
			w.Header().Set("Content-Type", "application/json")
			w.Write(encoded)
			return
		}
		s.v.mu.Lock()
		s.v.m.count(capResponse, 1)
		s.v.mu.Unlock()
		if maxRuns == 1 || len(snapshot.Runs) == 0 {
			s.fail(w, http.StatusInsufficientStorage, textTooLarge)
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
		return
	}
	detail, ok := s.v.RunDetail(id)
	if !ok {
		s.fail(w, http.StatusNotFound, textNotFound)
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
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.v.opts.Caps.TraceScan+10*time.Second)
	defer cancel()
	forest, err := s.v.traces.Trace(ctx, id)
	switch {
	case errors.Is(err, errNotFound):
		s.fail(w, http.StatusNotFound, textNotFound)
		return
	case errors.Is(err, errBusy):
		s.fail(w, http.StatusServiceUnavailable, textBusy)
		return
	case err != nil:
		// The error's text can name a record; the response says only that the read failed.
		s.fail(w, http.StatusBadGateway, textUnavailable)
		return
	}
	s.writeJSON(w, r, forest)
}

// transcript serves a page of a run's transcript; the query names exactly one of tail, before and from.
func (s *Server) transcript(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !runIDPattern.MatchString(id) {
		s.fail(w, http.StatusNotFound, textNotFound)
		return
	}
	page, ok := parsePage(r.URL.Query())
	if !ok {
		s.fail(w, http.StatusBadRequest, textBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.v.opts.Caps.TranscriptScan+10*time.Second)
	defer cancel()
	transcript, err := s.v.transcripts.Transcript(ctx, id, page)
	switch {
	case errors.Is(err, errNotFound):
		s.fail(w, http.StatusNotFound, textNotFound)
		return
	case errors.Is(err, errBusy):
		s.fail(w, http.StatusServiceUnavailable, textBusy)
		return
	case err != nil:
		s.fail(w, http.StatusBadGateway, textUnavailable)
		return
	}
	s.writeJSON(w, r, transcript)
}

// parsePage is the page query asks for: tail=N, or before=<seq> or from=<seq> with limit=N and the cursor's attempt
// and first; N is 1 to MaxPage. Anything else is refused.
func parsePage(query url.Values) (Page, bool) {
	var page Page
	integer := func(name string) (*int64, bool) {
		if !query.Has(name) {
			return nil, true
		}
		n, err := strconv.ParseInt(query.Get(name), 10, 64)
		if err != nil || n < 0 {
			return nil, false
		}
		return &n, true
	}
	count := func(name string) (int, bool) {
		if !query.Has(name) {
			return 0, true
		}
		n, err := strconv.ParseInt(query.Get(name), 10, 32)
		if err != nil || n < 1 || n > MaxPage {
			return 0, false
		}
		return int(n), true
	}
	var ok bool
	if page.Tail, ok = count("tail"); !ok {
		return Page{}, false
	}
	if page.Limit, ok = count("limit"); !ok {
		return Page{}, false
	}
	if page.Before, ok = integer("before"); !ok {
		return Page{}, false
	}
	if page.From, ok = integer("from"); !ok {
		return Page{}, false
	}
	attempt, ok := integer("attempt")
	if !ok {
		return Page{}, false
	}
	if page.First, ok = integer("first"); !ok {
		return Page{}, false
	}
	given := 0
	for _, set := range []bool{page.Tail > 0, page.Before != nil, page.From != nil} {
		if set {
			given++
		}
	}
	cursor := page.Before != nil || page.From != nil
	if given != 1 || cursor != (page.Limit > 0) || cursor != (attempt != nil) || !cursor && page.First != nil {
		return Page{}, false
	}
	if attempt != nil {
		page.Attempt = *attempt
	}
	return page, true
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
