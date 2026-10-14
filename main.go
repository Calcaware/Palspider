package main

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"encoding/xml"
	"fmt"
	stdhtml "html"
	"html/template"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/calcaware/palspider/crawler"
	"github.com/calcaware/palspider/engine"
	"github.com/calcaware/palspider/searcher"

	"github.com/calcaware/palspider/store"
)

//go:embed views/* public/*
var embeddedFS embed.FS

// version is reported by /api/status, /api/health, and the crawler User-Agent.
const version = "0.2.0"

// envFirst returns the value of the first defined environment variable.
func envFirst(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

func main() {
	workerCount := 50
	if n := envFirst("PALSPIDER_WORKERS", "INQUEST_WORKERS"); n != "" {
		if v, err := strconv.Atoi(n); err == nil && v > 0 {
			workerCount = v
		}
	}
	maxDepth := 10
	if n := os.Getenv("PALSPIDER_MAX_DEPTH"); n != "" {
		if v, err := strconv.Atoi(n); err == nil {
			maxDepth = v
		}
	}
	maxPages := 0
	if n := os.Getenv("PALSPIDER_MAX_PAGES"); n != "" {
		if v, err := strconv.Atoi(n); err == nil {
			maxPages = v
		}
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	dbPath := envFirst("PALSPIDER_DB", "INQUEST_DB")
	if dbPath == "" {
		dbPath = "palspider.db"
	}

	st, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}
	defer st.Close()

	e := engine.New(workerCount, maxDepth, maxPages, st)

	if err := e.Restore(); err != nil {
		log.Fatalf("Failed to restore: %v", err)
	}

	if e.Stats().Documents == 0 {
		seed(e)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	tmpl := parseTemplates()

	server := &http.Server{Addr: ":" + port, Handler: newMux(e, tmpl)}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("Shutting down...")
		e.Stop()
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	log.Printf("Palspider running at http://localhost:%s (%d workers)", port, workerCount)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// newMux registers every HTTP route and returns the handler with shared
// middleware applied (security headers, read-only CORS, optional auth).
func newMux(e *engine.Engine, tmpl *template.Template) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", handleSearch(e, tmpl))
	mux.HandleFunc("GET /", handleNotFound)
	mux.HandleFunc("GET /search", handleResults(e, tmpl))
	mux.HandleFunc("POST /crawl", handleCrawl(e))
	mux.HandleFunc("GET /crawl-status", handleCrawlStatus(e))
	mux.HandleFunc("GET /suggest", handleSuggest(e))
	mux.HandleFunc("GET /sitemap.xml", handleSitemap(e))

	mux.HandleFunc("GET /api/health", handleAPIHealth())
	mux.HandleFunc("GET /api/status", handleAPIStatus(e))
	mux.HandleFunc("GET /api/peers", handleAPIPeersList(e))
	mux.HandleFunc("POST /api/peers", handleAPIPeersAdd(e))
	mux.HandleFunc("DELETE /api/peers", handleAPIPeersRemove(e))
	mux.HandleFunc("GET /api/search", handleAPISearch(e))
	mux.HandleFunc("GET /api/related", handleAPIRelated(e))
	mux.HandleFunc("GET /api/export", handleAPIExport(e))
	mux.HandleFunc("GET /api/crawl", handleCrawlStatus(e))
	mux.HandleFunc("POST /api/crawl", handleAPICrawl(e))
	mux.HandleFunc("POST /api/crawl/pause", handleAPICrawlPause(e))
	mux.HandleFunc("POST /api/crawl/resume", handleAPICrawlResume(e))
	mux.HandleFunc("POST /api/crawl/clear", handleAPICrawlClear(e))
	mux.HandleFunc("POST /api/crawl/requeue", handleAPICrawlRequeue(e))
	mux.HandleFunc("GET /api/suggest", handleSuggest(e))
	mux.HandleFunc("GET /api/peers/gossip", handleAPIPeersGossip(e))

	sub, _ := fs.Sub(embeddedFS, "public")
	fileServer := http.FileServer(http.FS(sub))
	mux.Handle("GET /style.css", fileServer)
	mux.Handle("GET /palspider.png", fileServer)
	return wrap(mux)
}

// wrap applies headers shared by every route and, when PALSPIDER_AUTH is
// set, requires matching Basic credentials for mutating requests. Read-only
// JSON endpoints additionally advertise cross-origin access so other origins
// may read them; mutating endpoints never do.
func wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Palspider-Version", version)
		if isReadAPI(r) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		if !authorized(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="palspider"`)
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if !peerAuthorized(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="palspider"`)
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if r.Method == http.MethodGet && os.Getenv("PALSPIDER_PUBLIC_RATE_LIMIT") != "" {
			limit, err := strconv.Atoi(os.Getenv("PALSPIDER_PUBLIC_RATE_LIMIT"))
			if err == nil && limit > 0 {
				if !rateLimitRequest(r.RemoteAddr, limit, 60) {
					writeJSONError(w, http.StatusTooManyRequests, "rate limit exceeded")
					return
				}
			}
		}
		h.ServeHTTP(w, r)
	})
}

var publicRateLimiter = struct {
	mu   sync.Mutex
	seen map[string][]time.Time
}{
	seen: make(map[string][]time.Time),
}

func clientKey(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return strings.TrimSpace(remoteAddr)
}

func rateLimitRequest(remoteAddr string, limit, windowSeconds int) bool {
	if limit <= 0 {
		return true
	}
	key := clientKey(remoteAddr)
	publicRateLimiter.mu.Lock()
	defer publicRateLimiter.mu.Unlock()
	cutoff := time.Now().Add(-time.Duration(windowSeconds) * time.Second)
	entries := publicRateLimiter.seen[key]
	filtered := entries[:0]
	for _, ts := range entries {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}
	publicRateLimiter.seen[key] = filtered
	if len(filtered) >= limit {
		return false
	}
	publicRateLimiter.seen[key] = append(filtered, time.Now())
	return true
}

// isReadAPI reports whether the request is a safe read of JSON, where
// cross-origin access cannot change state.
func isReadAPI(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	p := r.URL.Path
	return strings.HasPrefix(p, "/api/") || p == "/suggest" || p == "/crawl-status"
}

// authorized enforces the optional PALSPIDER_AUTH policy: with no value
// configured everything is allowed; otherwise POST and DELETE requests must
// carry matching Basic credentials. GET and HEAD stay open so the web UI,
// peer mesh, and health checks keep working.
func authorized(r *http.Request) bool {
	spec := envFirst("PALSPIDER_AUTH")
	if spec == "" {
		return true
	}
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		return true
	}
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	wantUser, wantPass, _ := strings.Cut(spec, ":")
	return subtle.ConstantTimeCompare([]byte(user), []byte(wantUser)) == 1 &&
		subtle.ConstantTimeCompare([]byte(pass), []byte(wantPass)) == 1
}

func peerAuthorized(r *http.Request) bool {
	token := strings.TrimSpace(os.Getenv("PALSPIDER_PEER_TOKEN"))
	if token == "" {
		return true
	}
	switch r.URL.Path {
	case "/api/peers/gossip", "/api/search", "/api/status":
	default:
		return true
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		auth = strings.TrimSpace(auth[7:])
	}
	return subtle.ConstantTimeCompare([]byte(auth), []byte(token)) == 1
}

func seed(e *engine.Engine) {
	docs := []struct{ url, title, text string }{
		{"https://nodejs.org", "Node.js",
			"Node.js is a JavaScript runtime built on V8. It uses an event-driven non-blocking I/O model. Node.js is designed for building scalable network applications. npm is the package manager for Node.js with millions of packages."},
		{"https://expressjs.com", "Express.js",
			"Express is a minimal and flexible Node.js web application framework. It provides a robust set of features for web and mobile applications. Express is the most popular web framework for Node.js."},
		{"https://getbootstrap.com", "Bootstrap",
			"Bootstrap is a powerful front-end framework for building responsive mobile-first sites. It includes HTML CSS and JavaScript components. Bootstrap 5 is the latest version with improved performance."},
		{"https://developer.mozilla.org", "MDN Web Docs",
			"MDN Web Docs is a comprehensive resource for web developers. It covers HTML CSS JavaScript and web APIs. MDN is maintained by Mozilla and the open source community."},
		{"https://github.com", "GitHub",
			"GitHub is a platform for version control and collaboration. It lets you work together on projects using Git. GitHub hosts millions of repositories and is the largest source code host."},
		{"https://en.wikipedia.org/wiki/Search_engine", "Search Engine",
			"A search engine is a software system that finds information on the web. It uses web crawling indexing and ranking to return relevant results. Google Bing and DuckDuckGo are popular search engines."},
		{"https://www.cloudflare.com/learning/dns/what-is-dns/", "DNS",
			"DNS stands for Domain Name System. It translates domain names to IP addresses. DNS is a fundamental protocol that powers the internet by mapping human readable names to numerical addresses."},
	}
	for _, d := range docs {
		e.AddDocument(d.url, d.title, d.text)
	}
	log.Printf("Seeded %d documents", len(docs))
}

func parseTemplates() *template.Template {
	funcs := template.FuncMap{
		"highlightSnippet": highlightSnippet,
	}
	return template.Must(template.New("").Funcs(funcs).ParseFS(embeddedFS, "views/*.html"))
}

func highlightSnippet(s, query string, n int) template.HTML {
	if n <= 0 {
		return ""
	}
	runes := []rune(s)
	terms := crawler.Tokenize(query)
	if len(runes) == 0 {
		return ""
	}
	if len(runes) <= n {
		return template.HTML(renderHighlighted(runes, terms, 0, len(runes)))
	}

	lower := []rune(strings.ToLower(string(runes)))
	type span struct {
		start int
		end   int
		term  string
	}
	var matches []span
	for _, term := range terms {
		word := []rune(strings.ToLower(term))
		if len(word) == 0 || len(word) > len(lower) {
			continue
		}
		for i := 0; i+len(word) <= len(lower); i++ {
			if i > 0 && isSearchWordRune(lower[i-1]) {
				continue
			}
			if i+len(word) < len(lower) && isSearchWordRune(lower[i+len(word)]) {
				continue
			}
			matched := true
			for j := range word {
				if lower[i+j] != word[j] {
					matched = false
					break
				}
			}
			if matched {
				matches = append(matches, span{start: i, end: i + len(word), term: term})
			}
		}
	}

	start := 0
	if len(matches) > 0 {
		bestScore := -1
		for _, match := range matches {
			candidate := match.start - n/3
			if candidate < 0 {
				candidate = 0
			}
			if candidate+n > len(runes) {
				candidate = len(runes) - n
			}
			seen := make(map[string]struct{})
			score := 0
			for _, other := range matches {
				if other.start < candidate+n && other.end > candidate {
					score++
					seen[other.term] = struct{}{}
				}
			}
			score += len(seen) * 2
			if score > bestScore || (score == bestScore && candidate < start) {
				bestScore = score
				start = candidate
			}
		}
	}
	end := start + n
	if end > len(runes) {
		end = len(runes)
	}
	result := renderHighlighted(runes, terms, start, end)
	if start > 0 {
		result = "..." + result
	}
	if end < len(runes) {
		result += "..."
	}
	return template.HTML(result)
}

func renderHighlighted(source []rune, terms []string, start, end int) string {
	matched := make([]bool, end-start)
	lower := []rune(strings.ToLower(string(source)))
	for _, term := range terms {
		word := []rune(strings.ToLower(term))
		if len(word) == 0 {
			continue
		}
		for i := start; i+len(word) <= end; i++ {
			if i > 0 && isSearchWordRune(lower[i-1]) {
				continue
			}
			if i+len(word) < len(lower) && isSearchWordRune(lower[i+len(word)]) {
				continue
			}
			ok := true
			for j := range word {
				if lower[i+j] != word[j] {
					ok = false
					break
				}
			}
			if ok {
				for j := i; j < i+len(word); j++ {
					matched[j-start] = true
				}
			}
		}
	}

	var out strings.Builder
	for i := start; i < end; {
		isMatch := matched[i-start]
		j := i + 1
		for j < end && matched[j-start] == isMatch {
			j++
		}
		if isMatch {
			out.WriteString("<mark>")
		}
		out.WriteString(stdhtml.EscapeString(string(source[i:j])))
		if isMatch {
			out.WriteString("</mark>")
		}
		i = j
	}
	return out.String()
}

func isSearchWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r) || r == '-'
}

// writeJSON sends v as JSON with the given status code. HTML escaping is
// disabled so URLs and error messages stay readable in API responses.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}

// writeJSONError sends a machine-readable error object.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// validSeedURL accepts only absolute http(s) URLs.
func validSeedURL(raw string) bool {
	return crawler.CheckURLPolicy(raw) == nil
}

// render executes a template, logging failures after headers were written.
func render(w http.ResponseWriter, tmpl *template.Template, name string, data interface{}) {
	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		log.Printf("template %s: %v", name, err)
	}
}

func handleNotFound(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		writeJSONError(w, http.StatusNotFound, "not found")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(`<!DOCTYPE html>
<html lang="en" data-bs-theme="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>404 - Palspider</title>
  <link href="/style.css" rel="stylesheet">
</head>
<body class="d-flex flex-column min-vh-100 align-items-center justify-content-center text-center">
  <h1 class="display-3 fw-bold mb-2">404</h1>
  <p class="text-secondary mb-3">That page does not exist.</p>
  <a href="/" class="text-light">Back to search</a>
</body>
</html>
`))
}

// pageSize is the number of results per web-UI page; maxPage caps the
// searchable window at 100 results.
const (
	pageSize = 10
	maxPage  = 10

	// exportMaxLimit caps a single JSON export page so one request cannot
	// buffer an unbounded slice of the corpus.
	exportMaxLimit = 1000

	// sitemapMaxURLs caps a single sitemap so a response stays within the
	// 50,000 URL / 50 MB limit described by the sitemap protocol.
	sitemapMaxURLs = 50000
)

// searchURL builds a results-page link for the given query, page, and sort order.
func searchURL(query string, page int, sort string) string {
	values := url.Values{"q": {query}, "page": {strconv.Itoa(page)}}
	sortOrder := string(searcher.NormalizeSortOrder(sort))
	if sortOrder != "" && sortOrder != string(searcher.SortRelevance) {
		values.Set("sort", sortOrder)
	}
	return "/search?" + values.Encode()
}

func handleSearch(e *engine.Engine, tmpl *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sortOrder := string(searcher.NormalizeSortOrder(r.URL.Query().Get("sort")))
		data := map[string]interface{}{
			"Query":       "",
			"Sort":        sortOrder,
			"Stats":       e.Stats(),
			"CrawlNotice": r.URL.Query().Get("crawl"),
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		render(w, tmpl, "search.html", data)
	}
}

func handleResults(e *engine.Engine, tmpl *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		if query == "" {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		sortOrder := string(searcher.NormalizeSortOrder(r.URL.Query().Get("sort")))
		page := 1
		if p := r.URL.Query().Get("page"); p != "" {
			if v, err := strconv.Atoi(p); err == nil {
				page = v
			}
		}
		if page < 1 {
			page = 1
		}
		if page > maxPage {
			page = maxPage
		}

		offset := (page - 1) * pageSize
		window := e.SearchWithSort(query, offset+pageSize+1, sortOrder) // one extra detects a next page
		hasNext := len(window) > offset+pageSize
		switch {
		case hasNext:
			window = window[:offset+pageSize]
		case len(window) > offset:
			window = window[offset:]
		default:
			window = nil
		}
		related := make(map[int][]searcher.Result, len(window))
		if len(window) > 0 {
			result := window[0]
			related[result.ID] = e.Related(result.URL, 3)
		}

		data := map[string]interface{}{
			"Query":   query,
			"Sort":    sortOrder,
			"Results": window,
			"Related": related,
			"Stats":   e.Stats(),
			"Page":    page,
			"HasPrev": page > 1,
			"HasNext": hasNext,
			"PrevURL": searchURL(query, page-1, sortOrder),
			"NextURL": searchURL(query, page+1, sortOrder),
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		render(w, tmpl, "results.html", data)
	}
}

func handleCrawl(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		raw := strings.TrimSpace(r.FormValue("url"))
		if raw == "" {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		if !validSeedURL(raw) {
			http.Redirect(w, r, "/?crawl=invalid", http.StatusSeeOther)
			return
		}
		e.Seed(raw)
		http.Redirect(w, r, "/?crawl=started", http.StatusSeeOther)
	}
}

// handleAPICrawl accepts {"url": "..."} and queues the URL for crawling.
func handleAPICrawl(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if !validSeedURL(body.URL) {
			writeJSONError(w, http.StatusBadRequest, "url must be an absolute http or https URL")
			return
		}
		normalized := crawler.NormalizeURL(strings.TrimSpace(body.URL))
		e.Seed(normalized)
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted", "url": normalized})
	}
}

func handleCrawlStatus(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, e.CrawlStatus())
	}
}

func handleAPICrawlPause(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		e.Pause()
		writeJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "paused": true})
	}
}

func handleAPICrawlResume(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		e.Resume()
		writeJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "paused": false})
	}
}

func handleAPICrawlClear(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		e.ClearQueue()
		writeJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "queueSize": 0})
	}
}

func handleAPICrawlRequeue(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if body.URL == "" {
			writeJSONError(w, http.StatusBadRequest, "url required")
			return
		}
		if !e.Requeue(body.URL) {
			writeJSONError(w, http.StatusBadRequest, "url could not be requeued")
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted", "url": crawler.NormalizeURL(body.URL)})
	}
}

// handleSuggest serves term autocomplete. limit defaults to 5 and is clamped
// to [1, 20].
func handleSuggest(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := 5
		if l := r.URL.Query().Get("limit"); l != "" {
			v, err := strconv.Atoi(l)
			if err != nil || v < 1 {
				writeJSONError(w, http.StatusBadRequest, "limit must be an integer >= 1")
				return
			}
			limit = v
		}
		if limit > 20 {
			limit = 20
		}
		prefix := strings.TrimSpace(r.URL.Query().Get("q"))
		if prefix == "" {
			writeJSON(w, http.StatusOK, []string{})
			return
		}
		writeJSON(w, http.StatusOK, e.Suggest(prefix, limit))
	}
}

func handleAPIHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"app":     "palspider",
			"version": version,
		})
	}
}

func handleAPIStatus(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s := e.Stats()
		writeJSON(w, http.StatusOK, struct {
			App       string                     `json:"app"`
			Version   string                     `json:"version"`
			Documents int                        `json:"documents"`
			Terms     int                        `json:"terms"`
			Visited   int                        `json:"visited"`
			QueueSize int32                      `json:"queueSize"`
			Active    int32                      `json:"active"`
			Paused    bool                       `json:"paused"`
			Peers     int                        `json:"peers"`
			HostStats map[string]engine.HostStat `json:"hostStats,omitempty"`
		}{
			App:       "palspider",
			Version:   version,
			Documents: s.Documents,
			Terms:     s.Terms,
			Visited:   s.Visited,
			QueueSize: s.QueueSize,
			Active:    s.Active,
			Paused:    s.Paused,
			Peers:     len(e.Peers.List()),
			HostStats: s.HostStats,
		})
	}
}

func handleAPIPeersList(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, e.Peers.List())
	}
}

func handleAPIPeersAdd(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if body.URL == "" {
			writeJSONError(w, http.StatusBadRequest, "url required")
			return
		}
		if err := e.Peers.Add(body.URL); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func handleAPIPeersRemove(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if body.URL == "" {
			writeJSONError(w, http.StatusBadRequest, "url required")
			return
		}
		removed := e.Peers.Remove(body.URL)
		writeJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "removed": removed})
	}
}

// handleAPISearch serves local-only results (used by peers during fan-out).
// limit is clamped to [1, 100] (default 10), offset must be < 100, and
// remote=1 searches through the peer mesh as well. The response is always a
// JSON array, empty when nothing matched.
func filterSearchResults(results []searcher.Result, values url.Values) []searcher.Result {
	hostFilter := strings.TrimSpace(strings.ToLower(values.Get("host")))
	pathFilter := strings.TrimSpace(values.Get("path"))
	if hostFilter == "" && pathFilter == "" {
		return results
	}

	filtered := make([]searcher.Result, 0, len(results))
	for _, res := range results {
		if hostFilter != "" {
			parsed, err := url.Parse(res.URL)
			if err != nil {
				continue
			}
			host := strings.Trim(strings.ToLower(parsed.Hostname()), ".")
			hostFilterNorm := strings.Trim(strings.ToLower(hostFilter), ".")
			if host != hostFilterNorm && !strings.HasSuffix(host, "."+hostFilterNorm) {
				continue
			}
		}
		if pathFilter != "" {
			parsed, err := url.Parse(res.URL)
			if err != nil {
				continue
			}
			path := strings.TrimRight(parsed.EscapedPath(), "/")
			want := strings.TrimRight(pathFilter, "/")
			if path == "" {
				if want != "" && want != "/" {
					continue
				}
			} else if !strings.HasPrefix(path, want) {
				continue
			}
		}
		filtered = append(filtered, res)
	}
	return filtered
}

func handleAPISearch(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(r.URL.Query().Get("q"))
		if query == "" {
			writeJSONError(w, http.StatusBadRequest, "query required")
			return
		}
		limit := 10
		if l := r.URL.Query().Get("limit"); l != "" {
			v, err := strconv.Atoi(l)
			if err != nil || v < 1 {
				writeJSONError(w, http.StatusBadRequest, "limit must be an integer >= 1")
				return
			}
			if v > 100 {
				v = 100
			}
			limit = v
		}
		offset := 0
		if o := r.URL.Query().Get("offset"); o != "" {
			v, err := strconv.Atoi(o)
			if err != nil || v < 0 {
				writeJSONError(w, http.StatusBadRequest, "offset must be an integer >= 0")
				return
			}
			offset = v
		}
		if offset >= 100 {
			writeJSONError(w, http.StatusBadRequest, "offset must be less than 100")
			return
		}
		if offset+limit > 100 {
			limit = 100 - offset
		}

		sortOrder := string(searcher.NormalizeSortOrder(r.URL.Query().Get("sort")))
		window := offset + limit
		var results []searcher.Result
		if isRemoteSearch(r) {
			results = e.SearchWithSort(query, window, sortOrder)
		} else {
			results = e.SearchLocalWithSort(query, window, sortOrder)
		}
		results = filterSearchResults(results, r.URL.Query())
		if offset > 0 {
			if len(results) <= offset {
				results = nil
			} else {
				results = results[offset:]
			}
		}
		if results == nil {
			results = []searcher.Result{}
		}
		w.Header().Set("X-Palspider-Count", strconv.Itoa(len(results)))
		w.Header().Set("X-Palspider-Offset", strconv.Itoa(offset))
		w.Header().Set("X-Palspider-Limit", strconv.Itoa(limit))
		w.Header().Set("X-Palspider-Total", strconv.Itoa(len(filterSearchResults(e.SearchLocalWithSort(query, 100, sortOrder), r.URL.Query()))))
		writeJSON(w, http.StatusOK, results)
	}
}

func handleAPIRelated(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rawURL := strings.TrimSpace(r.URL.Query().Get("url"))
		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Host == "" ||
			(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
			writeJSONError(w, http.StatusBadRequest, "url must be an absolute http or https URL")
			return
		}
		limit := 5
		if raw := r.URL.Query().Get("limit"); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value < 1 {
				writeJSONError(w, http.StatusBadRequest, "limit must be an integer >= 1")
				return
			}
			limit = value
			if limit > 20 {
				limit = 20
			}
		}
		results := e.Related(crawler.NormalizeURL(rawURL), limit)
		if results == nil {
			results = []searcher.Result{}
		}
		writeJSON(w, http.StatusOK, results)
	}
}

// isRemoteSearch reports whether the caller asked for peer fan-out.
func isRemoteSearch(r *http.Request) bool {
	v := r.URL.Query().Get("remote")
	return v == "1" || strings.EqualFold(v, "true")
}

// handleAPIExport streams indexed documents. limit defaults to 100 and clamps
// to [1, exportMaxLimit]; offset must be >= 0. format defaults to json and may
// be ndjson to stream one document per line. Pagination totals are reported in
// the same X-Palspider-* headers as search.
func handleAPIExport(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if l := r.URL.Query().Get("limit"); l != "" {
			v, err := strconv.Atoi(l)
			if err != nil || v < 1 {
				writeJSONError(w, http.StatusBadRequest, "limit must be an integer >= 1")
				return
			}
			if v > exportMaxLimit {
				v = exportMaxLimit
			}
			limit = v
		}
		offset := 0
		if o := r.URL.Query().Get("offset"); o != "" {
			v, err := strconv.Atoi(o)
			if err != nil || v < 0 {
				writeJSONError(w, http.StatusBadRequest, "offset must be an integer >= 0")
				return
			}
			offset = v
		}
		format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
		if format == "" {
			format = "json"
		}
		if format != "json" && format != "ndjson" {
			writeJSONError(w, http.StatusBadRequest, "format must be json or ndjson")
			return
		}

		total := e.ExportTotal()
		count := 0
		if offset < total {
			count = total - offset
			if count > limit {
				count = limit
			}
		}
		w.Header().Set("X-Palspider-Count", strconv.Itoa(count))
		w.Header().Set("X-Palspider-Offset", strconv.Itoa(offset))
		w.Header().Set("X-Palspider-Limit", strconv.Itoa(limit))
		w.Header().Set("X-Palspider-Total", strconv.Itoa(total))

		if format == "ndjson" {
			w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			enc := json.NewEncoder(w)
			enc.SetEscapeHTML(false)
			if _, err := e.ExportEach(offset, limit, func(doc engine.ExportResult) error {
				return enc.Encode(doc)
			}); err != nil {
				// Headers are already committed, so a write failure can only
				// be logged rather than turned into an error response.
				log.Printf("export ndjson: %v", err)
			}
			return
		}

		docs, _ := e.ExportDocuments(offset, limit)
		if docs == nil {
			docs = []engine.ExportResult{}
		}
		writeJSON(w, http.StatusOK, docs)
	}
}

// handleSitemap streams an XML sitemap of indexed document URLs to the client.
// The optional host parameter restricts the output to one host and its
// subdomains, and limit caps the number of URLs (default and maximum
// sitemapMaxURLs). The document set is streamed rather than buffered, so a
// large index is written incrementally.
func handleSitemap(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host := strings.TrimSpace(r.URL.Query().Get("host"))
		limit := sitemapMaxURLs
		if l := r.URL.Query().Get("limit"); l != "" {
			v, err := strconv.Atoi(l)
			if err != nil || v < 1 {
				writeJSONError(w, http.StatusBadRequest, "limit must be an integer >= 1")
				return
			}
			if v < limit {
				limit = v
			}
		}

		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, xml.Header)
		io.WriteString(w, `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`+"\n")
		if _, err := e.SitemapEach(host, limit, func(rawURL string) error {
			_, werr := fmt.Fprintf(w, "  <url><loc>%s</loc></url>\n", xmlEscape(rawURL))
			return werr
		}); err != nil {
			// Headers are already committed, so a mid-stream write failure
			// can only be logged.
			log.Printf("sitemap: %v", err)
		}
		io.WriteString(w, "</urlset>\n")
	}
}

// xmlEscape escapes text for inclusion in an XML document.
func xmlEscape(s string) string {
	var b strings.Builder
	if err := xml.EscapeText(&b, []byte(s)); err != nil {
		return stdhtml.EscapeString(s)
	}
	return b.String()
}

func handleAPIPeersGossip(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, e.Peers.List())
	}
}
