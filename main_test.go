package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/calcaware/palspider/engine"
	"github.com/calcaware/palspider/searcher"
	"github.com/calcaware/palspider/store"
)

func newTestServer(t *testing.T) (*httptest.Server, func()) {
	ts, _, done := newTestServerE(t)
	return ts, done
}

func newTestServerE(t *testing.T) (*httptest.Server, *engine.Engine, func()) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	e := engine.New(2, 3, 0, st)
	if err := e.Restore(); err != nil {
		st.Close()
		t.Fatalf("restore: %v", err)
	}
	seed(e)
	ctx, cancel := context.WithCancel(context.Background())
	e.Start(ctx)
	ts := httptest.NewServer(newMux(e, parseTemplates()))
	return ts, e, func() {
		ts.Close()
		e.Stop()
		cancel()
		st.Close()
	}
}

func get(t *testing.T, rawURL string) (int, string, string) {
	t.Helper()
	resp, err := http.Get(rawURL)
	if err != nil {
		t.Fatalf("GET %s: %v", rawURL, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(b)
}

func getResp(t *testing.T, rawURL string) *http.Response {
	t.Helper()
	resp, err := http.Get(rawURL)
	if err != nil {
		t.Fatalf("GET %s: %v", rawURL, err)
	}
	return resp
}

func postJSON(t *testing.T, rawURL, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, rawURL, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", rawURL, err)
	}
	return resp
}

func deleteJSON(t *testing.T, rawURL, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, rawURL, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", rawURL, err)
	}
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response, v interface{}) {
	t.Helper()
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("decode %q: %v", b, err)
	}
}

func TestHomePage(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, ct, body := get(t, ts.URL+"/")
	if code != 200 {
		t.Errorf("GET / status = %d, want 200", code)
	}
	if !strings.Contains(ct, "text/html") {
		t.Errorf("GET / content-type = %q, want text/html", ct)
	}
	if !strings.Contains(body, "Palspider") {
		t.Error("GET / body missing app name")
	}
	if !strings.Contains(body, "Crawl frontier") {
		t.Error("GET / body missing the crawl frontier dialog")
	}
}

func TestNotFound(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, _, body := get(t, ts.URL+"/definitely/not/a/route")
	if code != 404 {
		t.Errorf("GET /definitely/not/a/route status = %d, want 404", code)
	}
	if !strings.Contains(body, "404") {
		t.Error("404 page body missing 404 marker")
	}
	code, _, _ = get(t, ts.URL+"/favicon.ico")
	if code != 404 {
		t.Errorf("GET /favicon.ico status = %d, want 404", code)
	}
}

func TestAPI404IsJSON(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, ct, body := get(t, ts.URL+"/api/unknown/thing")
	if code != 404 {
		t.Errorf("GET /api/unknown/thing status = %d, want 404", code)
	}
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("api 404 content-type = %q, want application/json", ct)
	}
	var e map[string]string
	if err := json.Unmarshal([]byte(body), &e); err != nil || e["error"] == "" {
		t.Errorf("api 404 body = %q, want {\"error\": ...}", body)
	}
}

func TestStaticAssets(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, ct, _ := get(t, ts.URL+"/palspider.png")
	if code != 200 || !strings.HasPrefix(ct, "image/png") {
		t.Errorf("GET /palspider.png = %d %q, want 200 image/png", code, ct)
	}
	code, ct, _ = get(t, ts.URL+"/style.css")
	if code != 200 || !strings.HasPrefix(ct, "text/css") {
		t.Errorf("GET /style.css = %d %q, want 200 text/css", code, ct)
	}
}

func TestSuggest(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, ct, body := get(t, ts.URL+"/suggest?q=Nod")
	if code != 200 {
		t.Errorf("GET /suggest?q=Nod status = %d, want 200", code)
	}
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("suggest content-type = %q, want application/json", ct)
	}
	var s []string
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatalf("suggest body: %v", err)
	}
	if len(s) == 0 || s[0] != "node" {
		t.Errorf("suggest q=Nod = %v, want node ranked first", s)
	}
	if !contains(s, "nodejs.org") {
		t.Errorf("suggest q=Nod = %v, want the seeded nodejs.org domain hint", s)
	}
	_, ct, _ = get(t, ts.URL+"/suggest")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("suggest with no q content-type = %q, want application/json", ct)
	}
}

func TestSuggestWithLimit(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, _, body := get(t, ts.URL+"/suggest?q=node&limit=2")
	if code != 200 {
		t.Errorf("GET /suggest?q=node&limit=2 status = %d", code)
	}
	var s []string
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatalf("suggest body: %v", err)
	}
	if len(s) != 2 || s[0] != "node" {
		t.Errorf("suggest = %v, want node first with one domain hint", s)
	}
	code, _, body = get(t, ts.URL+"/suggest?q=node&limit=foo")
	if code != 400 {
		t.Errorf("GET /suggest?limit=foo status = %d, want 400", code)
	}
	if !strings.Contains(body, "limit must be an integer") {
		t.Errorf("limit error = %q", body)
	}
	code, _, _ = get(t, ts.URL+"/suggest?q=ab")
	if code != 200 {
		t.Errorf("short query status = %d, want 200 (returns empty slice)", code)
	}
}

func TestSuggestMultiWordAndDomainHints(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()

	// Multi-word completion: only pages that already contain "search" can
	// finish the phrase, so "search eng" should surface "search engine".
	code, _, body := get(t, ts.URL+"/suggest?q="+url.QueryEscape("search eng"))
	if code != 200 {
		t.Fatalf("GET /suggest multi-word status = %d, want 200", code)
	}
	var s []string
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatalf("multi-word suggest body: %v", err)
	}
	if !contains(s, "search engine") {
		t.Fatalf("multi-word suggest = %v, want search engine", s)
	}

	// Domain hint: "node" should suggest the nodejs.org host.
	_, _, body = get(t, ts.URL+"/suggest?q=node")
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatalf("domain suggest body: %v", err)
	}
	if !contains(s, "nodejs.org") {
		t.Fatalf("domain suggest = %v, want nodejs.org", s)
	}
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func TestPublicRateLimit(t *testing.T) {
	t.Setenv("PALSPIDER_PUBLIC_RATE_LIMIT", "1")
	ts, done := newTestServer(t)
	defer done()

	resp := getResp(t, ts.URL+"/api/search?q=node")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first /api/search status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Palspider-Version"); got == "" {
		t.Fatal("first /api/search missing X-Palspider-Version header")
	}
	resp.Body.Close()

	resp = getResp(t, ts.URL+"/api/search?q=node")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second /api/search status = %d, want 429", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "rate limit exceeded") {
		t.Fatalf("rate limit body = %q, want rate limit exceeded", body)
	}
}
func TestAPIHealth(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, ct, body := get(t, ts.URL+"/api/health")
	if code != 200 || !strings.HasPrefix(ct, "application/json") {
		t.Errorf("GET /api/health = %d %q", code, ct)
	}
	var h struct {
		Status  string `json:"status"`
		App     string `json:"app"`
		Version string `json:"version"`
	}
	if err := json.Unmarshal([]byte(body), &h); err != nil {
		t.Fatalf("health body: %v", err)
	}
	if h.Status != "ok" || h.App != "palspider" || h.Version == "" {
		t.Errorf("health %+v, want ok/palspider/non-empty version", h)
	}
}

func TestAPIStatus(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, _, body := get(t, ts.URL+"/api/status")
	if code != 200 {
		t.Fatalf("GET /api/status status = %d, want 200", code)
	}
	var s struct {
		App       string `json:"app"`
		Version   string `json:"version"`
		Documents int    `json:"documents"`
		Terms     int    `json:"terms"`
		Visited   int    `json:"visited"`
		Peers     int    `json:"peers"`
		QueueSize int32  `json:"queueSize"`
		Active    int32  `json:"active"`
	}
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatalf("api/status body: %v", err)
	}
	if s.App != "palspider" {
		t.Errorf("app = %q, want palspider", s.App)
	}
	if s.Version == "" {
		t.Error("version field missing from /api/status")
	}
	if s.Documents != 7 {
		t.Errorf("documents = %d, want 7", s.Documents)
	}
	if s.Visited != s.Documents {
		t.Errorf("visited = %d, documents = %d, want equal", s.Visited, s.Documents)
	}
	if s.Terms <= 0 {
		t.Errorf("terms = %d, want > 0", s.Terms)
	}
	if s.Peers != 0 {
		t.Errorf("peers = %d, want 0", s.Peers)
	}
}

func TestSearchResults(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, _, body := get(t, ts.URL+"/search?q=node")
	if code != 200 {
		t.Errorf("GET /search status = %d, want 200", code)
	}
	if !strings.Contains(body, "results for") {
		t.Error("search page missing results marker")
	}
	if !strings.Contains(body, "Node.js") {
		t.Error("search page missing case-preserved title Node.js")
	}
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Get(ts.URL + "/search")
	if err != nil {
		t.Fatalf("GET /search: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Errorf("GET /search with no q status = %d, want 303", resp.StatusCode)
	}
}

func TestSearchPagination(t *testing.T) {
	ts, e, done := newTestServerE(t)
	defer done()
	for i := 1; i <= 12; i++ {
		e.AddDocument("https://example.com/p/"+string(rune('a'+i)), "Page", "zettapage term")
	}
	code, _, body := get(t, ts.URL+"/search?q=zettapage")
	if code != 200 {
		t.Fatalf("page 1 status = %d", code)
	}
	if !strings.Contains(body, "page 1") {
		t.Error("page 1 marker missing")
	}
	if !strings.Contains(body, "Next") {
		t.Error("page 1 missing Next link")
	}
	if strings.Contains(body, "Previous") {
		t.Error("page 1 should not show Previous")
	}

	code, _, body = get(t, ts.URL+"/search?q=zettapage&page=2")
	if code != 200 {
		t.Fatalf("page 2 status = %d", code)
	}
	if !strings.Contains(body, "page 2") {
		t.Error("page 2 marker missing")
	}
	if !strings.Contains(body, "Previous") {
		t.Error("page 2 missing Previous link")
	}

	code, _, body = get(t, ts.URL+"/search?q=zettapage&page=3")
	if code != 200 {
		t.Fatalf("page 3 status = %d", code)
	}
	if !strings.Contains(body, "No results") {
		t.Error("page 3 should show no results (12 docs, 10 per page)")
	}

	code, _, body = get(t, ts.URL+"/search?q=zettapage&page=999")
	if code != 200 {
		t.Fatalf("clamped page status = %d", code)
	}
	if !strings.Contains(body, "page 10") {
		t.Error("page 999 should clamp to page 10")
	}
}

func TestCrawlSeed(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()

	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.PostForm(ts.URL+"/crawl", url.Values{
		"url": {"http://127.0.0.1:9/testpage"},
	})
	if err != nil {
		t.Fatalf("POST /crawl: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Errorf("POST /crawl status = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/?crawl=started" {
		t.Errorf("POST /crawl location = %q, want /?crawl=started", loc)
	}

	code, _, body := get(t, ts.URL+"/?crawl=started")
	if code != 200 || !strings.Contains(body, "Crawl started") {
		t.Errorf("GET /?crawl=started = %d, alert present = %v", code, strings.Contains(body, "Crawl started"))
	}

	resp, err = noRedirect.PostForm(ts.URL+"/crawl", url.Values{
		"url": {"not-a-url"},
	})
	if err != nil {
		t.Fatalf("POST /crawl invalid: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Errorf("POST /crawl invalid status = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/?crawl=invalid" {
		t.Errorf("POST /crawl invalid location = %q, want /?crawl=invalid", loc)
	}
	code, _, body = get(t, ts.URL+"/?crawl=invalid")
	if code != 200 || !strings.Contains(body, "URL rejected") {
		t.Errorf("GET /?crawl=invalid = %d, alert present = %v", code, strings.Contains(body, "URL rejected"))
	}

	deadline := time.Now().Add(3 * time.Second)
	var status struct {
		QueueSize int `json:"queueSize"`
		Active    int `json:"active"`
		Visited   int `json:"visited"`
		Documents int `json:"documents"`
	}
	for {
		_, _, body = get(t, ts.URL+"/crawl-status")
		if json.Unmarshal([]byte(body), &status) != nil {
			t.Fatalf("crawl-status body: %s", body)
		}
		if status.Visited >= 8 && status.QueueSize == 0 && status.Active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("crawl-status did not settle: %+v", status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if status.Documents != 7 {
		t.Errorf("documents = %d, want 7 (blocked private fetch must not index)", status.Documents)
	}
}

func TestAPICrawl(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	resp := postJSON(t, ts.URL+"/api/crawl", `{"url":"https://127.0.0.1:9/Path/"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Errorf("POST /api/crawl status = %d, want 202 (body %s)", resp.StatusCode, b)
	}
	var m map[string]string
	decodeJSON(t, resp, &m)
	if m["status"] != "accepted" || !strings.Contains(m["url"], "127.0.0.1:9/Path") {
		t.Errorf("api/crawl response = %+v", m)
	}

	resp = postJSON(t, ts.URL+"/api/crawl", `{"url":"not-a-url"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("POST /api/crawl invalid status = %d, want 400", resp.StatusCode)
	}
	var e1 map[string]string
	decodeJSON(t, resp, &e1)
	if e1["error"] == "" {
		t.Error("invalid crawl response missing error field")
	}

	resp = postJSON(t, ts.URL+"/api/crawl", `{not json`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("POST /api/crawl bad JSON status = %d, want 400", resp.StatusCode)
	}

	code, _, body := get(t, ts.URL+"/api/crawl")
	if code != 200 {
		t.Errorf("GET /api/crawl status = %d, want 200", code)
	}
	var st map[string]interface{}
	if json.Unmarshal([]byte(body), &st) != nil {
		t.Fatalf("GET /api/crawl body: %s", body)
	}
	if _, ok := st["queueSize"]; !ok {
		t.Errorf("GET /api/crawl missing queueSize: %s", body)
	}
}

func TestAPICrawlQueue(t *testing.T) {
	ts, e, done := newTestServerE(t)
	defer done()

	e.Pause()
	e.Seed("https://example.com/a")
	e.Seed("https://other.example.org/b")

	resp := getResp(t, ts.URL+"/api/crawl/queue?limit=1")
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("GET /api/crawl/queue status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Palspider-Total"); got != "2" {
		t.Errorf("X-Palspider-Total = %q, want 2", got)
	}
	if got := resp.Header.Get("X-Palspider-Count"); got != "1" {
		t.Errorf("X-Palspider-Count = %q, want 1", got)
	}
	var snap engine.QueueSnapshot
	decodeJSON(t, resp, &snap)
	if snap.Total != 2 || snap.Queued != 2 || snap.Retrying != 0 || len(snap.Items) != 1 {
		t.Fatalf("queue snapshot = %+v, want 2 queued and one item on the page", snap)
	}
	if snap.Items[0].Host != "example.com" {
		t.Errorf("first item host = %q, want example.com", snap.Items[0].Host)
	}
	if snap.Hosts["example.com"].Queued != 1 || snap.Hosts["other.example.org"].Queued != 1 {
		t.Errorf("host backlog = %+v, want one queued URL per host", snap.Hosts)
	}

	resp = getResp(t, ts.URL+"/api/crawl/queue?host=example.com")
	decodeJSON(t, resp, &snap)
	if snap.Total != 1 || len(snap.Items) != 1 || snap.Items[0].Host != "example.com" {
		t.Errorf("host filter snapshot = %+v, want only example.com", snap)
	}

	for _, query := range []string{"limit=0", "limit=abc", "offset=-1", "offset=abc"} {
		code, _, body := get(t, ts.URL+"/api/crawl/queue?"+query)
		if code != http.StatusBadRequest {
			t.Errorf("GET /api/crawl/queue?%s = %d %q, want 400", query, code, body)
		}
	}
}

func TestAPICrawlAppliesExclusions(t *testing.T) {
	t.Setenv("PALSPIDER_EXCLUDE_PATHS", "/search,/login")
	ts, done := newTestServer(t)
	defer done()

	resp := postJSON(t, ts.URL+"/api/crawl", `{"url":"http://127.0.0.1:9/search?q=golang"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("POST /api/crawl excluded status = %d, want 400", resp.StatusCode)
	}
	var rejected map[string]string
	decodeJSON(t, resp, &rejected)
	if !strings.Contains(rejected["error"], "excluded") {
		t.Errorf("excluded crawl error = %q, want the exclusion reason", rejected["error"])
	}

	// Campaign parameters are dropped before the URL is queued.
	resp = postJSON(t, ts.URL+"/api/crawl", `{"url":"http://127.0.0.1:9/page?utm_source=news&id=7"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("POST /api/crawl with tracking params status = %d, want 202", resp.StatusCode)
	}
	var accepted map[string]string
	decodeJSON(t, resp, &accepted)
	if accepted["url"] != "http://127.0.0.1:9/page?id=7" {
		t.Errorf("seeded url = %q, want tracking parameters stripped", accepted["url"])
	}
}

func TestAPISearchValidation(t *testing.T) {
	ts, e, done := newTestServerE(t)
	defer done()
	e.AddDocument("https://example.com/a", "A", "alpha beta")
	e.AddDocument("https://example.com/b", "B", "alpha")

	code, ct, body := get(t, ts.URL+"/api/search?q=alpha&limit=1")
	if code != 200 || !strings.HasPrefix(ct, "application/json") {
		t.Errorf("GET /api/search?q=alpha&limit=1 = %d %q", code, ct)
	}
	var res []searcher.Result
	if json.Unmarshal([]byte(body), &res) != nil {
		t.Fatalf("search body: %v", body)
	}
	if len(res) != 1 {
		t.Errorf("results = %d, want 1", len(res))
	}

	code, _, body = get(t, ts.URL+"/api/search?q=alpha&limit=1&offset=1")
	if code != 200 {
		t.Errorf("GET with offset status = %d", code)
	}
	if json.Unmarshal([]byte(body), &res) != nil {
		t.Fatalf("offset body: %v", body)
	}
	if len(res) != 1 {
		t.Errorf("offset results = %d, want 1", len(res))
	}

	code, ct, body = get(t, ts.URL+"/api/search?q=alpha&limit=1&offset=1")
	if code != 200 || !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("GET /api/search pagination response = %d %q, want 200 JSON", code, ct)
	}
	if !strings.Contains(body, "\"url\":\"https://example.com/b\"") {
		t.Fatalf("offset page body = %q, want second result", body)
	}

	code, _, body = get(t, ts.URL+"/api/search?q=alpha&offset=100")
	if code != 400 || !strings.Contains(body, "offset must be less than 100") {
		t.Errorf("offset 100 = %d %q, want 400", code, body)
	}
	code, _, body = get(t, ts.URL+"/api/search?q=alpha&offset=-1")
	if code != 400 {
		t.Errorf("offset -1 = %d, want 400", code)
	}
	code, _, body = get(t, ts.URL+"/api/search?q=alpha&limit=0")
	if code != 400 || !strings.Contains(body, "limit must be an integer >= 1") {
		t.Errorf("limit 0 = %d %q, want 400", code, body)
	}
	code, _, _ = get(t, ts.URL+"/api/search?q=alpha&limit=abc")
	if code != 400 {
		t.Errorf("limit abc = %d, want 400", code)
	}
	code, _, body = get(t, ts.URL+"/api/search?q=")
	if code != 400 || !strings.Contains(body, "query required") {
		t.Errorf("empty query = %d %q, want 400 query required", code, body)
	}

	code, _, _ = get(t, ts.URL+"/api/search?q=alpha&limit=1000")
	if code != 200 {
		t.Errorf("limit 1000 (clamp) = %d, want 200", code)
	}

	code, _, body = get(t, ts.URL+"/api/search?q=nomatchzqx")
	if code != 200 || strings.TrimSpace(body) != "[]" {
		t.Errorf("no-match body = %q, want []", body)
	}
}

func TestAPIPeers(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	code, _, body := get(t, ts.URL+"/api/peers")
	if code != 200 || strings.TrimSpace(body) != "[]" {
		t.Errorf("GET /api/peers = %d %q, want 200 []", code, body)
	}

	resp := postJSON(t, ts.URL+"/api/peers", `{"url":"ftp://bad.example/"}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("POST /api/peers ftp scheme = %d, want 400", resp.StatusCode)
	}

	resp = postJSON(t, ts.URL+"/api/peers", `{"url":""}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("POST /api/peers empty url = %d, want 400", resp.StatusCode)
	}

	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/status":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"app":"palspider"}`)
		case "/api/peers/gossip":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `[]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer peerSrv.Close()

	resp = postJSON(t, ts.URL+"/api/peers", `{"url":"`+peerSrv.URL+`"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Errorf("POST /api/peers valid = %d, want 200 (body %s)", resp.StatusCode, b)
	}
	var add map[string]string
	decodeJSON(t, resp, &add)
	if add["status"] != "ok" {
		t.Errorf("add response = %+v", add)
	}

	code, _, body = get(t, ts.URL+"/api/peers")
	if code != 200 || !strings.Contains(body, peerSrv.URL) {
		t.Errorf("GET /api/peers after add = %d %q", code, body)
	}

	resp = deleteJSON(t, ts.URL+"/api/peers", `{"url":"https://unknown.example"}`)
	var rm map[string]interface{}
	decodeJSON(t, resp, &rm)
	if rm["removed"] != false {
		t.Errorf("remove unknown = %+v, want removed=false", rm)
	}

	resp = deleteJSON(t, ts.URL+"/api/peers", `{"url":""}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("remove empty url = %d, want 400", resp.StatusCode)
	}

	resp = deleteJSON(t, ts.URL+"/api/peers", `{"url":"`+peerSrv.URL+`"}`)
	decodeJSON(t, resp, &rm)
	if rm["removed"] != true {
		t.Errorf("remove existing = %+v, want removed=true", rm)
	}
	code, _, body = get(t, ts.URL+"/api/peers")
	if code != 200 || strings.TrimSpace(body) != "[]" {
		t.Errorf("GET /api/peers after remove = %d %q, want []", code, body)
	}
}

func TestCORSOnlyOnReads(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()
	resp := getResp(t, ts.URL+"/api/status")
	resp.Body.Close()
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("GET /api/status ACAO = %q, want *", resp.Header.Get("Access-Control-Allow-Origin"))
	}
	resp = getResp(t, ts.URL+"/suggest?q=node")
	resp.Body.Close()
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("GET /suggest ACAO = %q, want *", resp.Header.Get("Access-Control-Allow-Origin"))
	}

	resp = postJSON(t, ts.URL+"/api/crawl", `{"url":"https://example.com/"}`)
	resp.Body.Close()
	if acao := resp.Header.Get("Access-Control-Allow-Origin"); acao != "" {
		t.Errorf("POST /api/crawl ACAO = %q, want empty", acao)
	}
	resp = postJSON(t, ts.URL+"/api/peers", `{"url":"https://peer.example"}`)
	resp.Body.Close()
	if acao := resp.Header.Get("Access-Control-Allow-Origin"); acao != "" {
		t.Errorf("POST /api/peers ACAO = %q, want empty", acao)
	}

	resp = getResp(t, ts.URL+"/")
	resp.Body.Close()
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", resp.Header.Get("X-Content-Type-Options"))
	}
}

func TestCrawlPauseResumeAndHostStats(t *testing.T) {
	ts, _, done := newTestServerE(t)
	defer done()

	resp := postJSON(t, ts.URL+"/api/crawl/pause", `{}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/crawl/pause status = %d, want 200", resp.StatusCode)
	}
	var paused map[string]interface{}
	decodeJSON(t, resp, &paused)
	if paused["paused"] != true {
		t.Fatalf("pause response = %#v, want paused=true", paused)
	}

	code, _, body := get(t, ts.URL+"/api/status")
	if code != 200 {
		t.Fatalf("GET /api/status = %d, want 200", code)
	}
	if !strings.Contains(body, `"paused":true`) {
		t.Fatalf("status body = %s, want paused true", body)
	}

	resp = postJSON(t, ts.URL+"/api/crawl/resume", `{}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/crawl/resume status = %d, want 200", resp.StatusCode)
	}
	var resumed map[string]interface{}
	decodeJSON(t, resp, &resumed)
	if resumed["paused"] != false {
		t.Fatalf("resume response = %#v, want paused=false", resumed)
	}

	code, _, body = get(t, ts.URL+"/api/status")
	if code != 200 {
		t.Fatalf("GET /api/status after resume = %d, want 200", code)
	}
	if !strings.Contains(body, `"paused":false`) {
		t.Fatalf("status body after resume = %s, want paused false", body)
	}

}

func TestCrawlClearAndRequeue(t *testing.T) {
	ts, e, done := newTestServerE(t)
	defer done()

	e.Seed("https://example.com")
	resp := postJSON(t, ts.URL+"/api/crawl/clear", `{}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/crawl/clear status = %d, want 200", resp.StatusCode)
	}
	var cleared map[string]interface{}
	decodeJSON(t, resp, &cleared)
	if cleared["queueSize"] != float64(0) {
		t.Fatalf("clear response = %#v, want queueSize 0", cleared)
	}

	resp = postJSON(t, ts.URL+"/api/crawl/requeue", `{"url":"https://example.com"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /api/crawl/requeue status = %d, want 202", resp.StatusCode)
	}
	var requeued map[string]interface{}
	decodeJSON(t, resp, &requeued)
	if requeued["status"] != "accepted" {
		t.Fatalf("requeue response = %#v, want accepted", requeued)
	}
}

func TestPeerTokenGuardsPeerTraffic(t *testing.T) {
	t.Setenv("PALSPIDER_PEER_TOKEN", "peer-secret")
	req := httptest.NewRequest(http.MethodGet, "/api/peers/gossip", nil)
	rec := httptest.NewRecorder()
	wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/peers/gossip without bearer token = %d, want 401", rec.Code)
	}
	req.Header.Set("Authorization", "Bearer peer-secret")
	rec = httptest.NewRecorder()
	wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/peers/gossip with bearer token = %d, want 200", rec.Code)
	}
}

func TestFilterSearchResults(t *testing.T) {
	results := []searcher.Result{
		{URL: "https://docs.example.com/guide/install", Title: "Install guide"},
		{URL: "https://example.com/blog/post", Title: "Blog post"},
		{URL: "https://sub.example.com/help", Title: "Help"},
	}

	filtered := filterSearchResults(results, url.Values{"host": {"example.com"}})
	if len(filtered) != 3 {
		t.Fatalf("host filter = %d results, want 3 (exact host + subdomains)", len(filtered))
	}
	if filtered[0].URL != "https://docs.example.com/guide/install" {
		t.Fatalf("host filter returned %#v, want docs.example.com entry first", filtered)
	}

	filtered = filterSearchResults(results, url.Values{"path": {"/blog"}})
	if len(filtered) != 1 || filtered[0].URL != "https://example.com/blog/post" {
		t.Fatalf("path filter = %#v, want only blog entry", filtered)
	}
}

func TestAPIRelated(t *testing.T) {
	ts, e, done := newTestServerE(t)
	defer done()
	e.AddDocument("https://related.example/source", "Crawler search", "crawler search indexing ranking")
	e.AddDocument("https://related.example/match", "Search indexing", "crawler search indexing tools")

	code, _, body := get(t, ts.URL+"/api/related?url="+url.QueryEscape("https://related.example/source"))
	if code != http.StatusOK {
		t.Fatalf("GET /api/related status = %d, want 200: %s", code, body)
	}
	var results []searcher.Result
	if err := json.Unmarshal([]byte(body), &results); err != nil {
		t.Fatalf("decode related response: %v", err)
	}
	foundMatch := false
	for _, result := range results {
		if result.URL == "https://related.example/match" {
			foundMatch = true
		}
		if result.URL == "https://related.example/source" {
			t.Fatal("related response included the source document")
		}
	}
	if !foundMatch {
		t.Fatalf("related response = %#v, missing matching page", results)
	}

	code, _, body = get(t, ts.URL+"/api/related?url=not-a-url")
	if code != http.StatusBadRequest || !strings.Contains(body, "absolute http or https URL") {
		t.Fatalf("invalid related URL response = %d %q, want 400 validation error", code, body)
	}
}

func TestAPIExport(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()

	resp := getResp(t, ts.URL+"/api/export")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/export status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("export content-type = %q, want application/json", ct)
	}
	if got := resp.Header.Get("X-Palspider-Total"); got != "7" {
		t.Fatalf("X-Palspider-Total = %q, want 7", got)
	}
	if got := resp.Header.Get("X-Palspider-Count"); got != "7" {
		t.Fatalf("X-Palspider-Count = %q, want 7", got)
	}

	type doc struct {
		ID        int    `json:"id"`
		URL       string `json:"url"`
		Title     string `json:"title"`
		Text      string `json:"text"`
		LinkCount int    `json:"linkCount"`
	}
	var all []doc
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		t.Fatalf("decode export: %v", err)
	}
	if len(all) != 7 {
		t.Fatalf("export = %d documents, want 7", len(all))
	}
	for i, d := range all {
		if d.URL == "" || d.Title == "" {
			t.Fatalf("export[%d] missing url/title: %+v", i, d)
		}
		if i > 0 && d.ID <= all[i-1].ID {
			t.Fatalf("export not in ascending ID order at %d: %d after %d", i, d.ID, all[i-1].ID)
		}
	}

	code, _, body := get(t, ts.URL+"/api/export?limit=2&offset=1")
	if code != http.StatusOK {
		t.Fatalf("GET /api/export paginated status = %d, want 200", code)
	}
	var page []doc
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	if len(page) != 2 || page[0].ID != 1 || page[1].ID != 2 {
		t.Fatalf("paged export = %+v, want IDs 1 and 2", page)
	}

	if _, _, body = get(t, ts.URL+"/api/export?offset=99"); strings.TrimSpace(body) != "[]" {
		t.Fatalf("offset past end body = %q, want []", body)
	}

	if code, _, body = get(t, ts.URL+"/api/export?limit=0"); code != 400 || !strings.Contains(body, "limit must be an integer >= 1") {
		t.Fatalf("limit=0 = %d %q, want 400", code, body)
	}
	if code, _, _ = get(t, ts.URL+"/api/export?limit=abc"); code != 400 {
		t.Fatalf("limit=abc = %d, want 400", code)
	}
	if code, _, body = get(t, ts.URL+"/api/export?offset=-1"); code != 400 || !strings.Contains(body, "offset must be an integer >= 0") {
		t.Fatalf("offset=-1 = %d %q, want 400", code, body)
	}
	if code, _, _ = get(t, ts.URL+"/api/export?limit=100000"); code != 200 {
		t.Fatalf("limit=100000 = %d, want 200 (clamped)", code)
	}
}

func TestAPIExportNDJSON(t *testing.T) {
	ts, done := newTestServer(t)
	defer done()

	resp := getResp(t, ts.URL+"/api/export?format=ndjson")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/export?format=ndjson status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/x-ndjson") {
		t.Fatalf("ndjson content-type = %q, want application/x-ndjson", ct)
	}
	if got := resp.Header.Get("X-Palspider-Total"); got != "7" {
		t.Fatalf("X-Palspider-Total = %q, want 7", got)
	}
	if got := resp.Header.Get("X-Palspider-Count"); got != "7" {
		t.Fatalf("X-Palspider-Count = %q, want 7", got)
	}
	body, _ := io.ReadAll(resp.Body)
	text := strings.TrimRight(string(body), "\n")
	if strings.HasPrefix(strings.TrimSpace(text), "[") {
		t.Fatalf("ndjson body must not be a JSON array: %q", text)
	}
	lines := strings.Split(text, "\n")
	if len(lines) != 7 {
		t.Fatalf("ndjson lines = %d, want 7 (%q)", len(lines), text)
	}
	prevID := -1
	for i, line := range lines {
		var doc struct {
			ID    int    `json:"id"`
			URL   string `json:"url"`
			Title string `json:"title"`
		}
		if err := json.Unmarshal([]byte(line), &doc); err != nil {
			t.Fatalf("line %d is not valid JSON: %v (%q)", i, err, line)
		}
		if doc.URL == "" || doc.Title == "" {
			t.Fatalf("line %d missing url/title: %q", i, line)
		}
		if doc.ID <= prevID {
			t.Fatalf("line %d id %d not ascending after %d", i, doc.ID, prevID)
		}
		prevID = doc.ID
	}

	resp = getResp(t, ts.URL+"/api/export?format=ndjson&limit=2&offset=1")
	body, _ = io.ReadAll(resp.Body)
	if got := resp.Header.Get("X-Palspider-Count"); got != "2" {
		t.Fatalf("paged X-Palspider-Count = %q, want 2", got)
	}
	resp.Body.Close()
	lines = strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("paged ndjson lines = %d, want 2 (%q)", len(lines), body)
	}
	var first struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil || first.ID != 1 {
		t.Fatalf("paged ndjson first id = %d (err %v), want 1", first.ID, err)
	}

	resp = getResp(t, ts.URL+"/api/export?format=ndjson&offset=99")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(strings.TrimSpace(string(body))) != 0 {
		t.Fatalf("out-of-range ndjson body = %q, want empty", body)
	}

	if code, _, body := get(t, ts.URL+"/api/export?format=xml"); code != http.StatusBadRequest ||
		!strings.Contains(body, "format must be json or ndjson") {
		t.Fatalf("bad format = %d %q, want 400 validation error", code, body)
	}
}

func TestSitemapEndpoint(t *testing.T) {
	ts, e, done := newTestServerE(t)
	defer done()
	e.AddDocument("https://sitemap.example/page?a=1&b=2", "Query page", "sitemap query content")

	code, ct, body := get(t, ts.URL+"/sitemap.xml")
	if code != http.StatusOK {
		t.Fatalf("GET /sitemap.xml status = %d, want 200", code)
	}
	if !strings.HasPrefix(ct, "application/xml") {
		t.Fatalf("sitemap content-type = %q, want application/xml", ct)
	}
	if !strings.HasPrefix(body, xml.Header) {
		t.Fatalf("sitemap must start with the XML declaration, got %q", body[:min(len(body), 60)])
	}
	if !strings.Contains(body, `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`) {
		t.Fatalf("sitemap missing urlset root: %s", body)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "</urlset>") {
		t.Fatalf("sitemap must close the urlset element: %s", body)
	}
	if !strings.Contains(body, "<loc>https://nodejs.org/</loc>") {
		t.Fatalf("sitemap missing seeded document: %s", body)
	}
	// Query separators must be XML-escaped inside <loc>.
	if !strings.Contains(body, "a=1&amp;b=2") || strings.Contains(body, "a=1&b=2") {
		t.Fatalf("sitemap did not escape the ampersand: %s", body)
	}
	// Every URL is wrapped in a <loc> element.
	if counts := strings.Count(body, "<loc>"); counts != 8 {
		t.Fatalf("sitemap emitted %d URLs, want 8", counts)
	}

	code, _, body = get(t, ts.URL+"/sitemap.xml?host=nodejs.org")
	if code != http.StatusOK {
		t.Fatalf("GET /sitemap.xml?host nodejs.org status = %d, want 200", code)
	}
	if !strings.Contains(body, "<loc>https://nodejs.org/</loc>") {
		t.Fatalf("host-scoped sitemap missing nodejs.org: %s", body)
	}
	if strings.Contains(body, "expressjs.com") {
		t.Fatalf("host-scoped sitemap leaked another host: %s", body)
	}

	code, _, body = get(t, ts.URL+"/sitemap.xml?limit=2")
	if code != http.StatusOK {
		t.Fatalf("GET /sitemap.xml?limit=2 status = %d, want 200", code)
	}
	if counts := strings.Count(body, "<loc>"); counts != 2 {
		t.Fatalf("limited sitemap emitted %d URLs, want 2", counts)
	}

	code, _, body = get(t, ts.URL+"/sitemap.xml?limit=abc")
	if code != http.StatusBadRequest || !strings.Contains(body, "limit must be an integer") {
		t.Fatalf("bad sitemap limit = %d %q, want 400 validation error", code, body)
	}
}

func TestHighlightSnippetSelectsRelevantTextAndEscapesHTML(t *testing.T) {
	got := string(highlightSnippet("opening text with unrelated words, then Search Engine appears near the end", "search engine", 42))
	if !strings.Contains(got, "<mark>Search</mark> <mark>Engine</mark>") {
		t.Fatalf("snippet does not highlight the query terms: %s", got)
	}
	if !strings.HasPrefix(got, "...") {
		t.Fatalf("snippet should select a window around the query: %s", got)
	}
	got = string(highlightSnippet("<script>alert(1)</script>", "alert", 200))
	if !strings.Contains(got, "&lt;script&gt;") || strings.Contains(got, "<script>") {
		t.Fatalf("snippet did not escape HTML content: %s", got)
	}
}

func TestBasicAuthGuardsMutations(t *testing.T) {
	t.Setenv("PALSPIDER_AUTH", "alice:s3cret")
	ts, done := newTestServer(t)
	defer done()

	code, _, body := get(t, ts.URL+"/api/status")
	if code != 200 {
		t.Errorf("GET /api/status = %d, want 200 (reads stay open)", code)
	}
	_ = body

	resp := postJSON(t, ts.URL+"/api/crawl", `{"url":"https://example.com/"}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST /api/crawl without creds = %d, want 401", resp.StatusCode)
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Error("401 missing WWW-Authenticate header")
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/crawl", strings.NewReader(`{"url":"https://example.com/"}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("alice", "wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST with wrong creds: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST with wrong creds = %d, want 401", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/api/crawl", strings.NewReader(`{"url":"https://127.0.0.1:9/"}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("alice", "s3cret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST with correct creds: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("POST with correct creds = %d, want 202", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/api/peers", strings.NewReader(`{"url":"https://x.example"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE without creds: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("DELETE without creds = %d, want 401", resp.StatusCode)
	}
}
