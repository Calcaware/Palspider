package main

import (
	"context"
	"encoding/json"
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
	if strings.TrimSpace(body) != `["node"]` {
		t.Errorf("suggest q=Nod body = %s, want [\"node\"]", body)
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
	if len(s) != 1 || s[0] != "node" {
		t.Errorf("suggest = %v, want [node]", s)
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
	if code != 200 || !strings.Contains(body, "Invalid URL") {
		t.Errorf("GET /?crawl=invalid = %d, alert present = %v", code, strings.Contains(body, "Invalid URL"))
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
