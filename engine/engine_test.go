package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/calcaware/palspider/crawler"
	"github.com/calcaware/palspider/searcher"
)

func TestMergeResultsDedupesAndSorts(t *testing.T) {
	local := []searcher.Result{
		{URL: "https://a.com", Score: 1.0},
		{URL: "https://b.com", Score: 3.0},
	}
	remote := []searcher.Result{
		{URL: "https://b.com", Score: 99.0, PeerSource: "peer"},
		{URL: "https://c.com", Score: 2.0, PeerSource: "peer"},
	}

	merged := mergeResults(local, remote, 10)
	if len(merged) != 3 {
		t.Fatalf("merged = %d results, want 3 (deduped by URL)", len(merged))
	}
	if merged[0].URL != "https://b.com" || merged[0].PeerSource != "" {
		t.Errorf("top = %+v, want local copy of b.com to win", merged[0])
	}
	if merged[1].URL != "https://c.com" || merged[2].URL != "https://a.com" {
		t.Errorf("order = %v %v, want c.com then a.com", merged[1].URL, merged[2].URL)
	}
}

func TestMergeResultsLimit(t *testing.T) {
	local := []searcher.Result{
		{URL: "a", Score: 3},
		{URL: "b", Score: 2},
		{URL: "c", Score: 1},
	}
	if got := mergeResults(local, nil, 2); len(got) != 2 {
		t.Errorf("limit 2 got %d results", len(got))
	}
	if got := mergeResults(local, nil, 0); got != nil {
		t.Errorf("limit 0 = %v, want nil", got)
	}
}

func TestAddDocumentMarksVisitedAndDedupes(t *testing.T) {
	e := New(1, 10, 0, nil)

	e.AddDocument("https://Example.com/page", "First", "first document text")
	e.AddDocument("https://example.com/page/", "Second", "second document text here")

	if got := e.Indexer.DocCount(); got != 1 {
		t.Errorf("DocCount = %d, want 1 (same normalized URL must not re-index)", got)
	}
	if got := e.Stats().Visited; got != 1 {
		t.Errorf("Visited = %d, want 1 (AddDocument must mark the URL visited)", got)
	}
}

func TestSeedSkipsDuplicatesAndQueue(t *testing.T) {
	e := New(1, 10, 0, nil)

	e.Seed("https://example.com")
	e.Seed("https://example.com/")
	e.Seed("HTTPS://EXAMPLE.COM")

	if len(e.urlChan) != 1 {
		t.Errorf("queue = %d items, want 1 (seeds must dedupe after normalization)", len(e.urlChan))
	}
	if e.Stats().Visited != 1 {
		t.Errorf("Visited = %d, want 1", e.Stats().Visited)
	}
}

func TestStatsUsesTermCount(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.AddDocument("https://a.com", "A", "alpha beta gamma")
	s := e.Stats()
	if s.Documents != 1 {
		t.Errorf("Documents = %d, want 1", s.Documents)
	}
	if s.Terms != 3 {
		t.Errorf("Terms = %d, want 3", s.Terms)
	}
	if s.Visited != 1 {
		t.Errorf("Visited = %d, want 1", s.Visited)
	}
}

func TestEnqueueAllRollsBackPendingWhenQueueFull(t *testing.T) {
	e := New(1, 10, 0, nil)
	for i := 0; i < cap(e.urlChan); i++ {
		e.urlChan <- crawlItem{url: "filler"}
	}

	const target = "https://example.com/full"
	e.mu.Lock()
	e.pending[target] = struct{}{}
	e.mu.Unlock()

	e.enqueueAll(context.Background(), []crawlItem{{url: target, depth: 1}})

	e.mu.Lock()
	_, pending := e.pending[target]
	e.mu.Unlock()
	if pending {
		t.Error("pending mark not rolled back after queue-full drop; URL could never be rediscovered")
	}
}

func TestCrawlStatusUsesAtomicLoads(t *testing.T) {
	e := New(1, 10, 0, nil)
	cs := e.CrawlStatus()
	if cs.Crawling {
		t.Errorf("Crawling = true for idle engine with empty queue")
	}
	atomic.StoreInt32(&e.active, 1)
	atomic.StoreInt32(&e.queueSize, 3)
	cs = e.CrawlStatus()
	if !cs.Crawling || cs.Active != 1 || cs.QueueSize != 3 {
		t.Errorf("CrawlStatus = %+v, want active 1 queue 3 crawling true", cs)
	}
}

func TestExportDocumentsPaginates(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.AddDocument("https://a.com/page", "A", "alpha")
	e.AddDocument("https://b.com/page", "B", "beta")
	e.AddDocument("https://c.com/page", "C", "gamma")

	page, total := e.ExportDocuments(0, 2)
	if total != 3 || len(page) != 2 {
		t.Fatalf("ExportDocuments(0,2) = %d docs / total %d, want 2 / 3", len(page), total)
	}
	if page[0].URL != "https://a.com/page" || page[1].URL != "https://b.com/page" {
		t.Errorf("first page = %+v, want a.com then b.com", page)
	}
	if page[0].Title != "A" {
		t.Errorf("exported title = %q, want A", page[0].Title)
	}

	page, total = e.ExportDocuments(2, 10)
	if total != 3 || len(page) != 1 || page[0].URL != "https://c.com/page" {
		t.Fatalf("ExportDocuments(2,10) = %+v / total %d, want only c.com / 3", page, total)
	}

	if page, total = e.ExportDocuments(5, 10); total != 3 || len(page) != 0 {
		t.Fatalf("ExportDocuments(5,10) = %+v / total %d, want empty / 3", page, total)
	}

	if page, total = e.ExportDocuments(0, 0); total != 3 || len(page) != 3 {
		t.Fatalf("ExportDocuments(0,0) = %d docs, want all 3 (no cap)", len(page))
	}
}

func TestExportEachStreamsAndPaginates(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.AddDocument("https://a.com/page", "A", "alpha")
	e.AddDocument("https://b.com/page", "B", "beta")
	e.AddDocument("https://c.com/page", "C", "gamma")

	var got []ExportResult
	total, err := e.ExportEach(1, 1, func(doc ExportResult) error {
		got = append(got, doc)
		return nil
	})
	if err != nil {
		t.Fatalf("ExportEach: %v", err)
	}
	if total != 3 || len(got) != 1 || got[0].URL != "https://b.com/page" {
		t.Fatalf("ExportEach(1,1) = %+v / total %d, want b.com / 3", got, total)
	}

	stop := errors.New("stop")
	count := 0
	total, err = e.ExportEach(0, 0, func(ExportResult) error {
		count++
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("ExportEach error = %v, want stop", err)
	}
	if count != 1 {
		t.Fatalf("callback called %d times after error, want 1", count)
	}
	if total != 3 {
		t.Fatalf("ExportEach total after error = %d, want 3", total)
	}

	if got := e.ExportTotal(); got != 3 {
		t.Fatalf("ExportTotal = %d, want 3", got)
	}
}

func TestSitemapEachStreamsAndFiltersByHost(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.AddDocument("https://a.com/page", "A", "alpha")
	e.AddDocument("https://docs.a.com/guide", "Docs", "beta")
	e.AddDocument("https://b.com/page", "B", "gamma")
	e.AddDocument("https://nota.com/page", "N", "delta")

	var urls []string
	total, err := e.SitemapEach("a.com", 0, func(rawURL string) error {
		urls = append(urls, rawURL)
		return nil
	})
	if err != nil {
		t.Fatalf("SitemapEach: %v", err)
	}
	if total != 2 {
		t.Fatalf("SitemapEach total = %d, want 2 (a.com and subdomains)", total)
	}
	if len(urls) != 2 {
		t.Fatalf("SitemapEach wrote %d urls, want 2: %v", len(urls), urls)
	}
	for _, u := range urls {
		if !strings.Contains(u, "a.com") {
			t.Errorf("SitemapEach emitted %q outside host a.com", u)
		}
	}

	// limit caps the writes but the total still reflects every match.
	urls = nil
	total, err = e.SitemapEach("a.com", 1, func(rawURL string) error {
		urls = append(urls, rawURL)
		return nil
	})
	if err != nil {
		t.Fatalf("SitemapEach limit: %v", err)
	}
	if total != 2 || len(urls) != 1 {
		t.Fatalf("SitemapEach(limit=1) = %d urls / total %d, want 1 / 2", len(urls), total)
	}

	// An empty host matches every indexed URL.
	urls = nil
	total, err = e.SitemapEach("", 0, func(rawURL string) error {
		urls = append(urls, rawURL)
		return nil
	})
	if err != nil {
		t.Fatalf("SitemapEach all: %v", err)
	}
	if total != 4 || len(urls) != 4 {
		t.Fatalf("SitemapEach(all) = %d urls / total %d, want 4 / 4", len(urls), total)
	}

	// A callback error stops the walk and is returned to the caller.
	sentinel := errors.New("stop")
	if _, err := e.SitemapEach("", 0, func(string) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("SitemapEach error = %v, want sentinel", err)
	}
}

func TestScheduleRetrySkipsExcludedURLs(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/search"
	e.mu.Lock()
	e.visited[rawURL] = struct{}{}
	e.mu.Unlock()

	e.scheduleRetry(rawURL, 0, fmt.Errorf("%w: path %q", crawler.ErrExcluded, "/search"))

	e.mu.Lock()
	_, retried := e.retryAt[rawURL]
	e.mu.Unlock()
	if retried {
		t.Fatal("excluded URL must not be rescheduled for retry")
	}
}

func TestDiscoverSkipsExcludedLinks(t *testing.T) {
	t.Setenv("PALSPIDER_EXCLUDE_PATHS", "/search,/login")
	t.Setenv("PALSPIDER_MAX_QUERY_PARAMS", "2")

	e := New(1, 10, 0, nil)
	html := `<a href="/keep">keep</a>` +
		`<a href="/search?q=golang">search</a>` +
		`<a href="/searching">searching</a>` +
		`<a href="/trap?a=1&b=2&c=3">trap</a>`
	dirty, items := e.discover("https://example.com/", html, 0)

	want := map[string]bool{
		"https://example.com/keep":      true,
		"https://example.com/searching": true,
	}
	if len(items) != len(want) {
		t.Fatalf("discover queued %d items %v, want %d", len(items), items, len(want))
	}
	for _, item := range items {
		if !want[item.url] {
			t.Errorf("discover queued %q, want only crawlable links", item.url)
		}
	}
	// Excluded links still count toward in-degree so link weights stay right.
	if len(dirty) != 4 {
		t.Errorf("dirty link counts = %v, want all 4 discovered links", dirty)
	}
}

func TestDiscoverSkipsPerHostExcludedLinks(t *testing.T) {
	t.Setenv("PALSPIDER_EXCLUDE_PATHS", "/global")
	t.Setenv("PALSPIDER_HOST_EXCLUDE_PATHS", "example.com:/search")

	e := New(1, 10, 0, nil)
	html := `<a href="/keep">keep</a>` +
		`<a href="https://example.com/search?q=golang">own search</a>` +
		`<a href="https://other.org/search">other search</a>`
	dirty, items := e.discover("https://example.com/", html, 0)

	want := map[string]bool{
		"https://example.com/keep": true,
		"https://other.org/search": true,
	}
	if len(items) != len(want) {
		t.Fatalf("discover queued %d items %v, want %d", len(items), items, len(want))
	}
	for _, item := range items {
		if !want[item.url] {
			t.Errorf("discover queued %q, want only links outside the host rules", item.url)
		}
	}
	if len(dirty) != 3 {
		t.Errorf("dirty link counts = %v, want all 3 discovered links", dirty)
	}
}

func TestScheduleRetryDefersFailuresForAboutAnHour(t *testing.T) {
	e := New(1, 10, 0, nil)
	url := "https://example.com/retry"
	e.mu.Lock()
	e.visited[url] = struct{}{}
	e.mu.Unlock()

	e.scheduleRetry(url, 0, errors.New("temporary network failure"))

	e.mu.Lock()
	at, exists := e.retryAt[url]
	e.mu.Unlock()
	if !exists {
		t.Fatal("retry timestamp missing after failure")
	}
	// The wait carries a jitter of up to a tenth of the window.
	low := time.Hour - time.Hour*retryJitterPercent/100
	high := time.Hour + time.Hour*retryJitterPercent/100
	if wait := time.Until(at); wait < low || wait > high {
		t.Fatalf("retry window = %v, want between %v and %v", wait, low, high)
	}
	if delay := e.retryDelay(url); delay <= 0 || delay > high {
		t.Fatalf("retryDelay = %v, want a positive delay within %v", delay, high)
	}
}

func TestRetryBackoffGrowsWithEveryAttempt(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.retryWindow = time.Hour

	cases := []struct {
		attempt int
		base    time.Duration
	}{
		{1, time.Hour},
		{2, 2 * time.Hour},
		{3, 4 * time.Hour},
		{4, 4 * time.Hour},
	}
	for _, c := range cases {
		got := e.retryBackoff(c.attempt)
		low := c.base - c.base*retryJitterPercent/100
		high := c.base + c.base*retryJitterPercent/100
		if got < low || got > high {
			t.Errorf("retryBackoff(%d) = %v, want between %v and %v", c.attempt, got, low, high)
		}
	}
}

func TestRetryBackoffJittersTheWait(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.retryWindow = time.Hour

	low := time.Hour - time.Hour*retryJitterPercent/100
	high := time.Hour + time.Hour*retryJitterPercent/100
	seen := make(map[time.Duration]struct{})
	var minWait, maxWait time.Duration
	for i := 0; i < 200; i++ {
		got := e.retryBackoff(1)
		if got < low || got > high {
			t.Fatalf("retryBackoff(1) = %v, want between %v and %v", got, low, high)
		}
		seen[got] = struct{}{}
		if minWait == 0 || got < minWait {
			minWait = got
		}
		if got > maxWait {
			maxWait = got
		}
	}
	if len(seen) < 2 {
		t.Fatal("retryBackoff returned the same wait every time, so the jitter does nothing")
	}
	if maxWait-minWait < time.Minute {
		t.Errorf("jitter spread = %v, want the waits to be spread out", maxWait-minWait)
	}
}

func TestRetryBackoffSeparatesAttempts(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.retryWindow = time.Hour

	first := e.retryBackoff(1)
	second := e.retryBackoff(2)
	if second <= first {
		t.Errorf("retryBackoff(2) = %v, want more than the first attempt wait %v", second, first)
	}
}

func TestClearQueueDropsParkedRetries(t *testing.T) {
	e := New(1, 10, 0, nil)
	url := "https://example.com/retry"
	e.scheduleRetry(url, 0, errors.New("temporary network failure"))

	// A worker parks the URL while its retry window is still open.
	item := <-e.urlChan
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.parkRetry(ctx, item, 50*time.Millisecond)

	e.ClearQueue()
	time.Sleep(200 * time.Millisecond)

	if len(e.urlChan) != 0 {
		t.Fatalf("queue = %d items, want a parked retry to stay dropped", len(e.urlChan))
	}
}

func TestClearQueueForgetsDroppedURLs(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/page")
	e.ClearQueue()

	e.mu.Lock()
	_, visited := e.visited["https://example.com/page"]
	e.mu.Unlock()
	if visited {
		t.Error("a cleared URL is still marked visited, so it can never be crawled again")
	}

	e.Seed("https://example.com/page")
	if len(e.urlChan) != 1 {
		t.Fatalf("queue = %d items, want the cleared URL to be seedable again", len(e.urlChan))
	}
}

func TestScheduleRetryQueuesTheFailedURLAgain(t *testing.T) {
	e := New(1, 10, 0, nil)
	url := "https://example.com/retry"

	e.scheduleRetry(url, 0, errors.New("temporary network failure"))

	if len(e.urlChan) != 1 {
		t.Fatalf("queue = %d items, want the failed URL to be queued again", len(e.urlChan))
	}
	item := <-e.urlChan
	if item.url != url {
		t.Errorf("queued item = %q, want %q", item.url, url)
	}
	e.mu.Lock()
	_, pending := e.pending[url]
	e.mu.Unlock()
	if !pending {
		t.Error("the retried URL is not marked pending, so a second copy could be queued")
	}
}

func TestScheduleRetryStopsWhenTheBudgetIsSpent(t *testing.T) {
	e := New(1, 10, 0, nil)
	url := "https://example.com/dead"

	for i := 0; i < maxRetries; i++ {
		e.scheduleRetry(url, 0, errors.New("temporary network failure"))
	}
	if len(e.urlChan) != maxRetries {
		t.Fatalf("queue = %d items, want one per attempt", len(e.urlChan))
	}

	e.scheduleRetry(url, 0, errors.New("temporary network failure"))
	if len(e.urlChan) != maxRetries {
		t.Errorf("queue = %d items after the budget was spent, want %d", len(e.urlChan), maxRetries)
	}
	e.mu.Lock()
	_, visited := e.visited[url]
	e.mu.Unlock()
	if !visited {
		t.Error("a URL that ran out of retries is not marked visited, so it would be crawled again")
	}
}

func TestScheduleRetryIgnoresPermanentFailures(t *testing.T) {
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	missing := srv.URL + "/missing"
	_, err := crawler.FetchContext(context.Background(), missing, nil)
	if err == nil {
		t.Fatal("fetching a missing page returned no error")
	}
	if !crawler.IsPermanent(err) {
		t.Fatalf("error for a missing page = %v, want a permanent failure", err)
	}

	e := New(1, 10, 0, nil)
	e.scheduleRetry(missing, 0, err)

	if len(e.urlChan) != 0 {
		t.Errorf("queue = %d items, want a missing page to be dropped instead of retried", len(e.urlChan))
	}
	e.mu.Lock()
	_, retried := e.retryAt[missing]
	e.mu.Unlock()
	if retried {
		t.Error("a permanent failure was scheduled for a retry")
	}
}

func TestCrawlerRetriesAPageThatFailed(t *testing.T) {
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "1")
	t.Setenv(EnvRetryDelay, "1")

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			w.Header().Set("Content-Type", "text/plain")
			io.WriteString(w, "User-agent: *\nDisallow:\n")
			return
		}
		if atomic.AddInt32(&hits, 1) == 1 {
			// The first request fails, the retry succeeds.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, "<html><title>Retried</title><body>page that needed a retry</body></html>")
	}))
	defer srv.Close()

	e := New(1, 3, 0, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	defer e.Stop()
	e.Seed(srv.URL + "/page")

	deadline := time.Now().Add(15 * time.Second)
	for e.Indexer.DocCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the page was not indexed after the retry (%d requests)", atomic.LoadInt32(&hits))
		}
		time.Sleep(50 * time.Millisecond)
	}

	results := e.Search("retry", 5)
	if len(results) != 1 {
		t.Fatalf("search returned %d results, want the retried page", len(results))
	}
	if results[0].URL != srv.URL+"/page" {
		t.Errorf("indexed url = %q, want %q", results[0].URL, srv.URL+"/page")
	}
}

func TestDiscoverCapsLinksPerPage(t *testing.T) {
	t.Setenv(crawler.EnvMaxLinksPerPage, "2")

	e := New(1, 10, 0, nil)
	html := `<a href="/a">a</a><a href="/b">b</a><a href="/c">c</a>`
	_, items := e.discover("https://example.com/", html, 0)

	if len(items) != 2 {
		t.Fatalf("discover queued %d items %v, want 2 (the per-page cap)", len(items), items)
	}
	// Only the followed links are counted for in-degree; the dropped ones are
	// never even looked at.
	if got := e.LinkStore.Get("https://example.com/c"); got != 0 {
		t.Errorf("dropped link c has link count %d, want 0", got)
	}
}

func TestDiscoverEnforcesPerHostBudget(t *testing.T) {
	t.Setenv(crawler.EnvMaxPagesPerHost, "1")

	e := New(1, 10, 0, nil)
	// The host is already at its budget.
	e.mu.Lock()
	e.hostDocs["example.com"] = 1
	e.mu.Unlock()

	html := `<a href="/next">next</a><a href="https://other.com/page">other</a>`
	_, items := e.discover("https://example.com/", html, 0)

	if len(items) != 1 || items[0].url != "https://other.com/page" {
		t.Fatalf("discover queued %v, want only the link outside the host that is over budget", items)
	}
}

func TestHostHasBudgetUnlimitedByDefault(t *testing.T) {
	t.Setenv(crawler.EnvMaxPagesPerHost, "")
	e := New(1, 10, 0, nil)
	e.mu.Lock()
	e.hostDocs["example.com"] = 10000
	e.mu.Unlock()
	if !e.hostHasBudget("example.com") {
		t.Error("hostHasBudget = false with no configured budget, want true")
	}
}
