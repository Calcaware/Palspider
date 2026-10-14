package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

func TestScheduleRetryDefersFailuresForOneHour(t *testing.T) {
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
	if wait := time.Until(at); wait < 59*time.Minute || wait > time.Hour+time.Minute {
		t.Fatalf("retry window = %v, want roughly 1 hour", wait)
	}
	if delay := e.retryDelay(url); delay <= 0 || delay > time.Hour {
		t.Fatalf("retryDelay = %v, want a positive delay within 1 hour", delay)
	}
}
