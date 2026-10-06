package engine

import (
	"context"
	"sync/atomic"
	"testing"

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
