package engine

import (
	"context"
	"encoding/json"
	"log"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/calcaware/palspider/crawler"
	"github.com/calcaware/palspider/indexer"
	"github.com/calcaware/palspider/peer"
	"github.com/calcaware/palspider/searcher"
	"github.com/calcaware/palspider/store"
	"github.com/calcaware/palspider/trie"
)

const (
	// peerSearchTimeout bounds each individual peer request; peerSearchBudget
	// bounds the total time a search may spend waiting on peers.
	peerSearchTimeout = 1500 * time.Millisecond
	peerSearchBudget  = 2 * time.Second
)

type crawlItem struct {
	url   string
	depth int
}

type Engine struct {
	Indexer   *indexer.Indexer
	Trie      *trie.Trie
	Searcher  *searcher.Searcher
	LinkStore *indexer.LinkStore
	Peers     *peer.PeerManager
	Store     *store.Store
	limiter   *crawler.RateLimiter

	workers   int
	maxDepth  int
	maxPages  int
	urlChan   chan crawlItem
	visited   map[string]struct{}
	fpSeen    map[string]struct{}
	pending   map[string]struct{}
	mu        sync.Mutex
	wg        sync.WaitGroup
	active    int32
	cancel    context.CancelFunc
	queueSize int32
}

type Stats struct {
	Documents int   `json:"documents"`
	Terms     int   `json:"terms"`
	Visited   int   `json:"visited"`
	QueueSize int32 `json:"queueSize"`
	Active    int32 `json:"active"`
}

type CrawlStatus struct {
	QueueSize int32 `json:"queueSize"`
	Active    int32 `json:"active"`
	Crawling  bool  `json:"crawling"`
	Visited   int   `json:"visited"`
	Documents int   `json:"documents"`
	Terms     int   `json:"terms"`
}

// New creates an engine. maxDepth < 0 means unlimited crawl depth; maxPages
// <= 0 means unlimited pages.
func New(workerCount, maxDepth, maxPages int, st *store.Store) *Engine {
	ix := indexer.New()
	tr := trie.New()
	ls := indexer.NewLinkStore()

	e := &Engine{
		Indexer:   ix,
		Trie:      tr,
		Searcher:  searcher.New(ix, tr),
		LinkStore: ls,
		Peers:     peer.New(),
		Store:     st,
		limiter:   crawler.NewRateLimiter(200 * time.Millisecond),
		workers:   workerCount,
		maxDepth:  maxDepth,
		maxPages:  maxPages,
		urlChan:   make(chan crawlItem, 100000),
		visited:   make(map[string]struct{}),
		fpSeen:    make(map[string]struct{}),
		pending:   make(map[string]struct{}),
	}
	return e
}

func (e *Engine) Restore() error {
	if e.Store == nil {
		return nil
	}

	var docCount int

	err := e.Store.AllDocuments(func(id int, data []byte) error {
		var doc indexer.Document
		if err := json.Unmarshal(data, &doc); err != nil {
			return err
		}
		e.Indexer.RestoreDocument(doc)
		e.Trie.Insert(doc.URL, doc.ID)
		docCount++
		return nil
	})
	if err != nil {
		return err
	}

	err = e.Store.AllPostings(func(term string, data []byte) error {
		var postings map[int]int
		if err := json.Unmarshal(data, &postings); err != nil {
			return err
		}
		e.Indexer.RestorePostings(term, postings)
		return nil
	})
	if err != nil {
		return err
	}

	err = e.Store.AllLinkCounts(func(url string, count int) error {
		e.LinkStore.Set(url, count)
		return nil
	})
	if err != nil {
		return err
	}

	err = e.Store.AllVisited(func(url string) error {
		e.visited[url] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}

	err = e.Store.AllFingerprints(func(hash string) error {
		e.fpSeen[hash] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}

	nextID, err := e.Store.LoadNextDocID()
	if err != nil {
		return err
	}
	if nextID > 0 {
		e.Indexer.SetNextID(nextID)
	}

	log.Printf("Restored %d documents, %d visited, %d fingerprints from db",
		docCount, len(e.visited), len(e.fpSeen))
	return nil
}

// persistDoc writes the document, its postings, and any dirty link counts in
// one database transaction.
func (e *Engine) persistDoc(doc indexer.Document, terms []string, linkCounts map[string]int) {
	if e.Store == nil {
		return
	}
	postings := make(map[string]map[int]int, len(terms))
	seen := make(map[string]bool, len(terms))
	for _, term := range terms {
		if seen[term] {
			continue
		}
		seen[term] = true
		if p := e.Indexer.GetPostings(term); p != nil {
			postings[term] = p
		}
	}
	if err := e.Store.SaveDocumentBatch(doc.ID, doc, e.Indexer.NextID(), postings, linkCounts); err != nil {
		log.Printf("persist: %v", err)
	}
}

// markVisited records a URL as visited and its content fingerprint as seen,
// persisting both. It returns false when the URL was already known.
func (e *Engine) markVisited(normalized, fingerprint string) (urlKnown, fpKnown bool) {
	e.mu.Lock()
	if _, ok := e.visited[normalized]; ok {
		urlKnown = true
	} else {
		e.visited[normalized] = struct{}{}
	}
	if fingerprint != "" {
		if _, ok := e.fpSeen[fingerprint]; ok {
			fpKnown = true
		} else {
			e.fpSeen[fingerprint] = struct{}{}
		}
	}
	e.mu.Unlock()

	if e.Store != nil {
		if !urlKnown {
			e.Store.MarkVisited(normalized)
		}
		if fingerprint != "" && !fpKnown {
			e.Store.MarkFingerprint(fingerprint)
		}
	}
	return urlKnown, fpKnown
}

func (e *Engine) Seed(rawURL string) {
	normalized := crawler.NormalizeURL(rawURL)
	e.mu.Lock()
	if _, ok := e.visited[normalized]; ok {
		e.mu.Unlock()
		return
	}
	if _, ok := e.pending[normalized]; ok {
		e.mu.Unlock()
		return
	}
	e.visited[normalized] = struct{}{}
	e.mu.Unlock()

	if e.Store != nil {
		e.Store.MarkVisited(normalized)
	}

	if !e.enqueue(crawlItem{url: normalized, depth: 0}, 5*time.Second) {
		e.forgetVisited(normalized)
	}
}

// forgetVisited removes a URL that could not be queued so a later seed can
// try again instead of treating it as already crawled.
func (e *Engine) forgetVisited(normalized string) {
	e.mu.Lock()
	delete(e.visited, normalized)
	e.mu.Unlock()
	if e.Store != nil {
		e.Store.DeleteVisited(normalized)
	}
}

// enqueue pushes an item onto the crawl queue, waiting up to wait when the
// queue is full (wait <= 0 means do not wait). It reports whether the item
// was accepted; callers must roll back bookkeeping on failure.
func (e *Engine) enqueue(item crawlItem, wait time.Duration) bool {
	if wait <= 0 {
		select {
		case e.urlChan <- item:
			return true
		default:
			log.Printf("queue full, dropping %s", item.url)
			return false
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case e.urlChan <- item:
		return true
	case <-timer.C:
		log.Printf("queue full, dropping %s", item.url)
		return false
	}
}

// AddDocument indexes a page directly (seed data or hand-picked content). The
// URL is normalized, marked visited, and fingerprinted so the crawler will not
// fetch and index the same page again.
func (e *Engine) AddDocument(rawURL, title, text string) {
	normalized := crawler.NormalizeURL(rawURL)
	fp := crawler.ContentFingerprint(text)
	urlKnown, _ := e.markVisited(normalized, fp)
	if urlKnown {
		return
	}

	terms := crawler.Tokenize(text)
	id := e.Indexer.AddDocument(normalized, title, text, terms)
	e.Trie.Insert(normalized, id)

	linkCount := e.LinkStore.Get(normalized)
	e.Indexer.SetLinkCount(id, linkCount)

	e.persistDoc(indexer.Document{
		ID: id, URL: normalized, Title: title, Text: text, LinkCount: linkCount,
	}, terms, nil)
}

func (e *Engine) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	for range e.workers {
		e.wg.Add(1)
		go e.worker(ctx)
	}

	go e.monitorQueue(ctx)
}

func (e *Engine) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
}

func (e *Engine) Search(query string, limit int) []searcher.Result {
	localResults := e.Searcher.Search(query, limit)

	if e.Peers == nil || len(e.Peers.List()) == 0 {
		return localResults
	}

	ch := make(chan []searcher.Result, 1)
	go func() {
		peerResults := e.Peers.SearchAll(query, limit, peerSearchTimeout)
		var remote []searcher.Result
		for _, pr := range peerResults {
			if pr.Error == nil {
				remote = append(remote, pr.Results...)
			}
		}
		ch <- remote
	}()

	var remoteResults []searcher.Result
	select {
	case remoteResults = <-ch:
	case <-time.After(peerSearchBudget):
	}

	return mergeResults(localResults, remoteResults, limit)
}

func (e *Engine) SearchLocal(query string, limit int) []searcher.Result {
	return e.Searcher.Search(query, limit)
}

func mergeResults(local, remote []searcher.Result, limit int) []searcher.Result {
	if limit <= 0 {
		return nil
	}
	seen := make(map[string]bool, len(local)+len(remote))
	results := make([]searcher.Result, 0, len(local)+len(remote))

	for _, r := range local {
		if !seen[r.URL] {
			seen[r.URL] = true
			results = append(results, r)
		}
	}
	for _, r := range remote {
		if !seen[r.URL] {
			seen[r.URL] = true
			results = append(results, r)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit < len(results) {
		results = results[:limit]
	}
	return results
}

func (e *Engine) Suggest(prefix string, limit int) []string {
	return e.Searcher.Suggest(prefix, limit)
}

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	visited := len(e.visited)
	e.mu.Unlock()
	return Stats{
		Documents: e.Indexer.DocCount(),
		Terms:     e.Indexer.TermCount(),
		Visited:   visited,
		QueueSize: atomic.LoadInt32(&e.queueSize),
		Active:    atomic.LoadInt32(&e.active),
	}
}

func (e *Engine) CrawlStatus() CrawlStatus {
	e.mu.Lock()
	visited := len(e.visited)
	e.mu.Unlock()
	queueSize := atomic.LoadInt32(&e.queueSize)
	active := atomic.LoadInt32(&e.active)
	return CrawlStatus{
		QueueSize: queueSize,
		Active:    active,
		Crawling:  active > 0 || queueSize > 0,
		Visited:   visited,
		Documents: e.Indexer.DocCount(),
		Terms:     e.Indexer.TermCount(),
	}
}

func (e *Engine) monitorQueue(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			atomic.StoreInt32(&e.queueSize, int32(len(e.urlChan)))
		}
	}
}

func (e *Engine) worker(ctx context.Context) {
	defer e.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case item := <-e.urlChan:
			e.markDequeued(item)
			e.processURL(ctx, item)
		}
	}
}

// markDequeued moves a URL from pending to visited so the "discovered" stat
// reflects everything the crawler has picked up.
func (e *Engine) markDequeued(item crawlItem) {
	e.mu.Lock()
	delete(e.pending, item.url)
	_, known := e.visited[item.url]
	if !known {
		e.visited[item.url] = struct{}{}
	}
	e.mu.Unlock()

	if !known && e.Store != nil {
		e.Store.MarkVisited(item.url)
	}
}

func (e *Engine) processURL(ctx context.Context, item crawlItem) {
	rawURL := item.url
	atomic.AddInt32(&e.active, 1)
	defer atomic.AddInt32(&e.active, -1)

	html, err := crawler.FetchContext(ctx, rawURL, e.limiter)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("crawl: %s: %v", rawURL, err)
		}
		return
	}

	title, text := crawler.ExtractText(html)
	if text != "" {
		fp := crawler.ContentFingerprint(text)
		_, fpKnown := e.markVisited(rawURL, fp)
		if fpKnown {
			return
		}
		if _, exists := e.Indexer.IDForURL(rawURL); exists {
			return
		}
	}

	// Links are discovered even when the page has no indexable text so link
	// farms, SPAs, and noscript-only pages still lead the crawler onward.
	dirtyLinks, toEnqueue := e.discover(rawURL, html, item.depth)

	if text == "" {
		if e.Store != nil {
			if err := e.Store.SaveLinkCounts(dirtyLinks); err != nil {
				log.Printf("persist links: %v", err)
			}
		}
		e.enqueueAll(ctx, toEnqueue)
		return
	}

	terms := crawler.Tokenize(text)
	docID := e.Indexer.AddDocument(rawURL, title, text, terms)
	e.Trie.Insert(rawURL, docID)

	linkCount := e.LinkStore.Get(rawURL)
	e.Indexer.SetLinkCount(docID, linkCount)

	e.persistDoc(indexer.Document{
		ID: docID, URL: rawURL, Title: title, Text: text, LinkCount: linkCount,
	}, terms, dirtyLinks)

	e.enqueueAll(ctx, toEnqueue)
}

// discover extracts links from html, bumps in-degree counts, and returns the
// dirty counts plus the crawl targets that are within the depth and page
// limits and not already visited or pending.
func (e *Engine) discover(rawURL, html string, depth int) (map[string]int, []crawlItem) {
	base, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil
	}
	links := crawler.ExtractLinks(html, base)
	dirtyLinks := make(map[string]int, len(links))
	var toEnqueue []crawlItem
	for _, link := range links {
		e.LinkStore.Increment(link)
		dirtyLinks[link] = e.LinkStore.Get(link)

		normalized := crawler.NormalizeURL(link)
		childDepth := depth + 1
		if e.maxDepth >= 0 && childDepth > e.maxDepth {
			continue
		}
		if e.maxPages > 0 && e.Indexer.DocCount() >= e.maxPages {
			continue
		}

		e.mu.Lock()
		if _, visited := e.visited[normalized]; visited {
			e.mu.Unlock()
			continue
		}
		if _, pending := e.pending[normalized]; pending {
			e.mu.Unlock()
			continue
		}
		e.pending[normalized] = struct{}{}
		e.mu.Unlock()

		toEnqueue = append(toEnqueue, crawlItem{url: normalized, depth: childDepth})
	}
	return dirtyLinks, toEnqueue
}

// enqueueAll queues discovered URLs without ever blocking indefinitely: when
// the queue is full the pending mark is rolled back so another parent page
// (or a later re-crawl) can rediscover the URL.
func (e *Engine) enqueueAll(ctx context.Context, items []crawlItem) {
	for _, child := range items {
		if ctx.Err() != nil {
			return
		}
		if !e.enqueue(child, 0) {
			e.mu.Lock()
			delete(e.pending, child.url)
			e.mu.Unlock()
		}
	}
}
