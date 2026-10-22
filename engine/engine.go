package engine

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
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

	// maxRetries is how often a URL that failed with a transient error is
	// fetched again before the crawler gives up on it.
	maxRetries = 3

	// defaultRetryWindow is the base wait before a failed URL is fetched
	// again. The wait grows with every attempt so a host that stays down is
	// not hammered.
	defaultRetryWindow = time.Hour

	// retryJitterPercent is how far a retry wait may drift from its base
	// window in either direction. The spread keeps a group of workers from
	// hitting a recovered host all at once.
	retryJitterPercent = 10
)

// EnvRetryDelay names the variable that sets how long the crawler waits
// before it fetches a failed URL again. The value is a number of seconds and
// defaults to one hour.
const EnvRetryDelay = "PALSPIDER_RETRY_DELAY"

// retryWindowFromEnv reads the retry window from the environment. A missing,
// unparsable, or non-positive value falls back to the default.
func retryWindowFromEnv() time.Duration {
	value := strings.TrimSpace(os.Getenv(EnvRetryDelay))
	if value == "" {
		return defaultRetryWindow
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return defaultRetryWindow
	}
	return time.Duration(seconds) * time.Second
}

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

	workers     int
	maxDepth    int
	maxPages    int
	maxPerHost  int
	retryWindow time.Duration
	urlChan     chan crawlItem
	visited     map[string]struct{}
	fpSeen      map[string]struct{}
	pending     map[string]struct{}
	retryCounts map[string]int
	retryAt     map[string]time.Time
	queueGen    uint64
	hostStats   map[string]HostStat
	hostDocs    map[string]int
	paused      bool
	mu          sync.Mutex
	wg          sync.WaitGroup
	active      int32
	cancel      context.CancelFunc
	queueSize   int32
}

type HostStat struct {
	Requests  int    `json:"requests"`
	Successes int    `json:"successes"`
	Failures  int    `json:"failures"`
	LastError string `json:"lastError,omitempty"`
	LastSeen  string `json:"lastSeen,omitempty"`
}

type Stats struct {
	Documents int                 `json:"documents"`
	Terms     int                 `json:"terms"`
	Visited   int                 `json:"visited"`
	QueueSize int32               `json:"queueSize"`
	Active    int32               `json:"active"`
	Paused    bool                `json:"paused"`
	HostStats map[string]HostStat `json:"hostStats"`
}

type CrawlStatus struct {
	QueueSize int32               `json:"queueSize"`
	Active    int32               `json:"active"`
	Crawling  bool                `json:"crawling"`
	Visited   int                 `json:"visited"`
	Documents int                 `json:"documents"`
	Terms     int                 `json:"terms"`
	Paused    bool                `json:"paused"`
	HostStats map[string]HostStat `json:"hostStats"`
}

// New creates an engine. maxDepth < 0 means unlimited crawl depth; maxPages
// <= 0 means unlimited pages. The per-host page budget and per-page link cap
// come from the environment (PALSPIDER_MAX_PAGES_PER_HOST and
// PALSPIDER_MAX_LINKS_PER_PAGE).
func New(workerCount, maxDepth, maxPages int, st *store.Store) *Engine {
	ix := indexer.New()
	tr := trie.New()
	ls := indexer.NewLinkStore()

	e := &Engine{
		Indexer:     ix,
		Trie:        tr,
		Searcher:    searcher.New(ix, tr),
		LinkStore:   ls,
		Peers:       peer.New(),
		Store:       st,
		limiter:     crawler.NewRateLimiter(200 * time.Millisecond),
		workers:     workerCount,
		maxDepth:    maxDepth,
		maxPages:    maxPages,
		maxPerHost:  crawler.MaxPagesPerHost(),
		retryWindow: retryWindowFromEnv(),
		urlChan:     make(chan crawlItem, 100000),
		visited:     make(map[string]struct{}),
		fpSeen:      make(map[string]struct{}),
		pending:     make(map[string]struct{}),
		retryCounts: make(map[string]int),
		retryAt:     make(map[string]time.Time),
		hostStats:   make(map[string]HostStat),
		hostDocs:    make(map[string]int),
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
	e.countHostDoc(normalized)

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
	return e.SearchWithSort(query, limit, "")
}

func (e *Engine) SearchWithSort(query string, limit int, rawSort string) []searcher.Result {
	localResults := e.Searcher.SearchWithSort(query, limit, rawSort)

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

	return mergeResultsWithSort(localResults, remoteResults, limit, rawSort)
}

func (e *Engine) SearchLocal(query string, limit int) []searcher.Result {
	return e.Searcher.Search(query, limit)
}

func (e *Engine) SearchLocalWithSort(query string, limit int, rawSort string) []searcher.Result {
	return e.Searcher.SearchWithSort(query, limit, rawSort)
}

func (e *Engine) Related(rawURL string, limit int) []searcher.Result {
	return e.Searcher.Related(rawURL, limit)
}

// ExportResult is one document in a JSON export. It mirrors the indexed
// document without the internal bookkeeping fields.
type ExportResult struct {
	ID        int    `json:"id"`
	URL       string `json:"url"`
	Title     string `json:"title"`
	Text      string `json:"text"`
	LinkCount int    `json:"linkCount"`
}

// ExportTotal returns the number of indexed documents available for export.
func (e *Engine) ExportTotal() int {
	return e.Indexer.LiveDocCount()
}

// ExportEach streams indexed documents in ascending ID order to fn, applying
// offset and limit (limit <= 0 means no cap). The returned value is the total
// number of indexed documents so callers can report pagination metadata.
//
// Documents are handed to fn one at a time instead of being collected into a
// slice, so a streaming format such as NDJSON can be written without
// materializing the whole page in memory.
func (e *Engine) ExportEach(offset, limit int, fn func(ExportResult) error) (int, error) {
	docs := e.Indexer.AllDocs()
	total := len(docs)
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return total, nil
	}
	docs = docs[offset:]
	if limit > 0 && limit < len(docs) {
		docs = docs[:limit]
	}
	for _, doc := range docs {
		if err := fn(ExportResult{
			ID:        doc.ID,
			URL:       doc.URL,
			Title:     doc.Title,
			Text:      doc.Text,
			LinkCount: doc.LinkCount,
		}); err != nil {
			return total, err
		}
	}
	return total, nil
}

// ExportDocuments returns indexed documents in ascending ID order, applying
// offset and limit (limit <= 0 means no cap). The second return value is the
// total number of indexed documents so callers can report pagination metadata.
func (e *Engine) ExportDocuments(offset, limit int) ([]ExportResult, int) {
	var out []ExportResult
	total, _ := e.ExportEach(offset, limit, func(doc ExportResult) error {
		out = append(out, doc)
		return nil
	})
	return out, total
}

// SitemapEach streams the URLs of indexed documents in ascending ID order to
// fn, so a sitemap can be written without materializing an extra copy of the
// corpus. When host is non-empty only URLs whose hostname equals it or is a
// subdomain of it are emitted. limit caps the number of URLs written
// (limit <= 0 means no cap), but every matching document is still visited so
// the returned total reflects all eligible URLs rather than just the page.
func (e *Engine) SitemapEach(host string, limit int, fn func(rawURL string) error) (int, error) {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), ".")
	total := 0
	written := 0
	for _, doc := range e.Indexer.AllDocs() {
		if !hostMatchesURL(doc.URL, host) {
			continue
		}
		total++
		if limit > 0 && written >= limit {
			continue
		}
		if err := fn(doc.URL); err != nil {
			return total, err
		}
		written++
	}
	return total, nil
}

// hostMatchesURL reports whether rawURL belongs to host, matching the host
// itself or any subdomain. An empty host matches every URL.
func hostMatchesURL(rawURL, host string) bool {
	if host == "" {
		return true
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	got := strings.Trim(strings.ToLower(parsed.Hostname()), ".")
	return got == host || strings.HasSuffix(got, "."+host)
}

func mergeResults(local, remote []searcher.Result, limit int) []searcher.Result {
	return mergeResultsWithSort(local, remote, limit, string(searcher.SortRelevance))
}

func mergeResultsWithSort(local, remote []searcher.Result, limit int, rawSort string) []searcher.Result {
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

	mode := searcher.NormalizeSortOrder(rawSort)
	sort.Slice(results, func(i, j int) bool {
		switch mode {
		case searcher.SortNewest:
			if results[i].ID != results[j].ID {
				return results[i].ID > results[j].ID
			}
		case searcher.SortOldest:
			if results[i].ID != results[j].ID {
				return results[i].ID < results[j].ID
			}
		case searcher.SortMostLinked:
			if results[i].LinkCount != results[j].LinkCount {
				return results[i].LinkCount > results[j].LinkCount
			}
		default:
			if results[i].Score != results[j].Score {
				return results[i].Score > results[j].Score
			}
		}
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if results[i].LinkCount != results[j].LinkCount {
			return results[i].LinkCount > results[j].LinkCount
		}
		return results[i].ID < results[j].ID
	})

	if limit < len(results) {
		results = results[:limit]
	}
	return results
}

func (e *Engine) Suggest(prefix string, limit int) []string {
	return e.Searcher.Suggest(prefix, limit)
}

// ClearQueue drops everything that is waiting and forgets the URLs it
// dropped, so a later seed or link can pick them up again. Retries that are
// parked on their window are dropped as well, otherwise the queue fills up
// again on its own once those windows open.
func (e *Engine) ClearQueue() {
	e.mu.Lock()
	// The generation tells a parked retry that the queue was cleared while
	// it was waiting, so it does not put its URL back.
	e.queueGen++
	for url := range e.pending {
		delete(e.pending, url)
	}
	for url := range e.retryAt {
		delete(e.retryAt, url)
	}
	e.mu.Unlock()

	var dropped []string
	for {
		select {
		case item := <-e.urlChan:
			// A dropped URL is not visited any more, so it can be crawled
			// again later.
			e.mu.Lock()
			delete(e.visited, item.url)
			e.mu.Unlock()
			dropped = append(dropped, item.url)
			continue
		default:
		}
		break
	}
	if e.Store != nil && len(dropped) > 0 {
		if err := e.Store.DeleteVisitedBatch(dropped); err != nil {
			log.Printf("clear queue: %v", err)
		}
	}
	atomic.StoreInt32(&e.queueSize, 0)
}

func (e *Engine) Requeue(rawURL string) bool {
	normalized := crawler.NormalizeURL(rawURL)
	e.mu.Lock()
	delete(e.retryAt, normalized)
	delete(e.retryCounts, normalized)
	delete(e.pending, normalized)
	delete(e.visited, normalized)
	if e.Store != nil {
		e.Store.DeleteVisited(normalized)
	}
	e.mu.Unlock()
	return e.enqueue(crawlItem{url: normalized, depth: 0}, 5*time.Second)
}

func (e *Engine) Pause() {
	e.mu.Lock()
	e.paused = true
	e.mu.Unlock()
}

func (e *Engine) Resume() {
	e.mu.Lock()
	e.paused = false
	e.mu.Unlock()
}

func (e *Engine) Paused() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.paused
}

func (e *Engine) HostStats() map[string]HostStat {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]HostStat, len(e.hostStats))
	for k, v := range e.hostStats {
		out[k] = v
	}
	return out
}

// countHostDoc records that a page from rawURL's host was added to the index,
// which is what the per-host page budget counts against.
func (e *Engine) countHostDoc(rawURL string) {
	if e.maxPerHost <= 0 {
		return
	}
	host := hostOf(rawURL)
	if host == "" {
		return
	}
	e.mu.Lock()
	e.hostDocs[host]++
	e.mu.Unlock()
}

// hostHasBudget reports whether host still has room under the per-host page
// budget. It counts indexed pages only, so a host just over its budget stops
// queueing new links until pages are indexed or the budget is raised. Zero
// means unlimited.
func (e *Engine) hostHasBudget(host string) bool {
	if e.maxPerHost <= 0 {
		return true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.hostDocs[host] < e.maxPerHost
}

// hostOf returns the hostname of rawURL, or an empty string when it does not
// parse.
func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

func (e *Engine) recordHostStats(rawURL string, ok bool, err error) {
	host := hostOf(rawURL)
	if host == "" {
		return
	}
	e.mu.Lock()
	stat := e.hostStats[host]
	stat.Requests++
	if ok {
		stat.Successes++
	} else {
		stat.Failures++
		if err != nil {
			stat.LastError = err.Error()
		}
	}
	stat.LastSeen = time.Now().Format(time.RFC3339)
	e.hostStats[host] = stat
	e.mu.Unlock()
}

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	visited := len(e.visited)
	paused := e.paused
	hosts := make(map[string]HostStat, len(e.hostStats))
	for k, v := range e.hostStats {
		hosts[k] = v
	}
	e.mu.Unlock()
	return Stats{
		Documents: e.Indexer.DocCount(),
		Terms:     e.Indexer.TermCount(),
		Visited:   visited,
		QueueSize: atomic.LoadInt32(&e.queueSize),
		Active:    atomic.LoadInt32(&e.active),
		Paused:    paused,
		HostStats: hosts,
	}
}

func (e *Engine) CrawlStatus() CrawlStatus {
	e.mu.Lock()
	visited := len(e.visited)
	paused := e.paused
	hosts := make(map[string]HostStat, len(e.hostStats))
	for k, v := range e.hostStats {
		hosts[k] = v
	}
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
		Paused:    paused,
		HostStats: hosts,
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
			if e.Paused() {
				if !e.enqueue(item, 5*time.Second) {
					e.mu.Lock()
					delete(e.pending, item.url)
					e.mu.Unlock()
				}
				continue
			}
			if delay := e.retryDelay(item.url); delay > 0 {
				e.parkRetry(ctx, item, delay)
				continue
			}
			e.markDequeued(item)
			e.processURL(ctx, item)
		}
	}
}

// parkRetry holds a URL until its retry window opens and then puts it back
// on the queue. The wait ends early when the engine stops, and a queue that
// is cleared in the meantime drops the URL instead of restoring it.
func (e *Engine) parkRetry(ctx context.Context, item crawlItem, delay time.Duration) {
	e.mu.Lock()
	gen := e.queueGen
	e.mu.Unlock()

	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		// The URL goes back on the queue while the lock is held, so a queue
		// clear either happens before this check or after the push, never
		// in between.
		e.mu.Lock()
		if e.queueGen != gen {
			delete(e.pending, item.url)
			e.mu.Unlock()
			return
		}
		if !e.enqueue(item, 0) {
			delete(e.pending, item.url)
		}
		e.mu.Unlock()
	}()
}

func (e *Engine) retryDelay(rawURL string) time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	at, ok := e.retryAt[rawURL]
	if !ok {
		return 0
	}
	delay := time.Until(at)
	if delay < 0 {
		return 0
	}
	return delay
}

// retryBackoff returns how long to wait before the next attempt of a URL
// that already failed a number of times. The wait grows with every attempt
// and carries a random jitter, so a group of workers that failed on the
// same host does not hit it again all at once.
func (e *Engine) retryBackoff(attempt int) time.Duration {
	var base time.Duration
	switch {
	case attempt <= 1:
		base = e.retryWindow
	case attempt == 2:
		base = 2 * e.retryWindow
	default:
		base = 4 * e.retryWindow
	}
	if base <= 0 {
		return 0
	}
	// The wait drifts by up to a tenth of the window in either direction, so
	// the backoff keeps its shape while the exact moment moves.
	spread := int64(base * retryJitterPercent / 100)
	if spread < 1 {
		spread = 1
	}
	return base + time.Duration(rand.Int64N(2*spread+1)-spread)
}

// scheduleRetry puts a URL that failed with a transient error back on the
// queue so it is fetched again once the retry window opens. Permanent
// failures, such as a refused host, an excluded path, or a page that is not
// there, are dropped instead, and a URL that keeps failing is given up on
// once the retry budget is spent.
func (e *Engine) scheduleRetry(rawURL string, depth int, err error) {
	if err == nil || crawler.IsPermanent(err) {
		return
	}

	e.mu.Lock()
	attempt := e.retryCounts[rawURL] + 1
	e.retryCounts[rawURL] = attempt
	if attempt > maxRetries {
		// The retry budget is spent, so the URL counts as visited and is
		// not picked up again.
		delete(e.retryAt, rawURL)
		e.visited[rawURL] = struct{}{}
		e.mu.Unlock()
		if e.Store != nil {
			e.Store.MarkVisited(rawURL)
		}
		return
	}
	e.retryAt[rawURL] = time.Now().Add(e.retryBackoff(attempt))
	delete(e.visited, rawURL)
	// The URL is marked pending so another page that links to it does not
	// queue a second copy while the retry is waiting.
	e.pending[rawURL] = struct{}{}
	e.mu.Unlock()

	if e.Store != nil {
		e.Store.DeleteVisited(rawURL)
	}

	if !e.enqueue(crawlItem{url: rawURL, depth: depth}, 0) {
		e.mu.Lock()
		delete(e.pending, rawURL)
		e.mu.Unlock()
	}
}

// markDequeued moves a URL from pending to visited so the "discovered" stat
// reflects everything the crawler has picked up. The retry count is kept so
// a URL that keeps failing runs out of attempts instead of being retried
// forever.
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
		e.recordHostStats(rawURL, false, err)
		if ctx.Err() == nil {
			log.Printf("crawl: %s: %v", rawURL, err)
			e.scheduleRetry(rawURL, item.depth, err)
		}
		return
	}
	e.recordHostStats(rawURL, true, nil)
	e.mu.Lock()
	delete(e.retryCounts, rawURL)
	delete(e.retryAt, rawURL)
	e.mu.Unlock()

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
	e.countHostDoc(rawURL)

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
	links := crawler.LimitLinks(crawler.ExtractLinks(html, base))
	dirtyLinks := make(map[string]int, len(links))
	var toEnqueue []crawlItem
	for _, link := range links {
		e.LinkStore.Increment(link)
		dirtyLinks[link] = e.LinkStore.Get(link)

		normalized := crawler.NormalizeURL(link)
		if err := crawler.CheckCrawlExclusions(normalized); err != nil {
			// The link is still counted above so in-degree stays accurate,
			// but noisy routes and query traps never reach the queue.
			continue
		}
		childDepth := depth + 1
		if e.maxDepth >= 0 && childDepth > e.maxDepth {
			continue
		}
		if e.maxPages > 0 && e.Indexer.DocCount() >= e.maxPages {
			continue
		}
		// Per-host budgets keep one noisy site from filling the index. Links
		// that fall outside the budget are still counted for in-degree.
		if !e.hostHasBudget(hostOf(normalized)) {
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
