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

type Engine struct {
	Indexer   *indexer.Indexer
	Trie      *trie.Trie
	Searcher  *searcher.Searcher
	LinkStore *indexer.LinkStore
	Peers     *peer.PeerManager
	Store     *store.Store
	limiter   *crawler.RateLimiter

	workers    int
	urlChan    chan string
	visited    map[string]struct{}
	fpSeen     map[string]struct{}
	pending    map[string]struct{}
	mu         sync.Mutex
	wg         sync.WaitGroup
	active     int32
	cancel     context.CancelFunc
	queueSize  int32
}

type Stats struct {
	Documents int    `json:"documents"`
	Terms     int    `json:"terms"`
	Visited   int    `json:"visited"`
	QueueSize int32  `json:"queueSize"`
	Active    int32  `json:"active"`
}

type CrawlStatus struct {
	QueueSize int32 `json:"queueSize"`
	Active    int32 `json:"active"`
	Crawling  bool  `json:"crawling"`
	Visited   int   `json:"visited"`
	Documents int   `json:"documents"`
	Terms     int   `json:"terms"`
}

func New(workerCount int, st *store.Store) *Engine {
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
		urlChan:   make(chan string, 100000),
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

func (e *Engine) persistDoc(doc indexer.Document, terms []string) {
	if e.Store == nil {
		return
	}
	if err := e.Store.SaveDocument(doc.ID, doc); err != nil {
		log.Printf("save doc: %v", err)
	}
	seen := make(map[string]bool)
	for _, term := range terms {
		if seen[term] {
			continue
		}
		seen[term] = true
		postings := e.Indexer.GetPostings(term)
		if err := e.Store.SavePostings(term, postings); err != nil {
			log.Printf("save postings: %v", err)
		}
	}
	if err := e.Store.SaveNextDocID(e.Indexer.NextID()); err != nil {
		log.Printf("save next id: %v", err)
	}
}

func (e *Engine) Seed(url string) {
	normalized := crawler.NormalizeURL(url)
	e.mu.Lock()
	if _, ok := e.visited[normalized]; ok {
		e.mu.Unlock()
		return
	}
	e.visited[normalized] = struct{}{}
	e.mu.Unlock()

	if e.Store != nil {
		e.Store.MarkVisited(normalized)
	}

	e.urlChan <- normalized
}

func (e *Engine) AddDocument(rawURL, title, text string) {
	terms := crawler.Tokenize(text)
	id := e.Indexer.AddDocument(rawURL, title, text, terms)
	e.Trie.Insert(rawURL, id)

	linkCount := e.LinkStore.Get(rawURL)
	e.Indexer.SetLinkCount(id, linkCount)

	e.persistDoc(indexer.Document{
		ID: id, URL: rawURL, Title: title, Text: text, LinkCount: linkCount,
	}, terms)
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

	var remoteResults []searcher.Result
	if e.Peers != nil {
		peerResults := e.Peers.SearchAll(query, limit, 5*time.Second)
		for _, pr := range peerResults {
			if pr.Error == nil {
				remoteResults = append(remoteResults, pr.Results...)
			}
		}
	}

	return mergeResults(localResults, remoteResults, limit)
}

func (e *Engine) SearchLocal(query string, limit int) []searcher.Result {
	return e.Searcher.Search(query, limit)
}

func mergeResults(local, remote []searcher.Result, limit int) []searcher.Result {
	seen := make(map[string]*searcher.Result)

	for i := range local {
		seen[local[i].URL] = &local[i]
	}
	for i := range remote {
		r := remote[i]
		if _, ok := seen[r.URL]; !ok {
			seen[r.URL] = &r
		}
	}

	results := make([]searcher.Result, 0, len(seen))
	for _, r := range seen {
		results = append(results, *r)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit > len(results) {
		limit = len(results)
	}
	return results[:limit]
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
		Terms:     len(e.Indexer.AllTerms()),
		Visited:   visited,
		QueueSize: atomic.LoadInt32(&e.queueSize),
		Active:    atomic.LoadInt32(&e.active),
	}
}

func (e *Engine) CrawlStatus() CrawlStatus {
	e.mu.Lock()
	visited := len(e.visited)
	e.mu.Unlock()
	return CrawlStatus{
		QueueSize: atomic.LoadInt32(&e.queueSize),
		Active:    atomic.LoadInt32(&e.active),
		Crawling:  e.active > 0 || e.queueSize > 0,
		Visited:   visited,
		Documents: e.Indexer.DocCount(),
		Terms:     len(e.Indexer.AllTerms()),
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
		case rawURL := <-e.urlChan:
			e.processURL(ctx, rawURL)
		}
	}
}

func (e *Engine) processURL(ctx context.Context, rawURL string) {
	atomic.AddInt32(&e.active, 1)
	defer atomic.AddInt32(&e.active, -1)

	html, err := crawler.Fetch(rawURL, e.limiter)
	if err != nil {
		return
	}

	title, text := crawler.ExtractText(html)
	if text == "" {
		return
	}

	fp := crawler.ContentFingerprint(text)
	e.mu.Lock()
	if _, ok := e.fpSeen[fp]; ok {
		e.mu.Unlock()
		return
	}
	e.fpSeen[fp] = struct{}{}
	e.mu.Unlock()

	if e.Store != nil {
		e.Store.MarkFingerprint(fp)
	}

	terms := crawler.Tokenize(text)
	docID := e.Indexer.AddDocument(rawURL, title, text, terms)
	e.Trie.Insert(rawURL, docID)

	linkCount := e.LinkStore.Get(rawURL)
	e.Indexer.SetLinkCount(docID, linkCount)

	e.persistDoc(indexer.Document{
		ID: docID, URL: rawURL, Title: title, Text: text, LinkCount: linkCount,
	}, terms)

	base, err := url.Parse(rawURL)
	if err != nil {
		return
	}

	links := crawler.ExtractLinks(html, base)
	for _, link := range links {
		e.LinkStore.Increment(link)

		if e.Store != nil {
			e.Store.SaveLinkCount(link, e.LinkStore.Get(link))
		}

		normalized := crawler.NormalizeURL(link)

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

		select {
		case e.urlChan <- normalized:
		case <-ctx.Done():
			return
		}
	}
}
