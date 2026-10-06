package indexer

import (
	"sync"
)

type Document struct {
	ID        int
	URL       string
	Title     string
	Text      string
	LinkCount int
}

type Indexer struct {
	mu     sync.RWMutex
	docs   []Document
	nextID int
	index  map[string]map[int]int
	byURL  map[string]int
}

func New() *Indexer {
	return &Indexer{
		index: make(map[string]map[int]int),
		byURL: make(map[string]int),
	}
}

func (ix *Indexer) AddDocument(url, title, text string, terms []string) int {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	id := ix.nextID
	ix.nextID++
	ix.docs = append(ix.docs, Document{
		ID:    id,
		URL:   url,
		Title: title,
		Text:  text,
	})
	if _, ok := ix.byURL[url]; !ok {
		ix.byURL[url] = id
	}

	for _, term := range terms {
		postings, ok := ix.index[term]
		if !ok {
			postings = make(map[int]int)
			ix.index[term] = postings
		}
		postings[id]++
	}

	return id
}

func (ix *Indexer) SetLinkCount(docID, count int) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if docID >= 0 && docID < len(ix.docs) {
		ix.docs[docID].LinkCount = count
	}
}

func (ix *Indexer) GetDoc(id int) (Document, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if id < 0 || id >= len(ix.docs) {
		return Document{}, false
	}
	return ix.docs[id], true
}

func (ix *Indexer) GetPostings(term string) map[int]int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	postings, ok := ix.index[term]
	if !ok {
		return nil
	}
	result := make(map[int]int, len(postings))
	for k, v := range postings {
		result[k] = v
	}
	return result
}

func (ix *Indexer) DocCount() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.docs)
}

func (ix *Indexer) AllTerms() []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	terms := make([]string, 0, len(ix.index))
	for t := range ix.index {
		terms = append(terms, t)
	}
	return terms
}

// TermCount returns the vocabulary size without allocating.
func (ix *Indexer) TermCount() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.index)
}

// WalkTerms iterates the vocabulary under a single read lock, passing each
// term and its document frequency to fn.
func (ix *Indexer) WalkTerms(fn func(term string, df int)) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for t, p := range ix.index {
		fn(t, len(p))
	}
}

// IDForURL reports whether a document with this exact URL is indexed.
func (ix *Indexer) IDForURL(url string) (int, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	id, ok := ix.byURL[url]
	return id, ok
}

func (ix *Indexer) RestoreDocument(doc Document) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if doc.ID >= len(ix.docs) {
		newDocs := make([]Document, doc.ID+1)
		copy(newDocs, ix.docs)
		ix.docs = newDocs
	}
	ix.docs[doc.ID] = doc
	if _, ok := ix.byURL[doc.URL]; !ok {
		ix.byURL[doc.URL] = doc.ID
	}
	if doc.ID >= ix.nextID {
		ix.nextID = doc.ID + 1
	}
}

func (ix *Indexer) RestorePostings(term string, postings map[int]int) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.index[term] = postings
}

func (ix *Indexer) SetNextID(id int) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.nextID = id
}

func (ix *Indexer) NextID() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.nextID
}
