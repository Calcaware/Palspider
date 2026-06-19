package indexer

import "sync"

type LinkStore struct {
	mu      sync.Mutex
	counts  map[string]int
}

func NewLinkStore() *LinkStore {
	return &LinkStore{counts: make(map[string]int)}
}

func (ls *LinkStore) Increment(url string) {
	ls.mu.Lock()
	ls.counts[url]++
	ls.mu.Unlock()
}

func (ls *LinkStore) Get(url string) int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.counts[url]
}

func (ls *LinkStore) Set(url string, count int) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.counts[url] = count
}
