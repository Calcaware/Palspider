package trie

import (
	"net/url"
	"strings"
	"sync"
)

type Node struct {
	segment  string
	docIDs   map[int]struct{}
	mu       sync.RWMutex
	children map[string]*Node
}

type Trie struct {
	root *Node
	mu   sync.RWMutex
}

func New() *Trie {
	return &Trie{
		root: &Node{
			segment:  "",
			docIDs:   make(map[int]struct{}),
			children: make(map[string]*Node),
		},
	}
}

func (t *Trie) Insert(rawURL string, docID int) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return
	}
	segs := keySegments(u)

	t.mu.Lock()
	node := t.root
	for _, seg := range segs {
		child, ok := node.children[seg]
		if !ok {
			child = &Node{
				segment:  seg,
				docIDs:   make(map[int]struct{}),
				children: make(map[string]*Node),
			}
			node.children[seg] = child
		}
		node = child
	}
	node.mu.Lock()
	node.docIDs[docID] = struct{}{}
	node.mu.Unlock()
	t.mu.Unlock()
}

func (t *Trie) GetDocIDsUnder(rawURL string) map[int]struct{} {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	node := t.root
	for _, seg := range keySegments(u) {
		child, ok := node.children[seg]
		if !ok {
			return nil
		}
		node = child
	}

	result := make(map[int]struct{})
	collect(node, result)
	return result
}

func (t *Trie) Depth(rawURL string) int {
	u, err := url.Parse(rawURL)
	if err != nil {
		return 0
	}
	return len(segments(u.Path))
}

// keySegments scopes trie entries by host so identical paths on different
// websites are not counted as siblings. The trie lock must be held for any
// walk that continues into collect.
func keySegments(u *url.URL) []string {
	segs := make([]string, 0, len(segments(u.Path))+1)
	if u.Host != "" {
		segs = append(segs, strings.ToLower(u.Host))
	}
	return append(segs, segments(u.Path)...)
}

func segments(path string) []string {
	parts := strings.Split(path, "/")
	var out []string
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// collect reads children maps and must only be called while t.mu is held.
func collect(n *Node, result map[int]struct{}) {
	n.mu.RLock()
	for id := range n.docIDs {
		result[id] = struct{}{}
	}
	n.mu.RUnlock()
	for _, child := range n.children {
		collect(child, result)
	}
}
