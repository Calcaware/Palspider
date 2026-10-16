package indexer

import (
	"container/heap"
	"net/url"
	"sort"
	"strings"
)

// PrefixMatch is one autocomplete candidate together with the weight used to
// rank it against the other candidates of the same lookup.
type PrefixMatch struct {
	Text   string
	Weight int
}

// prefixTrie stores complete entries (vocabulary terms, host names) ordered by
// byte, so a prefix lookup walks one branch instead of visiting every entry.
// Children live in a small slice sorted by byte: no node holds many of them,
// and a slice is far cheaper than a map for that shape.
type prefixTrie struct {
	root trieNode
}

type trieNode struct {
	entry    string
	hasEntry bool
	children []trieChild
}

type trieChild struct {
	seg  byte
	node *trieNode
}

// childIndex returns the position of seg in n.children, which is also the
// place a new child for seg belongs. Callers must check the child at that
// position really uses seg.
func childIndex(n *trieNode, seg byte) int {
	return sort.Search(len(n.children), func(i int) bool {
		return n.children[i].seg >= seg
	})
}

// Insert stores entry in the trie. Storing the same entry twice is a no-op,
// and an empty entry is kept at the root so callers can still find it.
func (t *prefixTrie) Insert(entry string) {
	node := &t.root
	for i := 0; i < len(entry); i++ {
		seg := entry[i]
		idx := childIndex(node, seg)
		if idx < len(node.children) && node.children[idx].seg == seg {
			node = node.children[idx].node
			continue
		}
		child := &trieNode{}
		node.children = append(node.children, trieChild{})
		copy(node.children[idx+1:], node.children[idx:])
		node.children[idx] = trieChild{seg: seg, node: child}
		node = child
	}
	node.entry = entry
	node.hasEntry = true
}

// Walk reports every stored entry starting with prefix to fn in lexicographic
// order. An empty prefix visits the whole trie. The walk is iterative because
// entries can be long enough to make recursion pointless.
func (t *prefixTrie) Walk(prefix string, fn func(entry string)) {
	node := &t.root
	for i := 0; i < len(prefix); i++ {
		seg := prefix[i]
		idx := childIndex(node, seg)
		if idx >= len(node.children) || node.children[idx].seg != seg {
			return
		}
		node = node.children[idx].node
	}

	stack := []*trieNode{node}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current.hasEntry {
			fn(current.entry)
		}
		// Pushed in reverse so the lowest byte is popped first, which keeps
		// the reported order lexicographic.
		for i := len(current.children) - 1; i >= 0; i-- {
			stack = append(stack, current.children[i].node)
		}
	}
}

// matchBetter reports whether a should be offered before b: heavier weight
// first, then alphabetically so ties keep a stable order.
func matchBetter(a, b PrefixMatch) bool {
	if a.Weight != b.Weight {
		return a.Weight > b.Weight
	}
	return a.Text < b.Text
}

// matchHeap is a min heap ordered worst candidate first, so the root is the
// entry evicted when a better one arrives.
type matchHeap []PrefixMatch

func (h matchHeap) Len() int { return len(h) }

func (h matchHeap) Less(i, j int) bool { return matchBetter(h[j], h[i]) }

func (h matchHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *matchHeap) Push(x any) { *h = append(*h, x.(PrefixMatch)) }

func (h *matchHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	*h = old[:last]
	return item
}

// topMatches keeps the best limit candidates of a lookup in one pass, so a
// wide prefix such as a single letter does not drag the whole matching range
// into memory just to throw most of it away.
type topMatches struct {
	limit int
	kept  matchHeap
}

func newTopMatches(limit int) *topMatches {
	return &topMatches{limit: limit}
}

func (t *topMatches) add(m PrefixMatch) {
	if t.limit <= 0 {
		return
	}
	if len(t.kept) < t.limit {
		heap.Push(&t.kept, m)
		return
	}
	if matchBetter(m, t.kept[0]) {
		heap.Pop(&t.kept)
		heap.Push(&t.kept, m)
	}
}

// matches returns the kept candidates best first. The slice is never nil.
func (t *topMatches) matches() []PrefixMatch {
	out := make([]PrefixMatch, len(t.kept))
	copy(out, t.kept)
	sort.Slice(out, func(i, j int) bool { return matchBetter(out[i], out[j]) })
	return out
}

// TermsWithPrefix returns up to limit vocabulary terms starting with prefix,
// best first by document frequency and then alphabetically. An empty prefix
// reports the whole vocabulary, and a limit below 1 returns an empty slice.
// The lookup holds the index read lock and touches only the matching branch,
// so autocomplete does not scan the vocabulary on every keystroke.
func (ix *Indexer) TermsWithPrefix(prefix string, limit int) []PrefixMatch {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	top := newTopMatches(limit)
	ix.terms.Walk(prefix, func(term string) {
		// Read the document frequency straight from the postings map: the
		// lock is already held, and the trie only records which terms exist.
		top.add(PrefixMatch{Text: term, Weight: len(ix.index[term])})
	})
	return top.matches()
}

// HostsWithPrefix returns up to limit host names from indexed documents that
// start with prefix, best first by number of documents and then
// alphabetically. An empty prefix reports every known host, and a limit below
// 1 returns an empty slice.
func (ix *Indexer) HostsWithPrefix(prefix string, limit int) []PrefixMatch {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	top := newTopMatches(limit)
	ix.hosts.Walk(prefix, func(host string) {
		top.add(PrefixMatch{Text: host, Weight: len(ix.hostDocs[host])})
	})
	return top.matches()
}

// hostOf returns the name used for domain hints: lowercased, stripped of a
// leading or trailing dot, and empty when the URL has no usable host.
func hostOf(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(strings.Trim(parsed.Hostname(), "."))
}

// trackHost records docID under the host of rawURL. The document ID is stored
// rather than a plain counter so restoring the same document again leaves the
// weight unchanged instead of inflating it. Callers must hold the write lock.
func (ix *Indexer) trackHost(docID int, rawURL string) {
	host := hostOf(rawURL)
	if host == "" {
		return
	}
	if ix.hostDocs == nil {
		ix.hostDocs = make(map[string]map[int]struct{})
	}
	docs, ok := ix.hostDocs[host]
	if !ok {
		docs = make(map[int]struct{})
		ix.hostDocs[host] = docs
		ix.hosts.Insert(host)
	}
	docs[docID] = struct{}{}
}
