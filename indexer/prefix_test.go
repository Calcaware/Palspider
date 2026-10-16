package indexer

import (
	"fmt"
	"sync"
	"testing"
)

func TestPrefixTrieWalkIsLexicographic(t *testing.T) {
	var tr prefixTrie
	for _, entry := range []string{"nodes", "node", "net", "cat", "car", "node"} {
		tr.Insert(entry)
	}

	var all []string
	tr.Walk("", func(entry string) { all = append(all, entry) })
	want := []string{"car", "cat", "net", "node", "nodes"}
	if len(all) != len(want) {
		t.Fatalf("Walk(empty) = %v, want %v", all, want)
	}
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("Walk(empty) = %v, want %v", all, want)
		}
	}

	var branch []string
	tr.Walk("ca", func(entry string) { branch = append(branch, entry) })
	if len(branch) != 2 || branch[0] != "car" || branch[1] != "cat" {
		t.Fatalf("Walk(ca) = %v, want [car cat]", branch)
	}

	called := false
	tr.Walk("zz", func(string) { called = true })
	if called {
		t.Error("Walk(zz) reported entries for a prefix with no matches")
	}
}

func TestPrefixTrieKeepsEmptyEntry(t *testing.T) {
	var tr prefixTrie
	tr.Insert("")

	var got []string
	tr.Walk("", func(entry string) { got = append(got, entry) })
	if len(got) != 1 || got[0] != "" {
		t.Fatalf("Walk after empty insert = %q, want one empty entry", got)
	}
}

func TestTermsWithPrefixRanksByFrequency(t *testing.T) {
	ix := New()
	ix.AddDocument("https://a.com/1", "t", "", []string{"node", "nodejs", "network", "go"})
	ix.AddDocument("https://a.com/2", "t", "", []string{"node"})
	ix.AddDocument("https://a.com/3", "t", "", []string{"node"})

	got := ix.TermsWithPrefix("n", 10)
	want := []PrefixMatch{
		{Text: "node", Weight: 3},
		{Text: "network", Weight: 1},
		{Text: "nodejs", Weight: 1},
	}
	if len(got) != len(want) {
		t.Fatalf("TermsWithPrefix(n) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("TermsWithPrefix(n) = %v, want %v", got, want)
		}
	}

	if got := ix.TermsWithPrefix("nod", 10); len(got) != 2 || got[0].Text != "node" || got[1].Text != "nodejs" {
		t.Errorf("TermsWithPrefix(nod) = %v, want node then nodejs", got)
	}
	if got := ix.TermsWithPrefix("n", 1); len(got) != 1 || got[0].Text != "node" {
		t.Errorf("limited TermsWithPrefix(n) = %v, want just node", got)
	}
	if got := ix.TermsWithPrefix("n", 0); got == nil || len(got) != 0 {
		t.Errorf("TermsWithPrefix with limit 0 = %v, want an empty non-nil slice", got)
	}
	if got := ix.TermsWithPrefix("zz", 5); got == nil || len(got) != 0 {
		t.Errorf("TermsWithPrefix(zz) = %v, want an empty non-nil slice", got)
	}
	if got := ix.TermsWithPrefix("", 2); len(got) != 2 || got[0].Text != "node" || got[1].Text != "go" {
		t.Errorf("TermsWithPrefix(empty, 2) = %v, want node then go", got)
	}
}

func TestTermsWithPrefixSeesRestoredPostings(t *testing.T) {
	ix := New()
	ix.RestorePostings("node", map[int]int{0: 2, 1: 1})
	ix.RestorePostings("nodes", map[int]int{2: 1})

	got := ix.TermsWithPrefix("nod", 5)
	want := []PrefixMatch{
		{Text: "node", Weight: 2},
		{Text: "nodes", Weight: 1},
	}
	if len(got) != len(want) {
		t.Fatalf("TermsWithPrefix(nod) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("TermsWithPrefix(nod) = %v, want %v", got, want)
		}
	}
}

func TestHostsWithPrefixCountsDocuments(t *testing.T) {
	ix := New()
	ix.AddDocument("https://nodejs.org/a", "t", "text", nil)
	ix.AddDocument("https://nodejs.org/b", "t", "text", nil)
	ix.AddDocument("https://NodeJS.org./c", "t", "text", nil)
	ix.AddDocument("https://nodejs.dev/", "t", "text", nil)
	ix.AddDocument("https://example.com/", "t", "text", nil)

	got := ix.HostsWithPrefix("node", 5)
	want := []PrefixMatch{
		{Text: "nodejs.org", Weight: 3},
		{Text: "nodejs.dev", Weight: 1},
	}
	if len(got) != len(want) {
		t.Fatalf("HostsWithPrefix(node) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("HostsWithPrefix(node) = %v, want %v", got, want)
		}
	}

	if got := ix.HostsWithPrefix("node", 1); len(got) != 1 || got[0].Text != "nodejs.org" {
		t.Errorf("limited HostsWithPrefix(node) = %v, want just nodejs.org", got)
	}
	if got := ix.HostsWithPrefix("example", 5); len(got) != 1 || got[0].Text != "example.com" {
		t.Errorf("HostsWithPrefix(example) = %v, want example.com", got)
	}
	if got := ix.HostsWithPrefix("other", 5); got == nil || len(got) != 0 {
		t.Errorf("HostsWithPrefix(other) = %v, want an empty non-nil slice", got)
	}
}

func TestTrackHostIsIdempotentOnRestore(t *testing.T) {
	ix := New()
	doc := Document{ID: 0, URL: "https://nodejs.org/a", Title: "t", Text: "body"}
	ix.RestoreDocument(doc)
	// A second restore of the same snapshot must not inflate the host weight.
	ix.RestoreDocument(doc)

	got := ix.HostsWithPrefix("nodejs", 5)
	if len(got) != 1 || got[0].Weight != 1 {
		t.Errorf("host weight after a repeated restore = %v, want one document", got)
	}
}

func TestTrackHostIgnoresHostlessURLs(t *testing.T) {
	ix := New()
	ix.AddDocument("mailto:someone@example.com", "t", "text", nil)
	ix.AddDocument("://not a url", "t", "text", nil)

	if got := ix.HostsWithPrefix("", 5); got == nil || len(got) != 0 {
		t.Errorf("hosts = %v, want an empty non-nil slice", got)
	}
}

func TestPrefixLookupsAreConcurrencySafe(t *testing.T) {
	ix := New()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			term := fmt.Sprintf("term%d", worker)
			for i := 0; i < 25; i++ {
				ix.AddDocument(fmt.Sprintf("https://h%d.example/%d", worker, i), "t", "text", []string{term})
				ix.TermsWithPrefix("term", 3)
				ix.HostsWithPrefix("h", 3)
			}
		}(worker)
	}
	wg.Wait()

	got := ix.TermsWithPrefix("term", 10)
	if len(got) != 8 || got[0].Weight != 25 {
		t.Errorf("terms after concurrent adds = %v, want one term per worker with weight 25", got)
	}
	if hosts := ix.HostsWithPrefix("", 10); len(hosts) != 8 {
		t.Errorf("hosts after concurrent adds = %v, want one host per worker", hosts)
	}
}
