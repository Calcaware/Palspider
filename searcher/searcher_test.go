package searcher

import (
	"testing"

	"github.com/calcaware/palspider/crawler"
	"github.com/calcaware/palspider/indexer"
	"github.com/calcaware/palspider/trie"
)

func newSearcher() (*Searcher, *indexer.Indexer) {
	ix := indexer.New()
	return New(ix, trie.New()), ix
}

func addDoc(ix *indexer.Indexer, url, text string) int {
	return ix.AddDocument(url, url, text, crawler.Tokenize(text))
}

func TestSearchRanksByTFAndLinks(t *testing.T) {
	s, ix := newSearcher()
	hi := addDoc(ix, "https://a.com/1", "golang golang golang search engine")
	lo := addDoc(ix, "https://a.com/2", "golang")
	ix.SetLinkCount(hi, 20)

	results := s.Search("golang", 10)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].ID != hi {
		t.Errorf("top result = doc %d, want doc %d (higher tf and links)", results[0].ID, hi)
	}
	if results[0].Score <= results[1].Score {
		t.Errorf("scores not descending: %v then %v", results[0].Score, results[1].Score)
	}
	_ = lo
}

func TestSearchRespectsLimit(t *testing.T) {
	s, ix := newSearcher()
	for i := 0; i < 5; i++ {
		addDoc(ix, "https://a.com/x", "common word")
	}
	if got := s.Search("common", 3); len(got) != 3 {
		t.Errorf("got %d results, want 3", len(got))
	}
}

func TestSearchEmptyQueryAndNoMatches(t *testing.T) {
	s, ix := newSearcher()
	addDoc(ix, "https://a.com", "some text")
	if r := s.Search("", 10); r != nil {
		t.Errorf("empty query = %v, want nil", r)
	}
	if r := s.Search("zzzznotfound", 10); r != nil {
		t.Errorf("no-match query = %v, want nil", r)
	}
}

func TestSearchRejectsNonPositiveLimit(t *testing.T) {
	s, ix := newSearcher()
	addDoc(ix, "https://a.com", "golang text")
	if r := s.Search("golang", 0); len(r) != 0 {
		t.Errorf("limit 0 = %v, want no results", r)
	}
	if r := s.Search("golang", -3); len(r) != 0 {
		t.Errorf("limit -3 = %v, want no results instead of a panic", r)
	}
}

func TestSearchOrderIsDeterministic(t *testing.T) {
	s, ix := newSearcher()
	for i := 0; i < 8; i++ {
		addDoc(ix, "https://a.com/"+string(rune('a'+i)), "identical content here")
	}
	first := s.Search("identical", 8)
	for run := 0; run < 10; run++ {
		again := s.Search("identical", 8)
		for i := range first {
			if first[i].ID != again[i].ID {
				t.Fatalf("run %d differs at %d: doc %d vs %d", run, i, first[i].ID, again[i].ID)
			}
		}
	}
}

func TestSearchIsCaseInsensitive(t *testing.T) {
	s, ix := newSearcher()
	addDoc(ix, "https://a.com", "Golang Tutorial")
	if r := s.Search("GOLANG", 10); len(r) != 1 {
		t.Errorf("case-insensitive search returned %d results, want 1", len(r))
	}
}

func TestSuggestIsCaseInsensitive(t *testing.T) {
	s, ix := newSearcher()
	addDoc(ix, "https://a.com/1", "node network")
	addDoc(ix, "https://a.com/2", "node")

	got := s.Suggest("Nod", 5)
	if len(got) != 1 || got[0] != "node" {
		t.Errorf("Suggest(Nod) = %v, want [node]", got)
	}
	got = s.Suggest("NET", 5)
	if len(got) != 1 || got[0] != "network" {
		t.Errorf("Suggest(NET) = %v, want [network]", got)
	}
}

func TestSuggestRanksByFrequencyAndReturnsEmptySlice(t *testing.T) {
	s, ix := newSearcher()
	addDoc(ix, "https://a.com/1", "testing test")
	addDoc(ix, "https://a.com/2", "testing")
	addDoc(ix, "https://a.com/3", "testing")

	got := s.Suggest("test", 5)
	if len(got) == 0 || got[0] != "testing" {
		t.Errorf("Suggest(test) = %v, testing should rank first", got)
	}

	got = s.Suggest("", 5)
	if got == nil || len(got) != 0 {
		t.Errorf("Suggest(empty) = %v, want non-nil empty slice", got)
	}
	got = s.Suggest("qqqq", 5)
	if got == nil || len(got) != 0 {
		t.Errorf("Suggest(no match) = %v, want non-nil empty slice", got)
	}
}
