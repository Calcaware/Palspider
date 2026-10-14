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

func TestSearchBoostsTitleMatches(t *testing.T) {
	s, ix := newSearcher()
	titleDoc := addDoc(ix, "https://example.com/guide", "Golang guide for beginners")
	bodyDoc := addDoc(ix, "https://example.org/other", "This page talks about golang in the body text but not the title")

	results := s.Search("golang", 10)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].ID != titleDoc {
		t.Fatalf("top result = doc %d, want title-boosted doc %d", results[0].ID, titleDoc)
	}
	if results[0].URL == "https://example.org/other" {
		t.Fatalf("body-only match outranked title match: %#v", results)
	}
	if results[1].ID != bodyDoc {
		t.Fatalf("second result = doc %d, want %d", results[1].ID, bodyDoc)
	}
}

func TestSearchPrefersPhraseMatches(t *testing.T) {
	s, ix := newSearcher()
	phraseDoc := addDoc(ix, "https://a.com/phrase", "The exact phrase does matter here")
	otherDoc := addDoc(ix, "https://b.com/other", "This page mentions phrase but not the exact contiguous phrase it does matter")

	results := s.Search("\"phrase does\"", 10)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].ID != phraseDoc {
		t.Fatalf("phrase query should prefer contiguous phrase result; got %#v", results)
	}
	if results[1].ID != otherDoc {
		t.Fatalf("fall-back doc should remain second; got %#v", results)
	}
}

func TestSearchSupportsSortOptions(t *testing.T) {
	s, ix := newSearcher()
	oldDoc := addDoc(ix, "https://a.com/old", "golang tutorial")
	newDoc := addDoc(ix, "https://a.com/new", "golang tutorial")
	linkedDoc := addDoc(ix, "https://a.com/linked", "golang tutorial")
	ix.SetLinkCount(oldDoc, 1)
	ix.SetLinkCount(newDoc, 5)
	ix.SetLinkCount(linkedDoc, 9)

	if got := s.SearchWithSort("golang", 10, "newest"); got[0].ID != linkedDoc || got[1].ID != newDoc || got[2].ID != oldDoc {
		t.Fatalf("newest order = %#v, want [%d %d %d]", got, linkedDoc, newDoc, oldDoc)
	}
	if got := s.SearchWithSort("golang", 10, "oldest"); got[0].ID != oldDoc || got[1].ID != newDoc || got[2].ID != linkedDoc {
		t.Fatalf("oldest order = %#v, want [%d %d %d]", got, oldDoc, newDoc, linkedDoc)
	}
	if got := s.SearchWithSort("golang", 10, "most linked"); got[0].ID != linkedDoc || got[1].ID != newDoc || got[2].ID != oldDoc {
		t.Fatalf("most linked order = %#v, want [%d %d %d]", got, linkedDoc, newDoc, oldDoc)
	}
	if got := s.SearchWithSort("golang", 10, "unknown"); got[0].ID != linkedDoc || got[1].ID != newDoc || got[2].ID != oldDoc {
		t.Fatalf("default/fallback order = %#v, want relevance order [%d %d %d]", got, linkedDoc, newDoc, oldDoc)
	}
}

func TestRelatedRanksDocumentsBySharedTerms(t *testing.T) {
	s, ix := newSearcher()
	sourceURL := "https://example.com/source"
	relatedURL := "https://example.com/related"
	otherURL := "https://elsewhere.example/other"
	sourceText := "crawler search indexing ranking"
	relatedText := "crawler search indexing tools"
	otherText := "crawler garden vegetables"
	ix.AddDocument(sourceURL, "Crawler search", sourceText, crawler.Tokenize("Crawler search "+sourceText))
	ix.AddDocument(relatedURL, "Search indexing", relatedText, crawler.Tokenize("Search indexing "+relatedText))
	ix.AddDocument(otherURL, "Garden", otherText, crawler.Tokenize("Garden "+otherText))

	results := s.Related(sourceURL, 5)
	if len(results) != 2 {
		t.Fatalf("Related() returned %d results, want 2", len(results))
	}
	if results[0].URL != relatedURL {
		t.Fatalf("first related result = %q, want %q", results[0].URL, relatedURL)
	}
	if results[0].Score <= results[1].Score {
		t.Fatalf("related scores are not descending: %v then %v", results[0].Score, results[1].Score)
	}
	if got := s.Related("https://missing.example/", 5); len(got) != 0 {
		t.Fatalf("unknown source returned related results: %#v", got)
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

func TestSuggestIncludesDomainHints(t *testing.T) {
	s, ix := newSearcher()
	addDoc(ix, "https://nodejs.org/", "node runtime")
	addDoc(ix, "https://example.com/", "unrelated text")

	got := s.Suggest("node", 5)
	if len(got) == 0 || got[0] != "node" {
		t.Fatalf("Suggest(node) = %v, want node first", got)
	}
	found := false
	for _, v := range got {
		if v == "nodejs.org" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Suggest(node) = %v, want nodejs.org domain hint", got)
	}
}

func TestSuggestCompletesMultiWordPhrases(t *testing.T) {
	s, ix := newSearcher()
	addDoc(ix, "https://a.com/1", "search engine optimization guide")
	addDoc(ix, "https://a.com/2", "unrelated engine repair")

	got := s.Suggest("search eng", 5)
	if len(got) == 0 || got[0] != "search engine" {
		t.Fatalf("Suggest(search eng) = %v, want search engine", got)
	}
	// The standalone "engine" page must not contribute a suggestion because it
	// does not contain the preceding term.
	for _, v := range got {
		if v == "engine repair" {
			t.Fatalf("Suggest(search eng) = %v, leaked a suggestion from an unrelated document", got)
		}
	}
}
