package searcher

import (
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/calcaware/palspider/crawler"
	"github.com/calcaware/palspider/indexer"
)

// TestSuggestMatchesVocabularyScan keeps the prefix index honest by checking
// it against the long way round: scan every vocabulary term and every indexed
// document, then rank the matches exactly as the plain scan would.
func TestSuggestMatchesVocabularyScan(t *testing.T) {
	s, ix := newSearcher()
	docs := []struct{ url, text string }{
		{"https://nodejs.org/docs", "node nodejs runtime network node"},
		{"https://nodejs.org/api", "node api docs network"},
		{"https://nominet.example/", "node nominet registry"},
		{"https://noise.example/a", "node noise nodes"},
		{"https://noise.example/b", "nope noodle noise"},
		{"https://alpha.example/", "alpha alphabet alpine"},
		{"https://beta.example/", "beta betamax"},
		{"https://gamma.example/", "gamma"},
		{"https://nodejs.dev/", "node dev"},
		{"https://nodemon.example/", "nodemon node"},
	}
	for _, d := range docs {
		addDoc(ix, d.url, d.text)
	}
	// A document without terms still contributes a host hint, and a URL that
	// cannot be parsed must be ignored exactly like the plain scan did.
	ix.AddDocument("https://northwind.example/", "t", "nothing here", nil)
	ix.AddDocument("not a url", "t", "", []string{"noise"})

	prefixes := []string{"no", "nod", "node", "nn", "al", "be", "ga", "north", "nope", "zz"}
	limits := []int{1, 2, 3, 5, 20}

	for _, prefix := range prefixes {
		if len(crawler.Tokenize(prefix)) != 1 {
			continue
		}
		for _, limit := range limits {
			want := rankByVocabularyScan(ix, prefix, limit)
			if got := s.Suggest(prefix, limit); !sameStrings(got, want) {
				t.Fatalf("Suggest(%q, %d) = %v, want %v from the vocabulary scan", prefix, limit, got, want)
			}
		}
	}
}

// rankByVocabularyScan walks every term and document to build the ranked
// suggestion list that the prefix index answers without the walk.
func rankByVocabularyScan(ix *indexer.Indexer, prefix string, limit int) []string {
	counts := map[string]int{}
	var order []string
	add := func(text string, weight int) {
		if text == "" {
			return
		}
		if _, ok := counts[text]; !ok {
			order = append(order, text)
		}
		counts[text] += weight
	}

	ix.WalkTerms(func(term string, df int) {
		if strings.HasPrefix(term, prefix) {
			add(term, df)
		}
	})
	for _, doc := range ix.AllDocs() {
		parsed, err := url.Parse(doc.URL)
		if err != nil {
			continue
		}
		host := strings.ToLower(strings.Trim(parsed.Hostname(), "."))
		if host != "" && strings.HasPrefix(host, prefix) {
			add(host, 1)
		}
	}

	sort.Slice(order, func(i, j int) bool {
		if counts[order[i]] != counts[order[j]] {
			return counts[order[i]] > counts[order[j]]
		}
		return order[i] < order[j]
	})
	if limit > len(order) {
		limit = len(order)
	}
	out := make([]string, limit)
	copy(out, order[:limit])
	return out
}
