package searcher

import (
	"math"
	"sort"

	"github.com/calcaware/palspider/crawler"
	"github.com/calcaware/palspider/indexer"
	"github.com/calcaware/palspider/trie"
)

type Result struct {
	ID         int     `json:"id"`
	URL        string  `json:"url"`
	Title      string  `json:"title"`
	Text       string  `json:"text"`
	Score      float64 `json:"score"`
	LinkCount  int     `json:"linkCount"`
	PeerSource string  `json:"peerSource,omitempty"`
}

type Searcher struct {
	ix   *indexer.Indexer
	tr   *trie.Trie
}

func New(ix *indexer.Indexer, tr *trie.Trie) *Searcher {
	return &Searcher{ix: ix, tr: tr}
}

func (s *Searcher) Search(query string, limit int) []Result {
	terms := crawler.Tokenize(query)
	if len(terms) == 0 {
		return nil
	}

	N := s.ix.DocCount()
	if N == 0 {
		return nil
	}

	scores := make(map[int]float64)

	for _, term := range terms {
		postings := s.ix.GetPostings(term)
		if len(postings) == 0 {
			continue
		}

		idf := math.Log(float64(N)/float64(len(postings))) + 1

		for docID, tf := range postings {
			tfScore := 1 + math.Log(float64(tf))
			scores[docID] += tfScore * idf
		}
	}

	if len(scores) == 0 {
		return nil
	}

	matched := make(map[int]bool)
	for id := range scores {
		matched[id] = true
	}

	clusterBoost := make(map[int]float64)
	for docID := range scores {
		doc, ok := s.ix.GetDoc(docID)
		if !ok {
			continue
		}
		siblings := s.tr.GetDocIDsUnder(doc.URL)
		count := 0
		for sid := range siblings {
			if matched[sid] {
				count++
			}
		}
		if count > 1 {
			clusterBoost[docID] = math.Log(1 + float64(count))
		}

		linkBoost := 1 + 0.5*math.Log(1+float64(doc.LinkCount))
		scores[docID] *= linkBoost

		depth := s.tr.Depth(doc.URL)
		depthBonus := 1 + 1.0/(1.0+float64(depth))
		scores[docID] *= depthBonus
	}

	for docID, boost := range clusterBoost {
		scores[docID] *= 1 + boost*0.2
	}

	ranked := make([]int, 0, len(scores))
	for id := range scores {
		ranked = append(ranked, id)
	}

	sort.Slice(ranked, func(i, j int) bool {
		return scores[ranked[i]] > scores[ranked[j]]
	})

	if limit > len(ranked) {
		limit = len(ranked)
	}

	results := make([]Result, 0, limit)
	for _, id := range ranked[:limit] {
		doc, ok := s.ix.GetDoc(id)
		if !ok {
			continue
		}
		results = append(results, Result{
			ID:        doc.ID,
			URL:       doc.URL,
			Title:     doc.Title,
			Text:      doc.Text,
			Score:     math.Round(scores[id]*100) / 100,
			LinkCount: doc.LinkCount,
		})
	}
	return results
}

func (s *Searcher) Suggest(prefix string, limit int) []string {
	lower := prefix
	type match struct {
		term  string
		count int
	}
	var matches []match

	for _, term := range s.ix.AllTerms() {
		if len(term) >= len(lower) && term[:len(lower)] == lower {
			postings := s.ix.GetPostings(term)
			matches = append(matches, match{term, len(postings)})
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].count > matches[j].count
	})

	if limit > len(matches) {
		limit = len(matches)
	}

	out := make([]string, limit)
	for i, m := range matches[:limit] {
		out[i] = m.term
	}
	return out
}
