package searcher

import (
	"math"
	"net/url"
	"sort"
	"strings"
	"unicode"

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

type scoredRelated struct {
	result Result
	score  float64
}

type SortMode string

const (
	SortRelevance SortMode = "relevance"
	SortNewest     SortMode = "newest"
	SortOldest     SortMode = "oldest"
	SortMostLinked SortMode = "most_linked"
)

func NormalizeSortOrder(raw string) SortMode {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	normalized = strings.ReplaceAll(normalized, " ", "")

	switch normalized {
	case "", "relevance", "score":
		return SortRelevance
	case "newest":
		return SortNewest
	case "oldest":
		return SortOldest
	case "mostlinked", "mostlink", "linkcount":
		return SortMostLinked
	default:
		return SortRelevance
	}
}

type Searcher struct {
	ix *indexer.Indexer
	tr *trie.Trie
}

func New(ix *indexer.Indexer, tr *trie.Trie) *Searcher {
	return &Searcher{ix: ix, tr: tr}
}

func parseSearchQuery(query string) ([]string, []string) {
	terms := make([]string, 0, 8)
	phrases := make([]string, 0, 4)
	seenTerms := make(map[string]struct{})
	var current strings.Builder
	var phrase strings.Builder
	inPhrase := false
	flushTerm := func() {
		if current.Len() == 0 {
			return
		}
		for _, term := range crawler.Tokenize(current.String()) {
			if _, ok := seenTerms[term]; ok {
				continue
			}
			terms = append(terms, term)
			seenTerms[term] = struct{}{}
		}
		current.Reset()
	}
	flushPhrase := func() {
		if phrase.Len() == 0 {
			return
		}
		text := strings.TrimSpace(phrase.String())
		if text == "" {
			phrase.Reset()
			return
		}
		phraseTerms := crawler.Tokenize(text)
		if len(phraseTerms) > 0 {
			for _, term := range phraseTerms {
				if _, ok := seenTerms[term]; ok {
					continue
				}
				terms = append(terms, term)
				seenTerms[term] = struct{}{}
			}
		}
		if len(phraseTerms) > 1 {
			phrases = append(phrases, strings.Join(phraseTerms, " "))
		}
		phrase.Reset()
	}

	for _, r := range query {
		switch {
		case r == '"':
			if inPhrase {
				flushPhrase()
				inPhrase = false
				continue
			}
			flushTerm()
			inPhrase = true
		case inPhrase:
			phrase.WriteRune(r)
		case unicode.IsSpace(r):
			flushTerm()
		default:
			current.WriteRune(r)
		}
	}
	if inPhrase {
		flushPhrase()
	} else {
		flushTerm()
	}
	return terms, phrases
}

func termPositions(tokens []string) map[string][]int {
	positions := make(map[string][]int)
	for i, tok := range tokens {
		if tok == "" {
			continue
		}
		positions[tok] = append(positions[tok], i)
	}
	return positions
}

func phraseMatches(doc indexer.Document, phrase string) bool {
	phraseTerms := crawler.Tokenize(strings.ToLower(phrase))
	if len(phraseTerms) < 2 {
		return false
	}
	tokens := crawler.Tokenize(strings.ToLower(doc.Title + " " + doc.Text))
	for i := 0; i+len(phraseTerms) <= len(tokens); i++ {
		match := true
		for j, term := range phraseTerms {
			if tokens[i+j] != term {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func proximityBoost(doc indexer.Document, terms []string) float64 {
	if len(terms) < 2 {
		return 0
	}
	tokens := crawler.Tokenize(strings.ToLower(doc.Title + " " + doc.Text))
	positions := termPositions(tokens)
	best := math.MaxFloat64
	seen := 0
	for i := 0; i < len(terms); i++ {
		for j := i + 1; j < len(terms); j++ {
			if terms[i] == terms[j] {
				continue
			}
			arr1, ok1 := positions[terms[i]]
			arr2, ok2 := positions[terms[j]]
			if !ok1 || !ok2 {
				continue
			}
			seen++
			for _, a := range arr1 {
				for _, b := range arr2 {
					d := math.Abs(float64(a - b))
					if d < best {
						best = d
					}
				}
			}
		}
	}
	if seen == 0 || best == math.MaxFloat64 {
		return 0
	}
	return 20.0 / (best + 1.0)
}

// Search ranks indexed documents against the query terms. limit <= 0 returns
// no results; ties are broken by document ID so repeated searches and
// paginated requests return a stable order.
func (s *Searcher) Search(query string, limit int) []Result {
	return s.SearchWithSort(query, limit, string(SortRelevance))
}

func (s *Searcher) SearchWithSort(query string, limit int, rawSort string) []Result {
	if limit <= 0 {
		return nil
	}
	terms, phrases := parseSearchQuery(query)
	if len(terms) == 0 && len(phrases) == 0 {
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

	for _, phrase := range phrases {
		for docID := 0; docID < N; docID++ {
			doc, ok := s.ix.GetDoc(docID)
			if !ok {
				continue
			}
			if phraseMatches(doc, phrase) {
				scores[docID] += 8.0 * float64(len(crawler.Tokenize(phrase)))
			}
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

		titleText := strings.ToLower(doc.Title)
		hostBoost := 0.0
		for _, term := range terms {
			if term == "" {
				continue
			}
			hostBoost += float64(strings.Count(titleText, term)) * 1.5
			if parsed, err := url.Parse(doc.URL); err == nil {
				hostname := strings.ToLower(strings.Trim(parsed.Hostname(), "."))
				if strings.Contains(hostname, term) {
					hostBoost += 0.75
				}
			}
		}
		scores[docID] += hostBoost
	}

	for docID, boost := range clusterBoost {
		scores[docID] *= 1 + boost*0.2
	}

	for docID := range scores {
		doc, ok := s.ix.GetDoc(docID)
		if !ok {
			continue
		}
		scores[docID] += proximityBoost(doc, terms)
	}

	ranked := make([]int, 0, len(scores))
	for id := range scores {
		ranked = append(ranked, id)
	}

	sort.Slice(ranked, func(i, j int) bool {
		si, sj := scores[ranked[i]], scores[ranked[j]]
		if si != sj {
			return si > sj
		}
		return ranked[i] < ranked[j]
	})

	results := make([]Result, 0, len(ranked))
	for _, id := range ranked {
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

	sortResults(results, NormalizeSortOrder(rawSort))
	if limit > len(results) {
		limit = len(results)
	}
	if limit <= 0 {
		return nil
	}
	return results[:limit]
}

func sortResults(results []Result, mode SortMode) {
	sort.Slice(results, func(i, j int) bool {
		switch mode {
		case SortNewest:
			if results[i].ID != results[j].ID {
				return results[i].ID > results[j].ID
			}
		case SortOldest:
			if results[i].ID != results[j].ID {
				return results[i].ID < results[j].ID
			}
		case SortMostLinked:
			if results[i].LinkCount != results[j].LinkCount {
				return results[i].LinkCount > results[j].LinkCount
			}
		default:
			if results[i].Score != results[j].Score {
				return results[i].Score > results[j].Score
			}
		}
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if results[i].LinkCount != results[j].LinkCount {
			return results[i].LinkCount > results[j].LinkCount
		}
		return results[i].ID < results[j].ID
	})
}

// Related ranks other indexed documents by binary-term cosine similarity
// against the document identified by exact canonical URL.
func (s *Searcher) Related(rawURL string, limit int) []Result {
	if limit <= 0 {
		return nil
	}
	sourceID, ok := s.ix.IDForURL(rawURL)
	if !ok {
		return nil
	}
	source, ok := s.ix.GetDoc(sourceID)
	if !ok {
		return nil
	}
	sourceTerms := make(map[string]struct{})
	for _, term := range crawler.Tokenize(source.Title + " " + source.Text) {
		sourceTerms[term] = struct{}{}
	}
	if len(sourceTerms) == 0 {
		return nil
	}

	candidates := make([]scoredRelated, 0)
	for id := 0; id < s.ix.DocCount(); id++ {
		if id == sourceID {
			continue
		}
		doc, exists := s.ix.GetDoc(id)
		if !exists {
			continue
		}
		candidateTerms := make(map[string]struct{})
		for _, term := range crawler.Tokenize(doc.Title + " " + doc.Text) {
			candidateTerms[term] = struct{}{}
		}
		if len(candidateTerms) == 0 {
			continue
		}
		overlap := 0
		for term := range sourceTerms {
			if _, found := candidateTerms[term]; found {
				overlap++
			}
		}
		if overlap == 0 {
			continue
		}
		score := float64(overlap) / math.Sqrt(float64(len(sourceTerms))*float64(len(candidateTerms)))
		candidates = append(candidates, scoredRelated{
			result: Result{
				ID:        doc.ID,
				URL:       doc.URL,
				Title:     doc.Title,
				Text:      doc.Text,
				LinkCount: doc.LinkCount,
			},
			score: score,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].result.ID < candidates[j].result.ID
	})
	if limit > len(candidates) {
		limit = len(candidates)
	}
	results := make([]Result, limit)
	for i := range results {
		results[i] = candidates[i].result
		results[i].Score = math.Round(candidates[i].score*10000) / 10000
	}
	return results
}

// Suggest returns up to limit index terms starting with prefix, most common
// first. The comparison is case-insensitive.
func (s *Searcher) Suggest(prefix string, limit int) []string {
	lower := strings.ToLower(strings.TrimSpace(prefix))
	if lower == "" || limit <= 0 {
		return []string{}
	}

	type match struct {
		term  string
		count int
	}
	var matches []match

	s.ix.WalkTerms(func(term string, df int) {
		if strings.HasPrefix(term, lower) {
			matches = append(matches, match{term, df})
		}
	})

	sort.Slice(matches, func(i, j int) bool {
		if matches[i].count != matches[j].count {
			return matches[i].count > matches[j].count
		}
		return matches[i].term < matches[j].term
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
