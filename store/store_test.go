package store

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func jsonUnmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

type testDoc struct {
	ID    int
	URL   string
	Title string
	Text  string
}

func TestSaveDocumentBatchRoundtrip(t *testing.T) {
	s := openTestStore(t)

	doc := testDoc{ID: 0, URL: "https://a.com", Title: "A", Text: "body"}
	postings := map[string]map[int]int{
		"golang": {0: 2, 1: 1},
		"search": {0: 1},
	}
	links := map[string]int{"https://b.com": 3}

	if err := s.SaveDocumentBatch(0, doc, 2, postings, links); err != nil {
		t.Fatalf("SaveDocumentBatch: %v", err)
	}

	var gotDoc testDoc
	var docCount int
	err := s.AllDocuments(func(id int, data []byte) error {
		docCount++
		if id != 0 {
			t.Errorf("doc id = %d, want 0", id)
		}
		return jsonUnmarshal(data, &gotDoc)
	})
	if err != nil {
		t.Fatalf("AllDocuments: %v", err)
	}
	if docCount != 1 || gotDoc.URL != "https://a.com" {
		t.Errorf("docs = %d %+v", docCount, gotDoc)
	}

	var termCount int
	err = s.AllPostings(func(term string, data []byte) error {
		termCount++
		var p map[int]int
		if err := jsonUnmarshal(data, &p); err != nil {
			return err
		}
		if term == "golang" && (p[0] != 2 || p[1] != 1) {
			t.Errorf("golang postings = %v", p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("AllPostings: %v", err)
	}
	if termCount != 2 {
		t.Errorf("terms = %d, want 2", termCount)
	}

	var linkTotal int
	err = s.AllLinkCounts(func(url string, count int) error {
		linkTotal += count
		if url != "https://b.com" || count != 3 {
			t.Errorf("link %s = %d", url, count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("AllLinkCounts: %v", err)
	}
	if linkTotal != 3 {
		t.Errorf("link total = %d, want 3", linkTotal)
	}

	next, err := s.LoadNextDocID()
	if err != nil || next != 2 {
		t.Errorf("LoadNextDocID = %d, %v; want 2", next, err)
	}
}

func TestVisitedAndFingerprints(t *testing.T) {
	s := openTestStore(t)

	if err := s.MarkVisited("https://a.com/"); err != nil {
		t.Fatalf("MarkVisited: %v", err)
	}
	if err := s.MarkFingerprint("abc123"); err != nil {
		t.Fatalf("MarkFingerprint: %v", err)
	}

	visited := 0
	if err := s.AllVisited(func(url string) error {
		if url != "https://a.com/" {
			t.Errorf("visited = %q", url)
		}
		visited++
		return nil
	}); err != nil {
		t.Fatalf("AllVisited: %v", err)
	}
	if visited != 1 {
		t.Errorf("visited count = %d, want 1", visited)
	}

	fps := 0
	if err := s.AllFingerprints(func(hash string) error {
		if hash != "abc123" {
			t.Errorf("fingerprint = %q", hash)
		}
		fps++
		return nil
	}); err != nil {
		t.Fatalf("AllFingerprints: %v", err)
	}
	if fps != 1 {
		t.Errorf("fingerprint count = %d, want 1", fps)
	}
}
