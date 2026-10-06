package indexer

import "testing"

func TestAddDocumentBuildsPostings(t *testing.T) {
	ix := New()
	id := ix.AddDocument("https://a.com", "A", "golang golang go", []string{"golang", "golang", "go"})

	if id != 0 {
		t.Errorf("id = %d, want 0", id)
	}
	if ix.DocCount() != 1 {
		t.Errorf("DocCount = %d, want 1", ix.DocCount())
	}
	p := ix.GetPostings("golang")
	if p[0] != 2 {
		t.Errorf("golang postings = %v, want doc 0 with tf 2", p)
	}
	if ix.TermCount() != 2 {
		t.Errorf("TermCount = %d, want 2", ix.TermCount())
	}
}

func TestIDForURL(t *testing.T) {
	ix := New()
	ix.AddDocument("https://a.com/1", "t", "text here", []string{"text", "here"})
	if _, ok := ix.IDForURL("https://a.com/1"); !ok {
		t.Error("IDForURL did not find indexed URL")
	}
	if _, ok := ix.IDForURL("https://a.com/2"); ok {
		t.Error("IDForURL found URL that was never indexed")
	}
}

func TestWalkTerms(t *testing.T) {
	ix := New()
	ix.AddDocument("u", "t", "alpha beta alpha", []string{"alpha", "beta", "alpha"})
	ix.AddDocument("u2", "t", "alpha", []string{"alpha"})

	got := map[string]int{}
	ix.WalkTerms(func(term string, df int) {
		got[term] = df
	})
	if len(got) != 2 || got["alpha"] != 2 || got["beta"] != 1 {
		t.Errorf("WalkTerms = %v, want alpha:2 beta:1", got)
	}
}

func TestRestoreDocumentHandlesGaps(t *testing.T) {
	ix := New()
	ix.RestoreDocument(Document{ID: 3, URL: "https://a.com/3", Title: "t", Text: "body"})
	if ix.DocCount() != 4 {
		t.Errorf("DocCount = %d, want 4 (slice sized to ID)", ix.DocCount())
	}
	if id, ok := ix.IDForURL("https://a.com/3"); !ok || id != 3 {
		t.Errorf("IDForURL = %d, %v; want 3, true", id, ok)
	}
	doc, ok := ix.GetDoc(3)
	if !ok || doc.Title != "t" {
		t.Errorf("GetDoc(3) = %v, %v", doc, ok)
	}
	if ix.NextID() != 4 {
		t.Errorf("NextID = %d, want 4", ix.NextID())
	}
}

func TestSetLinkCountBounds(t *testing.T) {
	ix := New()
	id := ix.AddDocument("u", "t", "text", []string{"text"})
	ix.SetLinkCount(id, 7)
	doc, _ := ix.GetDoc(id)
	if doc.LinkCount != 7 {
		t.Errorf("LinkCount = %d, want 7", doc.LinkCount)
	}
	ix.SetLinkCount(99, 7)
	if ix.DocCount() != 1 {
		t.Errorf("SetLinkCount out of bounds grew docs: %d", ix.DocCount())
	}
}
