package trie

import (
	"fmt"
	"sync"
	"testing"
)

func TestInsertAndCollect(t *testing.T) {
	tr := New()
	tr.Insert("https://example.com/docs/a", 1)
	tr.Insert("https://example.com/docs/b", 2)
	tr.Insert("https://example.com/other/c", 3)

	ids := tr.GetDocIDsUnder("https://example.com/docs")
	if len(ids) != 2 {
		t.Errorf("docs under /docs = %v, want 2 docs", ids)
	}
	_, has1 := ids[1]
	_, has2 := ids[2]
	if !has1 || !has2 {
		t.Errorf("docs under /docs = %v, want docs 1 and 2", ids)
	}

	ids = tr.GetDocIDsUnder("https://example.com")
	if len(ids) != 3 {
		t.Errorf("docs under host = %v, want 3 docs", ids)
	}

	ids = tr.GetDocIDsUnder("https://example.com/missing")
	if len(ids) != 0 {
		t.Errorf("docs under missing path = %v, want none", ids)
	}
}

func TestHostScoping(t *testing.T) {
	tr := New()
	tr.Insert("https://a.com/x", 1)
	tr.Insert("https://b.com/x", 2)

	ids := tr.GetDocIDsUnder("https://a.com/x")
	_, has1 := ids[1]
	if len(ids) != 1 || !has1 {
		t.Errorf("a.com/x siblings = %v, want only doc 1 (hosts must not mix)", ids)
	}

	ids = tr.GetDocIDsUnder("https://a.com")
	_, has1 = ids[1]
	if len(ids) != 1 || !has1 {
		t.Errorf("a.com docs = %v, want only doc 1", ids)
	}
}

func TestDepth(t *testing.T) {
	tr := New()
	if d := tr.Depth("https://example.com/a/b/c"); d != 3 {
		t.Errorf("Depth = %d, want 3", d)
	}
	if d := tr.Depth("https://example.com/"); d != 0 {
		t.Errorf("Depth of root = %d, want 0", d)
	}
}

// TestConcurrentInsertAndCollect exercises the race that previously caused
// "fatal error: concurrent map iteration and map write": collect walked
// children maps while Insert wrote to them.
func TestConcurrentInsertAndCollect(t *testing.T) {
	for round := 0; round < 5; round++ {
		tr := New()
		tr.Insert("https://example.com/base/seed", 0)
		var wg sync.WaitGroup
		stop := make(chan struct{})

		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				i := 0
				for {
					select {
					case <-stop:
						return
					default:
						tr.Insert(fmt.Sprintf("https://example.com/base/w%d-%d", w, i), i)
						i++
					}
				}
			}(w)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30000; i++ {
				tr.GetDocIDsUnder("https://example.com/base")
			}
		}()

		for i := 0; i < 50; i++ {
			tr.GetDocIDsUnder("https://example.com/base/seed")
		}
		close(stop)
		wg.Wait()
	}
}
