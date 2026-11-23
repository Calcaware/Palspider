package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/calcaware/palspider/crawler"
	"github.com/calcaware/palspider/searcher"
	"github.com/calcaware/palspider/store"
)

func TestMergeResultsDedupesAndSorts(t *testing.T) {
	local := []searcher.Result{
		{URL: "https://a.com", Score: 1.0},
		{URL: "https://b.com", Score: 3.0},
	}
	remote := []searcher.Result{
		{URL: "https://b.com", Score: 99.0, PeerSource: "peer"},
		{URL: "https://c.com", Score: 2.0, PeerSource: "peer"},
	}

	merged := mergeResults(local, remote, 10)
	if len(merged) != 3 {
		t.Fatalf("merged = %d results, want 3 (deduped by URL)", len(merged))
	}
	if merged[0].URL != "https://b.com" || merged[0].PeerSource != "" {
		t.Errorf("top = %+v, want local copy of b.com to win", merged[0])
	}
	if merged[1].URL != "https://c.com" || merged[2].URL != "https://a.com" {
		t.Errorf("order = %v %v, want c.com then a.com", merged[1].URL, merged[2].URL)
	}
}

func TestMergeResultsLimit(t *testing.T) {
	local := []searcher.Result{
		{URL: "a", Score: 3},
		{URL: "b", Score: 2},
		{URL: "c", Score: 1},
	}
	if got := mergeResults(local, nil, 2); len(got) != 2 {
		t.Errorf("limit 2 got %d results", len(got))
	}
	if got := mergeResults(local, nil, 0); got != nil {
		t.Errorf("limit 0 = %v, want nil", got)
	}
}

func TestQueueSnapshotListsQueuedAndRetryingItems(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/seed")
	e.Seed("https://other.example.org/start")
	retryURL := "https://example.com/broken"
	e.scheduleRetry(retryURL, 2, errors.New("temporary network failure"))

	snap := e.QueueSnapshot("", 0, 0)
	if snap.Total != 3 || snap.Queued != 2 || snap.Retrying != 1 {
		t.Fatalf("snapshot counts = total %d queued %d retrying %d, want 3/2/1",
			snap.Total, snap.Queued, snap.Retrying)
	}
	if snap.QueueSize != 3 {
		t.Errorf("queueSize = %d, want 3", snap.QueueSize)
	}
	if len(snap.Items) != 3 {
		t.Fatalf("items = %d, want 3", len(snap.Items))
	}
	// Items are grouped by host and sorted by URL within a host.
	if snap.Items[0].URL != retryURL || snap.Items[0].Status != "retrying" {
		t.Errorf("first item = %+v, want the retrying example.com URL", snap.Items[0])
	}
	if snap.Items[0].Depth != 2 || snap.Items[0].RetryAt == "" {
		t.Errorf("retry item = %+v, want depth 2 and a retry timestamp", snap.Items[0])
	}
	host := snap.Hosts["example.com"]
	if host.Queued != 1 || host.Retrying != 1 {
		t.Errorf("example.com backlog = %+v, want 1 queued and 1 retrying", host)
	}

	page := e.QueueSnapshot("example.com", 1, 1)
	if page.Total != 2 || len(page.Items) != 1 {
		t.Fatalf("filtered page = total %d items %d, want 2/1", page.Total, len(page.Items))
	}
	if page.Items[0].URL != "https://example.com/seed" {
		t.Errorf("filtered page item = %q, want the queued seed", page.Items[0].URL)
	}
	if page.Queued != 1 || page.Retrying != 1 {
		t.Errorf("filtered totals = queued %d retrying %d, want 1/1", page.Queued, page.Retrying)
	}
}

func TestQueueSnapshotDropsEntriesWhenTheyAreDequeued(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/page")

	item := <-e.urlChan
	e.markDequeued(item)

	snap := e.QueueSnapshot("", 0, 0)
	if snap.Total != 0 || len(snap.Items) != 0 {
		t.Errorf("snapshot after dequeue = %+v, want an empty frontier", snap)
	}
	if snap.QueueSize != 0 {
		t.Errorf("queueSize after dequeue = %d, want 0", snap.QueueSize)
	}
}

func TestDequeueingAPageKeepsOtherRetrySlots(t *testing.T) {
	e := New(2, 10, 0, nil)
	retryURL := "https://example.com/broken"
	e.scheduleRetry(retryURL, 0, errors.New("temporary network failure"))

	// Crawling another page from the same host must not free the slot that
	// the retrying URL still holds.
	e.markDequeued(crawlItem{url: "https://example.com/page", depth: 1})
	e.mu.Lock()
	slots := e.hostRetries["example.com"]
	e.mu.Unlock()
	if slots != 1 {
		t.Fatalf("host retry slots = %d, want the retrying URL to keep its slot", slots)
	}

	item := <-e.urlChan
	e.markDequeued(item)
	e.mu.Lock()
	slots = e.hostRetries["example.com"]
	_, retrying := e.retryAt[retryURL]
	e.mu.Unlock()
	if slots != 0 || retrying {
		t.Errorf("after the retry attempt: slots %d, retrying %v, want 0 and false", slots, retrying)
	}
}

func TestRequeueReleasesRetrySlot(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/broken"
	e.scheduleRetry(rawURL, 0, errors.New("temporary network failure"))

	if !e.Requeue(rawURL) {
		t.Fatal("Requeue returned false")
	}
	e.mu.Lock()
	slots := e.hostRetries["example.com"]
	_, retrying := e.retryAt[rawURL]
	_, counted := e.retryCounts[rawURL]
	e.mu.Unlock()
	if slots != 0 || retrying || counted {
		t.Errorf("after requeue: slots %d retrying %v counted %v, want 0 false false", slots, retrying, counted)
	}
}

func TestDropURLRemovesAndSkipsQueuedURL(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/page"
	e.Seed(rawURL)

	if !e.DropURL(rawURL) {
		t.Fatal("DropURL returned false for a queued URL")
	}
	if snap := e.QueueSnapshot("", 0, 0); snap.Total != 0 {
		t.Fatalf("frontier total = %d after a drop, want 0", snap.Total)
	}

	// A copy that is already on the worker queue must not be fetched after
	// the drop, because the URL is no longer pending.
	item := <-e.urlChan
	if e.takeQueued(item) {
		t.Fatal("a stale copy was processed after the URL was dropped")
	}
	if e.DropURL(rawURL) {
		t.Fatal("dropping the same URL twice should return false")
	}

	if !e.Requeue(rawURL) {
		t.Fatal("Requeue returned false after a drop")
	}
	if snap := e.QueueSnapshot("", 0, 0); snap.Total != 1 {
		t.Errorf("frontier total = %d after requeue, want 1", snap.Total)
	}
}

func TestDropURLReleasesRetrySlot(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/broken"
	e.scheduleRetry(rawURL, 1, errors.New("temporary network failure"))

	if !e.DropURL(rawURL) {
		t.Fatal("DropURL returned false for a retrying URL")
	}
	e.mu.Lock()
	slots := e.hostRetries["example.com"]
	_, retrying := e.retryAt[rawURL]
	_, counted := e.retryCounts[rawURL]
	e.mu.Unlock()
	if slots != 0 || retrying || counted {
		t.Errorf("after a drop: slots %d retrying %v counted %v, want 0 false false", slots, retrying, counted)
	}
	if snap := e.QueueSnapshot("", 0, 0); snap.Total != 0 {
		t.Errorf("frontier total = %d after dropping a retry, want 0", snap.Total)
	}
}

func TestUndoDropURLRestoresQueuedURL(t *testing.T) {
	e := New(1, 10, 0, nil)
	dropped := "https://example.com/a"
	e.Seed(dropped)
	e.Seed("https://other.example.org/c")

	if !e.DropURL(dropped) {
		t.Fatal("DropURL returned false for a queued URL")
	}
	info := e.LastURLDropInfo()
	if info == nil || info.URL != dropped || info.At == "" {
		t.Fatalf("last URL drop = %+v, want the dropped URL and a timestamp", info)
	}
	if status := e.CrawlStatus(); status.LastURLDrop == nil || status.LastURLDrop.URL != dropped {
		t.Errorf("crawl status last URL drop = %+v, want the pending URL", status.LastURLDrop)
	}
	if snap := e.QueueSnapshot("", 0, 0); snap.LastURLDrop == nil || snap.LastURLDrop.URL != dropped {
		t.Errorf("queue last URL drop = %+v, want the pending URL", snap.LastURLDrop)
	}

	gotURL, restored := e.UndoDropURL()
	if gotURL != dropped || !restored {
		t.Fatalf("UndoDropURL = %q %v, want %q and true", gotURL, restored, dropped)
	}
	if e.LastURLDropInfo() != nil {
		t.Error("the last URL drop is still available after it was undone")
	}

	e.mu.Lock()
	_, visited := e.visited[dropped]
	depth, pending := e.pending[dropped]
	_, otherPending := e.pending["https://other.example.org/c"]
	e.mu.Unlock()
	if visited || !pending {
		t.Errorf("restored URL visited %v pending %v, want false and true", visited, pending)
	}
	if depth != 0 {
		t.Errorf("restored depth = %d, want 0", depth)
	}
	if !otherPending {
		t.Error("undoing one URL removed a different URL from the frontier")
	}
	item := <-e.urlChan
	if item.url != dropped {
		t.Errorf("restored queue item = %q, want %q", item.url, dropped)
	}

	if gotURL, restored := e.UndoDropURL(); gotURL != "" || restored {
		t.Errorf("second UndoDropURL = %q %v, want nothing left to restore", gotURL, restored)
	}
}

func TestUndoDropURLRestoresRetryWindow(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/broken"
	e.scheduleRetry(rawURL, 2, errors.New("temporary network failure"))
	<-e.urlChan

	if !e.DropURL(rawURL) {
		t.Fatal("DropURL returned false for a retrying URL")
	}
	if gotURL, restored := e.UndoDropURL(); gotURL != rawURL || !restored {
		t.Fatalf("UndoDropURL = %q %v, want %q and true", gotURL, restored, rawURL)
	}

	e.mu.Lock()
	at, retrying := e.retryAt[rawURL]
	slots := e.hostRetries["example.com"]
	count := e.retryCounts[rawURL]
	depth := e.pending[rawURL]
	e.mu.Unlock()
	if !retrying || !at.After(time.Now()) {
		t.Errorf("restored retry window = %v (set %v), want a future window", at, retrying)
	}
	if slots != 1 {
		t.Errorf("host retry slots = %d, want the restored retry to hold one", slots)
	}
	if count != 1 {
		t.Errorf("retry count = %d, want the earlier attempt to be kept", count)
	}
	if depth != 2 {
		t.Errorf("restored depth = %d, want 2", depth)
	}
	if len(e.urlChan) != 1 {
		t.Errorf("queue = %d items, want the restored retry to be queued", len(e.urlChan))
	}
}

func TestUndoDropURLReportsURLAlreadyRestored(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/a"
	e.Seed(rawURL)
	if !e.DropURL(rawURL) {
		t.Fatal("DropURL returned false for a queued URL")
	}
	if !e.Requeue(rawURL) {
		t.Fatal("Requeue failed for the dropped URL")
	}

	gotURL, restored := e.UndoDropURL()
	if gotURL != rawURL || restored {
		t.Fatalf("UndoDropURL = %q %v, want %q and false", gotURL, restored, rawURL)
	}
	if e.LastURLDropInfo() != nil {
		t.Error("the empty undo left a drop behind")
	}
	if snap := e.QueueSnapshot("", 0, 0); snap.Total != 1 || snap.Items[0].URL != rawURL {
		t.Errorf("frontier after an empty undo = %+v, want one queued URL", snap)
	}
}

func TestUndoDropURLByURLRestoresSelectedDrop(t *testing.T) {
	e := New(1, 10, 0, nil)
	first := "https://example.com/a"
	second := "https://example.com/b"
	other := "https://other.example.org/c"
	e.Seed(first)
	e.Seed(second)
	e.Seed(other)

	if !e.DropURL(first) || !e.DropURL(second) {
		t.Fatal("DropURL returned false for a queued URL")
	}
	drops := e.URLDropsInfo()
	if len(drops) != 2 || drops[0].URL != second || drops[1].URL != first ||
		drops[0].At == "" || drops[1].At == "" {
		t.Fatalf("URL drops = %+v, want %q then %q with timestamps", drops, second, first)
	}
	if snap := e.QueueSnapshot("", 0, 0); len(snap.URLDrops) != 2 ||
		snap.URLDrops[0].URL != second {
		t.Fatalf("queue URL drops = %+v, want the newest drop first", snap.URLDrops)
	}

	gotURL, restored, found := e.UndoDropURLByURL(second)
	if gotURL != second || !restored || !found {
		t.Fatalf("selected undo = %q %v %v, want %q and true", gotURL, restored, found, second)
	}
	if info := e.LastURLDropInfo(); info == nil || info.URL != first {
		t.Fatalf("last URL drop after a selected undo = %+v, want %q", info, first)
	}
	if drops := e.URLDropsInfo(); len(drops) != 1 || drops[0].URL != first {
		t.Fatalf("URL drops after a selected undo = %+v, want only %q", drops, first)
	}
	queued := map[string]int{}
	for len(e.urlChan) > 0 {
		queued[(<-e.urlChan).url]++
	}
	if queued[other] != 1 || queued[second] != 2 || queued[first] != 1 {
		t.Errorf("queue after a selected undo = %v, want one copy of %q and %q plus the restored %q",
			queued, first, other, second)
	}

	gotURL, restored, found = e.UndoDropURLByURL(first)
	if gotURL != first || !restored || !found {
		t.Fatalf("undo of the older drop = %q %v %v, want %q and true", gotURL, restored, found, first)
	}
	if e.LastURLDropInfo() != nil {
		t.Error("the last drop is still available after every drop was restored")
	}
	if item := <-e.urlChan; item.url != first {
		t.Errorf("last queued item = %q, want %q", item.url, first)
	}
}

func TestUndoDropURLByURLRejectsUnknownURL(t *testing.T) {
	e := New(1, 10, 0, nil)
	dropped := "https://example.com/a"
	missing := "https://example.com/missing"
	e.Seed(dropped)
	if !e.DropURL(dropped) {
		t.Fatal("DropURL returned false for a queued URL")
	}

	gotURL, restored, found := e.UndoDropURLByURL(missing)
	if gotURL != missing || restored || found {
		t.Fatalf("unknown selected undo = %q %v %v, want %q false false",
			gotURL, restored, found, missing)
	}
	if info := e.LastURLDropInfo(); info == nil || info.URL != dropped {
		t.Fatalf("last URL drop = %+v, want the unknown undo to leave it alone", info)
	}
	if snap := e.QueueSnapshot("", 0, 0); snap.Total != 0 {
		t.Errorf("frontier total = %d, want an unknown undo to leave it empty", snap.Total)
	}
}

func TestUndoDropURLKeepsHistoryWhenQueueIsFull(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.urlChan = make(chan crawlItem, 2)
	first := "https://example.com/a"
	other := "https://example.com/b"
	e.Seed(first)
	e.Seed(other)
	if !e.DropURL(first) {
		t.Fatal("DropURL returned false for a queued URL")
	}
	if len(e.urlChan) != cap(e.urlChan) {
		t.Fatalf("queue = %d items, want it full before the undo", len(e.urlChan))
	}

	gotURL, restored := e.UndoDropURL()
	if gotURL != first || restored {
		t.Fatalf("undo with a full queue = %q %v, want %q and false", gotURL, restored, first)
	}
	if info := e.LastURLDropInfo(); info == nil || info.URL != first {
		t.Fatalf("last URL drop after a full queue = %+v, want %q kept", info, first)
	}
	e.mu.Lock()
	_, visited := e.visited[first]
	_, pending := e.pending[first]
	e.mu.Unlock()
	if !visited || pending {
		t.Errorf("URL after a full queue visited %v pending %v, want visited and not pending", visited, pending)
	}

	e.markDequeued(<-e.urlChan)
	gotURL, restored = e.UndoDropURL()
	if gotURL != first || !restored {
		t.Fatalf("retry after a full queue = %q %v, want %q and true", gotURL, restored, first)
	}
}

func TestURLDropHistoryIsBoundedAndKeepsNewestCopy(t *testing.T) {
	var batch *droppedBatch
	for i := 0; i <= maxURLDrops+4; i++ {
		batch = prependURLDrop(batch, droppedURL{
			url: fmt.Sprintf("https://example.com/%d", i),
			at:  time.Now().Add(time.Duration(i) * time.Second),
		})
	}
	if len(batch.items) != maxURLDrops {
		t.Fatalf("history length = %d, want %d", len(batch.items), maxURLDrops)
	}
	newest := fmt.Sprintf("https://example.com/%d", maxURLDrops+4)
	if batch.items[0].url != newest {
		t.Errorf("newest drop = %q, want %q", batch.items[0].url, newest)
	}

	batch = prependURLDrop(batch, droppedURL{url: newest, at: time.Now().Add(time.Hour)})
	if len(batch.items) != maxURLDrops || batch.items[0].url != newest {
		t.Fatalf("history after a repeat drop = %+v, want %d entries with %q first",
			batch.items, maxURLDrops, newest)
	}
	count := 0
	for _, item := range batch.items {
		if item.url == newest {
			count++
		}
	}
	if count != 1 {
		t.Errorf("repeat drop appears %d times, want once", count)
	}
}

func TestUndoDropURLSelectedDropSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engine.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	first := "https://example.com/a"
	second := "https://example.com/b"
	e := New(1, 10, 0, st)
	e.Seed(first)
	e.Seed(second)
	if !e.DropURL(first) || !e.DropURL(second) {
		t.Fatal("DropURL returned false for a queued URL")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	again := New(1, 10, 0, st)
	if err := again.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	drops := again.URLDropsInfo()
	if len(drops) != 2 || drops[0].URL != second || drops[1].URL != first {
		t.Fatalf("URL drops after restart = %+v, want %q then %q", drops, second, first)
	}

	gotURL, restored, found := again.UndoDropURLByURL("HTTPS://EXAMPLE.COM/b")
	if gotURL != second || !restored || !found {
		t.Fatalf("selected undo after restart = %q %v %v, want %q and true",
			gotURL, restored, found, second)
	}
	if drops := again.URLDropsInfo(); len(drops) != 1 || drops[0].URL != first {
		t.Fatalf("URL drops after a selected undo = %+v, want only %q", drops, first)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store after undo: %v", err)
	}

	st, err = store.Open(path)
	if err != nil {
		t.Fatalf("reopen store after undo: %v", err)
	}
	defer st.Close()
	third := New(1, 10, 0, st)
	if err := third.Restore(); err != nil {
		t.Fatalf("Restore after undo: %v", err)
	}
	drops = third.URLDropsInfo()
	if len(drops) != 1 || drops[0].URL != first {
		t.Fatalf("URL drops after a second restart = %+v, want only %q", drops, first)
	}
}

func TestRestoreReadsLegacySingleURLDrop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engine.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	legacy := json.RawMessage(`{"at":"2026-01-02T03:04:05Z","items":[{"url":"https://example.com/a","depth":2,"retryCount":1}]}`)
	if err := st.SaveLastURLDrop(legacy); err != nil {
		t.Fatalf("save legacy drop: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	e := New(1, 10, 0, st)
	if err := e.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	info := e.LastURLDropInfo()
	if info == nil || info.URL != "https://example.com/a" || info.At != "2026-01-02T03:04:05Z" {
		t.Fatalf("legacy drop = %+v, want the saved URL and time", info)
	}
	if drops := e.URLDropsInfo(); len(drops) != 1 || drops[0].At != info.At {
		t.Fatalf("legacy drop history = %+v, want one entry with the saved time", drops)
	}

	if gotURL, restored := e.UndoDropURL(); gotURL != info.URL || !restored {
		t.Fatalf("undo of a legacy drop = %q %v, want %q and true", gotURL, restored, info.URL)
	}
	e.mu.Lock()
	depth := e.pending[info.URL]
	count := e.retryCounts[info.URL]
	e.mu.Unlock()
	if depth != 2 || count != 1 {
		t.Errorf("restored legacy drop = depth %d retries %d, want 2 and 1", depth, count)
	}
}

func TestClearQueueClearsLastURLDrop(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/a")
	if !e.DropURL("https://example.com/a") {
		t.Fatal("DropURL returned false for a queued URL")
	}

	e.ClearQueue()
	if e.LastURLDropInfo() != nil {
		t.Error("clearing the queue kept a single URL drop that could still be undone")
	}
	if gotURL, restored := e.UndoDropURL(); gotURL != "" || restored {
		t.Errorf("UndoDropURL after a clear = %q %v, want nothing", gotURL, restored)
	}
}

func TestDiscardLastURLDropLeavesFrontierAlone(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/a"
	e.Seed(rawURL)
	e.Seed("https://other.example.org/c")
	if !e.DropURL(rawURL) {
		t.Fatal("DropURL returned false for a queued URL")
	}

	if !e.DiscardLastURLDrop() {
		t.Fatal("DiscardLastURLDrop reported no drop to discard")
	}
	if e.DiscardLastURLDrop() {
		t.Error("DiscardLastURLDrop succeeded twice")
	}
	if info := e.LastURLDropInfo(); info != nil {
		t.Errorf("last URL drop = %+v, want it discarded", info)
	}
	if status := e.CrawlStatus(); status.LastURLDrop != nil {
		t.Errorf("crawl status last URL drop = %+v, want no pending drop", status.LastURLDrop)
	}

	snap := e.QueueSnapshot("", 0, 0)
	if snap.Total != 1 || snap.LastURLDrop != nil || len(snap.Items) != 1 ||
		snap.Items[0].URL != "https://other.example.org/c" {
		t.Errorf("frontier after discard = %+v, want only the other host and no drop", snap)
	}
	e.mu.Lock()
	_, visited := e.visited[rawURL]
	_, pending := e.pending[rawURL]
	e.mu.Unlock()
	if !visited || pending {
		t.Errorf("discarded URL visited %v pending %v, want visited and not pending", visited, pending)
	}
	if gotURL, restored := e.UndoDropURL(); gotURL != "" || restored {
		t.Errorf("UndoDropURL after discard = %q %v, want nothing", gotURL, restored)
	}
}

func TestUndoDropURLSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engine.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	e := New(1, 10, 0, st)
	rawURL := "https://example.com/a"
	e.Seed(rawURL)
	if !e.DropURL(rawURL) {
		t.Fatal("DropURL returned false for a queued URL")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	again := New(1, 10, 0, st)
	if err := again.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	info := again.LastURLDropInfo()
	if info == nil || info.URL != rawURL {
		t.Fatalf("last URL drop after restart = %+v, want %q", info, rawURL)
	}

	gotURL, restored := again.UndoDropURL()
	if gotURL != rawURL || !restored {
		t.Fatalf("UndoDropURL after restart = %q %v, want %q and true", gotURL, restored, rawURL)
	}
	again.mu.Lock()
	_, visited := again.visited[rawURL]
	_, pending := again.pending[rawURL]
	again.mu.Unlock()
	if visited || !pending {
		t.Errorf("restored URL visited %v pending %v, want false and true", visited, pending)
	}
	if len(again.urlChan) != 1 {
		t.Errorf("queue after restart undo = %d items, want 1", len(again.urlChan))
	}

	if err := st.Close(); err != nil {
		t.Fatalf("close store after undo: %v", err)
	}
	st, err = store.Open(path)
	if err != nil {
		t.Fatalf("reopen store after undo: %v", err)
	}
	defer st.Close()
	third := New(1, 10, 0, st)
	if err := third.Restore(); err != nil {
		t.Fatalf("Restore after undo: %v", err)
	}
	if info := third.LastURLDropInfo(); info != nil {
		t.Errorf("last URL drop came back after a second restart: %+v", info)
	}
}

func TestPrioritizeURLFrontLoadsQueuedCopy(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/first")
	target := "https://example.com/second"
	e.Seed(target)

	if !e.PrioritizeURL(target) {
		t.Fatal("PrioritizeURL returned false for a queued URL")
	}
	var prioritized crawlItem
	select {
	case prioritized = <-e.priorityChan:
	default:
		t.Fatal("no item was put on the priority queue")
	}
	if prioritized.url != target {
		t.Fatalf("prioritized URL = %q, want %q", prioritized.url, target)
	}
	if snap := e.QueueSnapshot("", 0, 0); snap.Total != 2 {
		t.Fatalf("frontier total = %d, want both copies before the prioritized one is taken", snap.Total)
	}

	if !e.takeQueued(prioritized) {
		t.Fatal("the prioritized copy should claim the pending URL")
	}

	// The ordinary copy is still in the worker queue but must be ignored.
drain:
	for {
		select {
		case item := <-e.urlChan:
			if item.url == target && e.takeQueued(item) {
				t.Fatal("a stale copy was processed after the prioritized copy")
			}
		default:
			break drain
		}
	}
	if snap := e.QueueSnapshot("", 0, 0); snap.Total != 1 {
		t.Errorf("frontier total = %d after taking the prioritized copy, want 1", snap.Total)
	}
}

func TestPrioritizeURLRefusesRetryingAndUnknownURLs(t *testing.T) {
	e := New(1, 10, 0, nil)
	retryURL := "https://example.com/broken"
	e.scheduleRetry(retryURL, 0, errors.New("temporary network failure"))

	if e.PrioritizeURL(retryURL) {
		t.Error("PrioritizeURL accepted a URL that is waiting on a retry")
	}
	if e.PrioritizeURL("https://example.com/missing") {
		t.Error("PrioritizeURL accepted a URL that is not in the frontier")
	}
}

func TestDropHostDropsQueuedAndRetryingURLs(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/a")
	e.Seed("https://www.example.com/b")
	e.Seed("https://other.example.org/c")
	retryURL := "https://example.com/broken"
	e.scheduleRetry(retryURL, 1, errors.New("temporary network failure"))

	if got := e.DropHost("example.com"); got != 3 {
		t.Fatalf("DropHost dropped %d URLs, want 3 (the host and its subdomain)", got)
	}

	e.mu.Lock()
	slots := e.hostRetries["example.com"]
	_, retrying := e.retryAt[retryURL]
	_, counted := e.retryCounts[retryURL]
	e.mu.Unlock()
	if slots != 0 || retrying || counted {
		t.Errorf("after a host drop: slots %d retrying %v counted %v, want 0 false false", slots, retrying, counted)
	}

	snap := e.QueueSnapshot("", 0, 0)
	if snap.Total != 1 || len(snap.Items) != 1 || snap.Items[0].URL != "https://other.example.org/c" {
		t.Fatalf("frontier after a host drop = %+v, want only the other host", snap)
	}

	// Copies that were already in the queue must not be fetched after the
	// drop, because those URLs are no longer pending.
	dropped := map[string]bool{
		"https://example.com/a":     true,
		"https://www.example.com/b": true,
		retryURL:                    true,
	}
drain:
	for {
		select {
		case item := <-e.urlChan:
			if !dropped[item.url] {
				continue
			}
			if e.takeQueued(item) {
				t.Fatalf("a stale copy of %q was processed after the host drop", item.url)
			}
		default:
			break drain
		}
	}

	if got := e.DropHost("missing.example"); got != 0 {
		t.Errorf("DropHost on an unknown host dropped %d URLs, want 0", got)
	}
	if got := e.DropHost(""); got != 0 {
		t.Errorf("DropHost with an empty host dropped %d URLs, want 0", got)
	}
}

func TestUndoDropHostRestoresQueuedURLs(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/a")
	e.Seed("https://www.example.com/b")
	e.Seed("https://other.example.org/c")

	if got := e.DropHost("example.com"); got != 2 {
		t.Fatalf("DropHost dropped %d URLs, want 2", got)
	}
	info := e.LastDropInfo()
	if info == nil || info.Host != "example.com" || info.URLs != 2 || info.At == "" {
		t.Fatalf("last drop = %+v, want the example.com batch with a timestamp", info)
	}
	if status := e.CrawlStatus(); status.LastDrop == nil || status.LastDrop.URLs != 2 {
		t.Errorf("crawl status last drop = %+v, want the pending batch", status.LastDrop)
	}

	host, restored := e.UndoDropHost()
	if host != "example.com" || restored != 2 {
		t.Fatalf("UndoDropHost = %q %d, want example.com and 2 URLs", host, restored)
	}
	if e.LastDropInfo() != nil {
		t.Error("the last drop is still available after it was undone")
	}

	snap := e.QueueSnapshot("", 0, 0)
	if snap.Total != 3 || snap.LastDrop != nil {
		t.Fatalf("frontier after undo = total %d lastDrop %+v, want 3 entries and no pending batch",
			snap.Total, snap.LastDrop)
	}
	for _, rawURL := range []string{
		"https://example.com/a",
		"https://www.example.com/b",
	} {
		e.mu.Lock()
		_, visited := e.visited[rawURL]
		_, pending := e.pending[rawURL]
		e.mu.Unlock()
		if visited {
			t.Errorf("%s is still visited after the undo", rawURL)
		}
		if !pending {
			t.Errorf("%s is not pending after the undo", rawURL)
		}
	}
	e.mu.Lock()
	_, otherPending := e.pending["https://other.example.org/c"]
	e.mu.Unlock()
	if !otherPending {
		t.Error("the URL from the other host was removed by the undo")
	}

	if host, restored := e.UndoDropHost(); host != "" || restored != 0 {
		t.Errorf("second UndoDropHost = %q %d, want nothing left to restore", host, restored)
	}
}

func TestUndoDropHostRestoresRetryWindow(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/broken"
	e.scheduleRetry(rawURL, 2, errors.New("temporary network failure"))
	// Remove the copy scheduleRetry queued so only the undo puts one back.
	<-e.urlChan

	if got := e.DropHost("example.com"); got != 1 {
		t.Fatalf("DropHost dropped %d URLs, want 1", got)
	}
	host, restored := e.UndoDropHost()
	if host != "example.com" || restored != 1 {
		t.Fatalf("UndoDropHost = %q %d, want example.com and 1 URL", host, restored)
	}

	e.mu.Lock()
	at, retrying := e.retryAt[rawURL]
	slots := e.hostRetries["example.com"]
	count := e.retryCounts[rawURL]
	depth := e.pending[rawURL]
	e.mu.Unlock()
	if !retrying || !at.After(time.Now()) {
		t.Errorf("retry window = %v (set %v), want a future window after the undo", at, retrying)
	}
	if slots != 1 {
		t.Errorf("host retry slots = %d, want the restored retry to hold one", slots)
	}
	if count != 1 {
		t.Errorf("retry count = %d, want the previous attempt to be kept", count)
	}
	if depth != 2 {
		t.Errorf("pending depth = %d, want 2", depth)
	}
	if len(e.urlChan) != 1 {
		t.Errorf("queue = %d items, want the restored retry to be queued again", len(e.urlChan))
	}
}

func TestClearQueueClearsLastDrop(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/a")
	if got := e.DropHost("example.com"); got != 1 {
		t.Fatalf("DropHost dropped %d URLs, want 1", got)
	}

	e.ClearQueue()
	if e.LastDropInfo() != nil {
		t.Error("clearing the queue kept a bulk drop that could still be undone")
	}
	if host, restored := e.UndoDropHost(); host != "" || restored != 0 {
		t.Errorf("UndoDropHost after a clear = %q %d, want nothing", host, restored)
	}
}

func TestUndoDropHostReportsBatchWithNoRestoredURLs(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/a"
	e.Seed(rawURL)
	if got := e.DropHost("example.com"); got != 1 {
		t.Fatalf("DropHost dropped %d URLs, want 1", got)
	}
	// A link discovery or requeue between the drop and the undo can put the
	// URL back, leaving the batch with nothing to restore.
	if !e.Requeue(rawURL) {
		t.Fatal("Requeue failed for the dropped URL")
	}

	host, restored := e.UndoDropHost()
	if host != "example.com" || restored != 0 {
		t.Fatalf("UndoDropHost = %q %d, want example.com and no restored URLs", host, restored)
	}
	if e.LastDropInfo() != nil {
		t.Error("the empty undo left a batch behind")
	}
	if host, restored := e.UndoDropHost(); host != "" || restored != 0 {
		t.Errorf("second UndoDropHost = %q %d, want nothing", host, restored)
	}
}

func TestDiscardLastDropLeavesFrontierAlone(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/a")
	e.Seed("https://other.example.org/c")
	if got := e.DropHost("example.com"); got != 1 {
		t.Fatalf("DropHost dropped %d URLs, want 1", got)
	}

	if !e.DiscardLastDrop() {
		t.Fatal("DiscardLastDrop reported no batch to discard")
	}
	if e.DiscardLastDrop() {
		t.Error("DiscardLastDrop succeeded twice")
	}
	if info := e.LastDropInfo(); info != nil {
		t.Errorf("last drop = %+v, want it discarded", info)
	}
	if status := e.CrawlStatus(); status.LastDrop != nil {
		t.Errorf("crawl status last drop = %+v, want no pending batch", status.LastDrop)
	}

	snap := e.QueueSnapshot("", 0, 0)
	if snap.Total != 1 || snap.LastDrop != nil || len(snap.Items) != 1 ||
		snap.Items[0].URL != "https://other.example.org/c" {
		t.Errorf("frontier after discard = %+v, want only the other host and no batch", snap)
	}
	e.mu.Lock()
	_, visited := e.visited["https://example.com/a"]
	_, pending := e.pending["https://example.com/a"]
	e.mu.Unlock()
	if !visited || pending {
		t.Errorf("discarded URL visited %v pending %v, want visited and not pending", visited, pending)
	}
	if host, restored := e.UndoDropHost(); host != "" || restored != 0 {
		t.Errorf("UndoDropHost after discard = %q %d, want nothing", host, restored)
	}
}

func TestDiscardLastDropSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engine.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	e := New(1, 10, 0, st)
	e.Seed("https://example.com/a")
	if got := e.DropHost("example.com"); got != 1 {
		t.Fatalf("DropHost dropped %d URLs, want 1", got)
	}
	if !e.DiscardLastDrop() {
		t.Fatal("DiscardLastDrop reported no batch to discard")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()

	again := New(1, 10, 0, st)
	if err := again.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if info := again.LastDropInfo(); info != nil {
		t.Errorf("last drop after restart = %+v, want it discarded", info)
	}
	if host, restored := again.UndoDropHost(); host != "" || restored != 0 {
		t.Errorf("UndoDropHost after restart = %q %d, want nothing", host, restored)
	}
}

func TestUndoDropHostSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engine.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	e := New(1, 10, 0, st)
	e.Seed("https://example.com/a")
	e.Seed("https://www.example.com/b")
	retryURL := "https://example.com/broken"
	e.scheduleRetry(retryURL, 2, errors.New("temporary network failure"))
	// Remove the copy scheduleRetry queued so the batch only holds bookkeeping.
	<-e.urlChan

	if got := e.DropHost("example.com"); got != 3 {
		t.Fatalf("DropHost dropped %d URLs, want 3", got)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	restored := New(1, 10, 0, st)
	if err := restored.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	info := restored.LastDropInfo()
	if info == nil || info.Host != "example.com" || info.URLs != 3 {
		t.Fatalf("last drop after restart = %+v, want the example.com batch with 3 URLs", info)
	}

	host, count := restored.UndoDropHost()
	if host != "example.com" || count != 3 {
		t.Fatalf("UndoDropHost after restart = %q %d, want example.com and 3 URLs", host, count)
	}

	restored.mu.Lock()
	at, retrying := restored.retryAt[retryURL]
	slots := restored.hostRetries["example.com"]
	attempts := restored.retryCounts[retryURL]
	_, pending := restored.pending[retryURL]
	_, visited := restored.visited[retryURL]
	restored.mu.Unlock()
	if !retrying || !at.After(time.Now()) {
		t.Errorf("restored retry window = %v (set %v), want a future window", at, retrying)
	}
	if slots != 1 {
		t.Errorf("host retry slots = %d, want the restored retry to hold one", slots)
	}
	if attempts != 1 {
		t.Errorf("retry count = %d, want the attempt count from before the restart", attempts)
	}
	if !pending || visited {
		t.Errorf("restored retry pending %v visited %v, want pending and not visited", pending, visited)
	}

	// The undo is consumed, so another restart must not bring the batch back.
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	st, err = store.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()

	again := New(1, 10, 0, st)
	if err := again.Restore(); err != nil {
		t.Fatalf("Restore after undo: %v", err)
	}
	if again.LastDropInfo() != nil {
		t.Error("the undone batch came back after a restart")
	}
}

func TestPrioritizeHostFrontLoadsOnlyQueuedURLs(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/a")
	e.Seed("https://example.com/b")
	e.Seed("https://other.example.org/c")
	retryURL := "https://example.com/broken"
	e.scheduleRetry(retryURL, 1, errors.New("temporary network failure"))

	if got := e.PrioritizeHost("example.com"); got != 2 {
		t.Fatalf("PrioritizeHost prioritized %d URLs, want 2 queued URLs", got)
	}

	prioritized := make(map[string]bool)
	for range 2 {
		select {
		case item := <-e.priorityChan:
			prioritized[item.url] = true
		default:
			t.Fatal("the priority queue holds fewer items than PrioritizeHost reported")
		}
	}
	if !prioritized["https://example.com/a"] || !prioritized["https://example.com/b"] {
		t.Errorf("prioritized = %v, want both queued example.com URLs", prioritized)
	}

	e.mu.Lock()
	_, retrying := e.retryAt[retryURL]
	e.mu.Unlock()
	if !retrying {
		t.Error("a retrying URL was moved out of its retry window")
	}
	if snap := e.QueueSnapshot("", 0, 0); snap.Total != 4 {
		t.Errorf("frontier total = %d, want every entry to stay in place", snap.Total)
	}

	if got := e.PrioritizeHost("missing.example"); got != 0 {
		t.Errorf("PrioritizeHost on an unknown host prioritized %d URLs, want 0", got)
	}
	if got := e.PrioritizeHost(""); got != 0 {
		t.Errorf("PrioritizeHost with an empty host prioritized %d URLs, want 0", got)
	}
}

func TestWaitWhilePausedBlocksUntilResume(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Pause()

	waited := make(chan bool, 1)
	go func() { waited <- e.waitWhilePaused(context.Background()) }()

	select {
	case <-waited:
		t.Fatal("waitWhilePaused returned while the crawler was paused")
	case <-time.After(20 * time.Millisecond):
	}

	e.Resume()
	select {
	case ok := <-waited:
		if !ok {
			t.Fatal("waitWhilePaused returned false after resume")
		}
	case <-time.After(time.Second):
		t.Fatal("waitWhilePaused did not return after resume")
	}
}

func TestWaitWhilePausedStopsWithContext(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Pause()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waited := make(chan bool, 1)
	go func() { waited <- e.waitWhilePaused(ctx) }()
	cancel()

	select {
	case ok := <-waited:
		if ok {
			t.Fatal("waitWhilePaused returned true after the context ended")
		}
	case <-time.After(time.Second):
		t.Fatal("waitWhilePaused did not return after the context ended")
	}
}

func TestPausedWorkerLeavesFrontierInPlace(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Pause()
	rawURL := "https://example.com/page"
	e.Seed(rawURL)
	if !e.PrioritizeURL(rawURL) {
		t.Fatal("PrioritizeURL returned false for a queued URL")
	}

	e.Start(context.Background())
	defer e.Stop()

	// The worker must not take either copy while the crawler is paused.
	// Before the fix it pulled the priority item and put it back on the
	// ordinary queue, which lost the priority action.
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := len(e.priorityChan); got != 1 {
			t.Fatalf("priority queue length = %d, want 1 while paused", got)
		}
		if got := len(e.urlChan); got != 1 {
			t.Fatalf("worker queue length = %d, want 1 while paused", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPermanentFailureClearsRetryStatus(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/gone"
	e.scheduleRetry(rawURL, 0, errors.New("temporary network failure"))

	e.scheduleRetry(rawURL, 0, fmt.Errorf("%w: path %q", crawler.ErrExcluded, "/gone"))

	e.mu.Lock()
	slots := e.hostRetries["example.com"]
	_, retrying := e.retryAt[rawURL]
	_, counted := e.retryCounts[rawURL]
	e.mu.Unlock()
	if slots != 0 || retrying || counted {
		t.Errorf("after a permanent failure: slots %d retrying %v counted %v, want all cleared",
			slots, retrying, counted)
	}
}

func TestAddDocumentMarksVisitedAndDedupes(t *testing.T) {
	e := New(1, 10, 0, nil)

	e.AddDocument("https://Example.com/page", "First", "first document text")
	e.AddDocument("https://example.com/page/", "Second", "second document text here")

	if got := e.Indexer.DocCount(); got != 1 {
		t.Errorf("DocCount = %d, want 1 (same normalized URL must not re-index)", got)
	}
	if got := e.Stats().Visited; got != 1 {
		t.Errorf("Visited = %d, want 1 (AddDocument must mark the URL visited)", got)
	}
}

func TestSeedSkipsDuplicatesAndQueue(t *testing.T) {
	e := New(1, 10, 0, nil)

	e.Seed("https://example.com")
	e.Seed("https://example.com/")
	e.Seed("HTTPS://EXAMPLE.COM")

	if len(e.urlChan) != 1 {
		t.Errorf("queue = %d items, want 1 (seeds must dedupe after normalization)", len(e.urlChan))
	}
	if e.Stats().Visited != 1 {
		t.Errorf("Visited = %d, want 1", e.Stats().Visited)
	}
}

func TestStatsUsesTermCount(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.AddDocument("https://a.com", "A", "alpha beta gamma")
	s := e.Stats()
	if s.Documents != 1 {
		t.Errorf("Documents = %d, want 1", s.Documents)
	}
	if s.Terms != 3 {
		t.Errorf("Terms = %d, want 3", s.Terms)
	}
	if s.Visited != 1 {
		t.Errorf("Visited = %d, want 1", s.Visited)
	}
}

func TestEnqueueAllRollsBackPendingWhenQueueFull(t *testing.T) {
	e := New(1, 10, 0, nil)
	for i := 0; i < cap(e.urlChan); i++ {
		e.urlChan <- crawlItem{url: "filler"}
	}

	const target = "https://example.com/full"
	e.mu.Lock()
	e.pending[target] = 1
	e.mu.Unlock()

	e.enqueueAll(context.Background(), []crawlItem{{url: target, depth: 1}})

	e.mu.Lock()
	_, pending := e.pending[target]
	e.mu.Unlock()
	if pending {
		t.Error("pending mark not rolled back after queue-full drop; URL could never be rediscovered")
	}
}

func TestCrawlStatusUsesAtomicLoads(t *testing.T) {
	e := New(1, 10, 0, nil)
	cs := e.CrawlStatus()
	if cs.Crawling {
		t.Errorf("Crawling = true for idle engine with empty queue")
	}
	atomic.StoreInt32(&e.active, 1)
	atomic.StoreInt32(&e.queueSize, 3)
	cs = e.CrawlStatus()
	if !cs.Crawling || cs.Active != 1 || cs.QueueSize != 3 {
		t.Errorf("CrawlStatus = %+v, want active 1 queue 3 crawling true", cs)
	}
}

func TestExportDocumentsPaginates(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.AddDocument("https://a.com/page", "A", "alpha")
	e.AddDocument("https://b.com/page", "B", "beta")
	e.AddDocument("https://c.com/page", "C", "gamma")

	page, total := e.ExportDocuments(0, 2)
	if total != 3 || len(page) != 2 {
		t.Fatalf("ExportDocuments(0,2) = %d docs / total %d, want 2 / 3", len(page), total)
	}
	if page[0].URL != "https://a.com/page" || page[1].URL != "https://b.com/page" {
		t.Errorf("first page = %+v, want a.com then b.com", page)
	}
	if page[0].Title != "A" {
		t.Errorf("exported title = %q, want A", page[0].Title)
	}

	page, total = e.ExportDocuments(2, 10)
	if total != 3 || len(page) != 1 || page[0].URL != "https://c.com/page" {
		t.Fatalf("ExportDocuments(2,10) = %+v / total %d, want only c.com / 3", page, total)
	}

	if page, total = e.ExportDocuments(5, 10); total != 3 || len(page) != 0 {
		t.Fatalf("ExportDocuments(5,10) = %+v / total %d, want empty / 3", page, total)
	}

	if page, total = e.ExportDocuments(0, 0); total != 3 || len(page) != 3 {
		t.Fatalf("ExportDocuments(0,0) = %d docs, want all 3 (no cap)", len(page))
	}
}

func TestExportEachStreamsAndPaginates(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.AddDocument("https://a.com/page", "A", "alpha")
	e.AddDocument("https://b.com/page", "B", "beta")
	e.AddDocument("https://c.com/page", "C", "gamma")

	var got []ExportResult
	total, err := e.ExportEach(1, 1, func(doc ExportResult) error {
		got = append(got, doc)
		return nil
	})
	if err != nil {
		t.Fatalf("ExportEach: %v", err)
	}
	if total != 3 || len(got) != 1 || got[0].URL != "https://b.com/page" {
		t.Fatalf("ExportEach(1,1) = %+v / total %d, want b.com / 3", got, total)
	}

	stop := errors.New("stop")
	count := 0
	total, err = e.ExportEach(0, 0, func(ExportResult) error {
		count++
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("ExportEach error = %v, want stop", err)
	}
	if count != 1 {
		t.Fatalf("callback called %d times after error, want 1", count)
	}
	if total != 3 {
		t.Fatalf("ExportEach total after error = %d, want 3", total)
	}

	if got := e.ExportTotal(); got != 3 {
		t.Fatalf("ExportTotal = %d, want 3", got)
	}
}

func TestSitemapEachStreamsAndFiltersByHost(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.AddDocument("https://a.com/page", "A", "alpha")
	e.AddDocument("https://docs.a.com/guide", "Docs", "beta")
	e.AddDocument("https://b.com/page", "B", "gamma")
	e.AddDocument("https://nota.com/page", "N", "delta")

	var urls []string
	total, err := e.SitemapEach("a.com", 0, func(rawURL string) error {
		urls = append(urls, rawURL)
		return nil
	})
	if err != nil {
		t.Fatalf("SitemapEach: %v", err)
	}
	if total != 2 {
		t.Fatalf("SitemapEach total = %d, want 2 (a.com and subdomains)", total)
	}
	if len(urls) != 2 {
		t.Fatalf("SitemapEach wrote %d urls, want 2: %v", len(urls), urls)
	}
	for _, u := range urls {
		if !strings.Contains(u, "a.com") {
			t.Errorf("SitemapEach emitted %q outside host a.com", u)
		}
	}

	// limit caps the writes but the total still reflects every match.
	urls = nil
	total, err = e.SitemapEach("a.com", 1, func(rawURL string) error {
		urls = append(urls, rawURL)
		return nil
	})
	if err != nil {
		t.Fatalf("SitemapEach limit: %v", err)
	}
	if total != 2 || len(urls) != 1 {
		t.Fatalf("SitemapEach(limit=1) = %d urls / total %d, want 1 / 2", len(urls), total)
	}

	// An empty host matches every indexed URL.
	urls = nil
	total, err = e.SitemapEach("", 0, func(rawURL string) error {
		urls = append(urls, rawURL)
		return nil
	})
	if err != nil {
		t.Fatalf("SitemapEach all: %v", err)
	}
	if total != 4 || len(urls) != 4 {
		t.Fatalf("SitemapEach(all) = %d urls / total %d, want 4 / 4", len(urls), total)
	}

	// A callback error stops the walk and is returned to the caller.
	sentinel := errors.New("stop")
	if _, err := e.SitemapEach("", 0, func(string) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("SitemapEach error = %v, want sentinel", err)
	}
}

func TestScheduleRetrySkipsExcludedURLs(t *testing.T) {
	e := New(1, 10, 0, nil)
	rawURL := "https://example.com/search"
	e.mu.Lock()
	e.visited[rawURL] = struct{}{}
	e.mu.Unlock()

	e.scheduleRetry(rawURL, 0, fmt.Errorf("%w: path %q", crawler.ErrExcluded, "/search"))

	e.mu.Lock()
	_, retried := e.retryAt[rawURL]
	e.mu.Unlock()
	if retried {
		t.Fatal("excluded URL must not be rescheduled for retry")
	}
}

func TestDiscoverSkipsExcludedLinks(t *testing.T) {
	t.Setenv("PALSPIDER_EXCLUDE_PATHS", "/search,/login")
	t.Setenv("PALSPIDER_MAX_QUERY_PARAMS", "2")

	e := New(1, 10, 0, nil)
	html := `<a href="/keep">keep</a>` +
		`<a href="/search?q=golang">search</a>` +
		`<a href="/searching">searching</a>` +
		`<a href="/trap?a=1&b=2&c=3">trap</a>`
	dirty, items := e.discover("https://example.com/", html, 0)

	want := map[string]bool{
		"https://example.com/keep":      true,
		"https://example.com/searching": true,
	}
	if len(items) != len(want) {
		t.Fatalf("discover queued %d items %v, want %d", len(items), items, len(want))
	}
	for _, item := range items {
		if !want[item.url] {
			t.Errorf("discover queued %q, want only crawlable links", item.url)
		}
	}
	// Excluded links still count toward in-degree so link weights stay right.
	if len(dirty) != 4 {
		t.Errorf("dirty link counts = %v, want all 4 discovered links", dirty)
	}
}

func TestDiscoverSkipsPerHostExcludedLinks(t *testing.T) {
	t.Setenv("PALSPIDER_EXCLUDE_PATHS", "/global")
	t.Setenv("PALSPIDER_HOST_EXCLUDE_PATHS", "example.com:/search")

	e := New(1, 10, 0, nil)
	html := `<a href="/keep">keep</a>` +
		`<a href="https://example.com/search?q=golang">own search</a>` +
		`<a href="https://other.org/search">other search</a>`
	dirty, items := e.discover("https://example.com/", html, 0)

	want := map[string]bool{
		"https://example.com/keep": true,
		"https://other.org/search": true,
	}
	if len(items) != len(want) {
		t.Fatalf("discover queued %d items %v, want %d", len(items), items, len(want))
	}
	for _, item := range items {
		if !want[item.url] {
			t.Errorf("discover queued %q, want only links outside the host rules", item.url)
		}
	}
	if len(dirty) != 3 {
		t.Errorf("dirty link counts = %v, want all 3 discovered links", dirty)
	}
}

func TestScheduleRetryDefersFailuresForAboutAnHour(t *testing.T) {
	e := New(1, 10, 0, nil)
	url := "https://example.com/retry"
	e.mu.Lock()
	e.visited[url] = struct{}{}
	e.mu.Unlock()

	e.scheduleRetry(url, 0, errors.New("temporary network failure"))

	e.mu.Lock()
	at, exists := e.retryAt[url]
	e.mu.Unlock()
	if !exists {
		t.Fatal("retry timestamp missing after failure")
	}
	// The wait carries a jitter of up to a tenth of the window.
	low := time.Hour - time.Hour*retryJitterPercent/100
	high := time.Hour + time.Hour*retryJitterPercent/100
	if wait := time.Until(at); wait < low || wait > high {
		t.Fatalf("retry window = %v, want between %v and %v", wait, low, high)
	}
	if delay := e.retryDelay(url); delay <= 0 || delay > high {
		t.Fatalf("retryDelay = %v, want a positive delay within %v", delay, high)
	}
}

func TestRetryBackoffGrowsWithEveryAttempt(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.retryWindow = time.Hour

	cases := []struct {
		attempt int
		base    time.Duration
	}{
		{1, time.Hour},
		{2, 2 * time.Hour},
		{3, 4 * time.Hour},
		{4, 4 * time.Hour},
	}
	for _, c := range cases {
		got := e.retryBackoff(c.attempt)
		low := c.base - c.base*retryJitterPercent/100
		high := c.base + c.base*retryJitterPercent/100
		if got < low || got > high {
			t.Errorf("retryBackoff(%d) = %v, want between %v and %v", c.attempt, got, low, high)
		}
	}
}

func TestRetryBackoffJittersTheWait(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.retryWindow = time.Hour

	low := time.Hour - time.Hour*retryJitterPercent/100
	high := time.Hour + time.Hour*retryJitterPercent/100
	seen := make(map[time.Duration]struct{})
	var minWait, maxWait time.Duration
	for i := 0; i < 200; i++ {
		got := e.retryBackoff(1)
		if got < low || got > high {
			t.Fatalf("retryBackoff(1) = %v, want between %v and %v", got, low, high)
		}
		seen[got] = struct{}{}
		if minWait == 0 || got < minWait {
			minWait = got
		}
		if got > maxWait {
			maxWait = got
		}
	}
	if len(seen) < 2 {
		t.Fatal("retryBackoff returned the same wait every time, so the jitter does nothing")
	}
	if maxWait-minWait < time.Minute {
		t.Errorf("jitter spread = %v, want the waits to be spread out", maxWait-minWait)
	}
}

func TestRetryBackoffSeparatesAttempts(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.retryWindow = time.Hour

	first := e.retryBackoff(1)
	second := e.retryBackoff(2)
	if second <= first {
		t.Errorf("retryBackoff(2) = %v, want more than the first attempt wait %v", second, first)
	}
}

func TestClearQueueDropsParkedRetries(t *testing.T) {
	e := New(1, 10, 0, nil)
	url := "https://example.com/retry"
	e.scheduleRetry(url, 0, errors.New("temporary network failure"))

	// A worker parks the URL while its retry window is still open.
	item := <-e.urlChan
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.parkRetry(ctx, item, 50*time.Millisecond)

	e.ClearQueue()
	time.Sleep(200 * time.Millisecond)

	if len(e.urlChan) != 0 {
		t.Fatalf("queue = %d items, want a parked retry to stay dropped", len(e.urlChan))
	}
}

func TestClearQueueForgetsDroppedURLs(t *testing.T) {
	e := New(1, 10, 0, nil)
	e.Seed("https://example.com/page")
	e.ClearQueue()

	e.mu.Lock()
	_, visited := e.visited["https://example.com/page"]
	e.mu.Unlock()
	if visited {
		t.Error("a cleared URL is still marked visited, so it can never be crawled again")
	}

	e.Seed("https://example.com/page")
	if len(e.urlChan) != 1 {
		t.Fatalf("queue = %d items, want the cleared URL to be seedable again", len(e.urlChan))
	}
}

func TestScheduleRetryQueuesTheFailedURLAgain(t *testing.T) {
	e := New(1, 10, 0, nil)
	url := "https://example.com/retry"

	e.scheduleRetry(url, 0, errors.New("temporary network failure"))

	if len(e.urlChan) != 1 {
		t.Fatalf("queue = %d items, want the failed URL to be queued again", len(e.urlChan))
	}
	item := <-e.urlChan
	if item.url != url {
		t.Errorf("queued item = %q, want %q", item.url, url)
	}
	e.mu.Lock()
	_, pending := e.pending[url]
	e.mu.Unlock()
	if !pending {
		t.Error("the retried URL is not marked pending, so a second copy could be queued")
	}
}

func TestScheduleRetryStopsWhenTheBudgetIsSpent(t *testing.T) {
	e := New(1, 10, 0, nil)
	url := "https://example.com/dead"

	for i := 0; i < maxRetries; i++ {
		e.scheduleRetry(url, 0, errors.New("temporary network failure"))
	}
	if len(e.urlChan) != maxRetries {
		t.Fatalf("queue = %d items, want one per attempt", len(e.urlChan))
	}

	e.scheduleRetry(url, 0, errors.New("temporary network failure"))
	if len(e.urlChan) != maxRetries {
		t.Errorf("queue = %d items after the budget was spent, want %d", len(e.urlChan), maxRetries)
	}
	e.mu.Lock()
	_, visited := e.visited[url]
	e.mu.Unlock()
	if !visited {
		t.Error("a URL that ran out of retries is not marked visited, so it would be crawled again")
	}
}

func TestScheduleRetryIgnoresPermanentFailures(t *testing.T) {
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	missing := srv.URL + "/missing"
	_, err := crawler.FetchContext(context.Background(), missing, nil)
	if err == nil {
		t.Fatal("fetching a missing page returned no error")
	}
	if !crawler.IsPermanent(err) {
		t.Fatalf("error for a missing page = %v, want a permanent failure", err)
	}

	e := New(1, 10, 0, nil)
	e.scheduleRetry(missing, 0, err)

	if len(e.urlChan) != 0 {
		t.Errorf("queue = %d items, want a missing page to be dropped instead of retried", len(e.urlChan))
	}
	e.mu.Lock()
	_, retried := e.retryAt[missing]
	e.mu.Unlock()
	if retried {
		t.Error("a permanent failure was scheduled for a retry")
	}
}

func TestCrawlerRetriesAPageThatFailed(t *testing.T) {
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "1")
	t.Setenv(EnvRetryDelay, "1")

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			w.Header().Set("Content-Type", "text/plain")
			io.WriteString(w, "User-agent: *\nDisallow:\n")
			return
		}
		if atomic.AddInt32(&hits, 1) == 1 {
			// The first request fails, the retry succeeds.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, "<html><title>Retried</title><body>page that needed a retry</body></html>")
	}))
	defer srv.Close()

	e := New(1, 3, 0, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	defer e.Stop()
	e.Seed(srv.URL + "/page")

	deadline := time.Now().Add(15 * time.Second)
	for e.Indexer.DocCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the page was not indexed after the retry (%d requests)", atomic.LoadInt32(&hits))
		}
		time.Sleep(50 * time.Millisecond)
	}

	results := e.Search("retry", 5)
	if len(results) != 1 {
		t.Fatalf("search returned %d results, want the retried page", len(results))
	}
	if results[0].URL != srv.URL+"/page" {
		t.Errorf("indexed url = %q, want %q", results[0].URL, srv.URL+"/page")
	}
}

func TestDiscoverCapsLinksPerPage(t *testing.T) {
	t.Setenv(crawler.EnvMaxLinksPerPage, "2")

	e := New(1, 10, 0, nil)
	html := `<a href="/a">a</a><a href="/b">b</a><a href="/c">c</a>`
	_, items := e.discover("https://example.com/", html, 0)

	if len(items) != 2 {
		t.Fatalf("discover queued %d items %v, want 2 (the per-page cap)", len(items), items)
	}
	// Only the followed links are counted for in-degree; the dropped ones are
	// never even looked at.
	if got := e.LinkStore.Get("https://example.com/c"); got != 0 {
		t.Errorf("dropped link c has link count %d, want 0", got)
	}
}

func TestDiscoverEnforcesPerHostBudget(t *testing.T) {
	t.Setenv(crawler.EnvMaxPagesPerHost, "1")

	e := New(1, 10, 0, nil)
	// The host is already at its budget.
	e.mu.Lock()
	e.hostDocs["example.com"] = 1
	e.mu.Unlock()

	html := `<a href="/next">next</a><a href="https://other.com/page">other</a>`
	_, items := e.discover("https://example.com/", html, 0)

	if len(items) != 1 || items[0].url != "https://other.com/page" {
		t.Fatalf("discover queued %v, want only the link outside the host that is over budget", items)
	}
}

func TestHostHasBudgetUnlimitedByDefault(t *testing.T) {
	t.Setenv(crawler.EnvMaxPagesPerHost, "")
	e := New(1, 10, 0, nil)
	e.mu.Lock()
	e.hostDocs["example.com"] = 10000
	e.mu.Unlock()
	if !e.hostHasBudget("example.com") {
		t.Error("hostHasBudget = false with no configured budget, want true")
	}
}
