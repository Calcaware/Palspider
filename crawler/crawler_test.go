package crawler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNormalizeURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"HTTPS://Example.COM/Path/", "https://example.com/Path"},
		{"https://example.com:443/x", "https://example.com/x"},
		{"http://example.com:80/x", "http://example.com/x"},
		{"https://example.com/x#section", "https://example.com/x"},
		{"https://example.com", "https://example.com/"},
		{"https://example.com/a//b", "https://example.com/a//b"},
	}
	for _, c := range cases {
		if got := NormalizeURL(c.in); got != c.want {
			t.Errorf("NormalizeURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestExtractTextPreservesCase(t *testing.T) {
	html := `<html><head><TITLE lang="en">Node.js Runtime</TITLE></head>` +
		`<body><p>Hello World Foo</p><script>var x = 1;</script><style>.a{}</style></body></html>`
	title, text := ExtractText(html)
	if title != "Node.js Runtime" {
		t.Errorf("title = %q, want original casing", title)
	}
	if text != "Hello World Foo" {
		t.Errorf("text = %q, want script/style stripped and casing kept", text)
	}
}

func TestExtractTextStripsBoilerplate(t *testing.T) {
	html := `<body><nav>Home About</nav><p>Real content here</p><footer>copyright</footer></body>`
	_, text := ExtractText(html)
	if strings.Contains(text, "Home") || strings.Contains(text, "copyright") {
		t.Errorf("nav/footer not stripped: %q", text)
	}
	if !strings.Contains(text, "Real content here") {
		t.Errorf("body text missing: %q", text)
	}
}

func TestExtractLinks(t *testing.T) {
	base, _ := url.Parse("https://example.com/dir/page.html")
	html := `
		<a href="/one">one</a>
		<a class="x" href='/two'>two</a>
		<a href=three>three</a>
		<a href="https://other.com/four" >four</a>
		<a href="#frag">frag</a>
		<a>no href</a>
		<a data-href="/not-a-href">bad</a>
		<link href="/style.css">
		<div data-href="/also-not">x</div>
		<area href="/map">
	`
	links := ExtractLinks(html, base)
	want := map[string]bool{
		"https://example.com/one":           true,
		"https://example.com/two":           true,
		"https://example.com/dir/three":     true,
		"https://other.com/four":            true,
		"https://example.com/dir/page.html": true,
	}
	if len(links) != len(want) {
		t.Errorf("got %d links %v, want %d", len(links), links, len(want))
	}
	for _, l := range links {
		if !want[l] {
			t.Errorf("unexpected link %q", l)
		}
	}
}

func TestExtractLinksIgnoresScriptContent(t *testing.T) {
	base, _ := url.Parse("https://example.com/")
	html := "<a href=\"/real\">real</a>" +
		"<script>el.innerHTML = '<a href=\"/fake-from-js\">x</a>';</script>" +
		"<style>.x { background: url('/fake-css'); }</style>"
	links := ExtractLinks(html, base)
	if len(links) != 1 || links[0] != "https://example.com/real" {
		t.Errorf("links = %v, want only the real anchor", links)
	}
}

func TestTokenize(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"Hello World", []string{"hello", "world"}},
		{"Node.js with state-of-the-art AI", []string{"node", "js", "with", "state-of-the-art", "ai"}},
		{"a b c", nil},
		{"--sep-- sep-", []string{"sep", "sep"}},
		{"CAFÉ déjà vu", []string{"café", "déjà", "vu"}},
	}
	for _, c := range cases {
		got := Tokenize(c.in)
		if len(got) != len(c.want) {
			t.Errorf("Tokenize(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("Tokenize(%q) = %v, want %v", c.in, got, c.want)
				break
			}
		}
	}
}

func TestContentFingerprintIgnoresWhitespace(t *testing.T) {
	a := ContentFingerprint("one   two\n\tthree")
	b := ContentFingerprint("one two three")
	if a != b {
		t.Errorf("fingerprints differ for whitespace variants: %s vs %s", a, b)
	}
	if ContentFingerprint(strings.Repeat("x", 600)) == ContentFingerprint(strings.Repeat("y", 600)) {
		t.Error("different long texts should not collide")
	}
}

func TestFetchBlocksPrivateAddresses(t *testing.T) {
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "")
	for _, u := range []string{"http://127.0.0.1:9/", "http://localhost:9/", "http://169.254.169.254/"} {
		if _, err := Fetch(u, nil); err == nil || !strings.Contains(err.Error(), "blocked") {
			t.Errorf("Fetch(%q) err = %v, want blocked error", u, err)
		}
	}
}

func TestFetchAllowsPrivateWhenConfigured(t *testing.T) {
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte("<html><title>Local Page</title><body>local body text</body></html>"))
	}))
	defer srv.Close()

	body, err := Fetch(srv.URL, NewRateLimiter(0))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !strings.Contains(body, "local body text") {
		t.Errorf("unexpected body %q", body)
	}
}

func TestFetchContextHonorsCancellation(t *testing.T) {
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := FetchContext(ctx, "http://127.0.0.1:9/", nil); err == nil {
		t.Fatal("FetchContext with canceled context returned nil error")
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestRateLimiterSerializesSameHost(t *testing.T) {
	r := NewRateLimiter(60 * time.Millisecond)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Wait("example.com")
		}()
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Errorf("3 concurrent waits on one host took %v, want >= 120ms (slots must be reserved)", elapsed)
	}
}

func TestRateLimiterPrunesStaleEntries(t *testing.T) {
	r := NewRateLimiter(time.Millisecond)
	for i := 0; i < limiterPruneSize+100; i++ {
		r.Wait(fmt.Sprintf("host-%d", i))
	}
	time.Sleep(5 * time.Millisecond)
	r.Wait("fresh")
	r.mu.Lock()
	size := len(r.last)
	r.mu.Unlock()
	if size >= limiterPruneSize {
		t.Errorf("rate limiter map not pruned: %d entries", size)
	}
}
