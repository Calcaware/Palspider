package crawler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
)

// clearExclusionEnv removes the exclusion overrides so the test sees the
// built-in defaults even when the shell exports them.
func clearExclusionEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"PALSPIDER_EXCLUDE_PATHS", "PALSPIDER_MAX_QUERY_PARAMS", "PALSPIDER_MAX_QUERY_LENGTH"} {
		old, wasSet := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
		t.Cleanup(func() {
			if wasSet {
				os.Setenv(name, old)
				return
			}
			os.Unsetenv(name)
		})
	}
}

func TestCheckCrawlExclusionsBlocksDefaultPaths(t *testing.T) {
	clearExclusionEnv(t)
	cases := []struct {
		raw      string
		excluded bool
	}{
		{"https://example.com/search", true},
		{"https://example.com/search?q=golang", true},
		{"https://example.com/Search/advanced", true},
		{"https://example.com/login", true},
		{"https://example.com/login/oauth?next=%2F", true},
		{"https://example.com/searching", false},
		{"https://example.com/api/search", false},
		{"https://example.com/", false},
		{"https://example.com/posts?q=golang", false},
	}
	for _, c := range cases {
		err := CheckCrawlExclusions(c.raw)
		if c.excluded && !errors.Is(err, ErrExcluded) {
			t.Errorf("CheckCrawlExclusions(%q) = %v, want ErrExcluded", c.raw, err)
		}
		if !c.excluded && err != nil {
			t.Errorf("CheckCrawlExclusions(%q) = %v, want nil", c.raw, err)
		}
	}
}

func TestCheckCrawlExclusionsPathOverride(t *testing.T) {
	clearExclusionEnv(t)
	t.Setenv("PALSPIDER_EXCLUDE_PATHS", "/private, members/")

	for _, u := range []string{"https://example.com/private", "https://example.com/members/area"} {
		if err := CheckCrawlExclusions(u); !errors.Is(err, ErrExcluded) {
			t.Errorf("CheckCrawlExclusions(%q) = %v, want ErrExcluded", u, err)
		}
	}
	if err := CheckCrawlExclusions("https://example.com/search"); err != nil {
		t.Errorf("overridden rules should allow /search, got %v", err)
	}

	// An explicitly empty value turns path exclusions off entirely.
	t.Setenv("PALSPIDER_EXCLUDE_PATHS", "")
	if err := CheckCrawlExclusions("https://example.com/login"); err != nil {
		t.Errorf("empty PALSPIDER_EXCLUDE_PATHS should allow /login, got %v", err)
	}
}

func TestCheckCrawlExclusionsCapsQueryParams(t *testing.T) {
	clearExclusionEnv(t)
	t.Setenv("PALSPIDER_MAX_QUERY_LENGTH", "0") // this test is about the parameter count only
	params := func(n int) string {
		var parts []string
		for i := 0; i < n; i++ {
			parts = append(parts, "p"+strings.Repeat("x", i+1)+"=1")
		}
		return "https://example.com/list?" + strings.Join(parts, "&")
	}

	if err := CheckCrawlExclusions(params(defaultMaxQueryParams)); err != nil {
		t.Errorf("query at the default limit = %v, want nil", err)
	}
	if err := CheckCrawlExclusions(params(defaultMaxQueryParams + 1)); !errors.Is(err, ErrExcluded) {
		t.Errorf("query over the default limit = %v, want ErrExcluded", err)
	}

	t.Setenv("PALSPIDER_MAX_QUERY_PARAMS", "2")
	if err := CheckCrawlExclusions(params(2)); err != nil {
		t.Errorf("two parameters under a limit of 2 = %v, want nil", err)
	}
	if err := CheckCrawlExclusions(params(3)); !errors.Is(err, ErrExcluded) {
		t.Errorf("three parameters under a limit of 2 = %v, want ErrExcluded", err)
	}

	// Zero and negative values disable the cap, anything unparsable falls back
	// to the default.
	t.Setenv("PALSPIDER_MAX_QUERY_PARAMS", "0")
	if err := CheckCrawlExclusions(params(50)); err != nil {
		t.Errorf("disabled cap = %v, want nil", err)
	}
	t.Setenv("PALSPIDER_MAX_QUERY_PARAMS", "-5")
	if err := CheckCrawlExclusions(params(50)); err != nil {
		t.Errorf("negative cap = %v, want nil", err)
	}
	t.Setenv("PALSPIDER_MAX_QUERY_PARAMS", "abc")
	if err := CheckCrawlExclusions(params(defaultMaxQueryParams + 1)); !errors.Is(err, ErrExcluded) {
		t.Errorf("unparsable cap = %v, want the default limit enforced", err)
	}
}

func TestCheckCrawlExclusionsCapsQueryLength(t *testing.T) {
	clearExclusionEnv(t)
	long := "https://example.com/list?data=" + strings.Repeat("x", defaultMaxQueryLength)
	if err := CheckCrawlExclusions(long); !errors.Is(err, ErrExcluded) {
		t.Errorf("oversized query = %v, want ErrExcluded", err)
	}
	short := "https://example.com/list?data=" + strings.Repeat("x", 200)
	if err := CheckCrawlExclusions(short); err != nil {
		t.Errorf("short query = %v, want nil", err)
	}

	t.Setenv("PALSPIDER_MAX_QUERY_LENGTH", "32")
	if err := CheckCrawlExclusions(short); !errors.Is(err, ErrExcluded) {
		t.Errorf("query over a limit of 32 = %v, want ErrExcluded", err)
	}
	t.Setenv("PALSPIDER_MAX_QUERY_LENGTH", "0")
	if err := CheckCrawlExclusions(long); err != nil {
		t.Errorf("disabled cap = %v, want nil", err)
	}
}

func TestCheckCrawlExclusionsIgnoresMalformedURLs(t *testing.T) {
	clearExclusionEnv(t)
	for _, raw := range []string{"", "not-a-url", "://broken"} {
		if err := CheckCrawlExclusions(raw); err != nil {
			t.Errorf("CheckCrawlExclusions(%q) = %v, want nil for the other checks to handle", raw, err)
		}
	}
}

func TestNormalizeURLStripsTrackingParams(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://example.com/p?utm_source=news&utm_medium=email", "https://example.com/p"},
		{"https://example.com/p?page=2&utm_campaign=spring&fbclid=abc", "https://example.com/p?page=2"},
		{"https://example.com/p?UTM_SOURCE=x", "https://example.com/p"},
		{"https://example.com/p?gclid=1&msclkid=2", "https://example.com/p"},
		{"https://example.com/p?ref=home&utm_source=x", "https://example.com/p?ref=home"},
		{"https://example.com/p?id=7", "https://example.com/p?id=7"},
		{"https://example.com/p?b=2&a=1", "https://example.com/p?b=2&a=1"},
		{"https://example.com/p?a=%zz&utm_source=x", "https://example.com/p?a=%zz"},
	}
	for _, c := range cases {
		if got := NormalizeURL(c.in); got != c.want {
			t.Errorf("NormalizeURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFetchStopsAtExcludedRedirect(t *testing.T) {
	clearExclusionEnv(t)
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "1")
	t.Setenv("PALSPIDER_DENYLIST", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/start":
			http.Redirect(w, r, "/login?next=%2F", http.StatusFound)
		case "/login":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write([]byte("<html><body>login page</body></html>"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	_, err := Fetch(srv.URL+"/start", NewRateLimiter(0))
	if !errors.Is(err, ErrExcluded) {
		t.Fatalf("Fetch following an excluded redirect = %v, want ErrExcluded", err)
	}
}

func TestFetchStopsAtRedirectOutsideAllowlist(t *testing.T) {
	clearExclusionEnv(t)
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "1")
	t.Setenv("PALSPIDER_ALLOWLIST", "127.0.0.1")
	t.Setenv("PALSPIDER_DENYLIST", "")
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			parsed, err := url.Parse(srv.URL)
			if err == nil {
				http.Redirect(w, r, "http://localhost:"+parsed.Port()+"/target", http.StatusFound)
			}
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte("<html><body>target page</body></html>"))
	}))
	defer srv.Close()

	_, err := Fetch(srv.URL+"/start", NewRateLimiter(0))
	if err == nil || !strings.Contains(err.Error(), "not allowed by policy") {
		t.Fatalf("Fetch redirecting outside the allowlist = %v, want a policy error", err)
	}
}

func TestFetchRejectsExcludedURL(t *testing.T) {
	clearExclusionEnv(t)
	t.Setenv("PALSPIDER_ALLOW_PRIVATE", "1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte("<html><body>docs body</body></html>"))
	}))
	defer srv.Close()

	_, err := Fetch(srv.URL+"/search?q=golang", NewRateLimiter(0))
	if !errors.Is(err, ErrExcluded) {
		t.Fatalf("Fetch on an excluded path = %v, want ErrExcluded", err)
	}
	if err := CheckURLPolicy(srv.URL + "/login"); !errors.Is(err, ErrExcluded) {
		t.Errorf("CheckURLPolicy on an excluded path = %v, want ErrExcluded", err)
	}

	body, err := Fetch(srv.URL+"/docs", NewRateLimiter(0))
	if err != nil {
		t.Fatalf("Fetch on an allowed path: %v", err)
	}
	if !strings.Contains(body, "docs body") {
		t.Errorf("Fetch body = %q, want the page content", body)
	}
}
