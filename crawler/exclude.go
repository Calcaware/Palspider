package crawler

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// ErrExcluded marks a URL that the exclusion rules rejected because its path
// is on the noisy-route list or its query string is oversized. Callers check
// it with errors.Is so these URLs are dropped instead of retried like
// transient failures.
var ErrExcluded = errors.New("excluded by crawl policy")

// defaultExcludePaths is used when PALSPIDER_EXCLUDE_PATHS is not set at all.
// Setting the variable to an empty value turns path exclusions off.
var defaultExcludePaths = []string{"/search", "/login"}

const (
	// defaultMaxQueryParams and defaultMaxQueryLength bound the query string
	// of a crawlable URL. Query heavy endpoints are usually search forms,
	// session identifiers, or faceted navigation that generate endless
	// near-duplicate pages.
	defaultMaxQueryParams = 8
	defaultMaxQueryLength = 1024
)

// excludePaths returns the path prefixes that are never crawled.
func excludePaths() []string {
	if value, ok := os.LookupEnv("PALSPIDER_EXCLUDE_PATHS"); ok {
		return splitCSV(value)
	}
	return defaultExcludePaths
}

// crawlLimit reads a limit from the environment. Missing, non-numeric, or
// unparsable values fall back to def; zero or a negative value disables the
// limit.
func crawlLimit(name string, def int) int {
	value, ok := os.LookupEnv(name)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return def
	}
	return n
}

// matchesExcludePath reports whether path falls under prefix. Matching is
// case-insensitive and stops at segment boundaries, so "/search" excludes
// "/search" and "/search/advanced" but not "/searching".
func matchesExcludePath(path, prefix string) bool {
	prefix = strings.ToLower(strings.TrimSpace(prefix))
	if prefix == "" {
		return false
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return true // "/" excludes every path
	}
	path = strings.ToLower(path)
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// queryParamCount counts the "&" separated pairs of a raw query string
// without parsing it, so malformed pairs still count toward the limit.
func queryParamCount(rawQuery string) int {
	if rawQuery == "" {
		return 0
	}
	return strings.Count(rawQuery, "&") + 1
}

// CheckCrawlExclusions reports whether a URL falls under the crawl exclusion
// rules: a configured path prefix, more query parameters than allowed, or a
// query string longer than allowed. The returned error wraps ErrExcluded.
// Malformed URLs return nil so the scheme and host checks can report them.
func CheckCrawlExclusions(rawURL string) error {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" {
		return nil
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	for _, prefix := range excludePaths() {
		if matchesExcludePath(path, prefix) {
			return fmt.Errorf("%w: path %q matches %q", ErrExcluded, path, prefix)
		}
	}
	if max := crawlLimit("PALSPIDER_MAX_QUERY_PARAMS", defaultMaxQueryParams); max > 0 {
		if count := queryParamCount(u.RawQuery); count > max {
			return fmt.Errorf("%w: %d query parameters over the %d allowed", ErrExcluded, count, max)
		}
	}
	if max := crawlLimit("PALSPIDER_MAX_QUERY_LENGTH", defaultMaxQueryLength); max > 0 {
		if length := len(u.RawQuery); length > max {
			return fmt.Errorf("%w: query string of %d bytes over the %d allowed", ErrExcluded, length, max)
		}
	}
	return nil
}

// trackingParams are analytics and advertising click identifiers. They never
// change page content, so they are dropped while normalizing a URL and the
// same page linked with different campaign parameters is crawled once.
var trackingParams = map[string]bool{
	"fbclid":      true,
	"gclid":       true,
	"gbraid":      true,
	"wbraid":      true,
	"msclkid":     true,
	"dclid":       true,
	"twclid":      true,
	"igshid":      true,
	"yclid":       true,
	"mc_cid":      true,
	"mc_eid":      true,
	"_ga":         true,
	"vero_conv":   true,
	"vero_id":     true,
	"wickedid":    true,
	"oly_anon_id": true,
	"oly_enc_id":  true,
	"srsltid":     true,
}

func isTrackingParam(key string) bool {
	key = strings.ToLower(key)
	if strings.HasPrefix(key, "utm_") {
		return true
	}
	return trackingParams[key]
}

// stripTrackingQuery removes tracking parameters from a raw query string and
// reports whether anything was removed. Surviving parameters keep their
// original order and encoding, including values that url.ParseQuery would
// reject.
func stripTrackingQuery(rawQuery string) (string, bool) {
	parts := strings.Split(rawQuery, "&")
	kept := parts[:0]
	for _, part := range parts {
		key, _, _ := strings.Cut(part, "=")
		if decoded, err := url.PathUnescape(key); err == nil {
			key = decoded
		}
		if isTrackingParam(key) {
			continue
		}
		kept = append(kept, part)
	}
	if len(kept) == len(parts) {
		return rawQuery, false
	}
	return strings.Join(kept, "&"), true
}
