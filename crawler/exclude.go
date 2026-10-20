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

// EnvMaxPagesPerHost and EnvMaxLinksPerPage name the variables that bound the
// crawl. MaxPagesPerHost caps how many pages one host may contribute to the
// index; MaxLinksPerPage caps how many links are followed from a single page.
// Both default to zero, which means unlimited.
const (
	EnvMaxPagesPerHost = "PALSPIDER_MAX_PAGES_PER_HOST"
	EnvMaxLinksPerPage = "PALSPIDER_MAX_LINKS_PER_PAGE"
)

// EnvHostExcludePaths names the variable holding route exclusions that apply
// to a single host instead of to every crawl target.
const EnvHostExcludePaths = "PALSPIDER_HOST_EXCLUDE_PATHS"

// hostExcludeRule is one entry of PALSPIDER_HOST_EXCLUDE_PATHS: a host
// pattern and the path prefixes that are never crawled for that host.
type hostExcludeRule struct {
	host  string
	paths []string
}

// hostExcludeRules parses PALSPIDER_HOST_EXCLUDE_PATHS. Entries are separated
// by semicolons or newlines and read as "host pattern:path, path", so
// `example.com:/search, /login; forum.example.org:/thread` skips /search and
// /login on example.com and its subdomains and /thread on forum.example.org.
// The first colon splits the pattern from the path list, so patterns are
// hostnames like the allowlist ones and never carry a port. Entries without a
// colon or without any path are dropped so one typo does not discard the rest.
func hostExcludeRules() []hostExcludeRule {
	value, ok := os.LookupEnv(EnvHostExcludePaths)
	if !ok {
		return nil
	}
	var rules []hostExcludeRule
	for _, entry := range strings.FieldsFunc(value, func(r rune) bool {
		return r == ';' || r == '\n' || r == '\r'
	}) {
		host, rawPaths, found := strings.Cut(entry, ":")
		if !found {
			continue
		}
		host = strings.ToLower(strings.TrimSpace(host))
		paths := splitCSV(rawPaths)
		if host == "" || len(paths) == 0 {
			continue
		}
		rules = append(rules, hostExcludeRule{host: host, paths: paths})
	}
	return rules
}

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

// MaxPagesPerHost returns the per-host document budget. Zero means unlimited.
func MaxPagesPerHost() int {
	return crawlLimit(EnvMaxPagesPerHost, 0)
}

// MaxLinksPerPage returns how many links may be followed from one page.
// Zero means unlimited.
func MaxLinksPerPage() int {
	return crawlLimit(EnvMaxLinksPerPage, 0)
}

// LimitLinks truncates links to the value of PALSPIDER_MAX_LINKS_PER_PAGE.
// The input order is preserved, so callers that rank links keep their ranking.
func LimitLinks(links []string) []string {
	if max := MaxLinksPerPage(); max > 0 && len(links) > max {
		return links[:max]
	}
	return links
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
// rules: a configured path prefix, a path prefix excluded for its host, more
// query parameters than allowed, or a query string longer than allowed. The
// returned error wraps ErrExcluded and names the rule that matched.
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
	for _, rule := range hostExcludeRules() {
		if !matchesHostPattern(u.Hostname(), rule.host) {
			continue
		}
		for _, prefix := range rule.paths {
			if matchesExcludePath(path, prefix) {
				return fmt.Errorf("%w: path %q matches %q for host %q", ErrExcluded, path, prefix, rule.host)
			}
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
