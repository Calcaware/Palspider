package crawler

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// UserAgent identifies the crawler to remote servers.
const UserAgent = "Palspider/0.2"

type RateLimiter struct {
	mu    sync.Mutex
	last  map[string]time.Time
	delay time.Duration
}

const limiterPruneSize = 4096

func NewRateLimiter(delay time.Duration) *RateLimiter {
	return &RateLimiter{last: make(map[string]time.Time), delay: delay}
}

// Wait blocks until the next request to host may start. Slots are reserved
// under the lock so concurrent workers cannot wake up and fire together.
func (r *RateLimiter) Wait(host string) {
	now := time.Now()
	r.mu.Lock()
	start := now
	if earliest, ok := r.last[host]; ok && earliest.After(now) {
		start = earliest
	}
	r.last[host] = start.Add(r.delay)
	if len(r.last) > limiterPruneSize {
		for h, t := range r.last {
			if t.Before(now) {
				delete(r.last, h)
			}
		}
	}
	r.mu.Unlock()
	if wait := time.Until(start); wait > 0 {
		time.Sleep(wait)
	}
}

var defaultClient = &http.Client{
	Timeout: 10 * time.Second,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		return checkHostAllowed(req.URL.Host)
	},
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

// dialContext resolves the target once and dials only addresses that pass the
// private-network policy. The pre-fetch check in Fetch performs its own
// lookup, so verifying again with the same resolution used for dialing closes
// the DNS-rebinding window in between.
func dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if privateNetworkAllowed() {
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	host = strings.Trim(host, "[]")

	var candidates []net.IPAddr
	if ip := net.ParseIP(host); ip != nil {
		if blockedIP(ip) {
			return nil, fmt.Errorf("blocked address %s", ip)
		}
		candidates = []net.IPAddr{{IP: ip}}
	} else {
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, ipa := range ips {
			if !blockedIP(ipa.IP) {
				candidates = append(candidates, ipa)
			}
		}
		if len(candidates) == 0 {
			return nil, fmt.Errorf("host %s resolves only to blocked addresses", host)
		}
	}

	var lastErr error
	for _, ipa := range candidates {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(ipa.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func privateNetworkAllowed() bool {
	v := os.Getenv("PALSPIDER_ALLOW_PRIVATE")
	return v == "1" || strings.EqualFold(v, "true")
}

// checkHostAllowed blocks requests to loopback, link-local, and private
// addresses unless PALSPIDER_ALLOW_PRIVATE is set. Every resolved IP must be
// allowed.
func checkHostAllowed(hostport string) error {
	if privateNetworkAllowed() {
		return nil
	}
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return fmt.Errorf("empty host")
	}
	if strings.EqualFold(host, "localhost") {
		return fmt.Errorf("blocked host %q", host)
	}
	if ip := net.ParseIP(host); ip != nil {
		if blockedIP(ip) {
			return fmt.Errorf("blocked address %s", ip)
		}
		return nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return err
	}
	for _, ip := range ips {
		if blockedIP(ip) {
			return fmt.Errorf("host %s resolves to blocked address %s", host, ip)
		}
	}
	return nil
}

func blockedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified()
}

// Fetch downloads and decodes a page, retrying once on transport errors. It
// is FetchContext with a background context.
func Fetch(rawURL string, limiter *RateLimiter) (string, error) {
	return FetchContext(context.Background(), rawURL, limiter)
}

// FetchContext downloads a page, respecting ctx for cancellation and the
// per-host rate limiter between attempts.
func FetchContext(ctx context.Context, rawURL string, limiter *RateLimiter) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	if err := checkHostAllowed(u.Host); err != nil {
		return "", err
	}

	if limiter != nil {
		limiter.Wait(u.Host)
	}

	body, err := doFetch(ctx, rawURL)
	if err != nil && !isServerError(err) && ctx.Err() == nil {
		if limiter != nil {
			limiter.Wait(u.Host)
		}
		body, err = doFetch(ctx, rawURL)
	}
	if err != nil {
		return "", err
	}
	return body, nil
}

// serverError marks failures where the server responded, so retrying
// immediately would not help.
type serverError struct{ msg string }

func (e *serverError) Error() string { return e.msg }

func isServerError(err error) bool {
	var se *serverError
	return errors.As(err, &se)
}

func doFetch(ctx context.Context, rawURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", UserAgent)

	resp, err := defaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", &serverError{fmt.Sprintf("HTTP %d", resp.StatusCode)}
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		return "", &serverError{fmt.Sprintf("unsupported content-type: %s", ct)}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5<<20))
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// ExtractText pulls out the page title and visible body text, preserving the
// original casing. The original casing is kept because titles and snippets are
// shown to users; matching lowercases later in Tokenize.
func ExtractText(html string) (title, text string) {
	title = strings.TrimSpace(extractContent(html, "title"))

	body := extractContent(html, "body")
	if body == "" {
		body = html
	}

	for _, tag := range []string{"script", "style", "nav", "footer", "header", "noscript"} {
		body = stripBlocks(body, tag)
	}

	body = stripHTML(body)
	body = strings.Join(strings.Fields(body), " ")
	return title, body
}

// ExtractLinks returns normalized http(s) targets of <a href> attributes.
// Script, style, and noscript blocks are stripped first so JavaScript template
// strings are not mistaken for real anchors. Double-quoted, single-quoted, and
// unquoted values are all supported.
func ExtractLinks(html string, base *url.URL) []string {
	for _, tag := range []string{"script", "style", "noscript"} {
		html = stripBlocks(html, tag)
	}
	seen := make(map[string]bool)
	var links []string
	lower := strings.ToLower(html)
	idx := 0
	for {
		i := strings.Index(lower[idx:], "<a")
		if i == -1 {
			break
		}
		i += idx
		nameEnd := i + 2
		if nameEnd < len(lower) && !isSpaceByte(lower[nameEnd]) && lower[nameEnd] != '>' {
			idx = nameEnd
			continue
		}
		gt := tagEnd(html, nameEnd)
		if gt == -1 {
			break
		}
		raw, ok := hrefValue(html[i : gt+1])
		idx = gt + 1
		if !ok {
			continue
		}
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			continue
		}
		resolved := base.ResolveReference(parsed)
		if resolved.Scheme != "http" && resolved.Scheme != "https" {
			continue
		}
		normalized := NormalizeURL(resolved.String())
		if !seen[normalized] {
			seen[normalized] = true
			links = append(links, normalized)
		}
	}
	return links
}

// hrefValue extracts the value of the href attribute from a tag such as
// `<a class="x" href='/page' >`.
func hrefValue(tag string) (string, bool) {
	lower := strings.ToLower(tag)
	idx := 0
	for {
		i := strings.Index(lower[idx:], "href")
		if i == -1 {
			return "", false
		}
		i += idx
		if i > 0 {
			before := lower[i-1]
			if !isSpaceByte(before) && before != '"' && before != '\'' {
				idx = i + 4
				continue
			}
		}
		j := i + 4
		for j < len(tag) && isSpaceByte(tag[j]) {
			j++
		}
		if j >= len(tag) || tag[j] != '=' {
			idx = i + 4
			continue
		}
		j++
		for j < len(tag) && isSpaceByte(tag[j]) {
			j++
		}
		if j >= len(tag) {
			return "", false
		}
		if c := tag[j]; c == '"' || c == '\'' {
			k := strings.IndexByte(tag[j+1:], c)
			if k == -1 {
				return "", false
			}
			return tag[j+1 : j+1+k], true
		}
		k := j
		for k < len(tag) && !isSpaceByte(tag[k]) && tag[k] != '>' {
			k++
		}
		return tag[j:k], true
	}
}

func NormalizeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Host)
	if port := u.Port(); port != "" &&
		((u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443")) {
		host = strings.TrimSuffix(host, ":"+port)
	}
	u.Host = host
	u.Fragment = ""
	u.RawFragment = ""
	path := strings.TrimSuffix(u.Path, "/")
	if path == "" {
		path = "/"
	}
	u.Path = path
	u.RawPath = ""
	return u.String()
}

func ContentFingerprint(text string) string {
	normalized := strings.Join(strings.Fields(text), " ")
	if len(normalized) > 500 {
		normalized = normalized[:500]
	}
	h := md5.Sum([]byte(normalized))
	return fmt.Sprintf("%x", h)
}

// Tokenize lowercases text and splits it into terms. Terms shorter than two
// runes are dropped and leading/trailing hyphens are trimmed.
func Tokenize(text string) []string {
	var tokens []string
	var buf strings.Builder
	flush := func() {
		if buf.Len() == 0 {
			return
		}
		tok := strings.Trim(buf.String(), "-")
		buf.Reset()
		if utf8.RuneCountInString(tok) < 2 {
			return
		}
		tokens = append(tokens, tok)
	}
	for _, r := range strings.ToLower(text) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			buf.WriteRune(r)
		} else if unicode.IsLetter(r) || unicode.IsDigit(r) {
			buf.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return tokens
}

// indexFold finds the first case-insensitive occurrence of sub in s at or
// after from.
func indexFold(s, sub string, from int) int {
	if from < 0 {
		from = 0
	}
	n, m := len(s), len(sub)
	if m == 0 || n < m {
		return -1
	}
	for i := from; i+m <= n; i++ {
		if strings.EqualFold(s[i:i+m], sub) {
			return i
		}
	}
	return -1
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f'
}

// afterTagName reports whether the byte right after a tag name closes it
// ("<body>" or "<body ...>" but not "<bodyguard>").
func afterTagName(s string, i int) bool {
	if i >= len(s) {
		return false
	}
	c := s[i]
	return c == '>' || c == '/' || isSpaceByte(c)
}

// tagEnd returns the index of the '>' that closes the tag starting at from,
// ignoring '>' inside quoted attribute values.
func tagEnd(s string, from int) int {
	var quote byte
	for i := from; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '"' || c == '\'' {
			quote = c
			continue
		}
		if c == '>' {
			return i
		}
	}
	return -1
}

func extractContent(s, tag string) string {
	prefix := "<" + tag
	from := 0
	start := -1
	for {
		idx := indexFold(s, prefix, from)
		if idx == -1 {
			return ""
		}
		if afterTagName(s, idx+len(prefix)) {
			start = idx
			break
		}
		from = idx + len(prefix)
	}

	gt := tagEnd(s, start)
	if gt == -1 {
		return ""
	}
	contentStart := gt + 1

	closeTag := "</" + tag + ">"
	end := indexFold(s, closeTag, contentStart)
	if end == -1 {
		return ""
	}
	return s[contentStart:end]
}

func stripBlocks(s, tag string) string {
	openPrefix := "<" + tag
	closeTag := "</" + tag + ">"
	var result strings.Builder
	from := 0
	for {
		start := indexFold(s, openPrefix, from)
		if start == -1 {
			break
		}
		if !afterTagName(s, start+len(openPrefix)) {
			from = start + len(openPrefix)
			continue
		}
		gt := tagEnd(s, start)
		if gt == -1 {
			break
		}
		tagEndIdx := gt + 1
		end := indexFold(s, closeTag, tagEndIdx)
		if end == -1 {
			break
		}
		result.WriteString(s[:start])
		s = s[end+len(closeTag):]
		from = 0
	}
	result.WriteString(s)
	return result.String()
}

func stripHTML(s string) string {
	var result strings.Builder
	inTag := false
	for _, r := range s {
		if r == '<' {
			inTag = true
		} else if r == '>' {
			inTag = false
		} else if !inTag {
			result.WriteRune(r)
		}
	}
	return result.String()
}
