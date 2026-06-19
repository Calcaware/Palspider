package crawler

import (
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"
)

type RateLimiter struct {
	mu    sync.Mutex
	last  map[string]time.Time
	delay time.Duration
}

func NewRateLimiter(delay time.Duration) *RateLimiter {
	return &RateLimiter{last: make(map[string]time.Time), delay: delay}
}

func (r *RateLimiter) Wait(host string) {
	r.mu.Lock()
	last, ok := r.last[host]
	elapsed := time.Since(last)
	if ok && elapsed < r.delay {
		wait := r.delay - elapsed
		r.mu.Unlock()
		time.Sleep(wait)
	} else {
		r.last[host] = time.Now()
		r.mu.Unlock()
	}
}

var defaultClient = &http.Client{Timeout: 10 * time.Second}

func Fetch(rawURL string, limiter *RateLimiter) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	if limiter != nil {
		limiter.Wait(u.Host)
	}

	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Palspider/0.1")

	resp, err := defaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		loc := resp.Header.Get("Location")
		if loc == "" {
			return "", fmt.Errorf("redirect with no location")
		}
		abs, err := url.Parse(loc)
		if err != nil {
			return "", err
		}
		resolved := abs.ResolveReference(u)
		return Fetch(resolved.String(), limiter)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		return "", fmt.Errorf("unsupported content-type: %s", ct)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5<<20))
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func ExtractText(html string) (title, text string) {
	lower := strings.ToLower(html)

	title = extractTag(lower, "title")

	body := extractContent(lower, "body")
	if body == "" {
		body = lower
	}

	for _, tag := range []string{"script", "style", "nav", "footer", "header", "noscript"} {
		body = stripBlocks(body, tag)
	}

	body = stripHTML(body)
	body = strings.Join(strings.Fields(body), " ")
	return title, body
}

func ExtractLinks(html string, base *url.URL) []string {
	seen := make(map[string]bool)
	var links []string
	lower := strings.ToLower(html)
	idx := 0
	for {
		hrefIdx := strings.Index(lower[idx:], "href=\"")
		if hrefIdx == -1 {
			break
		}
		start := idx + hrefIdx + 6
		end := strings.IndexByte(html[start:], '"')
		if end == -1 {
			break
		}
		raw := html[start : start+end]
		idx = start + end + 1

		parsed, err := url.Parse(raw)
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

func NormalizeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
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

func Tokenize(text string) []string {
	var tokens []string
	var buf strings.Builder
	for _, r := range strings.ToLower(text) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			buf.WriteRune(r)
		} else if unicode.IsLetter(r) || unicode.IsDigit(r) {
			buf.WriteRune(r)
		} else {
			if buf.Len() > 1 {
				tokens = append(tokens, buf.String())
			}
			buf.Reset()
		}
	}
	if buf.Len() > 1 {
		tokens = append(tokens, buf.String())
	}
	return tokens
}

func extractTag(s, tag string) string {
	open := "<" + tag + ">"
	close := "</" + tag + ">"
	start := strings.Index(s, open)
	if start == -1 {
		return ""
	}
	start += len(open)
	end := strings.Index(s[start:], close)
	if end == -1 {
		return ""
	}
	return strings.TrimSpace(s[start : start+end])
}

func extractContent(s, tag string) string {
	prefix := "<" + tag
	start := strings.Index(s, prefix)
	if start == -1 {
		return ""
	}
	closeBracket := strings.IndexByte(s[start:], '>')
	if closeBracket == -1 {
		return ""
	}
	contentStart := start + closeBracket + 1

	closeTag := "</" + tag + ">"
	end := strings.Index(s[contentStart:], closeTag)
	if end == -1 {
		return ""
	}
	return s[contentStart : contentStart+end]
}

func stripBlocks(s, tag string) string {
	var result strings.Builder
	openPrefix := "<" + tag
	closeTag := "</" + tag + ">"
	for {
		start := strings.Index(s, openPrefix)
		if start == -1 {
			break
		}
		closeBracket := strings.IndexByte(s[start:], '>')
		if closeBracket == -1 {
			break
		}
		tagEnd := start + closeBracket + 1
		end := strings.Index(s[tagEnd:], closeTag)
		if end == -1 {
			break
		}
		result.WriteString(s[:start])
		s = s[tagEnd+end+len(closeTag):]
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
