package crawler

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var robotPolicyMu sync.Mutex
var robotPolicies = map[string]robotPolicyCache{}

const robotPolicyTTL = 30 * time.Minute

type robotPolicyCache struct {
	fetched time.Time
	allow   []string
	deny    []string
}

type robotsPolicy struct {
	allow []string
	deny  []string
}

func robotsEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("PALSPIDER_ALLOW_ROBOTS")))
	if v == "" {
		return true
	}
	return v != "0" && v != "false" && v != "no"
}

func parseRobotsPolicy(body string) *robotsPolicy {
	policy := &robotsPolicy{}
	inMatchGroup := false
	for _, raw := range strings.Split(body, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		if key == "user-agent" {
			inMatchGroup = strings.EqualFold(value, "*") || strings.EqualFold(value, "palspider")
			continue
		}
		if !inMatchGroup {
			continue
		}
		if key == "allow" {
			policy.allow = append(policy.allow, value)
			continue
		}
		if key == "disallow" {
			policy.deny = append(policy.deny, value)
		}
	}
	if len(policy.allow) == 0 && len(policy.deny) == 0 {
		return nil
	}
	return policy
}

func robotsPolicyForHost(host string) (*robotsPolicy, error) {
	if !robotsEnabled() {
		return nil, nil
	}
	robotPolicyMu.Lock()
	cached, ok := robotPolicies[host]
	robotPolicyMu.Unlock()
	if ok && time.Since(cached.fetched) < robotPolicyTTL {
		return &robotsPolicy{allow: cached.allow, deny: cached.deny}, nil
	}

	robotsURL := (&url.URL{Scheme: "https", Host: host, Path: "/robots.txt"}).String()
	if strings.HasPrefix(host, "localhost") || strings.Contains(host, ":") {
		robotsURL = (&url.URL{Scheme: "http", Host: host, Path: "/robots.txt"}).String()
	}
	resp, err := defaultClient.Get(robotsURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	policy := parseRobotsPolicy(string(body))
	if policy == nil {
		robotPolicyMu.Lock()
		robotPolicies[host] = robotPolicyCache{fetched: time.Now()}
		robotPolicyMu.Unlock()
		return nil, nil
	}
	robotPolicyMu.Lock()
	robotPolicies[host] = robotPolicyCache{fetched: time.Now(), allow: policy.allow, deny: policy.deny}
	robotPolicyMu.Unlock()
	return policy, nil
}

// isRobotsAllowed reports whether robots.txt lets the crawler fetch rawURL.
func isRobotsAllowed(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return true
	}
	policy, err := robotsPolicyForHost(u.Host)
	if err != nil || policy == nil {
		return true
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	return robotsAllows(policy, path)
}

// robotsAllows resolves overlapping rules the way the robots exclusion
// protocol requires: the rule matching the longest prefix of the path wins,
// and an Allow rule wins when it is at least as long as the Disallow rule it
// collides with. A path no rule matches is allowed, and "Disallow: /" matches
// every path instead of only the bare root.
func robotsAllows(policy *robotsPolicy, path string) bool {
	if policy == nil {
		return true
	}
	denyLen, allowLen := -1, -1
	for _, deny := range policy.deny {
		if n := matchedRuleLen(path, deny); n > denyLen {
			denyLen = n
		}
	}
	for _, allow := range policy.allow {
		if n := matchedRuleLen(path, allow); n > allowLen {
			allowLen = n
		}
	}
	if allowLen >= 0 && allowLen >= denyLen {
		return true
	}
	return denyLen < 0
}

// matchedRuleLen returns the length of the rule prefix that applies to path,
// or -1 when the rule does not match at all. Rules are prefix patterns with an
// optional trailing "*", so a longer matching rule is the more specific one.
func matchedRuleLen(path, rule string) int {
	rule = strings.TrimSpace(rule)
	if rule == "" {
		return -1
	}
	if strings.HasSuffix(rule, "*") {
		rule = strings.TrimSuffix(rule, "*")
	}
	if !strings.HasPrefix(path, rule) {
		return -1
	}
	return len(rule)
}
