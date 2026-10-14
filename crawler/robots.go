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
	for _, deny := range policy.deny {
		if matchesPathRule(path, deny) {
			return false
		}
	}
	for _, allow := range policy.allow {
		if matchesPathRule(path, allow) {
			return true
		}
	}
	return true
}

func matchesPathRule(path, rule string) bool {
	rule = strings.TrimSpace(rule)
	if rule == "" {
		return false
	}
	if rule == "/" {
		return path == "/"
	}
	if strings.HasSuffix(rule, "*") {
		rule = strings.TrimSuffix(rule, "*")
		return strings.HasPrefix(path, rule)
	}
	return path == rule || strings.HasPrefix(path, rule)
}
