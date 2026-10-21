package crawler

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var robotPolicyMu sync.Mutex
var robotPolicies = map[string]robotPolicyCache{}

const robotPolicyTTL = 30 * time.Minute

// robotsAgentToken is the product token the crawler identifies itself with.
// A robots.txt group that names it applies to the crawler, and the wildcard
// group applies only when no group names it.
const robotsAgentToken = "palspider"

// maxCrawlDelay caps how long a site's Crawl-delay may slow the crawler
// down, so one host cannot stall a crawl for hours.
const maxCrawlDelay = 5 * time.Minute

type robotPolicyCache struct {
	fetched    time.Time
	allow      []string
	deny       []string
	crawlDelay time.Duration
}

type robotsPolicy struct {
	allow      []string
	deny       []string
	crawlDelay time.Duration
}

// robotsGroup is one user-agent group from a robots.txt file: the agents it
// names plus the rules that apply to them.
type robotsGroup struct {
	agents     []string
	allow      []string
	deny       []string
	crawlDelay time.Duration
	hasDelay   bool
	hasRules   bool
}

func robotsEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("PALSPIDER_ALLOW_ROBOTS")))
	if v == "" {
		return true
	}
	return v != "0" && v != "false" && v != "no"
}

// parseRobotsGroups splits a robots.txt body into its user-agent groups. A
// user-agent line starts a new group, unless the group being read has not
// seen a rule yet, in which case the agents share one group.
func parseRobotsGroups(body string) []robotsGroup {
	var groups []robotsGroup
	var current *robotsGroup

	flush := func() {
		if current != nil {
			groups = append(groups, *current)
			current = nil
		}
	}

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
			if current != nil && current.hasRules {
				flush()
			}
			if current == nil {
				current = &robotsGroup{}
			}
			current.agents = append(current.agents, strings.ToLower(value))
			continue
		}
		if current == nil {
			// A rule before any user-agent line belongs to no group.
			continue
		}
		switch key {
		case "allow":
			current.allow = append(current.allow, value)
			current.hasRules = true
		case "disallow":
			current.deny = append(current.deny, value)
			current.hasRules = true
		case "crawl-delay":
			if !current.hasDelay {
				if delay, ok := parseCrawlDelay(value); ok {
					current.crawlDelay = delay
					current.hasDelay = true
				}
			}
		}
	}
	flush()
	return groups
}

// parseCrawlDelay reads a Crawl-delay value, which is a number of seconds.
// Fractional values are accepted. A value that is not a number or is not
// positive is ignored, and long waits are capped so a single site cannot
// stall the crawl.
func parseCrawlDelay(value string) (time.Duration, bool) {
	seconds, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || seconds <= 0 {
		return 0, false
	}
	delay := time.Duration(seconds * float64(time.Second))
	if delay > maxCrawlDelay {
		delay = maxCrawlDelay
	}
	return delay, true
}

// groupNames reports whether the group applies to the given agent token.
func groupNames(group robotsGroup, agent string) bool {
	for _, a := range group.agents {
		if a == agent {
			return true
		}
	}
	return false
}

// parseRobotsPolicy returns the rules that apply to the crawler, or nil when
// the file says nothing about it. A group that names the crawler wins over
// the wildcard group, and the rules of every group with the same agent are
// combined, as the robots exclusion protocol expects.
func parseRobotsPolicy(body string) *robotsPolicy {
	groups := parseRobotsGroups(body)

	matching := make([]robotsGroup, 0, len(groups))
	for _, group := range groups {
		if groupNames(group, robotsAgentToken) {
			matching = append(matching, group)
		}
	}
	if len(matching) == 0 {
		for _, group := range groups {
			if groupNames(group, "*") {
				matching = append(matching, group)
			}
		}
	}
	if len(matching) == 0 {
		return nil
	}

	policy := &robotsPolicy{}
	for _, group := range matching {
		policy.allow = append(policy.allow, group.allow...)
		policy.deny = append(policy.deny, group.deny...)
		if group.hasDelay && policy.crawlDelay == 0 {
			policy.crawlDelay = group.crawlDelay
		}
	}
	if len(policy.allow) == 0 && len(policy.deny) == 0 && policy.crawlDelay == 0 {
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
		return &robotsPolicy{allow: cached.allow, deny: cached.deny, crawlDelay: cached.crawlDelay}, nil
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
	robotPolicies[host] = robotPolicyCache{
		fetched:    time.Now(),
		allow:      policy.allow,
		deny:       policy.deny,
		crawlDelay: policy.crawlDelay,
	}
	robotPolicyMu.Unlock()
	return policy, nil
}

// robotsVerdict returns the robots policy that applies to rawURL and whether
// the path is allowed by it. A missing or unreadable robots.txt allows
// everything, so a broken file never stops a crawl.
func robotsVerdict(rawURL string) (*robotsPolicy, bool) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return nil, true
	}
	policy, err := robotsPolicyForHost(u.Host)
	if err != nil || policy == nil {
		return nil, true
	}
	return policy, robotsAllows(policy, robotsPath(u))
}

// isRobotsAllowed reports whether robots.txt lets the crawler fetch rawURL.
func isRobotsAllowed(rawURL string) bool {
	_, allowed := robotsVerdict(rawURL)
	return allowed
}

// robotsPath returns the path the robots rules are matched against.
func robotsPath(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	return path
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
