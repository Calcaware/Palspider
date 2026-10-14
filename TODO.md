# Palspider improvement plan

This document is a prioritized plan to address the main risks, correctness gaps, and feature opportunities identified in the current codebase.

Status note: the high-priority production-safety work in sections 1.1 through 1.4 is now implemented in the codebase and reflected above as complete. The remaining unchecked items in sections 2 and 3 are feature-roadmap or product-quality improvements intended for future milestones rather than required correctness fixes.

## 1. High-priority fixes

### 1.1 Improve crawl resilience and retry policy

Goal: reduce false negatives caused by transient failures, and make crawl decisions more accurate.

Tasks:
- [x] Add a distinct retry state for failed URLs instead of treating all dequeued URLs as permanently visited.
- [x] Track retry counts and backoff timing per URL.
- [x] Requeue transient failures through a lightweight retry window so failed pages can be revisited without losing crawl progress.
- [x] Limit retries to a bounded retry window instead of leaving failed pages permanently blacklisted.
- [x] Add tests covering temporary retries and queue re-entry behavior.

Why:
- Right now, the crawler marks a URL as visited when it is dequeued, even before the fetch succeeds. That makes the system efficient, but it reduces recoverability when a page fails for temporary reasons.

### 1.2 Add crawl policy controls

Goal: make crawl behavior safer and more configurable for production use.

Tasks:
- [x] Add allowlist and denylist support for domain patterns and URL prefixes.
- [x] Add robots.txt checks and explicit policy handling for disallowed paths.
- [x] Support crawl exclusions for noisy or irrelevant routes such as `/search`, `/login`, large query-string-heavy URLs, and tracking URLs.
- [x] Add configuration for max pages per host and max links per page.
- [x] Add a “stay in scope” policy for seeds and discovered links.
- [x] Document default behavior and override behavior in the README and API docs.

Why:
- Without crawl policy controls, Palspider is excellent for indexing general content, but not yet ideal for constrained or production-safe web crawling.

### 1.3 Harden the peer mesh

Goal: reduce trust assumptions and improve reliability of distributed search.

Tasks:
- [x] Add explicit peer trust configuration (allowlist or default-off federation mode).
- [x] Support TLS-enabled peers and validate certificates properly.
- [x] Add peer authentication or shared-secret support for gossip and search fan-out.
- [x] Prevent peer loops and gossip storms by tracking seen gossip rounds and peer metadata expiration.
- [x] Add a timeout and retry budget for gossip exchanges separate from search fan-out.
- [x] Add a peer health dashboard showing connected, unreachable, stale, and denied peers.

Why:
- The current implementation trusts peers too much and validates only a minimal app identifier. This is fine for a local mesh but risky for broader deployment.

### 1.4 Improve operational observability

Goal: make the system diagnosable in production.

Tasks:
- [x] Add per-host crawl stats: requests, successes, failures, rate-limit waits, last-fetch time, and blocked URLs.
- [x] Add crawl error counters split by HTTP code and network failure category.
- [x] Add endpoint-level metrics for search latency, peer latency, and queue depth.
- [x] Add structured logs for blocked addresses, invalid URLs, and peer failures.
- [x] Add a metrics export or admin endpoint for Prometheus-style scraping or JSON dashboards.
- [x] Add a simple health dashboard view in the web UI with live crawl diagnostics.

Why:
- The project is nicely structured, but production operators need more than queue counts to debug crawl health and performance.

## 2. Medium-priority correctness and UX improvements

### 2.1 Improve result quality and ranking

Tasks:
- [x] Add ranking boosts for title matches and host/domain authority.
- [x] Add support for phrase queries and multi-term proximity weighting.
- [x] Add better snippet selection and highlight logic for query terms.
- [x] Add “related results” or “similar page” exploration.
- [x] Add result filters by host and path when available.

### 2.2 Improve HTML and content extraction

Tasks:
- [x] Handle more broken HTML patterns and malformed documents more gracefully.
- [x] Detect and strip more non-content sections consistently.
- [x] Support PDF/doc extraction optionally for non-HTML sites.
- [ ] Add better handling for dynamic pages and JS-rendered content, if the crawler should support those in the future.

### 2.3 Improve API ergonomics

Tasks:
- [x] Add explicit validation for malformed query values across all handlers.
- [x] Support JSON body validation with clearer error messages for all POST/DELETE routes.
- [x] Add pagination metadata to API responses where relevant.
- [x] Add API versioning strategy or compatibility notes for future changes.
- [x] Add rate limiting and abuse protections for public endpoints.

## 3. Feature roadmap

### 3.1 Search features

- [x] Add result highlighting in the web UI.
- [x] Add host filters and path filters.
- [x] Add sort order options (relevance, newest, oldest, most linked).
- [ ] Add search suggestions that include multi-word completions and domain hints.
- [ ] Add saved search history or bookmarkable searches.

### 3.2 Crawl management features

- [ ] Add a crawl queue inspector.
- [ ] Add pause/resume/restart controls for the crawler.
- [ ] Add per-domain crawl budgets and priority scheduling.
- [ ] Add domain-level status and queue backlog display.
- [ ] Add export of crawl progress and seeded URLs.

### 3.3 P2P and federation features

- [ ] Add peer categories: trusted, private, external.
- [ ] Add a peer score and last-seen health model.
- [ ] Add peer-to-peer search result provenance display in the UI.
- [ ] Add a peer discovery mechanism beyond manual seeding and gossip.
- [ ] Add optional TLS and mutual-auth support for peer traffic.

### 3.4 Data and admin features

- [ ] Add sitemap generation.
- [x] Add JSON export of indexed documents or top results.
- [ ] Add streaming NDJSON export (with `format=ndjson`) so large corpora can be exported without buffering the whole page.
- [ ] Add the ability to delete or reindex specific domains.
- [ ] Add crawl snapshots and resume support after crashes.
- [ ] Add a database integrity check and maintenance command.

## 4. Validation strategy

Tasks:
- [ ] Add regression tests for every bug fix and behavior change.
- [ ] Add concurrency tests for retry queues, peer fan-out, and queue state transitions.
- [ ] Validate behavior under delayed peers, unreachable peers, and transient HTTP failures.
- [ ] Run the full Go test suite after each milestone and keep the repository green.
- [ ] Add end-to-end API tests for crawl, search, suggest, peer add/remove, and auth flows.

## 5. Recommended implementation order

### Phase 1: Correctness and safety
1. Fix retry/visited lifecycle for crawled URLs.
2. Add crawl policy controls and robots handling.
3. Add peer trust and auth protections.
4. Add tests for transient failures and peer behavior.

### Phase 2: Observability and operations
5. Add metrics and structured logging.
6. Add crawl diagnostics and health dashboard.
7. Add domain and queue management controls.

### Phase 3: Product-level improvements
8. Improve ranking and snippet quality.
9. Add result filters and advanced search UX.
10. Add admin/export features and federation improvements.

## 6. Suggested success criteria

The project should be considered improved when:
- transient crawl failures no longer cause permanent loss of crawl opportunity,
- the peer mesh can be deployed safely in trusted or private environments,
- operators can diagnose crawl health without reading logs manually,
- the search experience feels better than a raw TF-IDF list,
- the API is predictable, documented, and reliable under a variety of inputs,
- the project is easy to run and maintain as a self-hosted service.

## 7. Notes

This roadmap intentionally prioritizes the risks that affect correctness and trust before feature expansion. The biggest near-term wins will come from improving crawl retry semantics, adding domain policy controls, and hardening the peer mesh.
