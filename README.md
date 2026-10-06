# Palspider

A P2P crawl-and-search engine. No AI overviews. Just links weighted by people.

![Palspider](public/palspider.png)

Palspider is a self-hosted web crawler, indexer, and search engine written in
Go with no runtime dependencies outside of bbolt. Seed a URL and a worker pool
crawls outward from it, builds a TF-IDF index weighted by in-degree links, and
serves results over a dark-themed web UI. Multiple Palspider instances can be
joined into a peer mesh: queries fan out to every peer, results are merged and
deduplicated by URL, and peer lists gossip automatically.

## Features

- **Crawling** - 50 concurrent workers (configurable), cross-domain, 200 ms
  per-host rate limit, 10 s fetch timeout, 5 MB page cap, one automatic retry
  on network errors, content fingerprints to skip duplicate pages. Crawl depth
  and total page count are capped.
- **Safety** - the crawler refuses loopback, link-local, and private addresses
  by default so a malicious seed URL cannot probe your LAN or cloud metadata
  endpoint. Set `PALSPIDER_ALLOW_PRIVATE=1` to crawl local hosts on purpose.
- **Indexing** - in-memory inverted index (term -> doc ID -> term frequency)
  plus a URL trie scoped by host, used for URL clustering.
- **Ranking** - TF-IDF scoring with in-degree link weighting, URL-depth bonus,
  and sibling-cluster boost. No models, no embeddings.
- **Autocomplete** - term suggestions ranked by document frequency,
  case-insensitive, debounced client-side.
- **P2P mesh** - add peer nodes from the Settings modal; queries fan out
  concurrently with a hard 2 s time budget, results merge deduplicated by URL,
  peer lists gossip on contact, and peers track last-seen status.
- **Persistence** - index, visited URLs, fingerprints, and link counts are
  stored in a single bbolt database and restored on boot. Each crawled page
  persists in one transaction.
- **Web UI** - Bootstrap 5 dark/light theme, live crawl stats, seed-URL
  dialog, peer management, paginated results (10 per page, 100 max), 404 page.
  Templates and assets are embedded in the binary (`embed.FS`).
- **JSON API** - health, status, search (with offset paging), crawl, suggest,
  peers, and gossip endpoints for scripting. Read-only endpoints are
  cross-origin readable; mutations are protected by optional basic auth.
  Full reference: [docs/API.md](docs/API.md).
- **Optional auth** - set `PALSPIDER_AUTH=user:pass` and `POST`/`DELETE`
  endpoints require matching Basic credentials while `GET` reads stay open, so
  the UI, the peer mesh, and health checks keep working.
- **Tests** - unit tests for every package, including a concurrency test for
  the trie that reproduces the old crash.

## Quick start

Requirements: Go 1.26 or newer.

```sh
go build -o palspider .
./palspider
```

Then open http://localhost:3000. On first run (empty database) seven seed
documents are inserted so search works immediately. Seed your own URL with the
+ button (or `POST /crawl`) to start crawling.

Run from source without building:

```sh
go run .
```

## Configuration

Everything is configured through environment variables:

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP listen port. |
| `PALSPIDER_WORKERS` | `50` | Number of crawler worker goroutines. |
| `PALSPIDER_MAX_DEPTH` | `10` | Link hops from a seed. `-1` means unlimited. |
| `PALSPIDER_MAX_PAGES` | `0` | Stop queueing new pages after this many documents. `0` means unlimited. |
| `PALSPIDER_DB` | `palspider.db` | Path to the bbolt database file. |
| `PALSPIDER_ALLOW_PRIVATE` | unset | Set to `1` to allow crawling loopback and private-network hosts. |
| `PALSPIDER_AUTH` | unset | `user:pass` - require Basic credentials on `POST`/`DELETE` API routes. |
| `INQUEST_WORKERS`, `INQUEST_DB` | - | Old names, still accepted as a fallback. |

## How it works

### Architecture

```
                        +--------------------------------------------+
  Browser  ------------>|  main.go  (ServeMux, embedded views)       |
  Peer     ------------>|   /  /search  /crawl  /suggest  /api/*     |
                        +-------------------+------------------------+
                                            |
                        +-------------------v------------------------+
                        |  engine.Engine - orchestration              |
                        |   urlChan (100k buffer) --> N workers       |
                        |   visited / pending / fpSeen dedupe sets    |
                        +---+--------------------+-------------------+
                            |                    |
               +------------v---+       +--------v----------+
               | crawler        |       | indexer           |
               | fetch, rate    |       | inverted index,   |
               | limit, tokenize|       | docs, link counts |
               | extract, dedupe|       +--------+----------+
               +----------------+                |
                        +------------------------v-------------------+
                        | store.Store (bbolt)      trie.Trie (URLs)   |
                        +--------------------+-----------------------+
                                             |
                        +--------------------v-----------------------+
                        | peer.PeerManager - validate, gossip,        |
                        | concurrent fan-out search, merge/dedupe     |
                        +---------------------------------------------+
```

### Package layout

| Path | Responsibility |
|---|---|
| `main.go` | Entrypoint: HTTP routes, HTML and JSON handlers, template parsing, seed data, graceful shutdown. |
| `crawler/` | `Fetch` (per-host rate limiter, private-address guard, retry, content-type and size checks), `ExtractText` (case-preserving tag stripping), `ExtractLinks` (quoted and unquoted hrefs, script-safe), `NormalizeURL`, `Tokenize`, `ContentFingerprint`. |
| `engine/` | Worker pool, URL queue with depth tracking, dedupe sets, persistence, local plus distributed search, stats. |
| `indexer/` | `Indexer` (documents, inverted index, URL lookup) and `LinkStore` (in-degree counts per URL). |
| `trie/` | Path-segment trie keyed by host: subtree doc IDs (cluster boost) and depth. |
| `searcher/` | TF-IDF ranking with boosts; prefix autocomplete. |
| `store/` | Thin bbolt wrapper: documents, postings, link counts, visited, fingerprints, metadata, single-transaction batch writes. |
| `peer/` | Peer registry, status validation, one-shot gossip with in-flight guard, concurrent fan-out search, last-seen tracking. |
| `views/` | Go HTML templates (`search.html`, `results.html`). |
| `public/` | `style.css`, logo. Embedded via `//go:embed`. |

### Crawl pipeline

1. **Seed** - `POST /crawl` or the built-in seed list normalizes the URL, marks
   it visited, and pushes it onto the queue at depth 0.
2. **Fetch** - a worker pulls the URL, waits on the per-host rate limiter, and
   downloads up to 5 MB of `text/html`. Network errors are retried once;
   HTTP errors are logged and skipped.
3. **Dedupe** - the page text is fingerprinted (MD5 of the first 500 normalized
   characters); known fingerprints and already-indexed URLs are skipped.
4. **Index** - text is tokenized (lowercased, `a-z0-9-` plus Unicode letters,
   two or more runes), posted to the inverted index, inserted into the URL
   trie, then the document, its postings, and the page's link counts are
   written in one database transaction.
5. **Discover** - every `<a href>` on the page (script blocks stripped) is
   resolved, normalized, counted as an in-degree link, and enqueued if it is
   not already visited or pending and is within the depth and page limits.
6. **Accounting** - when a worker dequeues a URL it moves from pending to
   visited, so the "discovered" counter reflects real crawl progress.

### Ranking

For each query, every matching term contributes:

```
tf-idf     = (1 + ln(term frequency)) * (1 + ln(N / document frequency))
linkBoost  = 1 + 0.5 * ln(1 + inbound link count)
depthBonus = 1 + 1 / (1 + URL path depth)
cluster    = 1 + 0.2 * ln(1 + matched sibling pages on the same host and path)

score = sum over query terms of (tf-idf * linkBoost * depthBonus * cluster)
```

Results are sorted by descending score, rounded to two decimals.

### Storage schema (bbolt)

| Bucket | Key | Value |
|---|---|---|
| `docs` | doc ID (8-byte big endian) | JSON document `{ID, URL, Title, Text, LinkCount}` |
| `index` | term | JSON `map[docID]termFrequency` |
| `links` | URL | 8-byte big endian inbound-link count |
| `visited` | URL | `0x01` |
| `fingerprints` | MD5 hex | `0x01` |
| `meta` | `nextDocID` | 8-byte big endian next document ID |

Everything is loaded back into memory on startup (`Engine.Restore`).

### P2P mesh

- `POST /api/peers` validates a candidate by fetching `<peer>/api/status` and
  checking `"app":"palspider"` before registering it.
- Adding a peer triggers a one-shot gossip request to it; peers it knows are
  added and validated in turn. Only one gossip exchange per peer runs at a
  time.
- `GET /search` fans out to all peers concurrently (1.5 s per peer, 2 s total
  budget), tags results with `peerSource`, and merges local and remote results
  deduplicated by URL. The local copy wins on duplicates.
- Peer status and last-seen timestamps are updated on every exchange and are
  returned by `GET /api/peers`.
- Peers are in-memory only; they are not persisted to the database.

## HTTP API

Full reference with request/response examples: [docs/API.md](docs/API.md).

### HTML routes

| Method and path | Description |
|---|---|
| `GET /` | Homepage: search box, live stats, modals (about, settings, seed). |
| `GET /search?q=...&page=N` | Results page, 10 per page up to page 10. Redirects to `/` if `q` is empty. |
| `POST /crawl` | Form field `url` - seed a URL, then redirect to `/?crawl=started` (or `?crawl=invalid`). |
| `GET /crawl-status` | JSON crawl status (the UI polls this every 3 seconds). |
| `GET /suggest?q=...&limit=N` | JSON array of term suggestions (default 5, max 20). |
| anything else | Plain 404 page. |

### JSON API

| Method and path | Description |
|---|---|
| `GET /api/health` | Liveness probe: `{"status":"ok","app":"palspider","version":...}` |
| `GET /api/status` | `{"app","version","documents","terms","visited","queueSize","active","peers"}` |
| `GET /api/search?q=...&limit=10&offset=0&remote=0` | Search results. `limit` clamps to [1, 100], `offset` must be < 100, `remote=1` fans out to peers. Always returns an array. |
| `GET /api/crawl` | Alias of `GET /crawl-status`. |
| `POST /api/crawl` | `{"url":"https://..."}` - seed a URL, returns `202` with the normalized URL. |
| `GET /api/suggest?q=...&limit=5` | Alias of `GET /suggest`. |
| `GET /api/peers` | List registered peers with status and last-seen. |
| `POST /api/peers` | `{"url":"http://host:3000"}` - validate and add a peer. |
| `DELETE /api/peers` | `{"url":"..."}` - remove a peer; returns `{"removed":true|false}`. |
| `GET /api/peers/gossip` | Peer list for gossip propagation. |

Authentication: when `PALSPIDER_AUTH` is set, every `POST` and `DELETE`
endpoint above requires matching Basic credentials (401 with a
`WWW-Authenticate` header otherwise); `GET` endpoints stay open. Every error
is `{"error":"..."}` with an appropriate status code.

Static assets: `GET /style.css` and `GET /palspider.png`.

Examples:

```sh
curl 'http://localhost:3000/api/search?q=node&limit=5&offset=0'
curl 'http://localhost:3000/api/health'
curl -X POST http://localhost:3000/api/crawl -d '{"url":"https://example.org"}'
curl -X POST http://localhost:3000/api/peers -d '{"url":"http://192.168.1.5:3000"}'
```

## Development

```sh
go build ./...      # compile everything
go vet ./...        # static analysis
go test ./...       # run the test suite
go test -race ./... # same, with the race detector (needs CGO / gcc)
```

The test suite covers URL normalization, text and link extraction, tokenizing,
rate limiting, the private-address guard, trie concurrency, index bookkeeping,
ranking and suggestions, batch persistence, result merging, and peer URL
handling.

## Known issues

Things that are still wrong or missing after the current round of fixes:

1. **No robots.txt support.** The crawler does not read `robots.txt` or
   sitemaps, and there is no per-host politeness delay beyond the fixed
   200 ms limiter.
2. **Everything lives in RAM.** All documents and postings are held in memory,
   so a big crawl grows without bound. `PALSPIDER_MAX_PAGES` is the only
   brake.
3. **Postings are rewritten in full.** Each new document rewrites the entire
   postings list for every one of its terms. Writes are batched into one
   transaction per page now, but the payload still grows with the corpus.
4. **Auth covers state changes, not reads.** `PALSPIDER_AUTH` protects the
   mutating endpoints; the searchable index itself is open to anyone who can
   reach the port. This is by design (peers and the UI need unauthenticated
   reads) but means sensitive crawls are visible to LAN users.
5. **Gossip is O(n^2) in the worst case.** Each newly learned peer costs one
   validation request, and knowledge transitive floods through the mesh. The
   in-flight guard prevents duplicates, not the total volume.
6. **Autocomplete scans the whole vocabulary** on every keystroke (no term
   trie yet). The URL trie is a different structure.
7. **Dead peers are marked but never removed.** A search may still spend up to
   its 2 s budget waiting on an unreachable peer before falling back to local
   results.
8. **The crawl frontier is not persisted.** Visited URLs survive restarts, but
   links that were queued and not yet processed are rediscovered on the next
   crawl of their parent.
9. **No charset handling.** Pages that are not UTF-8 will be indexed with
   mangled bytes.
10. **Link discovery can drop URLs under load.** The frontier is a fixed-size
    buffer; when it fills, newly discovered links are not queued (the crawl
    never blocks, but those URLs are lost until their parent is crawled
    again). The in-memory pending set is rolled back in step so the UI counters
    stay consistent.
11. **`/favicon.ico` returns 404.** The logo is served at `/palspider.png` and
    linked with `<link rel="icon">`; requests to the classic path are not
    redirected.
12. **Fingerprints can false-positive.** Dedupe hashes only the first 500
    characters of page text, so paginated pages with identical openings may
    be skipped.
13. **Peer scores are merged raw.** TF-IDF depends on each peer's corpus size,
    so scores from different nodes are not directly comparable.
14. **HTML entities in titles are not decoded** (a title containing `&amp;`
    will display literally), and link extraction still picks up anchors
    inside HTML comments.

## Roadmap / possible features

- robots.txt and sitemap support, per-host politeness queues, crawl logs with
  fetch failure reasons.
- A PageRank pass over the collected link graph to complement TF-IDF.
- Phrase queries ("..."), exclusion (-term), stemming, query-term highlighting
  in snippets, and `site:` style operators.
- A dedicated prefix trie for autocomplete instead of the linear scan.
- Asynchronous peer fan-out that streams results as they arrive and gives up
  cleanly when the budget expires.
- Peer health checks with automatic eviction, plus a persisted peer list.
- Persisting the frontier so restarts resume instead of rediscovering.
- An admin dashboard (queue depth, worker activity, recent failures, index
  size, peer health), Prometheus metrics, structured logging.
- Charset detection, gzip responses, cache headers.
- Dockerfile and a docker compose setup for a two or three node mesh demo.
- Index export, backup, merge, and reindex commands.

## License

[GPL-3.0](LICENSE)
