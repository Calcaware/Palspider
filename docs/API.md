# Palspider HTTP API

Palspider exposes a small, scriptable JSON API alongside its web UI. Every
response is `application/json` with a `X-Content-Type-Options: nosniff`
header. Error responses use the shape `{"error":"<message>"}` with a non-2xx
status code.

This document describes version `0.2.0` of the API. Version is also reported
at runtime by `GET /api/health` and `GET /api/status`.

## Conventions

- Base URL: `http://<host>:<port>` (default port `3000`).
- All JSON request bodies must be no larger than 1 MB. Oversized or malformed
  bodies are rejected with `400`.
- Query parameters are read via `r.URL.Query()`; repeated parameters use the
  first value.
- Read-only endpoints (`GET`/`HEAD`) send `Access-Control-Allow-Origin: *` so
  browsers on other origins can call them. Mutating endpoints (`POST`,
  `DELETE`) never send a CORS header.
- When `PALSPIDER_AUTH=user:pass` is set, `POST` and `DELETE` requests must
  carry matching HTTP Basic credentials (`Authorization: Basic ...`). Failing
  that, the server responds `401` with a `WWW-Authenticate: Basic
  realm="palspider"` header. `GET` and `HEAD` requests stay open so the web
  UI, the peer mesh, and health checks continue to work. See
  "Authentication" below.

### Common status codes

| Code | Meaning |
|---|---|
| `200` | Success. |
| `202` | Accepted (used by `POST /api/crawl`). |
| `400` | Bad request: missing/invalid parameter or body. |
| `401` | Missing or wrong credentials (only when `PALSPIDER_AUTH` is set). |
| `404` | Unknown `/api/*` path. |
| `405` | Method not allowed for a registered path. |

## Endpoints

### `GET /api/health`

Liveness probe. Useful for orchestrators, Docker health checks, and the
peer-validation handshake.

```json
{
  "status": "ok",
  "app": "palspider",
  "version": "0.2.0"
}
```

### `GET /api/status`

Engine statistics and crawl state.

No parameters.

```json
{
  "app": "palspider",
  "version": "0.2.0",
  "documents": 1204,
  "terms": 18450,
  "visited": 1289,
  "queueSize": 0,
  "active": 0,
  "peers": 2
}
```

Field meanings:

| Field | Type | Meaning |
|---|---|---|
| `documents` | int | Number of indexed documents. |
| `terms` | int | Number of distinct index terms. |
| `visited` | int | URLs discovered so far (pending items move to visited when dequeued). |
| `queueSize` | int | URLs currently waiting to be crawled. |
| `active` | int | Workers currently processing a URL. |
| `peers` | int | Number of registered peers. |

### `GET /api/search`

Full-text search over the inverted index. The response is always a JSON
array, even when nothing matched (an empty array `[]`).

Query parameters:

| Parameter | Default | Range | Meaning |
|---|---|---|---|
| `q` | (required) | - | Query terms. Leading/trailing whitespace is trimmed. Empty or missing `q` is a `400` error. |
| `limit` | `10` | `1` to `100` | Maximum number of results to return. Values above `100` are clamped to `100`; non-numeric or less than `1` are `400` errors. |
| `offset` | `0` | `0` to `99` | Number of results to skip before returning. The searchable window is capped so that `offset + limit` never exceeds `100` (the limit is reduced if needed). `offset >= 100` is a `400` error. |
| `remote` | `0` | `0`, `1`, `true` | When `1` or `true`, results are also fetched from every registered peer (see peer fan-out). Defaults to local search only. |

Example:

```sh
curl 'http://localhost:3000/api/search?q=node&limit=5&offset=0'
```

```json
[
  {
    "id": 1,
    "url": "https://nodejs.org",
    "title": "Node.js",
    "text": "Node.js is a JavaScript runtime built on V8...",
    "score": 0.87,
    "linkCount": 14
  },
  {
    "id": 42,
    "url": "https://expressjs.com",
    "title": "Express.js",
    "text": "Express is a minimal and flexible Node.js web application framework...",
    "score": 0.31,
    "linkCount": 6,
    "peerSource": "http://192.168.1.5:3000"
  }
]
```

Result object fields:

| Field | Type | Meaning |
|---|---|---|
| `id` | int | Local document ID. |
| `url` | string | Canonical page URL. |
| `title` | string | Page title (case preserved). |
| `text` | string | Extracted page text. |
| `score` | float | TF-IDF link-weighted score, rounded to two decimals. |
| `linkCount` | int | Number of inbound links discovered so far. |
| `peerSource` | string | Present only for results fetched from a peer (`remote=1`); the peer's URL. |

### `GET /api/crawl` and `GET /crawl-status`

Crawl queue snapshot. `GET /api/crawl` is an alias for `GET /crawl-status`.

```json
{
  "queueSize": 3,
  "active": 2,
  "visited": 120,
  "documents": 105
}
```

This is what the web UI polls every three seconds.

### `POST /api/crawl`

Seed a URL into the crawl frontier. Queues asynchronously; the crawler
workers pick it up on their own schedule.

Request body:

```json
{"url": "https://example.org/docs"}
```

The URL must be absolute with an `http` or `https` scheme and a non-empty
host. It is normalized (scheme and host lowercased, default ports stripped,
trailing slash removed, fragment dropped) before being queued.

Success (`202 Accepted`):

```json
{
  "status": "accepted",
  "url": "https://example.org/docs"
}
```

`url` is the normalized form. Errors: `400` with a JSON error object when the
body is not valid JSON or the URL is not acceptable.

Note: like the web form, this endpoint respects the crawler's private-address
guard. Seeding a loopback or link-local URL succeeds (it is queued) but the
worker logs `blocked address` and skips it unless `PALSPIDER_ALLOW_PRIVATE=1`.

### `GET /api/suggest` and `GET /suggest`

Term autocomplete. Returns JSON array of suggestion strings ranked by
document frequency, case-insensitive.

Query parameters:

| Parameter | Default | Range | Meaning |
|---|---|---|---|
| `q` | (required) | - | Term prefix. Missing or empty `q` returns `[]` (not an error). |
| `limit` | `5` | `1` to `20` | Maximum suggestions. Non-numeric or less than `1` is a `400` error; values above `20` are clamped. |

Example:

```sh
curl 'http://localhost:3000/api/suggest?q=nod&limit=3'
```

```json
["node", "nodejs", "nonblocking"]
```

### `GET /api/peers`

List registered peers.

```json
[
  {
    "url": "http://192.168.1.5:3000",
    "status": "connected",
    "lastSeen": "2026-10-06T08:12:05Z"
  }
]
```

Field meanings:

| Field | Type | Meaning |
|---|---|---|
| `url` | string | Peer's base URL (`scheme://host[:port]`). |
| `status` | string | `connected` after a successful exchange, `unreachable` after a failure. |
| `lastSeen` | string | RFC 3339 timestamp of the last exchange. May be omitted. |

### `POST /api/peers`

Validate and register a peer. The candidate is verified by fetching
`<peer>/api/status` and requiring an HTTP 200 with `"app":"palspider"` in the
body. Peer URLs must use `http` or `https`.

Request body:

```json
{"url": "http://192.168.1.5:3000"}
```

Success (`200`):

```json
{"status": "ok"}
```

Errors: `400` with a JSON error object for an empty URL, a non-http(s)
scheme, or a peer that fails validation (including unreachable candidates).

Adding a peer triggers an asynchronous gossip exchange with it. See
"Peer mesh" in the README.

### `DELETE /api/peers`

Remove a peer. Matching uses the same normalization as `POST /api/peers`
(`scheme://host[:port]`), so trailing paths and trailing slashes are ignored.

Request body:

```json
{"url": "http://192.168.1.5:3000"}
```

Success (`200`):

```json
{"status": "ok", "removed": true}
```

`removed` is `true` when a peer was actually removed, `false` when it was not
registered. An empty URL is a `400` error.

### `GET /api/peers/gossip`

Returns the peer list (same shape as `GET /api/peers`). Used by the gossip
protocol so one peer's known mesh can be shared with the others. It is a read
endpoint and is intentionally open even when basic auth is configured.

## Authentication

Optional. Set the environment variable before starting Palspider:

```sh
PALSPIDER_AUTH=alice:s3cret ./palspider
```

- `GET` and `HEAD` requests are never challenged, so the web UI, health
  checks, and the peer mesh keep working without credentials.
- `POST` and `DELETE` requests (the only endpoints that change state) must
  include correct Basic credentials, otherwise `401` with a
  `WWW-Authenticate: Basic realm="palspider"` header.

Example:

```sh
curl -u alice:s3cret -X POST http://localhost:3000/api/peers \
  -d '{"url":"http://192.168.1.6:3000"}'
```

Rationale: this is a hardening layer against drive-by CSRF-style mutations
from other sites. It is not TLS; run Palspider behind HTTPS (reverse proxy or
SSH tunnel) if credentials travel over the network.

## Cross-origin access (CORS)

Read-only JSON endpoints set `Access-Control-Allow-Origin: *`:

- All `GET` and `HEAD` `/api/*` routes
- `GET /suggest`
- `GET /crawl-status`

Mutating requests (`POST`/`DELETE`) never send a CORS header, and no
`Access-Control-Allow-Credentials`, methods, or headers are advertised. This
keeps cross-origin callers to reads only, matching the CSRF posture above.

## Errors

Every error response is JSON with exactly one field:

```json
{"error": "query required"}
```

Messages are stable enough to match on, but treat them as human-readable. The
status code is the contract; the message may change.

## Versioning

The API is versioned by its own `version` field in `/api/health` and
`/api/status` rather than by URL prefix. Breaking changes will bump the
version reported there. New additive fields and endpoints do not.

## Peer mesh integration

Two endpoints exist specifically to let Palspider instances interoperate:

- `GET /api/status` - used by `POST /api/peers` to validate a candidate
  (`"app":"palspider"` check).
- `GET /api/peers/gossip` - returns the known mesh for propagation.
- `GET /api/search` - used by fan-out search. Peers call it without `remote=1`
  so a search request never loops back through the mesh.

Any Palspider node can therefore be both a web UI and a fan-out target for
other nodes.