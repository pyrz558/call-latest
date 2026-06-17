# call-latest

> **Only the latest async call should win.**

[![npm version](https://img.shields.io/npm/v/call-latest.svg)](https://www.npmjs.com/package/call-latest)
[![bundle size](https://img.shields.io/bundlephobia/minzip/call-latest)](https://bundlephobia.com/package/call-latest)
[![license](https://img.shields.io/npm/l/call-latest.svg)](./LICENSE)
[![tests](https://img.shields.io/badge/tests-passing-brightgreen)](#development)

**Zero dependencies · ~2.5 KB · TypeScript-first · Works everywhere**

`call-latest` now centers around `createSmartSearch` for production search flows. It still exposes low-level helpers (`latest`, `dedupe`, `latestDedupe`) when you want manual control.

```ts
import {
  createFetchSearchAdapter,
  createSmartSearch,
  dispatchCancelSignal,
} from "call-latest";

const runSearch = createFetchSearchAdapter({ endpoint: "/api/search" });

const smart = createSmartSearch(runSearch, {
  enableDelta: true,
  itemId: (x: { id: string }) => x.id,
  onDistributedCancel: (oldCallId) =>
    dispatchCancelSignal("/api/search/cancel", oldCallId),
});

const result = await smart.search("react");
```

---

## The problem

You have seen this bug a hundred times:

1. User types `rea` → fetch starts
2. User types `react` → second fetch starts
3. First response arrives **later** and overwrites fresh results

Debounce delays the request. `useEffect` cleanup is boilerplate. `AbortController` is manual wiring every time.

**`call-latest` drops stale responses automatically.**

---

## Install

```bash
npm install call-latest
```

```bash
yarn add call-latest
```

```bash
pnpm add call-latest
```

```bash
bun add call-latest
```

---

## Usage

### Recommended: `createSmartSearch` controller

Use this as your default integration path. It bundles latest-call safety, aborts,
adaptive debounce, cache, retry, graceful degradation, telemetry, a11y state,
and distributed cancellation hooks.

```ts
import { createSmartSearch, createFetchSearchAdapter } from "call-latest";

const runSearch = createFetchSearchAdapter({ endpoint: "/api/search" });

const smart = createSmartSearch(runSearch, {
  cacheMaxEntries: 50,
  backtrackTtlMs: 300_000,
  swr: true,
  retry: { attempts: 4, baseDelayMs: 250, jitterRatio: 0.3 },
});

const result = await smart.search("iphone");
```

### Low-level APIs

### `latest` — drop stale responses

```ts
import { latest, isStale } from "call-latest";

const search = latest(async (query: string) => {
  const res = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
  return res.json();
});

async function onInput(query: string) {
  try {
    const results = await search(query);
    render(results); // always matches the latest query
  } catch (error) {
    if (isStale(error)) return; // older keystroke — ignore silently
    throw error;
  }
}
```

### With `fetch` abort (recommended)

Cancels in-flight network requests when a newer call starts — saves bandwidth and kills races at the source.

```ts
import { latest, isStale, type LatestContext } from "call-latest";

const search = latest(
  async (query: string, { signal }: LatestContext) => {
    const res = await fetch(`/api/search?q=${query}`, { signal });
    return res.json();
  },
  { abort: true },
);
```

### `dedupe` — one request for identical concurrent calls

```ts
import { dedupe } from "call-latest";

const getUser = dedupe(async (id: string) => {
  const res = await fetch(`/api/users/${id}`);
  return res.json();
});

// Three simultaneous clicks → one network request, three awaiters
await Promise.all([getUser("42"), getUser("42"), getUser("42")]);
```

### `latestDedupe` — the search-box combo

Combines both patterns: identical concurrent queries share one request, and slower older responses are dropped when the query changes.

```ts
import { latestDedupe } from "call-latest";

const search = latestDedupe(async (query: string) => {
  const res = await fetch(`/api/search?q=${query}`);
  return res.json();
});
```

### `createSmartSearch` — all advanced behaviors in one controller

Includes:
- Adaptive debounce (typing speed + network quality)
- Graceful degradation (`normal` / `conserve`)
- Distributed cancellation hook (cancel frame to edge/CDN)
- Backtrack cache
- Delta merge support
- Speculative prefetch
- Local-first short-circuit (WASM / SQLite / DuckDB)
- Optional worker offload hook

```ts
import { createSmartSearch } from "call-latest";

const smart = createSmartSearch(
  async (query, prev, ctx) => {
    const res = await fetch(`/api/search?q=${query}`, { signal: ctx.signal });
    return res.json(); // full response OR delta: { add, removeIds, version }
  },
  {
    metrics: () => ({ rttMs: 220, errorRate: 0.01, status503Rate: 0 }),
    onDistributedCancel: (oldId) => sendCancelFrameToEdge(oldId),
    localSearch: (query) => localWasmIndex.search(query), // return null to fallback network
  },
);

const result = await smart.search("iphone");
```

### Fetch adapter (Cancel endpoint + ETag/Delta merge)

```ts
import {
  createFetchSearchAdapter,
  createSmartSearch,
  dispatchCancelSignal,
} from "call-latest";

const endpoint = "/api/search";
const cancelEndpoint = "/api/search/cancel";

const runSearch = createFetchSearchAdapter({
  endpoint,
});

const smart = createSmartSearch(runSearch, {
  enableDelta: true,
  itemId: (x: { id: string }) => x.id,
  cacheMaxEntries: 50,      // LRU
  backtrackTtlMs: 300_000,  // TTL: 5 min
  swr: true,
  retry: { attempts: 4, baseDelayMs: 250, jitterRatio: 0.3 }, // exp backoff + jitter
  onDistributedCancel: (oldCallId) => {
    dispatchCancelSignal(cancelEndpoint, oldCallId);
  },
  onMetrics: (m) => console.log(m),
  onA11yState: (s) => announceToAriaLive(s.message),
});
```

Server can return either full response:

```json
{ "items": [{ "id": "1", "title": "iphone" }], "version": "etag-v5" }
```

Or delta response:

```json
{ "add": [{ "id": "2", "title": "iphone 16" }], "removeIds": ["9"], "version": "etag-v6" }
```

### Cloudflare Worker Example

The `examples/cloudflare-worker.ts` file in this repository demonstrates two endpoints:

- `GET /api/search` → Search endpoint returning ETag + versioned results
- `POST /api/search/cancel` → Cancellation endpoint triggered by `dispatchCancelSignal`

By binding this to your Worker within `wrangler.toml`, you can run your client-side `call-latest` requests with a true edge-level cancellation + delta architecture.


## API reference

### `latest(fn, options?)`

Wraps an async function. Only the latest call can settle.

| Option | Default | Description |
|--------|---------|-------------|
| `abort` | `false` | Append `{ signal, callId }` as last arg; abort previous work |
| `onStale` | — | Callback when a call is superseded |

**Methods on the wrapped function:**

| Method | Description |
|--------|-------------|
| `reset()` | Invalidate all in-flight calls |
| `current()` | Number of calls made (0 before first call) |

---

### `dedupe(fn, options?)`

Coalesces concurrent calls with the same key into one shared promise.

| Option | Default | Description |
|--------|---------|-------------|
| `key` | `JSON.stringify(args)` | Cache key; return `null` to skip deduplication |

**Methods:** `pending()` · `clear()`

---

### `latestDedupe(fn, options?)`

`latest(dedupe(fn))` — best for autocomplete and search UIs.

---

### `isStale(error)`

Returns `true` if the error means the call was superseded. Safe across bundle boundaries.

```ts
import { isStale, StaleError } from "call-latest";

isStale(new StaleError()); // true
isStale({ code: "STALE" }); // true
```

---

## Framework examples

### React

```tsx
import { useState } from "react";
import { latest, isStale } from "call-latest";

const searchUsers = latest(async (q: string) => {
  const res = await fetch(`/api/users?q=${q}`);
  return res.json();
});

export function UserSearch() {
  const [results, setResults] = useState([]);

  return (
    <input
      placeholder="Search users…"
      onChange={async (e) => {
        try {
          setResults(await searchUsers(e.target.value));
        } catch (err) {
          if (!isStale(err)) throw err;
        }
      }}
    />
  );
}
```

### Vue 3

```ts
import { ref } from "vue";
import { latest, isStale } from "call-latest";

const search = latest(async (q: string) => {
  const res = await fetch(`/api/search?q=${q}`);
  return res.json();
});

const results = ref([]);
const query = ref("");

async function onInput(value: string) {
  query.value = value;
  try {
    results.value = await search(value);
  } catch (err) {
    if (!isStale(err)) throw err;
  }
}
```

### Node.js

```ts
import { latest } from "call-latest";

const loadConfig = latest(async (env: string) => {
  const res = await fetch(`https://config.example.com/${env}`);
  return res.json();
});

// Rapid env switches — only the last config is applied
loadConfig("staging");
loadConfig("production");
```

---

## Why not debounce?

| | debounce | call-latest |
|---|----------|-------------|
| Delays execution | ✅ waits N ms | ❌ runs immediately |
| Drops stale **responses** | ❌ | ✅ |
| Framework-agnostic | awkward | ✅ |
| Cancels `fetch` | manual | built-in (`abort: true`) |
| Size | varies | ~2.5 KB, zero deps |

Debounce **delays when work starts**. `call-latest` lets work run in parallel but **only the latest result counts**. Use both together if you need both behaviors.

---

## When to use

| Scenario | Function |
|----------|----------|
| Search / autocomplete | `latest` or `latestDedupe` |
| Tab or route switching | `latest` with `{ abort: true }` |
| Double-click / spam submit | `dedupe` |
| Typeahead with network dedup | `latestDedupe` |
| Pagination "next page" spam | `latest` |
| Unmount / reset component | `.reset()` |

---

## Development

```bash
git clone https://github.com/YOUR_USERNAME/call-latest.git
cd call-latest
npm install
npm test        # run tests
npm run build   # build dist/
```

---

## Migration guide

If you previously used low-level `latest(...)` directly for search input flows,
prefer moving to `createSmartSearch(...).search(query)` as the primary entrypoint.

```ts
// before
const search = latest(fn);
await search(query);

// after
const smart = createSmartSearch(runSearch, options);
await smart.search(query);
```

Low-level APIs are still available for specialized/custom control paths.

---

## Debugging integration

For quick integration debugging, enable either:

- `debug: true` (logs internal events via `console.debug`)
- `onMetrics: (metric) => { ... }` (structured telemetry stream)

```ts
const smart = createSmartSearch(runSearch, {
  debug: true,
  onMetrics: (m) => console.log("metric", m),
});
```

This helps you see debounce decisions, retries, cache hits/misses, SWR refreshes,
and mode transitions while wiring the feature.

---

## License

[MIT](./LICENSE) © 2026

---

<p align="center">
  <sub>Built for every developer who has debugged a stale fetch at 2 AM.</sub>
</p>
