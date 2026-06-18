import { latest, isStale, StaleError, neverAborts, type LatestContext } from "./_core.js";
export * from "./_core.js";

export type DebounceMode = "fixed" | "adaptive";
export type PerformanceMode = "normal" | "conserve";

export type QueryMetrics = {
  rttMs: number;
  errorRate: number;
  status503Rate: number;
};

export type SmartSearchContext = {
  signal: AbortSignal;
  callId: number;
  mode: PerformanceMode;
  /**
   * Send cancel frame for superseded IDs to edge/CDN.
   * Consumer wires this to transport (HTTP/3, WS, SSE, etc.).
   */
  cancelRemote: (supersededCallId: number) => void;
};

export type SearchResponse<TItem> = {
  items: TItem[];
  /** Optional hash/version for delta protocols. */
  version?: string;
};

export type SearchDelta<TItem> = {
  add?: TItem[];
  removeIds?: Array<string | number>;
  version?: string;
};

type SearchState<TItem> = {
  query: string;
  version?: string;
  items: TItem[];
};

export type SmartSearchOptions<TItem> = {
  /** Minimum debounce window in milliseconds. */
  minDebounceMs?: number;
  /** Maximum debounce window in milliseconds. */
  maxDebounceMs?: number;
  /** Baseline debounce used by fixed mode and adaptive weighting. */
  baseDebounceMs?: number;
  /** Fixed or adaptive debounce strategy. */
  debounceMode?: DebounceMode;
  /**
   * Called on each result to convert item to stable ID for delta/backtrack cache.
   * Required when `enableDelta` is true.
   */
  itemId?: (item: TItem) => string | number;
  /** Enable `{ add, removeIds }` delta merge behavior. */
  enableDelta?: boolean;
  /** Enable in-memory backtrack cache for repeated queries. */
  enableBacktrackCache?: boolean;
  /** Time-to-live for each cached query entry. */
  backtrackTtlMs?: number;
  /** Maximum number of cached queries (LRU eviction). */
  cacheMaxEntries?: number;
  /** Return stale cached result immediately and refresh in background. */
  swr?: boolean;
  /** Called when SWR background refresh returns fresh data. */
  onSWRUpdate?: (query: string, result: SearchResponse<TItem>) => void;
  /** Enable speculative prefetch flow when prediction hooks are provided. */
  enableSpeculativePrefetch?: boolean;
  /**
   * Return likely next queries for speculative warmup (ex: "iph" -> ["iphone"]).
   */
  predictNextQueries?: (query: string) => string[];
  /**
   * Fire-and-forget network/API prefetch. Result may be cached by upstream.
   */
  prefetch?: (query: string) => Promise<void> | void;
  /**
   * Called when a call becomes stale. Useful for distributed cancellation.
   */
  onDistributedCancel?: (supersededCallId: number) => void;
  /**
   * Observe network state for graceful degradation.
   */
  metrics?: () => QueryMetrics;
  /**
   * Called when mode changes due to load/errors.
   */
  onModeChange?: (mode: PerformanceMode) => void;
  /**
   * Receive loading state synchronized with latest call ID.
   */
  onLoadingChange?: (isLoading: boolean) => void;
  /**
   * Optional local-first provider (WASM/SQLite/DuckDB/in-memory index).
   * If returns non-null, network is skipped.
   */
  localSearch?: (query: string) => Promise<SearchResponse<TItem> | null> | SearchResponse<TItem> | null;
  /**
   * Optional worker offload callback. Library sends final query/callId.
   * Worker can run heavy scoring/reranking and return transformed items.
   */
  offload?: (
    payload: { query: string; callId: number; items: TItem[] },
  ) => Promise<TItem[]> | TItem[];
  /**
   * Retry config for transient failures (503/429/network).
   * Uses exponential backoff with jitter.
   */
  retry?: {
    /** Total attempts including the first call. */
    attempts?: number;
    /** Initial retry delay. */
    baseDelayMs?: number;
    /** Maximum backoff cap. */
    maxDelayMs?: number;
    /** Extra randomization percentage applied to delay. */
    jitterRatio?: number;
    /** Override retry decision per error. */
    shouldRetry?: (error: unknown) => boolean;
  };
  /** Structured telemetry stream for Datadog/Grafana/custom analytics. */
  onMetrics?: (metric: SearchMetric) => void;
  /** Accessibility state stream for aria-live announcements. */
  onA11yState?: (state: A11yState) => void;
  /**
   * Enables console-level diagnostics for integration/debug sessions.
   * When `true`, logs internal events with `[call-latest]` prefix.
   * You can pass a custom logger to route debug events.
   */
  debug?: boolean | ((event: string, payload: unknown) => void);
};

type SmartSearchFn<TItem> = (
  query: string,
) => Promise<SearchResponse<TItem>> & { cancel?: () => void };

type SmartSearchController<TItem> = {
  search: (query: string) => Promise<SearchResponse<TItem>>;
  currentDebounce: () => number;
  mode: () => PerformanceMode;
  reset: () => void;
};

export type SearchMetric =
  | { type: "CACHE_HIT"; query: string; durationMs: number }
  | { type: "CACHE_MISS"; query: string; durationMs: number }
  | { type: "NETWORK_OK"; query: string; durationMs: number; callId: number }
  | { type: "NETWORK_ERROR"; query: string; durationMs: number; callId: number; message: string }
  | { type: "RETRY"; query: string; attempt: number; waitMs: number }
  | { type: "SWR_REVALIDATED"; query: string; durationMs: number }
  | { type: "WORKER_TIME"; query: string; durationMs: number }
  | { type: "DELTA_MERGE"; query: string; addCount: number; removeCount: number };

export type A11yState =
  | { type: "loading"; message: string }
  | { type: "results"; message: string; count: number }
  | { type: "empty"; message: string }
  | { type: "error"; message: string };

/**
 * Build a compact "Google-like" search orchestrator.
 *
 * This is the recommended public controller for production search experiences.
 * It composes cancellation, caching, retries, and adaptive behavior behind a
 * single `search(query)` method.
 *
 * @param runSearch Core async search executor. It receives:
 * - `query`: user query string
 * - `prev`: previous successful state (`query`, `version`, `items`) for delta protocols
 * - `ctx`: runtime context (`signal`, `callId`, `mode`, `cancelRemote`)
 *
 * Return either:
 * - full payload: `{ items, version? }`
 * - delta payload: `{ add?, removeIds?, version? }` (when `enableDelta` is on)
 *
 * @param options Smart orchestration options (debounce, cache, local-first, retry, telemetry).
 *
 * @returns Controller with:
 * - `search(query)`: resolves with latest/final result
 * - `currentDebounce()`: currently computed debounce window
 * - `mode()`: current load mode (`normal` or `conserve`)
 * - `reset()`: clears runtime state and invalidates in-flight work
 *
 * Build features:
 * - Adaptive debounce
 * - Latest-call + AbortController + distributed cancel signal
 * - Early stale exit before heavy post-processing
 * - Graceful degradation (normal/conserve)
 * - Speculative prefetch hooks
 * - Backtrack cache + optional delta merge
 * - Optional local-first (WASM/SQLite/DuckDB) short-circuit
 * - Optional worker offload hook
 */
export function createSmartSearch<TItem>(
  runSearch: (
    query: string,
    prev: SearchState<TItem> | null,
    ctx: SmartSearchContext,
  ) => Promise<SearchResponse<TItem> | SearchDelta<TItem>>,
  options: SmartSearchOptions<TItem> = {},
): SmartSearchController<TItem> {
  const {
    minDebounceMs = 150,
    maxDebounceMs = 900,
    baseDebounceMs = 300,
    debounceMode = "adaptive",
    itemId,
    enableDelta = false,
    enableBacktrackCache = true,
    backtrackTtlMs = 120_000,
    cacheMaxEntries = 50,
    swr = true,
    onSWRUpdate,
    enableSpeculativePrefetch = true,
    predictNextQueries,
    prefetch,
    onDistributedCancel,
    metrics,
    onModeChange,
    onLoadingChange,
    localSearch,
    offload,
    retry,
    onMetrics,
    onA11yState,
    debug = false,
  } = options;

  let latestCallId = 0;
  let mode: PerformanceMode = "normal";
  let lastKeystrokeAt = 0;
  let avgTypingGapMs = baseDebounceMs;
  let prevState: SearchState<TItem> | null = null;
  const cache = new Map<string, { value: SearchResponse<TItem>; at: number }>();
  const debugLog =
    typeof debug === "function"
      ? debug
      : debug
        ? (event: string, payload: unknown) => {
            // Intentionally lightweight runtime tracing for integration debugging.
            // eslint-disable-next-line no-console
            console.debug(`[call-latest] ${event}`, payload);
          }
        : undefined;

  const emitMetric = (metric: SearchMetric) => {
    onMetrics?.(metric);
    debugLog?.("metric", metric);
  };
  const emitA11y = (state: A11yState) => {
    onA11yState?.(state);
    debugLog?.("a11y", state);
  };

  const setMode = (next: PerformanceMode) => {
    if (mode !== next) {
      mode = next;
      onModeChange?.(mode);
      debugLog?.("mode-change", { mode });
    }
  };

  const evaluateMode = () => {
    if (!metrics) return;
    const m = metrics();
    const overloaded = m.rttMs > 600 || m.status503Rate > 0.04 || m.errorRate > 0.08;
    setMode(overloaded ? "conserve" : "normal");
  };

  const debounceForNow = (): number => {
    if (mode === "conserve") {
      return clamp(800, minDebounceMs, maxDebounceMs);
    }
    if (debounceMode === "fixed") {
      return clamp(baseDebounceMs, minDebounceMs, maxDebounceMs);
    }
    const adaptive = 0.65 * avgTypingGapMs + 0.35 * baseDebounceMs;
    return clamp(Math.round(adaptive), minDebounceMs, maxDebounceMs);
  };

  const touchTyping = () => {
    const now = Date.now();
    if (lastKeystrokeAt > 0) {
      const gap = now - lastKeystrokeAt;
      avgTypingGapMs = clamp(
        Math.round(avgTypingGapMs * 0.7 + gap * 0.3),
        minDebounceMs,
        1400,
      );
    }
    lastKeystrokeAt = now;
  };

  const getCached = (query: string): SearchResponse<TItem> | null => {
    if (!enableBacktrackCache) return null;
    const hit = cache.get(query);
    if (!hit) return null;
    if (Date.now() - hit.at > backtrackTtlMs) {
      cache.delete(query);
      return null;
    }
    // LRU touch
    cache.delete(query);
    cache.set(query, hit);
    return hit.value;
  };

  const setCached = (query: string, value: SearchResponse<TItem>) => {
    if (!enableBacktrackCache) return;
    if (cache.has(query)) {
      cache.delete(query);
    }
    cache.set(query, { value, at: Date.now() });
    while (cache.size > cacheMaxEntries) {
      const oldest = cache.keys().next().value as string | undefined;
      if (!oldest) break;
      cache.delete(oldest);
    }
  };

  const mergeDelta = (
    prev: SearchState<TItem> | null,
    delta: SearchDelta<TItem>,
  ): SearchResponse<TItem> => {
    if (!enableDelta || !itemId || !prev) {
      return { items: delta.add ?? [], version: delta.version };
    }
    emitMetric({
      type: "DELTA_MERGE",
      query: prev.query,
      addCount: delta.add?.length ?? 0,
      removeCount: delta.removeIds?.length ?? 0,
    });
    const next = new Map<string | number, TItem>();
    for (const item of prev.items) next.set(itemId(item), item);
    for (const rid of delta.removeIds ?? []) next.delete(rid);
    for (const item of delta.add ?? []) next.set(itemId(item), item);
    return { items: [...next.values()], version: delta.version ?? prev.version };
  };

  const call = latest(
    async (
      query: string,
      searchCallId: number,
      ctx: LatestContext,
    ): Promise<SearchResponse<TItem>> => {
      const startedAt = Date.now();
      evaluateMode();
      onLoadingChange?.(true);
      emitA11y({ type: "loading", message: "Arama yapiliyor..." });
      const latestWrapperCallId = ctx.callId;

      // Backtrack cache: instant answer for deleted/revisited query.
      const backtrack = getCached(query);
      if (backtrack) {
        emitMetric({ type: "CACHE_HIT", query, durationMs: Date.now() - startedAt });
        if (backtrack.items.length === 0) {
          emitA11y({ type: "empty", message: "Sonuc bulunamadi." });
        } else {
          emitA11y({
            type: "results",
            message: `${backtrack.items.length} sonuc bulundu.`,
            count: backtrack.items.length,
          });
        }
        if (swr) {
          debugLog?.("swr-start", { query, callId: searchCallId });
          setTimeout(() => {
            void revalidateInBackground(query, searchCallId);
          }, 0);
        }
        if (searchCallId === latestCallId) onLoadingChange?.(false);
        return backtrack;
      }
      emitMetric({ type: "CACHE_MISS", query, durationMs: Date.now() - startedAt });

      // Local-first: WASM / local DB can satisfy search with zero network.
      if (localSearch) {
        const local = await localSearch(query);
        if (local) {
          setCached(query, local);
          prevState = { query, version: local.version, items: local.items };
          if (searchCallId === latestCallId) onLoadingChange?.(false);
          return local;
        }
      }

      const smartCtx: SmartSearchContext = {
        signal: ctx.signal,
        callId: searchCallId,
        mode,
        cancelRemote: (supersededCallId: number) => onDistributedCancel?.(supersededCallId),
      };

      const raw = await runWithRetry(
        () => runSearch(query, prevState, smartCtx),
        query,
        retry,
        emitMetric,
      );

      // Early exit before heavy work/json-transform if response is stale.
      if (searchCallId !== latestCallId) {
        throw new StaleError();
      }

      const resolved = isDelta<TItem>(raw) ? mergeDelta(prevState, raw) : raw;
      const workerStarted = Date.now();
      const items = offload
        ? await offload({ query, callId: searchCallId, items: resolved.items })
        : resolved.items;
      if (offload) {
        emitMetric({
          type: "WORKER_TIME",
          query,
          durationMs: Date.now() - workerStarted,
        });
      }
      const finalResult: SearchResponse<TItem> = { items, version: resolved.version };
      prevState = { query, version: finalResult.version, items: finalResult.items };
      setCached(query, finalResult);
      emitMetric({
        type: "NETWORK_OK",
        query,
        callId: searchCallId,
        durationMs: Date.now() - startedAt,
      });
      if (finalResult.items.length === 0) {
        emitA11y({ type: "empty", message: "Sonuc bulunamadi." });
      } else {
        emitA11y({
          type: "results",
          message: `${finalResult.items.length} sonuc bulundu.`,
          count: finalResult.items.length,
        });
      }

      if (searchCallId === latestCallId) onLoadingChange?.(false);

      if (enableSpeculativePrefetch && mode === "normal" && prefetch && predictNextQueries) {
        const guesses = predictNextQueries(query);
        debugLog?.("prefetch-candidates", { query, guesses: guesses.slice(0, 2) });
        for (const guess of guesses.slice(0, 2)) {
          if (!guess || guess === query) continue;
          void Promise.resolve(prefetch(guess)).catch(() => {});
        }
      }

      return finalResult;
    },
    {
      abort: true,
      onStale: () => {
        onLoadingChange?.(false);
      },
    },
  );

  const revalidateInBackground = async (query: string, callId: number) => {
    try {
      const smartCtx: SmartSearchContext = {
        signal: neverAborts,
        callId,
        mode,
        cancelRemote: (supersededCallId: number) => onDistributedCancel?.(supersededCallId),
      };
      const startedAt = Date.now();
      const raw = await runWithRetry(
        () => runSearch(query, prevState, smartCtx),
        query,
        retry,
        emitMetric,
      );
      const result = isDelta<TItem>(raw) ? mergeDelta(prevState, raw) : raw;
      setCached(query, result);
      if (prevState?.query === query) {
        prevState = { query, version: result.version, items: result.items };
      }
      onSWRUpdate?.(query, result);
      debugLog?.("swr-done", { query, callId, itemCount: result.items.length });
      emitMetric({ type: "SWR_REVALIDATED", query, durationMs: Date.now() - startedAt });
    } catch {
      // SWR runs best-effort; ignore.
    }
  };

  const wait = (ms: number) =>
    ms <= 0
      ? Promise.resolve()
      : new Promise<void>((resolve) => {
          setTimeout(resolve, ms);
        });

  const search = async (query: string): Promise<SearchResponse<TItem>> => {
    touchTyping();

    // Synchronous fast path for cache hits — bypasses wait()/latest() overhead.
    // When the result is already cached, there is no async work to debounce or
    // race-condition-protect, so we return immediately.
    const cached = getCached(query);
    if (cached) {
      latestCallId++;
      if (latestCallId > 1) {
        onDistributedCancel?.(latestCallId - 1);
      }
      emitMetric({ type: "CACHE_HIT", query, durationMs: 0 });
      if (cached.items.length === 0) {
        emitA11y({ type: "empty", message: "Sonuc bulunamadi." });
      } else {
        emitA11y({
          type: "results",
          message: `${cached.items.length} sonuc bulundu.`,
          count: cached.items.length,
        });
      }
      if (swr) {
        debugLog?.("swr-start-fast-path", { query, callId: latestCallId });
        setTimeout(() => {
          void revalidateInBackground(query, latestCallId);
        }, 0);
      }
      return cached;
    }

    const previousId = latestCallId;
    latestCallId++;
    if (previousId > 0) {
      onDistributedCancel?.(previousId);
      debugLog?.("distributed-cancel", { previousId, nextId: latestCallId });
    }
    const debounceMs = debounceForNow();
    debugLog?.("debounce", { query, debounceMs, mode });
    await wait(debounceMs);
    const promise = call(query, latestCallId);
    void promise.catch((error) => {
      if (isStale(error)) {
        return;
      }
    });
    try {
      return await promise;
    } catch (error) {
      if (isStale(error)) {
        throw error;
      }
      emitMetric({
        type: "NETWORK_ERROR",
        query,
        callId: latestCallId,
        durationMs: 0,
        message: getErrorMessage(error),
      });
      emitA11y({ type: "error", message: "Arama sirasinda hata olustu." });
      throw error;
    }
  };

  const reset = () => {
    latestCallId++;
    call.reset();
    cache.clear();
    prevState = null;
    onLoadingChange?.(false);
  };

  return {
    search,
    currentDebounce: debounceForNow,
    mode: () => mode,
    reset,
  };
}

export type FetchAdapterOptions<TItem> = {
  endpoint: string;
  fetcher?: typeof fetch;
  headers?: Record<string, string>;
  /**
   * Sends cancellation signal to edge so backend can kill old heavy queries early.
   * Defaults to `${endpoint}/cancel`.
   */
  cancelEndpoint?: string;
  /**
   * POST body builder for cancel frame payload.
   */
  cancelPayload?: (callId: number) => unknown;
  /**
   * Read response and optionally short-circuit parse for stale call.
   */
  parse?: (response: Response) => Promise<SearchResponse<TItem> | SearchDelta<TItem>>;
  /**
   * Builds request URL/body.
   */
  buildRequest?: (query: string, prevVersion?: string) => {
    url?: string;
    init?: RequestInit;
  };
};

/**
 * Fetch adapter with:
 * - cancel beacon to edge/backend
 * - ETag/If-None-Match and version-aware delta fetch
 */
export function createFetchSearchAdapter<TItem>(
  options: FetchAdapterOptions<TItem>,
): (
  query: string,
  prev: SearchState<TItem> | null,
  ctx: SmartSearchContext,
) => Promise<SearchResponse<TItem> | SearchDelta<TItem>> {
  const fetcher = options.fetcher ?? fetch;
  const parse =
    options.parse ??
    (async (response: Response) =>
      (await response.json()) as SearchResponse<TItem> | SearchDelta<TItem>);

  return async (query, prev, ctx) => {
    const prevVersion = prev?.version;
    const built = options.buildRequest?.(query, prevVersion) ?? {};
    const url = built.url ?? `${options.endpoint}?q=${encodeURIComponent(query)}`;
    const headers: Record<string, string> = {
      ...options.headers,
      ...(built.init?.headers as Record<string, string> | undefined),
    };
    if (prevVersion) {
      headers["If-None-Match"] = prevVersion;
    }
    headers["X-Call-Latest-Id"] = String(ctx.callId);
    headers["X-Call-Latest-Mode"] = ctx.mode;
    const res = await fetcher(url, {
      ...built.init,
      signal: ctx.signal,
      headers,
    });
    if (res.status === 304 && prev) {
      return { add: [], removeIds: [], version: prev.version };
    }
    const nextEtag = res.headers.get("ETag") ?? undefined;
    const payload = await parse(res);
    if (!isDelta(payload) && !payload.version && nextEtag) {
      return { ...payload, version: nextEtag };
    }
    if (isDelta(payload) && !payload.version && nextEtag) {
      return { ...payload, version: nextEtag };
    }
    return payload;
  };
}

/**
 * Fire-and-forget cancel signal for distributed cancellation.
 * Uses `sendBeacon` if available, falls back to fetch keepalive.
 */
export function dispatchCancelSignal(
  endpoint: string,
  callId: number,
  payload?: unknown,
  fetcher: typeof fetch = fetch,
): void {
  const body = JSON.stringify(payload ?? { callId });
  try {
    if (typeof navigator !== "undefined" && typeof navigator.sendBeacon === "function") {
      const blob = new Blob([body], { type: "application/json" });
      navigator.sendBeacon(endpoint, blob);
      return;
    }
  } catch {
    // fallback below
  }
  void fetcher(endpoint, {
    method: "POST",
    keepalive: true,
    headers: { "content-type": "application/json" },
    body,
  });
}

function isDelta<TItem>(
  value: SearchResponse<TItem> | SearchDelta<TItem>,
): value is SearchDelta<TItem> {
  return "add" in value || "removeIds" in value;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

async function runWithRetry<T>(
  task: () => Promise<T>,
  query: string,
  retry: SmartSearchOptions<unknown>["retry"] | undefined,
  emitMetric?: (metric: SearchMetric) => void,
): Promise<T> {
  const attempts = Math.max(1, retry?.attempts ?? 1);
  const baseDelayMs = retry?.baseDelayMs ?? 250;
  const maxDelayMs = retry?.maxDelayMs ?? 4000;
  const jitterRatio = retry?.jitterRatio ?? 0.25;
  const shouldRetry = retry?.shouldRetry ?? defaultShouldRetry;
  let attempt = 0;
  let lastError: unknown;
  while (attempt < attempts) {
    try {
      return await task();
    } catch (error) {
      lastError = error;
      attempt++;
      if (attempt >= attempts || !shouldRetry(error)) {
        throw error;
      }
      const exp = Math.min(maxDelayMs, baseDelayMs * 2 ** (attempt - 1));
      const jitter = Math.round(exp * jitterRatio * Math.random());
      const waitMs = exp + jitter;
      emitMetric?.({ type: "RETRY", query, attempt, waitMs });
      await sleep(waitMs);
    }
  }
  throw lastError;
}

function defaultShouldRetry(error: unknown): boolean {
  if (isStale(error)) return false;
  if (error instanceof DOMException && error.name === "AbortError") return false;
  const status = (error as { status?: number } | undefined)?.status;
  return status === 429 || status === 503 || status === 504 || status === undefined;
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return "Unknown error";
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
