/**
 * Thrown when a call is superseded by a newer invocation.
 * Check with `isStale(error)` instead of `instanceof` across bundle boundaries.
 */
export class StaleError extends Error {
  readonly name = "StaleError";
  readonly code = "STALE" as const;

  constructor(message = "Superseded by a newer call") {
    super(message);
  }
}

/** Returns true if `error` means the call was discarded because a newer one started. */
export function isStale(error: unknown): error is StaleError {
  return (
    error instanceof StaleError ||
    (typeof error === "object" &&
      error !== null &&
      "code" in error &&
      (error as { code: unknown }).code === "STALE")
  );
}

export type LatestContext = {
  /** AbortSignal aborted when a newer call starts (only with `{ abort: true }`). */
  signal: AbortSignal;
  /** 1-based index of this invocation. */
  callId: number;
};

export type LatestOptions = {
  /**
   * Append `{ signal, callId }` as the last argument and abort in-flight work
   * when a newer call starts. Ideal for `fetch` and other cancellable APIs.
   * @default false
   */
  abort?: boolean;
  /** Called when a call is superseded before it finishes. */
  onStale?: (args: unknown[]) => void;
};

type AnyAsyncFn = (...args: never[]) => Promise<unknown>;

type LatestFn<T extends AnyAsyncFn, Abort extends boolean> = Abort extends true
  ? T extends (...args: [...infer A, LatestContext]) => infer R
    ? (...args: A) => R
    : never
  : T;

type LatestWrapped<T extends AnyAsyncFn, Abort extends boolean> = LatestFn<
  T,
  Abort
> & {
  /** Drop all in-flight work and reset the generation counter. */
  reset: () => void;
  /** Number of the most recent invocation (0 before the first call). */
  current: () => number;
};

/**
 * Wrap an async function so only the **latest** invocation can resolve or reject.
 *
 * Older in-flight calls reject with {@link StaleError}. Use {@link isStale} to
 * ignore them cleanly.
 *
 * @example
 * ```ts
 * const search = latest(async (query: string) => {
 *   const res = await fetch(`/api/search?q=${query}`);
 *   return res.json();
 * });
 *
 * search("re");      // eventually stale
 * search("react");   // only this result matters
 * ```
 *
 * @example With abort (cancels in-flight `fetch`)
 * ```ts
 * const search = latest(
 *   async (query: string, { signal }: LatestContext) => {
 *     const res = await fetch(`/api/search?q=${query}`, { signal });
 *     return res.json();
 *   },
 *   { abort: true },
 * );
 * ```
 */
export function latest<T extends AnyAsyncFn>(
  fn: T,
  options?: LatestOptions & { abort?: false },
): LatestWrapped<T, false>;
export function latest<T extends AnyAsyncFn>(
  fn: T,
  options: LatestOptions & { abort: true },
): LatestWrapped<T, true>;
export function latest<T extends AnyAsyncFn>(
  fn: T,
  options: LatestOptions = {},
): LatestWrapped<T, boolean> {
  const { abort = false, onStale } = options;
  let token = 0;
  let calls = 0;
  let controller: AbortController | undefined;

  const wrapped = (...args: unknown[]) => {
    const id = ++token;
    calls++;

    if (abort) {
      controller?.abort(new StaleError());
      controller = new AbortController();
    }

    const context: LatestContext = {
      signal: controller?.signal ?? neverAborts,
      callId: calls,
    };

    const callArgs = abort ? [...args, context] : args;
    const run = (fn as unknown as (...a: unknown[]) => ReturnType<T>)(...callArgs);

    return Promise.resolve(run).then(
      (value) => {
        if (id !== token) {
          onStale?.(args);
          throw new StaleError();
        }
        return value;
      },
      (error) => {
        if (id !== token) {
          onStale?.(args);
          throw new StaleError();
        }
        throw error;
      },
    );
  };

  wrapped.reset = () => {
    token++;
    if (abort) {
      controller?.abort(new StaleError());
      controller = undefined;
    }
  };

  wrapped.current = () => calls;

  return wrapped as LatestWrapped<T, boolean>;
}

export type DedupeOptions<T extends AnyAsyncFn> = {
  /**
   * Cache key for each call. Defaults to `JSON.stringify(args)`.
   * Return `null` to skip deduplication for that call.
   */
  key?: (...args: Parameters<T>) => string | null;
};

type DedupeWrapped<T extends AnyAsyncFn> = ((...args: Parameters<T>) => ReturnType<T>) & {
  /** Number of in-flight deduplicated calls. */
  pending: () => number;
  clear: () => void;
};

/**
 * Coalesce concurrent calls with the same key into one shared promise.
 *
 * Perfect for "fetch user profile" buttons hammered before the first request
 * finishes — one network call, many awaiters, all get the same result.
 *
 * @example
 * ```ts
 * const getUser = dedupe(async (id: string) => {
 *   return fetch(`/api/users/${id}`).then((r) => r.json());
 * });
 *
 * await Promise.all([getUser("42"), getUser("42"), getUser("42")]); // 1 fetch
 * ```
 */
export function dedupe<T extends AnyAsyncFn>(
  fn: T,
  options: DedupeOptions<T> = {},
): DedupeWrapped<T> {
  const { key = defaultKey } = options;
  const pending = new Map<string, ReturnType<T>>();

  const wrapped = ((...args: Parameters<T>) => {
    const cacheKey = (key as (...a: unknown[]) => string | null)(...args);

    if (cacheKey === null) {
      return (fn as unknown as (...a: unknown[]) => ReturnType<T>)(...args);
    }

    const existing = pending.get(cacheKey);
    if (existing) {
      return existing;
    }

    const promise = Promise.resolve(
      (fn as unknown as (...a: unknown[]) => ReturnType<T>)(...args),
    ).finally(() => {
      if (pending.get(cacheKey) === promise) {
        pending.delete(cacheKey);
      }
    }) as ReturnType<T>;

    pending.set(cacheKey, promise);
    return promise;
  }) as DedupeWrapped<T>;

  wrapped.pending = () => pending.size;
  wrapped.clear = () => pending.clear();

  return wrapped;
}

/**
 * Compose {@link latest} + {@link dedupe}: same-args calls share one request,
 * and only the latest *distinct* args can win.
 *
 * The pattern search UIs actually need — rapid typing dedupes identical
 * keystrokes, but "rea" → "react" correctly drops the slower response.
 */
export function latestDedupe<T extends AnyAsyncFn>(
  fn: T,
  options: LatestOptions & DedupeOptions<T> = {},
): LatestWrapped<T, boolean> {
  const { abort = false, onStale, key } = options;
  const deduped = dedupe(fn, { key });

  if (abort) {
    return latest(deduped as unknown as T, { abort: true, onStale }) as LatestWrapped<
      T,
      boolean
    >;
  }

  return latest(deduped as unknown as T, { onStale }) as LatestWrapped<T, boolean>;
}

const neverAborts = new AbortController().signal;

function defaultKey(args: unknown[]): string {
  return JSON.stringify(args, replacer);
}

function replacer(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") {
    return value.toString();
  }
  return value;
}

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
  minDebounceMs?: number;
  maxDebounceMs?: number;
  baseDebounceMs?: number;
  debounceMode?: DebounceMode;
  /**
   * Called on each result to convert item to stable ID for delta/backtrack cache.
   * Required when `enableDelta` is true.
   */
  itemId?: (item: TItem) => string | number;
  enableDelta?: boolean;
  enableBacktrackCache?: boolean;
  backtrackTtlMs?: number;
  cacheMaxEntries?: number;
  swr?: boolean;
  onSWRUpdate?: (query: string, result: SearchResponse<TItem>) => void;
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
  retry?: {
    attempts?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
    jitterRatio?: number;
    shouldRetry?: (error: unknown) => boolean;
  };
  onMetrics?: (metric: SearchMetric) => void;
  onA11yState?: (state: A11yState) => void;
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
 * Build a compact "Google-like" search orchestrator:
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
  } = options;

  let latestCallId = 0;
  let mode: PerformanceMode = "normal";
  let lastKeystrokeAt = 0;
  let avgTypingGapMs = baseDebounceMs;
  let prevState: SearchState<TItem> | null = null;
  const cache = new Map<string, { value: SearchResponse<TItem>; at: number }>();
  const emitMetric = (metric: SearchMetric) => onMetrics?.(metric);
  const emitA11y = (state: A11yState) => onA11yState?.(state);

  const setMode = (next: PerformanceMode) => {
    if (mode !== next) {
      mode = next;
      onModeChange?.(mode);
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
      avgTypingGapMs = clamp(Math.round(avgTypingGapMs * 0.7 + gap * 0.3), 60, 1400);
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
    async (query: string, ctx: LatestContext): Promise<SearchResponse<TItem>> => {
      const startedAt = Date.now();
      evaluateMode();
      onLoadingChange?.(true);
      emitA11y({ type: "loading", message: "Arama yapiliyor..." });
      const myCallId = ctx.callId;

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
          void revalidateInBackground(query, myCallId);
        }
        if (myCallId === latestCallId) onLoadingChange?.(false);
        return backtrack;
      }
      emitMetric({ type: "CACHE_MISS", query, durationMs: Date.now() - startedAt });

      // Local-first: WASM / local DB can satisfy search with zero network.
      if (localSearch) {
        const local = await localSearch(query);
        if (local) {
          setCached(query, local);
          prevState = { query, version: local.version, items: local.items };
          if (myCallId === latestCallId) onLoadingChange?.(false);
          return local;
        }
      }

      const smartCtx: SmartSearchContext = {
        signal: ctx.signal,
        callId: myCallId,
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
      if (myCallId !== latestCallId) {
        throw new StaleError();
      }

      const resolved = isDelta<TItem>(raw) ? mergeDelta(prevState, raw) : raw;
      const workerStarted = Date.now();
      const items = offload
        ? await offload({ query, callId: myCallId, items: resolved.items })
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
        callId: myCallId,
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

      if (myCallId === latestCallId) onLoadingChange?.(false);

      if (enableSpeculativePrefetch && mode === "normal" && prefetch && predictNextQueries) {
        const guesses = predictNextQueries(query);
        for (const guess of guesses.slice(0, 2)) {
          if (!guess || guess === query) continue;
          void Promise.resolve(prefetch(guess));
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
      emitMetric({ type: "SWR_REVALIDATED", query, durationMs: Date.now() - startedAt });
    } catch {
      // SWR runs best-effort; ignore.
    }
  };

  const wait = (ms: number) =>
    new Promise<void>((resolve) => {
      setTimeout(resolve, ms);
    });

  const search = async (query: string): Promise<SearchResponse<TItem>> => {
    touchTyping();
    const previousId = latestCallId;
    latestCallId++;
    if (previousId > 0) {
      onDistributedCancel?.(previousId);
    }
    const debounceMs = debounceForNow();
    await wait(debounceMs);
    try {
      return await call(query);
    } catch (error) {
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
