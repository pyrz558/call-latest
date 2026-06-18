/**
 * Core primitives shared across all call-latest modules.
 *
 * This file is intentionally minimal — it contains only `StaleError`, `isStale`,
 * `latest`, `dedupe`, and `latestDedupe`. Advanced features (smart search,
 * pipeline, observable, batching, streams) live in their own subpath modules
 * so tree-shaking is maximized and the core bundle stays tiny.
 *
 * @module
 */

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

export type AnyAsyncFn = (...args: never[]) => Promise<unknown>;

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
    const promise = Promise.resolve(run).then(
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
    void promise.catch(() => {
      // Attach a noop handler immediately so stale/aborted rejections
      // do not become unhandled between call creation and consumer handlers.
    });
    return promise;
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

export const neverAborts = new AbortController().signal;

function defaultKey(args: unknown[]): string {
  return JSON.stringify(args, replacer);
}

function replacer(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") {
    return value.toString();
  }
  return value;
}
