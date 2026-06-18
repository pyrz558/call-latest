import {
  latest as _latest,
  dedupe,
  isStale,
  type LatestOptions,
} from './_core.js';
import { type ObservableEvent } from './observable.js';

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/** Any async function. Used as the bound for generic operators. */
export type AnyFn = (...args: any[]) => Promise<any>;

/**
 * A composable transform — takes a function and returns a wrapped version.
 * Apply multiple operators via `.pipe(op1, op2, op3)`.
 */
export type Operator = (fn: AnyFn) => AnyFn;

// ---------------------------------------------------------------------------
// Error classes
// ---------------------------------------------------------------------------

/** Thrown by `timeoutOp` when the wrapped call exceeds the time limit. */
export class TimeoutError extends Error {
  readonly name = 'TimeoutError' as const;
  readonly code = 'TIMEOUT' as const;

  constructor(message = 'Timed out') {
    super(message);
  }
}

/** Returns true if `err` is a TimeoutError (works across bundle boundaries). */
export function isTimeout(err: unknown): err is TimeoutError {
  return (
    err instanceof TimeoutError ||
    (typeof err === 'object' &&
      err !== null &&
      'code' in err &&
      (err as { code: unknown }).code === 'TIMEOUT')
  );
}

/** Thrown by `throttleOp` when the minimum interval between calls has not elapsed. */
export class ThrottledError extends Error {
  readonly name = 'ThrottledError' as const;
  readonly code = 'THROTTLED' as const;

  constructor(message = 'Call throttled') {
    super(message);
  }
}

/** Returns true if `err` is a ThrottledError (works across bundle boundaries). */
export function isThrottled(err: unknown): err is ThrottledError {
  return (
    err instanceof ThrottledError ||
    (typeof err === 'object' &&
      err !== null &&
      'code' in err &&
      (err as { code: unknown }).code === 'THROTTLED')
  );
}

// ---------------------------------------------------------------------------
// PipeableChain
// ---------------------------------------------------------------------------

/**
 * A callable wrapper produced by `pipeLatest` / `latest`.
 * Supports `.pipe(...operators)` to compose behaviour left-to-right.
 */
export interface PipeableChain<T extends AnyFn = AnyFn> {
  (...args: Parameters<T>): ReturnType<T>;
  /** Drop all in-flight work and reset the generation counter. */
  reset(): void;
  /** Number of the most recent invocation (0 before the first call). */
  current(): number;
  /**
   * Apply one or more operators left-to-right and return a new PipeableChain.
   * Example: `.pipe(retryOp(3), timeoutOp(5000))`
   */
  pipe(...operators: Operator[]): PipeableChain;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

type InternalLatestWrapped = {
  (...args: any[]): Promise<any>;
  reset: () => void;
  current: () => number;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function defaultRetryCheck(error: unknown): boolean {
  if (isStale(error)) return false;
  const status = (error as { status?: number })?.status;
  if (status !== undefined && status >= 400 && status < 500) return false;
  return true;
}

function createPipeable<T extends AnyFn>(
  latestWrapped: InternalLatestWrapped,
  pipedFn: AnyFn,
): PipeableChain<T> {
  const chain = ((...args: any[]) => pipedFn(...args)) as PipeableChain<T>;

  chain.reset = () => latestWrapped.reset();
  chain.current = () => latestWrapped.current();
  chain.pipe = (...operators: Operator[]): PipeableChain => {
    let fn: AnyFn = pipedFn;
    for (const op of operators) {
      fn = op(fn);
    }
    return createPipeable(latestWrapped, fn);
  };

  return chain;
}

// ---------------------------------------------------------------------------
// Main export
// ---------------------------------------------------------------------------

/**
 * Wrap an async function with `latest()` semantics and return a `PipeableChain`
 * that supports composable operators via `.pipe(op1, op2, ...)`.
 *
 * @example
 * ```ts
 * const search = pipeLatest(fetchResults)
 *   .pipe(retryOp(3), timeoutOp(5000), logOp('search'));
 *
 * search('react'); // only the latest call can resolve
 * ```
 */
export function pipeLatest<T extends AnyFn>(fn: T, options?: LatestOptions): PipeableChain<T> {
  const latestWrapped = _latest(fn as any, options as any) as InternalLatestWrapped;
  return createPipeable<T>(latestWrapped, latestWrapped);
}

/** Alias for `pipeLatest` — drop-in replacement for `latest` from `call-latest`. */
export { pipeLatest as latest };

// ---------------------------------------------------------------------------
// Operator: dedupeOp
// ---------------------------------------------------------------------------

export type DedupeOpOptions = {
  /** Override cache key derivation. Receives the args array; return null to skip dedup. */
  key?: (args: any[]) => string | null;
};

/**
 * Coalesce concurrent calls with the same key into one shared promise.
 * Uses `dedupe()` from `call-latest` internally.
 */
export function dedupeOp(options?: DedupeOpOptions): Operator {
  return (fn: AnyFn): AnyFn => {
    const wrapped = dedupe(fn as any, {
      key: options?.key ? (...args: any[]) => options.key!(args) : undefined,
    } as any) as AnyFn;
    return (...args: any[]) => wrapped(...args);
  };
}

// ---------------------------------------------------------------------------
// Operator: retryOp
// ---------------------------------------------------------------------------

export type RetryConfig = {
  /** Max total attempts (first try + retries). Default: 3. */
  attempts?: number;
  /** Base delay before first retry in ms. Default: 250. */
  baseDelayMs?: number;
  /** Maximum per-retry delay in ms. Default: 5000. */
  maxDelayMs?: number;
  /** Fraction of computed delay added as random jitter. Default: 0.3. */
  jitterRatio?: number;
  /**
   * Return true to allow a retry for this error.
   * Default: retry unless stale or 4xx HTTP status.
   * StaleError is NEVER retried regardless of this function.
   */
  shouldRetry?: (err: unknown) => boolean;
};

/**
 * Retry the wrapped function on failure with exponential backoff + jitter.
 * StaleErrors are ALWAYS propagated immediately and never retried.
 */
export function retryOp(config?: RetryConfig): Operator {
  return (fn: AnyFn): AnyFn => {
    return async (...args: any[]): Promise<any> => {
      const attempts = Math.max(1, config?.attempts ?? 3);
      const baseDelayMs = config?.baseDelayMs ?? 250;
      const maxDelayMs = config?.maxDelayMs ?? 5000;
      const jitterRatio = config?.jitterRatio ?? 0.3;
      const shouldRetry = config?.shouldRetry ?? defaultRetryCheck;

      let attempt = 0;
      let lastError: unknown;

      while (attempt < attempts) {
        try {
          return await fn(...args);
        } catch (error) {
          // StaleError must propagate immediately — never retry
          if (isStale(error)) throw error;

          lastError = error;
          attempt++;

          if (attempt >= attempts || !shouldRetry(error)) {
            throw error;
          }

          const exp = Math.min(maxDelayMs, baseDelayMs * 2 ** (attempt - 1));
          const jitter = Math.round(exp * jitterRatio * Math.random());
          await sleep(exp + jitter);
        }
      }
      throw lastError;
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: timeoutOp
// ---------------------------------------------------------------------------

/**
 * Reject with `TimeoutError` if the wrapped call takes longer than `ms` milliseconds.
 */
export function timeoutOp(ms: number): Operator {
  return (fn: AnyFn): AnyFn => {
    return (...args: any[]): Promise<any> => {
      let timerId: ReturnType<typeof setTimeout> | null = null;

      const timeoutP = new Promise<never>((_, reject) => {
        timerId = setTimeout(
          () => reject(new TimeoutError(`Timed out after ${ms}ms`)),
          ms,
        );
      });

      return Promise.race([fn(...args), timeoutP]).finally(() => {
        if (timerId !== null) clearTimeout(timerId);
      });
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: tapOp
// ---------------------------------------------------------------------------

export type TapHooks = {
  /** Fires before the call is dispatched. */
  onCall?: (args: any[]) => void;
  /** Fires when the call resolves successfully. */
  onResult?: (result: any) => void;
  /** Fires when the call rejects with a non-stale error. */
  onError?: (err: unknown) => void;
  /** Fires when the call is superseded (StaleError). */
  onStale?: (args: any[]) => void;
};

/**
 * Observe calls without modifying results or errors.
 * `onStale` fires for StaleErrors; `onError` fires for everything else.
 */
export function tapOp(hooks: TapHooks): Operator {
  return (fn: AnyFn): AnyFn => {
    return async (...args: any[]): Promise<any> => {
      hooks.onCall?.(args);
      try {
        const result = await fn(...args);
        hooks.onResult?.(result);
        return result;
      } catch (err) {
        if (isStale(err)) {
          hooks.onStale?.(args);
        } else {
          hooks.onError?.(err);
        }
        throw err;
      }
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: catchOp
// ---------------------------------------------------------------------------

/**
 * Catch non-stale errors and return `handler(err)` instead.
 * StaleErrors are NEVER caught — they propagate unchanged.
 */
export function catchOp(handler: (err: unknown) => unknown): Operator {
  return (fn: AnyFn): AnyFn => {
    return async (...args: any[]): Promise<any> => {
      try {
        return await fn(...args);
      } catch (err) {
        if (isStale(err)) throw err;
        return handler(err);
      }
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: mapOp
// ---------------------------------------------------------------------------

/**
 * Transform the fulfilled result. Errors pass through unchanged.
 */
export function mapOp(transform: (result: any) => any): Operator {
  return (fn: AnyFn): AnyFn => {
    return async (...args: any[]): Promise<any> => {
      return transform(await fn(...args));
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: throttleOp
// ---------------------------------------------------------------------------

/**
 * Enforce a minimum `ms` gap between actual calls.
 * Calls that arrive too quickly are dropped with `ThrottledError`.
 */
export function throttleOp(ms: number): Operator {
  return (fn: AnyFn): AnyFn => {
    let lastCallAt = 0;

    return async (...args: any[]): Promise<any> => {
      const now = Date.now();
      if (now - lastCallAt < ms) {
        throw new ThrottledError('Call throttled: minimum interval not elapsed');
      }
      lastCallAt = now;
      return fn(...args);
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: abortWithOp
// ---------------------------------------------------------------------------

/**
 * Race the call against an external `AbortSignal`.
 * If the signal fires before the call resolves, the returned promise rejects
 * with the signal's reason (or a generic AbortError). The error is NOT caught
 * — it propagates naturally.
 */
export function abortWithOp(externalSignal: AbortSignal): Operator {
  return (fn: AnyFn): AnyFn => {
    return (...args: any[]): Promise<any> => {
      if (externalSignal.aborted) {
        return Promise.reject(
          externalSignal.reason ?? new DOMException('Aborted', 'AbortError'),
        );
      }

      return new Promise<any>((resolve, reject) => {
        function onAbort(): void {
          reject(externalSignal.reason ?? new DOMException('Aborted', 'AbortError'));
        }

        externalSignal.addEventListener('abort', onAbort, { once: true });

        fn(...args).then(
          (value: unknown) => {
            externalSignal.removeEventListener('abort', onAbort);
            resolve(value);
          },
          (error: unknown) => {
            externalSignal.removeEventListener('abort', onAbort);
            reject(error);
          },
        );
      });
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: logOp
// ---------------------------------------------------------------------------

/**
 * Log call lifecycle events to the console.
 * Format: `[prefix] CALL args=[...] | RESULT value | STALE | ERROR message`
 * Returns result/error unchanged.
 */
export function logOp(prefix = '[call-latest]'): Operator {
  return (fn: AnyFn): AnyFn => {
    return async (...args: any[]): Promise<any> => {
      console.log(`${prefix} CALL args=${JSON.stringify(args)}`);
      try {
        const result = await fn(...args);
        console.log(`${prefix} RESULT`, result);
        return result;
      } catch (err) {
        if (isStale(err)) {
          const callId = (err as { callId?: number }).callId;
          console.log(
            `${prefix} STALE${callId !== undefined ? ` callId=${callId}` : ''}`,
          );
        } else {
          const msg = err instanceof Error ? err.message : String(err);
          console.log(`${prefix} ERROR ${msg}`);
        }
        throw err;
      }
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: collectOp
// ---------------------------------------------------------------------------

export type CollectStats = {
  /** Total number of call invocations. */
  calls: number;
  /** Number of calls that were superseded (StaleError). */
  stale: number;
  /** Number of calls that rejected with a non-stale error. */
  errors: number;
  /** Round-trip latencies in ms for every call (including stale/error). */
  latencies: number[];
};

/**
 * Mutate a `stats` object with telemetry for each call.
 * Useful for testing and monitoring without external dependencies.
 */
export function collectOp(stats: CollectStats): Operator {
  return (fn: AnyFn): AnyFn => {
    return async (...args: any[]): Promise<any> => {
      stats.calls++;
      const start = Date.now();
      try {
        const result = await fn(...args);
        stats.latencies.push(Date.now() - start);
        return result;
      } catch (err) {
        stats.latencies.push(Date.now() - start);
        if (isStale(err)) {
          stats.stale++;
        } else {
          stats.errors++;
        }
        throw err;
      }
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: observeOp
// ---------------------------------------------------------------------------

/**
 * Emit structured {@link ObservableEvent}s into a callback for every call
 * lifecycle stage. This is the pipeline-operator equivalent of `observe()`.
 *
 * Events emitted: `CALL_START`, `RESOLVED`, `STALE_ABORT`, `REJECTED`.
 *
 * @example
 * ```ts
 * const search = pipeLatest(fetchResults).pipe(
 *   retryOp(3),
 *   observeOp((e) => console.log(e.type, e)),
 * );
 * ```
 */
export function observeOp(
  onEvent: (event: ObservableEvent) => void,
  options?: {
    /** Only emit events matching this predicate. */
    filter?: (event: ObservableEvent) => boolean;
    /** 0.0–1.0 sampling rate. Default: 1.0. */
    sampleRate?: number;
  },
): Operator {
  const filter = options?.filter;
  const sampleRate = options?.sampleRate ?? 1.0;
  let callId = 0;

  function emit(event: ObservableEvent): void {
    if (filter !== undefined && !filter(event)) return;
    if (sampleRate < 1.0 && Math.random() >= sampleRate) return;
    try { onEvent(event); } catch { /* intentionally swallowed */ }
  }

  return (fn: AnyFn): AnyFn => {
    return (...args: any[]): Promise<any> => {
      const id = ++callId;
      const startTs = Date.now();
      emit({ type: 'CALL_START', callId: id, args, timestamp: startTs });

      return fn(...args).then(
        (value: unknown) => {
          const now = Date.now();
          emit({ type: 'RESOLVED', callId: id, latencyMs: now - startTs, timestamp: now });
          return value;
        },
        (error: unknown) => {
          const now = Date.now();
          if (isStale(error)) {
            emit({ type: 'STALE_ABORT', callId: id, args, latencyMs: now - startTs, timestamp: now });
          } else {
            emit({ type: 'REJECTED', callId: id, error, latencyMs: now - startTs, timestamp: now });
          }
          throw error;
        },
      );
    };
  };
}

// ---------------------------------------------------------------------------
// Operator: abortMergeOp
// ---------------------------------------------------------------------------

/**
 * Merge an external `AbortSignal` into the call's abort lifecycle.
 *
 * Unlike `abortWithOp` (which simply races against the signal), this operator
 * creates a **composite AbortController** that combines the library's internal
 * cancellation with the caller's external signal. The first to abort wins.
 *
 * This enables **distributed cancellation**: even when the user cancels from
 * the UI (e.g. component unmount), the abort propagates all the way through
 * the fetch layer.
 *
 * The `AbortSignal` is injected as the *last* argument of the underlying
 * function so it can be passed straight to `fetch(..., { signal })`.
 *
 * @example
 * ```ts
 * const controller = new AbortController();
 *
 * const search = pipeLatest(fetchFn).pipe(
 *   abortMergeOp(() => controller.signal),
 * );
 *
 * // When controller.abort() fires, the fetch is cancelled AND the
 * // promise rejects immediately — no dangling network request.
 * ```
 */
export function abortMergeOp(
  getSignal: () => AbortSignal | null | undefined,
): Operator {
  return (fn: AnyFn): AnyFn => {
    return (...args: any[]): Promise<any> => {
      const externalSignal = getSignal();

      // No external signal — pass through unchanged.
      if (!externalSignal) return fn(...args);

      // Already aborted before the call even starts.
      if (externalSignal.aborted) {
        return Promise.reject(
          externalSignal.reason ?? new DOMException('Aborted', 'AbortError'),
        );
      }

      // Build a composite controller that merges both signals.
      const composite = new AbortController();

      function onExternalAbort(): void {
        composite.abort(
          externalSignal!.reason ?? new DOMException('Aborted', 'AbortError'),
        );
      }

      externalSignal.addEventListener('abort', onExternalAbort, { once: true });

      // Inject the composite signal as the last argument.
      const callArgs = [...args, composite.signal];

      return new Promise<any>((resolve, reject) => {
        function onCompositeAbort(): void {
          externalSignal!.removeEventListener('abort', onExternalAbort);
          reject(
            composite.signal.reason ?? new DOMException('Aborted', 'AbortError'),
          );
        }

        composite.signal.addEventListener('abort', onCompositeAbort, { once: true });

        fn(...callArgs).then(
          (value: unknown) => {
            externalSignal!.removeEventListener('abort', onExternalAbort);
            composite.signal.removeEventListener('abort', onCompositeAbort);
            resolve(value);
          },
          (error: unknown) => {
            externalSignal!.removeEventListener('abort', onExternalAbort);
            composite.signal.removeEventListener('abort', onCompositeAbort);
            reject(error);
          },
        );
      });
    };
  };
}
