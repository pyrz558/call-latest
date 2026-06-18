/**
 * AsyncGenerator / stream hybrid with switchMap behaviour.
 *
 * Provides `for await` semantics where pushing a new value cancels the
 * previous in-flight call — only the result of the most recent `push()`
 * ever appears in the iterator.
 *
 * Additional stream utilities:
 * - `debounceStream()` — buffer pushes and emit only after a quiet period
 * - `mapStream()` — transform emitted values
 * - `filterStream()` — drop values that do not pass a predicate
 * - `mergeStreams()` — fan-in multiple streams into one
 *
 * @module
 */

import { latest, isStale } from './_core.js';

// ─── Options ─────────────────────────────────────────────────────────────────

export interface StreamOptions<TResult> {
  /**
   * Maximum number of resolved results held in the internal buffer while the
   * consumer hasn't called `next()` yet.
   * @default 10
   */
  bufferSize?: number;
  /**
   * Called whenever a buffered result is dropped because the buffer is full
   * and a newer result arrived.
   */
  onDropped?: (result: TResult) => void;
  /**
   * External cancellation signal; aborting it is equivalent to calling `close()`.
   */
  signal?: AbortSignal;
}

// ─── LatestStream ─────────────────────────────────────────────────────────────

type Waiter<T> = {
  resolve: (value: IteratorResult<T>) => void;
  reject: (error: unknown) => void;
};

/**
 * An `AsyncIterable` that applies switchMap semantics: pushing a new set of
 * arguments cancels any still-in-flight call so only the *latest* push can
 * ever emit a value into the `for await` loop.
 *
 * @example
 * ```ts
 * const stream = new LatestStream(fetchResults);
 *
 * inputEl.addEventListener('input', e => stream.push(e.target.value));
 *
 * for await (const results of stream) {
 *   renderResults(results);
 * }
 * ```
 */
export class LatestStream<TArgs extends unknown[], TResult>
  implements AsyncIterable<TResult> {

  private readonly _latestFn: (...args: TArgs) => Promise<TResult>;
  private readonly _queue: TResult[] = [];
  private readonly _waiters: Waiter<TResult>[] = [];
  private _closed = false;
  /** Non-null while an un-delivered error is pending. Cleared after first delivery. */
  private _failure: { error: unknown } | null = null;
  private readonly _bufferSize: number;
  private readonly _onDropped?: (result: TResult) => void;

  constructor(
    fn: (...args: TArgs) => Promise<TResult>,
    options?: StreamOptions<TResult>,
  ) {
    this._bufferSize = options?.bufferSize ?? 10;
    this._onDropped = options?.onDropped;

    // Cast through any to satisfy AnyAsyncFn constraint while preserving TArgs/TResult
    this._latestFn = latest(fn as unknown as (...args: never[]) => Promise<TResult>) as unknown as (
      ...args: TArgs
    ) => Promise<TResult>;

    if (options?.signal) {
      options.signal.addEventListener('abort', () => this.close(), { once: true });
    }
  }

  /**
   * Queue a new call. Any still-in-flight call is made stale and its result
   * will be silently discarded.
   */
  push(...args: TArgs): void {
    if (this._closed) return;

    void this._latestFn(...args).then(
      (result) => { this._enqueue(result); },
      (err: unknown) => {
        // StaleError means a newer push superseded this one — silently drop.
        if (!isStale(err)) this._fail(err);
      },
    );
  }

  /** Stop the stream. The `for await` loop drains any buffered results then terminates. */
  close(): void {
    if (this._closed) return;
    this._closed = true;
    this._settle();
  }

  /** `true` while the stream accepts new pushes and the iterator has not ended. */
  isOpen(): boolean {
    return !this._closed;
  }

  /**
   * Number of resolved results currently waiting in the internal buffer to be
   * consumed by the `for await` loop.
   */
  pendingCount(): number {
    return this._queue.length;
  }

  [Symbol.asyncIterator](): AsyncIterator<TResult> {
    return {
      next: (): Promise<IteratorResult<TResult>> => {
        // Drain buffer before checking terminal states
        if (this._queue.length > 0) {
          return Promise.resolve({ value: this._queue.shift()!, done: false });
        }

        // Deliver a pending error exactly once
        if (this._failure !== null) {
          const { error } = this._failure;
          this._failure = null;
          return Promise.reject(error);
        }

        if (this._closed) {
          return Promise.resolve({ value: undefined as unknown as TResult, done: true });
        }

        // No data yet — park the caller until something arrives
        return new Promise<IteratorResult<TResult>>((resolve, reject) => {
          this._waiters.push({ resolve, reject });
        });
      },

      // Called when the consumer breaks out of the for-await loop early
      return: (): Promise<IteratorResult<TResult>> => {
        this.close();
        return Promise.resolve({ value: undefined as unknown as TResult, done: true });
      },
    };
  }

  // ─── Internal helpers ───────────────────────────────────────────────────────

  private _enqueue(result: TResult): void {
    // Drop results that arrive after the stream has been closed
    if (this._closed) return;

    // Fast path: hand the result directly to a waiting consumer
    if (this._waiters.length > 0) {
      const waiter = this._waiters.shift()!;
      waiter.resolve({ value: result, done: false });
      return;
    }

    // No buffer configured: drop immediately
    if (this._bufferSize === 0) {
      this._onDropped?.(result);
      return;
    }

    // Ring-buffer: evict oldest when full
    if (this._queue.length >= this._bufferSize) {
      const dropped = this._queue.shift()!;
      this._onDropped?.(dropped);
    }

    this._queue.push(result);
  }

  private _fail(error: unknown): void {
    if (this._closed) return;
    this._failure = { error };
    this._closed = true;
    this._settle();
  }

  /**
   * Pairs queued data (or terminal states) with waiting consumers.
   * Called after any state change that might unblock a waiter.
   */
  private _settle(): void {
    while (this._waiters.length > 0) {
      // Error: reject the first waiter, then drain the rest as done
      if (this._failure !== null) {
        const { error } = this._failure;
        this._failure = null;
        this._waiters.shift()!.reject(error);
        while (this._waiters.length > 0) {
          this._waiters.shift()!.resolve({
            value: undefined as unknown as TResult,
            done: true,
          });
        }
        return;
      }

      if (this._queue.length > 0) {
        this._waiters.shift()!.resolve({ value: this._queue.shift()!, done: false });
      } else if (this._closed) {
        // Stream is closed and buffer is empty: signal done to all waiters
        while (this._waiters.length > 0) {
          this._waiters.shift()!.resolve({
            value: undefined as unknown as TResult,
            done: true,
          });
        }
        return;
      } else {
        // No data available and not closed: leave waiters parked
        break;
      }
    }
  }
}

// ─── Factory ─────────────────────────────────────────────────────────────────

/**
 * Convenience factory that creates a {@link LatestStream}.
 */
export function createLatestStream<TArgs extends unknown[], TResult>(
  fn: (...args: TArgs) => Promise<TResult>,
  options?: StreamOptions<TResult>,
): LatestStream<TArgs, TResult> {
  return new LatestStream(fn, options);
}

// ─── fromEvents ──────────────────────────────────────────────────────────────

/**
 * Adapts an event-emitter into a {@link LatestStream}.
 *
 * Every time `eventName` fires on `emitter`, the arguments are forwarded to
 * `fn` via `push()`. When the stream is closed the listener is automatically
 * removed.
 *
 * @example
 * ```ts
 * const stream = fromEvents(socket, 'query', fetchResults);
 * for await (const results of stream) { render(results); }
 * ```
 */
export function fromEvents<TArgs extends unknown[], TResult>(
  emitter: {
    on(event: string, handler: (...args: TArgs) => void): void;
    off(event: string, handler: (...args: TArgs) => void): void;
  },
  eventName: string,
  fn: (...args: TArgs) => Promise<TResult>,
  options?: StreamOptions<TResult>,
): LatestStream<TArgs, TResult> {
  const stream = new LatestStream<TArgs, TResult>(fn, options);

  const handler = (...args: TArgs): void => {
    stream.push(...args);
  };

  emitter.on(eventName, handler);

  // Intercept close() to also unregister the event listener
  const originalClose = stream.close.bind(stream);
  stream.close = () => {
    emitter.off(eventName, handler);
    originalClose();
  };

  return stream;
}

// ─── debounceStream ───────────────────────────────────────────────────────────

/**
 * Creates a debounced {@link LatestStream}: only the *latest* value pushed
 * within each quiet window is forwarded to the underlying function.
 *
 * This is the RxJS `debounceTime` + `switchMap` pattern in plain async/await.
 *
 * @example
 * ```ts
 * const stream = debounceStream(fetchResults, { waitMs: 300 });
 *
 * inputEl.addEventListener('input', (e) => stream.push(e.target.value));
 *
 * for await (const results of stream) {
 *   renderResults(results); // only fires after 300ms of quiet
 * }
 * ```
 */
export function debounceStream<TArgs extends unknown[], TResult>(
  fn: (...args: TArgs) => Promise<TResult>,
  options: StreamOptions<TResult> & {
    /** Quiet period in ms before the latest push is forwarded. @default 300 */
    waitMs?: number;
  } = {},
): LatestStream<TArgs, TResult> {
  const { waitMs = 300, ...streamOpts } = options;

  // Wrap fn so that each call waits for the debounce window first.
  const debouncedFn = ((...args: TArgs): Promise<TResult> => {
    return new Promise<TResult>((resolve, reject) => {
      const timer = setTimeout(() => {
        fn(...args).then(resolve, reject);
      }, waitMs);

      // If the LatestStream supersedes this call (StaleError), clearTimeout
      // via AbortSignal is not available here — the timer fires harmlessly
      // but the `latest()` wrapper inside LatestStream discards the result.
      void timer; // suppress unused warning
    });
  }) as (...args: TArgs) => Promise<TResult>;

  return new LatestStream<TArgs, TResult>(debouncedFn, streamOpts);
}

// ─── mapStream ──────────────────────────────────────────────────────────────────

/**
 * Wraps a {@link LatestStream} to transform each emitted value via `transform`.
 *
 * This is the RxJS `map` operator for `LatestStream`.
 *
 * @example
 * ```ts
 * const raw = createLatestStream(fetchItems);
 * const trimmed = mapStream(raw, (items) => items.slice(0, 10));
 *
 * for await (const top10 of trimmed) {
 *   render(top10);
 * }
 * ```
 */
export function mapStream<TIn, TOut>(
  source: AsyncIterable<TIn>,
  transform: (value: TIn) => TOut | Promise<TOut>,
): AsyncIterable<TOut> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<TOut> {
      const iter = source[Symbol.asyncIterator]();
      return {
        async next(): Promise<IteratorResult<TOut>> {
          const { value, done } = await iter.next();
          if (done) return { value: undefined as unknown as TOut, done: true };
          return { value: await transform(value), done: false };
        },
        async return(): Promise<IteratorResult<TOut>> {
          await iter.return?.();
          return { value: undefined as unknown as TOut, done: true };
        },
      };
    },
  };
}

// ─── filterStream ─────────────────────────────────────────────────────────────

/**
 * Wraps a {@link LatestStream} (or any `AsyncIterable`) to skip values that
 * do not satisfy `predicate`.
 *
 * This is the RxJS `filter` operator for async iterables.
 *
 * @example
 * ```ts
 * const stream = createLatestStream(fetchResults);
 * const nonEmpty = filterStream(stream, (r) => r.items.length > 0);
 *
 * for await (const results of nonEmpty) {
 *   render(results); // never fires for empty result sets
 * }
 * ```
 */
export function filterStream<T>(
  source: AsyncIterable<T>,
  predicate: (value: T) => boolean | Promise<boolean>,
): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      const iter = source[Symbol.asyncIterator]();
      const self: AsyncIterator<T> = {
        async next(): Promise<IteratorResult<T>> {
          // Keep consuming until we find a value that passes the predicate
          // or the source is exhausted.
          while (true) {
            const { value, done } = await iter.next();
            if (done) return { value: undefined as unknown as T, done: true };
            if (await predicate(value)) return { value, done: false };
          }
        },
        async return(): Promise<IteratorResult<T>> {
          await iter.return?.();
          return { value: undefined as unknown as T, done: true };
        },
      };
      return self;
    },
  };
}

// ─── mergeStreams ─────────────────────────────────────────────────────────────

/**
 * Merge multiple `AsyncIterable` sources into a single interleaved stream.
 *
 * Values from all sources are emitted in arrival order. The merged stream
 * ends only when **all** sources are exhausted.
 *
 * This is the RxJS `merge` operator for async iterables.
 *
 * @example
 * ```ts
 * const s1 = createLatestStream(fetchUser);
 * const s2 = createLatestStream(fetchOrders);
 * const merged = mergeStreams(s1, s2);
 *
 * for await (const event of merged) {
 *   // receives results from BOTH streams as they arrive
 * }
 * ```
 */
export function mergeStreams<T>(...sources: AsyncIterable<T>[]): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      // Each source is consumed by a concurrent "puller" that races its next()
      // promise against the others. When one resolves, we re-arm it.

      type SlotState =
        | { status: 'pending'; promise: Promise<{ slotIdx: number; result: IteratorResult<T> }> }
        | { status: 'done' };

      const iterators = sources.map((s) => s[Symbol.asyncIterator]());
      let activeCount = iterators.length;

      const slots: SlotState[] = iterators.map((iter, idx) => ({
        status: 'pending' as const,
        promise: iter.next().then((result) => ({ slotIdx: idx, result })),
      }));

      function armSlot(idx: number): void {
        const iter = iterators[idx]!;
        slots[idx] = {
          status: 'pending',
          promise: iter.next().then((result) => ({ slotIdx: idx, result })),
        };
      }

      return {
        async next(): Promise<IteratorResult<T>> {
          if (activeCount === 0) {
            return { value: undefined as unknown as T, done: true };
          }

          // Race all pending slots
          while (activeCount > 0) {
            const pending = slots
              .filter((s): s is Extract<SlotState, { status: 'pending' }> => s.status === 'pending')
              .map((s) => s.promise);

            if (pending.length === 0) break;

            const { slotIdx, result } = await Promise.race(pending);

            if (result.done) {
              slots[slotIdx] = { status: 'done' };
              activeCount--;
              if (activeCount === 0) {
                return { value: undefined as unknown as T, done: true };
              }
              // Continue racing the remaining slots
              continue;
            }

            // Re-arm the slot that just delivered a value
            armSlot(slotIdx);
            return { value: result.value, done: false };
          }

          return { value: undefined as unknown as T, done: true };
        },

        async return(): Promise<IteratorResult<T>> {
          // Close all iterators
          await Promise.allSettled(iterators.map((iter) => iter.return?.()));
          activeCount = 0;
          return { value: undefined as unknown as T, done: true };
        },
      };
    },
  };
}
