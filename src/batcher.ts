/**
 * Backpressure / windowed batching.
 *
 * Collects individual requests over a sliding time window and dispatches them
 * together as a single batch call, dramatically reducing downstream load for
 * "fetch many individual items" patterns (DataLoader-style).
 *
 * Features:
 * - Per-window key deduplication (same key → same promise)
 * - Forced flush when `maxBatchSize` is reached before the window closes
 * - Explicit `flush()` for test-friendly, eager dispatch
 * - Graceful `destroy()` that rejects all pending callers
 * - Structured observability via `onEvent` callback
 * - Per-batch latency tracking via `stats()`
 */

import { type ObservableEvent } from './observable.js';

// ---------------------------------------------------------------------------
// Error classes
// ---------------------------------------------------------------------------

/**
 * Carries batch-level context when a batch-level error needs to surface keys.
 * The batcher itself propagates the raw batchFn error; this class is exported
 * for callers who want to rethrow with key context.
 */
export class BatchError extends Error {
  readonly keys: unknown[];

  constructor(message: string, keys: unknown[]) {
    super(message);
    this.name = 'BatchError';
    this.keys = keys;
  }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type BatcherStats = {
  /** Total calls to `batcher(key)` since creation. */
  totalCalls: number;
  /** Number of batch dispatches fired so far. */
  totalBatches: number;
  /** Average number of unique keys per batch dispatch. */
  avgBatchSize: number;
  /** Calls served from an existing pending promise (key dedup). */
  savedCalls: number;
  /** The configured collection window in ms. */
  windowMs: number;
  /** The configured maximum keys per batch before a forced flush. */
  maxBatchSize: number;
  /** Average batch dispatch latency in ms (0 if no batches yet). */
  avgBatchLatencyMs: number;
  /** p95 batch dispatch latency in ms (0 if fewer than 20 batches). */
  p95BatchLatencyMs: number;
};

export interface BatcherOptions<TKey, TResult> {
  /** Collect for this many ms before flushing. Default: 50. */
  windowMs: number;
  /** Maximum unique keys per batch; triggers an immediate flush. Default: 100. */
  maxBatchSize: number;
  /** Derive a string cache key from a TKey. Default: JSON.stringify. */
  keyFn?: (key: TKey) => string;
  /** Called after a successful batch with keys + corresponding results. */
  onBatch?: (keys: TKey[], results: TResult[]) => void;
  /** Called when batchFn throws, before propagating to callers. */
  onError?: (err: unknown, keys: TKey[]) => void;
  /**
   * Structured observability callback. Receives `BATCH_FLUSH` events after
   * every successful batch dispatch, enabling integration with the
   * `createEventBus()` and `observe()` ecosystem.
   */
  onEvent?: (event: ObservableEvent) => void;
}

export interface Batcher<TKey, TResult> {
  /** Enqueue a key and return a promise that resolves with its result. */
  (key: TKey): Promise<TResult>;
  /** Force immediate dispatch of the current queue; resolves when done. */
  flush(): Promise<void>;
  /** Number of unique keys currently waiting to be batched. */
  pendingCount(): number;
  /** Snapshot of cumulative statistics. */
  stats(): BatcherStats;
  /** Cancel the pending flush timer, reject all waiting callers, and disable the batcher. */
  destroy(): void;
}

// ---------------------------------------------------------------------------
// Internal queue entry
// ---------------------------------------------------------------------------

type QueueEntry<TKey, TResult> = {
  key: TKey;
  callbacks: Array<{
    resolve: (result: TResult) => void;
    reject: (err: unknown) => void;
  }>;
};

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Create a batching dispatcher.
 *
 * @param batchFn  Called with an array of deduplicated keys; must return an
 *                 array of results in the **same order** as the keys.
 * @param options  Optional tuning (windowMs, maxBatchSize, keyFn, hooks).
 *
 * @example
 * ```ts
 * const getUsers = createBatcher(
 *   async (ids: string[]) => fetchUsers(ids),   // one request for many ids
 *   { windowMs: 20 },
 * );
 *
 * // Hundreds of concurrent callers — only a handful of batch fetches.
 * const user = await getUsers(userId);
 * ```
 */
export function createBatcher<TKey, TResult>(
  batchFn: (keys: TKey[]) => Promise<TResult[]>,
  options?: Partial<BatcherOptions<TKey, TResult>>,
): Batcher<TKey, TResult> {
  const windowMs = options?.windowMs ?? 50;
  const maxBatchSize = options?.maxBatchSize ?? 100;
  const keyFn: (key: TKey) => string =
    options?.keyFn ?? ((key: TKey) => JSON.stringify(key));

  // Pending queue: keyStr → { original key + list of (resolve, reject) }
  const queue = new Map<string, QueueEntry<TKey, TResult>>();

  let flushTimer: ReturnType<typeof setTimeout> | null = null;
  let destroyed = false;

  // Cumulative stats
  let totalCalls = 0;
  let totalBatches = 0;
  let totalBatchedKeys = 0; // sum of batch sizes across all dispatches
  let savedCalls = 0;       // calls served from an existing in-window promise
  const batchLatencies: number[] = []; // latency of each processBatch() call

  // ── Core async processor ─────────────────────────────────────────────────

  async function processBatch(
    batch: Map<string, QueueEntry<TKey, TResult>>,
  ): Promise<void> {
    const entries = Array.from(batch.values());
    const keys = entries.map((e) => e.key);
    const batchStartedAt = Date.now();

    totalBatches++;
    totalBatchedKeys += keys.length;

    try {
      const results = await batchFn(keys);
      const batchLatencyMs = Date.now() - batchStartedAt;
      batchLatencies.push(batchLatencyMs);
      options?.onBatch?.(keys, results);

      // Emit BATCH_FLUSH observability event
      try {
        options?.onEvent?.({
          type: 'BATCH_FLUSH',
          batchSize: keys.length,
          timestamp: Date.now(),
        });
      } catch { /* intentionally swallowed */ }

      entries.forEach(({ callbacks }, index) => {
        if (index >= results.length) {
          const err = new RangeError(
            `batchFn returned fewer results than keys: missing result at index ${index}`,
          );
          callbacks.forEach((cb) => cb.reject(err));
        } else {
          callbacks.forEach((cb) => cb.resolve(results[index] as TResult));
        }
      });
    } catch (err) {
      batchLatencies.push(Date.now() - batchStartedAt);
      options?.onError?.(err, keys);
      entries.forEach(({ callbacks }) => callbacks.forEach((cb) => cb.reject(err)));
    }
  }

  // ── Synchronous snapshot-and-clear ───────────────────────────────────────

  function flushCurrent(): Promise<void> {
    if (queue.size === 0) return Promise.resolve();

    // Cancel any pending timer so it doesn't fire redundantly.
    if (flushTimer !== null) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }

    // Atomically snapshot and drain the queue BEFORE any await.
    const batch = new Map(queue);
    queue.clear();

    return processBatch(batch);
  }

  function scheduleFlush(): void {
    if (flushTimer !== null) return; // timer already running
    flushTimer = setTimeout(() => {
      flushTimer = null;
      void flushCurrent();
    }, windowMs);
  }

  // ── Public callable ───────────────────────────────────────────────────────

  function batcherImpl(key: TKey): Promise<TResult> {
    if (destroyed) {
      return Promise.reject(new Error('Batcher destroyed'));
    }

    totalCalls++;
    const keyStr = keyFn(key);

    return new Promise<TResult>((resolve, reject) => {
      const existing = queue.get(keyStr);

      if (existing) {
        // Same key already pending in this window — share its promise.
        savedCalls++;
        existing.callbacks.push({ resolve, reject });
        return;
      }

      // Add this key to the queue first, then decide how to flush.
      queue.set(keyStr, { key, callbacks: [{ resolve, reject }] });

      if (queue.size >= maxBatchSize) {
        // Hit the size cap — flush the full batch (including this key) now.
        // flushCurrent() synchronously clears the queue, so the next caller
        // starts a fresh window.
        void flushCurrent();
      } else {
        scheduleFlush();
      }
    });
  }

  // ── Attach interface methods ──────────────────────────────────────────────

  return Object.assign(batcherImpl, {
    flush(): Promise<void> {
      return flushCurrent();
    },

    pendingCount(): number {
      return queue.size;
    },

    stats(): BatcherStats {
      const sorted = batchLatencies.slice().sort((a, b) => a - b);
      const avgBatchLatencyMs = sorted.length > 0
        ? sorted.reduce((a, b) => a + b, 0) / sorted.length
        : 0;
      const p95Index = sorted.length >= 20
        ? Math.floor(0.95 * (sorted.length - 1))
        : sorted.length - 1;
      const p95BatchLatencyMs = sorted.length > 0 ? (sorted[p95Index] ?? 0) : 0;

      return {
        totalCalls,
        totalBatches,
        avgBatchSize: totalBatches > 0 ? totalBatchedKeys / totalBatches : 0,
        savedCalls,
        windowMs,
        maxBatchSize,
        avgBatchLatencyMs,
        p95BatchLatencyMs,
      };
    },

    destroy(): void {
      destroyed = true;
      if (flushTimer !== null) {
        clearTimeout(flushTimer);
        flushTimer = null;
      }
      const err = new Error('Batcher destroyed');
      queue.forEach(({ callbacks }) => callbacks.forEach((cb) => cb.reject(err)));
      queue.clear();
    },
  }) as Batcher<TKey, TResult>;
}
