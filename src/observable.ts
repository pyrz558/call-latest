/**
 * Observability wrapper — structured event stream for debugging.
 *
 * Wraps any async function (or `latest()`-wrapped function) and emits
 * JSON-serializable events for every call lifecycle stage.
 *
 * @module
 */

import { latest, isStale, type LatestOptions } from './_core.js';

// ─── Event types ─────────────────────────────────────────────────────────────

export type ObservableEvent =
  | { type: 'CALL_START';          callId: number; args: unknown[];  timestamp: number }
  | { type: 'STALE_ABORT';         callId: number; args: unknown[];  latencyMs: number; timestamp: number }
  | { type: 'RESOLVED';            callId: number; latencyMs: number; timestamp: number }
  | { type: 'REJECTED';            callId: number; error: unknown;   latencyMs: number; timestamp: number }
  | { type: 'RESET';               timestamp: number }
  | { type: 'RETRY';               callId: number; attempt: number;  delayMs: number; timestamp: number }
  | { type: 'TIMEOUT';             callId: number; latencyMs: number; timestamp: number }
  | { type: 'BATCH_FLUSH';         batchSize: number; timestamp: number }
  | { type: 'CACHE_HIT';           callId: number; latencyMs: number; timestamp: number }
  | { type: 'CACHE_MISS';          callId: number; timestamp: number }
  | { type: 'DISTRIBUTED_CANCEL';  callId: number; supersededCallId: number; timestamp: number };

// ─── Options ─────────────────────────────────────────────────────────────────

export interface ObservableOptions {
  /** Receives every lifecycle event. Should not throw; errors are silently swallowed. */
  onEvent: (event: ObservableEvent) => void;
  /** Only emit events that pass this predicate. */
  filter?: (event: ObservableEvent) => boolean;
  /**
   * Fraction of events to emit, from 0.0 (none) to 1.0 (all).
   * @default 1.0
   */
  sampleRate?: number;
  /**
   * Keep the last N events in an in-memory ring buffer, retrievable via `getBuffer()`.
   * @default 0 (disabled)
   */
  bufferSize?: number;
}

// ─── observe ─────────────────────────────────────────────────────────────────

export type ObservedFn<T extends (...args: any[]) => Promise<any>> = T & {
  /**
   * If the wrapped function has a `.reset()` method (e.g. from `latest()`),
   * this forwards the call and also emits a RESET event.
   */
  reset?(): void;
  /**
   * If the wrapped function has a `.current()` method, this forwards the call.
   */
  current?(): number;
  /**
   * Returns a snapshot of the ring buffer (only populated when `bufferSize > 0`).
   */
  getBuffer(): ObservableEvent[];
};

/**
 * Wraps `fn` and emits {@link ObservableEvent}s for every call lifecycle stage.
 *
 * @example
 * ```ts
 * const search = observe(latest(fetchResults), {
 *   onEvent(e) { console.log(e); },
 *   bufferSize: 100,
 * });
 * await search('react');
 * ```
 */
export function observe<T extends (...args: any[]) => Promise<any>>(
  fn: T,
  options: ObservableOptions,
): ObservedFn<T> {
  const { onEvent, filter, sampleRate = 1.0, bufferSize = 0 } = options;
  let callId = 0;
  const buffer: ObservableEvent[] = [];

  function emit(event: ObservableEvent): void {
    // Apply filter predicate
    if (filter !== undefined && !filter(event)) return;

    // Apply probabilistic sampling (sampleRate=0 drops all, sampleRate=1 keeps all)
    // Math.random() ∈ [0, 1) so >= 0 is always true → sampleRate=0 always drops
    if (sampleRate < 1.0 && Math.random() >= sampleRate) return;

    // Update ring buffer
    if (bufferSize > 0) {
      buffer.push(event);
      if (buffer.length > bufferSize) {
        buffer.shift();
      }
    }

    // Deliver — swallow any errors from the handler
    try {
      onEvent(event);
    } catch {
      // intentionally swallowed per spec
    }
  }

  const wrapped = ((...args: unknown[]) => {
    const id = ++callId;
    const startTs = Date.now();

    emit({ type: 'CALL_START', callId: id, args, timestamp: startTs });

    const promise = (fn as (...a: unknown[]) => Promise<unknown>)(...args);

    return promise.then(
      (value: unknown) => {
        const now = Date.now();
        emit({ type: 'RESOLVED', callId: id, latencyMs: now - startTs, timestamp: now });
        return value;
      },
      (error: unknown) => {
        const now = Date.now();
        if (isStale(error)) {
          emit({
            type: 'STALE_ABORT',
            callId: id,
            args,
            latencyMs: now - startTs,
            timestamp: now,
          });
        } else {
          emit({
            type: 'REJECTED',
            callId: id,
            error,
            latencyMs: now - startTs,
            timestamp: now,
          });
        }
        throw error;
      },
    );
  }) as unknown as T;

  const result = wrapped as ObservedFn<T>;
  const anyFn = fn as Record<string, unknown>;

  result.getBuffer = () => [...buffer];

  if (typeof anyFn['reset'] === 'function') {
    result.reset = () => {
      (anyFn['reset'] as () => void)();
      emit({ type: 'RESET', timestamp: Date.now() });
    };
  }

  if (typeof anyFn['current'] === 'function') {
    result.current = () => (anyFn['current'] as () => number)();
  }

  return result;
}

// ─── createEventBus ──────────────────────────────────────────────────────────

export interface EventBus {
  /** Deliver an event to all current subscribers (also stored in history). */
  emit(event: ObservableEvent): void;
  /** Subscribe to all future events. Returns an unsubscribe function. */
  subscribe(handler: (event: ObservableEvent) => void): () => void;
  /** Subscribe to the next event only; automatically unsubscribes after firing. */
  subscribeOnce(handler: (event: ObservableEvent) => void): () => void;
  /** Remove all subscribers. */
  clear(): void;
  /** Return last `limit` events from history (or all events when `limit` is omitted). */
  history(limit?: number): ObservableEvent[];
}

/**
 * Creates a lightweight pub/sub event bus for {@link ObservableEvent}s.
 *
 * Useful for aggregating events from multiple observed functions into a single
 * stream or for fan-out notification patterns.
 */
export function createEventBus(): EventBus {
  const handlers = new Set<(event: ObservableEvent) => void>();
  const _history: ObservableEvent[] = [];

  return {
    emit(event: ObservableEvent): void {
      _history.push(event);
      // Snapshot before iterating so mutations during delivery are safe
      for (const handler of [...handlers]) {
        try {
          handler(event);
        } catch {
          // intentionally swallowed
        }
      }
    },

    subscribe(handler: (event: ObservableEvent) => void): () => void {
      handlers.add(handler);
      return () => {
        handlers.delete(handler);
      };
    },

    subscribeOnce(handler: (event: ObservableEvent) => void): () => void {
      const wrapper = (event: ObservableEvent): void => {
        handlers.delete(wrapper);
        handler(event);
      };
      handlers.add(wrapper);
      return () => {
        handlers.delete(wrapper);
      };
    },

    clear(): void {
      handlers.clear();
    },

    history(limit?: number): ObservableEvent[] {
      if (limit !== undefined) {
        return _history.slice(-limit);
      }
      return [..._history];
    },
  };
}

// ─── observeLatest ────────────────────────────────────────────────────────────

export type ObserveLatestOptions = ObservableOptions & LatestOptions;

/**
 * Convenience factory: wraps `fn` with `latest()` semantics **and** full
 * observability in one call.
 *
 * This is the recommended shorthand when you want both stale-call safety
 * and a structured event stream without manually composing `latest(observe(fn))`.
 *
 * @example
 * ```ts
 * const search = observeLatest(fetchResults, {
 *   abort: true,
 *   onEvent(e) {
 *     if (e.type === 'STALE_ABORT') {
 *       console.log('stale:', e.callId, 'latency:', e.latencyMs);
 *     }
 *   },
 *   bufferSize: 50,
 * });
 *
 * search('react');   // fires CALL_START, then STALE_ABORT when superseded
 * search('reactjs'); // fires CALL_START, then RESOLVED
 * ```
 */
export function observeLatest<T extends (...args: any[]) => Promise<any>>(
  fn: T,
  options: ObserveLatestOptions,
): ObservedFn<T> {
  const { onEvent, filter, sampleRate, bufferSize, abort, onStale } = options as ObserveLatestOptions & {
    onEvent: (event: ObservableEvent) => void;
    filter?: (event: ObservableEvent) => boolean;
    sampleRate?: number;
    bufferSize?: number;
    abort?: boolean;
    onStale?: (args: unknown[]) => void;
  };

  const latestFn = latest(fn as any, { abort, onStale } as any) as unknown as T;
  return observe(latestFn, { onEvent, filter, sampleRate, bufferSize });
}
