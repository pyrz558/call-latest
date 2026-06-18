import { describe, expect, it, vi } from 'vitest';
import {
  pipeLatest,
  latest,
  dedupeOp,
  retryOp,
  timeoutOp,
  tapOp,
  catchOp,
  mapOp,
  throttleOp,
  abortWithOp,
  logOp,
  collectOp,
  TimeoutError,
  isTimeout,
  ThrottledError,
  isThrottled,
  type CollectStats,
} from './pipeline.js';
import { isStale, StaleError } from './index.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Returns a promise + external resolver pair. */
function deferred<T>(): { promise: Promise<T>; resolve: (v: T) => void; reject: (e: unknown) => void } {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

// ---------------------------------------------------------------------------
// 1. pipeLatest — behaves like latest()
// ---------------------------------------------------------------------------

describe('pipeLatest', () => {
  it('works like latest() — stale calls reject with StaleError', async () => {
    const { promise: first, resolve: resolveFirst } = deferred<number>();

    const fn = vi.fn(async (n: number) => {
      if (n === 1) return first;
      return n;
    });

    const wrapped = pipeLatest(fn);
    const p1 = wrapped(1);
    const p2 = wrapped(2);

    resolveFirst(999); // too late for call 1 — it is already stale

    await expect(p1).rejects.toBeInstanceOf(StaleError);
    await expect(p2).resolves.toBe(2);
    expect(fn).toHaveBeenCalledTimes(2);
  });

  // ---------------------------------------------------------------------------
  // 2. dedupeOp
  // ---------------------------------------------------------------------------

  it('.pipe(dedupeOp()) deduplicates concurrent calls with the same key', async () => {
    const { promise: shared, resolve: resolveShared } = deferred<string>();

    const fn = vi.fn(async (_key: string) => shared);

    const wrapped = pipeLatest(fn).pipe(dedupeOp());
    const p1 = wrapped('a');
    const p2 = wrapped('a'); // same args → same dedupe key

    resolveShared('hit');

    const [r1, r2] = await Promise.all([p1, p2]);
    expect(r1).toBe('hit');
    expect(r2).toBe('hit');
    // The underlying fn was invoked only once — the second call coalesced
    expect(fn).toHaveBeenCalledTimes(1);
  });

  // ---------------------------------------------------------------------------
  // 3. retryOp — success after transient failures
  // ---------------------------------------------------------------------------

  it('.pipe(retryOp({ attempts: 3 })) retries failed calls', async () => {
    let callCount = 0;
    const fn = vi.fn(async () => {
      callCount++;
      if (callCount < 3) throw new Error('transient');
      return 'success';
    });

    const wrapped = pipeLatest(fn).pipe(
      retryOp({ attempts: 3, baseDelayMs: 0, jitterRatio: 0 }),
    );

    await expect(wrapped()).resolves.toBe('success');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  // ---------------------------------------------------------------------------
  // 4. retryOp — StaleError is never retried
  // ---------------------------------------------------------------------------

  it('.pipe(retryOp()) does not retry StaleError', async () => {
    const { promise: first, resolve: resolveFirst } = deferred<void>();

    const fn = vi.fn(async (n: number) => {
      if (n === 1) {
        await first;
        return 1;
      }
      return 2;
    });

    const wrapped = pipeLatest(fn).pipe(
      retryOp({ attempts: 3, baseDelayMs: 0, jitterRatio: 0 }),
    );

    const p1 = wrapped(1);
    const p2 = wrapped(2); // supersedes call 1

    resolveFirst(); // let call 1 try to settle — but it's already stale

    await expect(p1).rejects.toBeInstanceOf(StaleError);
    await expect(p2).resolves.toBe(2);
    // fn was invoked exactly twice — no additional retry attempts for stale
    expect(fn).toHaveBeenCalledTimes(2);
  });

  // ---------------------------------------------------------------------------
  // 5. timeoutOp — throws TimeoutError for slow functions
  // ---------------------------------------------------------------------------

  it('.pipe(timeoutOp(50)) throws TimeoutError for slow functions', async () => {
    const fn = async () =>
      new Promise<string>((r) => setTimeout(() => r('late'), 300));

    const wrapped = pipeLatest(fn).pipe(timeoutOp(50));
    await expect(wrapped()).rejects.toBeInstanceOf(TimeoutError);
  });

  // ---------------------------------------------------------------------------
  // 6. isTimeout helper
  // ---------------------------------------------------------------------------

  it('isTimeout(err) returns true for TimeoutError and duck-typed objects', () => {
    expect(isTimeout(new TimeoutError())).toBe(true);
    expect(isTimeout(new Error('other'))).toBe(false);
    expect(isTimeout({ code: 'TIMEOUT' })).toBe(true);
    expect(isTimeout(null)).toBe(false);
    expect(isTimeout(undefined)).toBe(false);
  });

  // ---------------------------------------------------------------------------
  // 7. tapOp — onResult callback
  // ---------------------------------------------------------------------------

  it('.pipe(tapOp({ onResult })) fires callback with the resolved value', async () => {
    const onResult = vi.fn();
    const wrapped = pipeLatest(async () => 42).pipe(tapOp({ onResult }));

    const result = await wrapped();

    expect(result).toBe(42);
    expect(onResult).toHaveBeenCalledOnce();
    expect(onResult).toHaveBeenCalledWith(42);
  });

  // ---------------------------------------------------------------------------
  // 8. tapOp — onError callback
  // ---------------------------------------------------------------------------

  it('.pipe(tapOp({ onError })) fires callback on non-stale errors', async () => {
    const onError = vi.fn();
    const boom = new Error('oops');
    const fn = async () => { throw boom; };

    const wrapped = pipeLatest(fn).pipe(tapOp({ onError }));
    await expect(wrapped()).rejects.toThrow('oops');

    expect(onError).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(boom);
  });

  // ---------------------------------------------------------------------------
  // 9. tapOp — onStale callback
  // ---------------------------------------------------------------------------

  it('.pipe(tapOp({ onStale })) fires callback when the call is superseded', async () => {
    const onStale = vi.fn();
    const { promise: first, resolve: resolveFirst } = deferred<void>();

    const fn = vi.fn(async (n: number) => {
      if (n === 1) {
        await first;
        return 1;
      }
      return 2;
    });

    const wrapped = pipeLatest(fn).pipe(tapOp({ onStale }));
    const p1 = wrapped(1);
    const p2 = wrapped(2); // supersedes p1

    resolveFirst();

    await expect(p1).rejects.toBeInstanceOf(StaleError);
    await p2;

    expect(onStale).toHaveBeenCalledOnce();
  });

  // ---------------------------------------------------------------------------
  // 10. catchOp — catches non-stale errors
  // ---------------------------------------------------------------------------

  it('.pipe(catchOp(handler)) catches non-stale errors and returns fallback', async () => {
    const fn = async (): Promise<string> => { throw new Error('caught me'); };
    const wrapped = pipeLatest(fn).pipe(catchOp(() => 'fallback'));

    await expect(wrapped()).resolves.toBe('fallback');
  });

  // ---------------------------------------------------------------------------
  // 11. catchOp — does NOT catch StaleError
  // ---------------------------------------------------------------------------

  it('.pipe(catchOp(handler)) does NOT catch StaleError', async () => {
    const { promise: first, resolve: resolveFirst } = deferred<void>();

    const fn = vi.fn(async (n: number) => {
      if (n === 1) {
        await first;
        return 1;
      }
      return 2;
    });

    const wrapped = pipeLatest(fn).pipe(catchOp(() => 'should not reach this'));
    const p1 = wrapped(1);
    const p2 = wrapped(2);

    resolveFirst();

    await expect(p1).rejects.toBeInstanceOf(StaleError);
    await expect(p2).resolves.toBe(2);
  });

  // ---------------------------------------------------------------------------
  // 12. mapOp — transforms the result
  // ---------------------------------------------------------------------------

  it('.pipe(mapOp(x => x * 2)) transforms the resolved value', async () => {
    const wrapped = pipeLatest(async () => 21).pipe(mapOp((x) => x * 2));
    await expect(wrapped()).resolves.toBe(42);
  });

  // ---------------------------------------------------------------------------
  // 13. throttleOp — drops too-fast calls
  // ---------------------------------------------------------------------------

  it('.pipe(throttleOp(100)) drops calls that arrive before the interval elapses', async () => {
    const fn = async () => 'ok';
    const wrapped = pipeLatest(fn).pipe(throttleOp(100));

    await expect(wrapped()).resolves.toBe('ok'); // first call allowed through
    await expect(wrapped()).rejects.toBeInstanceOf(ThrottledError); // too soon
  });

  // ---------------------------------------------------------------------------
  // 14. collectOp — accumulates stats
  // ---------------------------------------------------------------------------

  it('.pipe(collectOp(stats)) accumulates call, error, and latency statistics', async () => {
    const stats: CollectStats = { calls: 0, stale: 0, errors: 0, latencies: [] };

    const fn = async (n: number) => {
      if (n === 99) throw new Error('boom');
      return n;
    };

    const wrapped = pipeLatest(fn).pipe(collectOp(stats));
    await wrapped(1);
    await wrapped(2);
    await expect(wrapped(99)).rejects.toThrow('boom');

    expect(stats.calls).toBe(3);
    expect(stats.errors).toBe(1);
    expect(stats.stale).toBe(0);
    expect(stats.latencies).toHaveLength(3);
    // Every latency should be a non-negative number
    expect(stats.latencies.every((l) => l >= 0)).toBe(true);
  });

  // ---------------------------------------------------------------------------
  // 15. All operators chained together
  // ---------------------------------------------------------------------------

  it('.pipe(dedupeOp(), retryOp(), timeoutOp()) — all three chained', async () => {
    let callCount = 0;
    const fn = vi.fn(async () => {
      callCount++;
      if (callCount < 2) throw new Error('transient');
      return 'done';
    });

    const wrapped = pipeLatest(fn).pipe(
      dedupeOp(),
      retryOp({ attempts: 2, baseDelayMs: 0, jitterRatio: 0 }),
      timeoutOp(5_000),
    );

    await expect(wrapped()).resolves.toBe('done');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  // ---------------------------------------------------------------------------
  // 16. reset() and current() through the pipe chain
  // ---------------------------------------------------------------------------

  it('reset() makes in-flight calls stale; current() counts invocations', async () => {
    const { promise: slow, resolve: resolveSlow } = deferred<string>();
    const fn = async () => slow;

    const wrapped = pipeLatest(fn).pipe(mapOp((v) => v + '!'));

    expect(wrapped.current()).toBe(0);

    const p = wrapped();
    expect(wrapped.current()).toBe(1);

    wrapped.reset(); // invalidates the in-flight call

    resolveSlow('x'); // fn resolves, but the call was already stale

    await expect(p).rejects.toBeInstanceOf(StaleError);
    // current() still reflects total invocation count (reset doesn't zero it)
    expect(wrapped.current()).toBe(1);
  });

  // ---------------------------------------------------------------------------
  // 17. abortWithOp — propagates external abort
  // ---------------------------------------------------------------------------

  it('abortWithOp(signal) rejects when the external signal is aborted', async () => {
    const controller = new AbortController();
    const { promise: slow, resolve: resolveSlowFn } = deferred<string>();
    const fn = async () => slow;

    const wrapped = pipeLatest(fn).pipe(abortWithOp(controller.signal));
    const p = wrapped();

    controller.abort(new Error('cancelled'));

    await expect(p).rejects.toThrow('cancelled');

    resolveSlowFn('cleanup'); // release the dangling internal promise
  });

  // ---------------------------------------------------------------------------
  // 18. .pipe() returns a new PipeableChain
  // ---------------------------------------------------------------------------

  it('.pipe() returns a new PipeableChain with reset, current, and pipe methods', async () => {
    const chain1 = pipeLatest(async () => 1);
    const chain2 = chain1.pipe(mapOp((x) => x + 1));
    const chain3 = chain2.pipe(mapOp((x) => x * 10));

    // All three are distinct callable objects
    expect(chain1).not.toBe(chain2);
    expect(chain2).not.toBe(chain3);

    // Each exposes the PipeableChain interface
    for (const chain of [chain2, chain3]) {
      expect(typeof chain).toBe('function');
      expect(typeof chain.pipe).toBe('function');
      expect(typeof chain.reset).toBe('function');
      expect(typeof chain.current).toBe('function');
    }

    // Operators compose correctly
    await expect(chain3()).resolves.toBe(20); // (1 + 1) * 10
  });

  // ---------------------------------------------------------------------------
  // 19. logOp — does not modify results
  // ---------------------------------------------------------------------------

  it('logOp does not modify the resolved value or re-throw errors', async () => {
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    const fn = async () => ({ data: 'important' });
    const wrapped = pipeLatest(fn).pipe(logOp('test-prefix'));

    const result = await wrapped();

    expect(result).toEqual({ data: 'important' });
    expect(consoleSpy).toHaveBeenCalled();

    consoleSpy.mockRestore();
  });

  // ---------------------------------------------------------------------------
  // 20. StaleError propagates unchanged through every operator
  // ---------------------------------------------------------------------------

  it('StaleError propagates through all operators without being swallowed or retried', async () => {
    const stats: CollectStats = { calls: 0, stale: 0, errors: 0, latencies: [] };
    const onError = vi.fn();
    const { promise: first, resolve: resolveFirst } = deferred<void>();

    const fn = vi.fn(async (n: number) => {
      if (n === 1) {
        await first;
        return 1;
      }
      return 2;
    });

    // Stack every operator that has special stale-error handling on top of each other
    const wrapped = pipeLatest(fn).pipe(
      collectOp(stats),       // records stale separately from errors
      tapOp({ onError }),     // fires onError only for non-stale errors
      catchOp(() => 'nope'),  // MUST NOT catch StaleError
      retryOp({ attempts: 3, baseDelayMs: 0 }), // MUST NOT retry StaleError
    );

    const p1 = wrapped(1);
    const p2 = wrapped(2); // supersedes p1

    resolveFirst();

    await expect(p1).rejects.toBeInstanceOf(StaleError);
    await expect(p2).resolves.toBe(2);

    // tapOp must not fire onError for stale (it fires onStale, which is not wired here)
    expect(onError).not.toHaveBeenCalled();
    // collectOp tracks the stale call correctly
    expect(stats.stale).toBe(1);
    expect(stats.errors).toBe(0);
    expect(stats.calls).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// isThrottled helper
// ---------------------------------------------------------------------------

describe('isThrottled', () => {
  it('returns true for ThrottledError and duck-typed objects', () => {
    expect(isThrottled(new ThrottledError())).toBe(true);
    expect(isThrottled(new Error('other'))).toBe(false);
    expect(isThrottled({ code: 'THROTTLED' })).toBe(true);
    expect(isThrottled(null)).toBe(false);
    expect(isThrottled(undefined)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// latest re-export
// ---------------------------------------------------------------------------

describe('latest (re-export of pipeLatest)', () => {
  it('is an alias for pipeLatest and returns a full PipeableChain', async () => {
    const fn = async () => 'hello';
    const wrapped = latest(fn);

    await expect(wrapped()).resolves.toBe('hello');
    expect(typeof wrapped.pipe).toBe('function');
    expect(typeof wrapped.reset).toBe('function');
    expect(typeof wrapped.current).toBe('function');
  });
});

// ---------------------------------------------------------------------------
// StaleError / isStale re-exports from index (smoke check)
// ---------------------------------------------------------------------------

describe('StaleError / isStale (from index.js)', () => {
  it('isStale returns true for StaleError instances', () => {
    const err = new StaleError();
    expect(isStale(err)).toBe(true);
    expect(isStale(new Error('plain'))).toBe(false);
    expect(isStale({ code: 'STALE' })).toBe(true);
  });
});
