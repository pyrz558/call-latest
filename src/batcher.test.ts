import { describe, it, expect, vi } from 'vitest';
import { createBatcher } from './batcher.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Silence unhandled-rejection noise for promises we intentionally abandon. */
function sink(p: Promise<unknown>): void {
  void p.catch(() => {});
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('createBatcher', () => {
  it('batches concurrent calls within window', async () => {
    const batchFn = vi.fn(async (keys: string[]) => keys.map((k) => `result:${k}`));
    const batcher = createBatcher(batchFn, { windowMs: 10 });

    const [r1, r2, r3] = await Promise.all([batcher('a'), batcher('b'), batcher('c')]);

    // All three arrived before the window closed → one batch call
    expect(batchFn).toHaveBeenCalledTimes(1);
    expect(batchFn).toHaveBeenCalledWith(['a', 'b', 'c']);
    expect(r1).toBe('result:a');
    expect(r2).toBe('result:b');
    expect(r3).toBe('result:c');
  });

  it('flushes when maxBatchSize is reached before windowMs', async () => {
    // windowMs is effectively infinite; only maxBatchSize should trigger flushes.
    const batchFn = vi.fn(async (keys: string[]) => keys.map((k) => `r:${k}`));
    const batcher = createBatcher(batchFn, { windowMs: 60_000, maxBatchSize: 10 });

    // 100 unique keys — should produce exactly 10 batches of 10.
    const calls = Array.from({ length: 100 }, (_, i) => batcher(`key${i}`));
    await Promise.all(calls);

    expect(batchFn).toHaveBeenCalledTimes(10);
    batchFn.mock.calls.forEach((args) => {
      expect((args[0] as string[]).length).toBe(10);
    });
  });

  it('deduplicates same key within window', async () => {
    const batchFn = vi.fn(async (keys: string[]) => keys.map((k) => `result:${k}`));
    const batcher = createBatcher(batchFn, { windowMs: 10 });

    // 5 concurrent calls with the exact same key
    const results = await Promise.all(
      Array.from({ length: 5 }, () => batcher('sameKey')),
    );

    // batchFn should receive the key exactly once
    expect(batchFn).toHaveBeenCalledTimes(1);
    expect(batchFn).toHaveBeenCalledWith(['sameKey']);

    // Every caller gets the same result
    expect(results).toEqual(Array<string>(5).fill('result:sameKey'));
  });

  it('different keys get independent results', async () => {
    const batchFn = vi.fn(async (keys: string[]) => keys.map((k) => `r:${k}`));
    const batcher = createBatcher(batchFn, { windowMs: 10 });

    const [ra, rb, rc] = await Promise.all([batcher('a'), batcher('b'), batcher('c')]);

    expect(ra).toBe('r:a');
    expect(rb).toBe('r:b');
    expect(rc).toBe('r:c');
  });

  it('flush() forces immediate dispatch without waiting for windowMs', async () => {
    // windowMs is intentionally huge so only flush() drives the dispatch.
    const batchFn = vi.fn(async (keys: string[]) => keys.map((k) => `r:${k}`));
    const batcher = createBatcher(batchFn, { windowMs: 60_000 });

    const p = batcher('a');

    // Explicit flush — should resolve before windowMs ever fires.
    await batcher.flush();

    expect(batchFn).toHaveBeenCalledOnce();
    const result = await p;
    expect(result).toBe('r:a');
  });

  it('pendingCount() tracks items in queue', async () => {
    const batchFn = vi.fn(async (keys: string[]) => keys.map((k) => `r:${k}`));
    const batcher = createBatcher(batchFn, { windowMs: 60_000 });

    sink(batcher('a'));
    sink(batcher('b'));
    sink(batcher('a')); // duplicate — does NOT increase pendingCount

    expect(batcher.pendingCount()).toBe(2); // only 2 unique keys pending

    await batcher.flush();

    expect(batcher.pendingCount()).toBe(0);
  });

  it('stats() tracks batches and savings correctly', async () => {
    const batchFn = vi.fn(async (keys: string[]) => keys.map((k) => `r:${k}`));
    const batcher = createBatcher(batchFn, { windowMs: 10, maxBatchSize: 100 });

    // 4 calls: 'a' twice (dedup), 'b', 'c' — results in 1 batch of 3 unique keys.
    await Promise.all([batcher('a'), batcher('a'), batcher('b'), batcher('c')]);

    const s = batcher.stats();
    expect(s.totalCalls).toBe(4);
    expect(s.totalBatches).toBe(1);
    expect(s.avgBatchSize).toBe(3);   // 3 unique keys / 1 batch
    expect(s.savedCalls).toBe(1);     // the second 'a' was served from cache
    expect(s.windowMs).toBe(10);
    expect(s.maxBatchSize).toBe(100);
  });

  it('batchFn error propagates to all callers in batch', async () => {
    const batchError = new Error('batch failed');
    const batchFn = vi.fn(async (_keys: string[]) => {
      throw batchError;
    });
    const batcher = createBatcher<string, string>(batchFn, { windowMs: 10 });

    const results = await Promise.allSettled([batcher('a'), batcher('b'), batcher('c')]);

    // Every caller in the batch should receive the same rejection.
    expect(results).toHaveLength(3);
    results.forEach((r) => {
      expect(r.status).toBe('rejected');
      expect((r as PromiseRejectedResult).reason).toBe(batchError);
    });

    // batchFn was called once despite 3 callers.
    expect(batchFn).toHaveBeenCalledTimes(1);
  });

  it('destroy() rejects all pending calls', async () => {
    const batchFn = vi.fn(async (keys: string[]) => keys.map((k) => `r:${k}`));
    // Large windowMs ensures the timer never fires on its own during the test.
    const batcher = createBatcher(batchFn, { windowMs: 60_000 });

    const p1 = batcher('a');
    const p2 = batcher('b');

    // Destroy immediately — both promises should reject.
    batcher.destroy();

    await expect(p1).rejects.toThrow('Batcher destroyed');
    await expect(p2).rejects.toThrow('Batcher destroyed');

    // batchFn must never have been invoked.
    expect(batchFn).not.toHaveBeenCalled();
  });

  it('large volume: 10K calls with 50 unique keys', async () => {
    const batchFn = vi.fn(async (keys: string[]) => keys.map((k) => `r:${k}`));
    // windowMs=20 lets the timer flush; maxBatchSize=100 won't cap the 50-key batch.
    const batcher = createBatcher(batchFn, { windowMs: 20, maxBatchSize: 100 });

    // All 10 000 calls are synchronous — they all land in the same window.
    const calls = Array.from({ length: 10_000 }, (_, i) => batcher(`key${i % 50}`));
    const results = await Promise.all(calls);

    // Every caller must receive a result.
    expect(results).toHaveLength(10_000);

    // Spot-check: same key index → same result string.
    for (let i = 0; i < 10_000; i += 500) {
      expect(results[i]).toBe(`r:key${i % 50}`);
    }

    // All calls happened synchronously before any timer fired, so there
    // are at most 50 unique keys queued — batchFn should have been called
    // a very small number of times (≤ 5 in practice; 1 is typical).
    expect(batchFn.mock.calls.length).toBeLessThan(5);

    // 10 000 total calls, 50 unique per window → 9 950 were saved.
    const s = batcher.stats();
    expect(s.totalCalls).toBe(10_000);
    expect(s.savedCalls).toBe(9_950);
    expect(s.avgBatchSize).toBeLessThanOrEqual(50);
  });
});
