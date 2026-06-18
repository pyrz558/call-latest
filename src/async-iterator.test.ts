import { describe, it, expect, vi } from 'vitest';
import {
  LatestStream,
  createLatestStream,
  fromEvents,
  type StreamOptions,
} from './async-iterator.js';

// ─── Helpers ─────────────────────────────────────────────────────────────────

/** Resolves after one macrotask — lets all pending microtasks flush first. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

/** Sleep for `ms` milliseconds. */
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Collect up to `max` values from `stream` then close it. */
async function collect<T>(stream: LatestStream<any, T>, max: number): Promise<T[]> {
  const results: T[] = [];
  for await (const value of stream) {
    results.push(value);
    if (results.length >= max) {
      stream.close();
    }
  }
  return results;
}

// ─── LatestStream ────────────────────────────────────────────────────────────

describe('LatestStream', () => {
  it('for await receives results from push()', async () => {
    const fn = async (v: string) => v + '!';
    const stream = createLatestStream(fn);

    const iterPromise = collect(stream, 3);

    // Push one at a time, waiting for each to resolve before the next push,
    // so each call is the "latest" at the time it resolves.
    stream.push('a'); await tick();
    stream.push('b'); await tick();
    stream.push('c'); await tick();

    const results = await iterPromise;
    expect(results).toEqual(['a!', 'b!', 'c!']);
  });

  it('only latest push result appears when multiple arrive before consumer', async () => {
    // slow=100ms pushed first, fast=10ms pushed second
    // 'fast' is token=2; 'slow' will be stale when it resolves at 100ms
    const fn = async (label: string, delayMs: number) => {
      await sleep(delayMs);
      return `${label}-result`;
    };

    const stream = createLatestStream(fn);

    stream.push('slow', 100);
    stream.push('fast', 10);

    // Wait for both to settle
    await sleep(150);
    stream.close();

    const results: string[] = [];
    for await (const r of stream) {
      results.push(r);
    }

    expect(results).toEqual(['fast-result']);
    // 'slow-result' must never appear
    expect(results.includes('slow-result')).toBe(false);
  });

  it('switchMap: new push cancels previous in-flight call', async () => {
    const fn = async (v: number) => {
      await sleep(30);
      return v;
    };

    const stream = createLatestStream(fn);

    // Fire 5 calls in rapid succession; only the last (4) should survive
    for (let i = 0; i < 5; i++) {
      stream.push(i);
    }

    await sleep(80);
    stream.close();

    const results: number[] = [];
    for await (const r of stream) {
      results.push(r);
    }

    expect(results).toHaveLength(1);
    expect(results[0]).toBe(4);
  });

  it('close() stops the stream after buffered results are drained', async () => {
    const fn = async (v: number) => v * 10;
    const stream = createLatestStream(fn);

    // Push three values sequentially so each resolves and buffers before next push
    stream.push(1); await tick();
    stream.push(2); await tick();
    stream.push(3); await tick();

    // All three are now buffered; close the stream
    stream.close();

    const results: number[] = [];
    for await (const r of stream) {
      results.push(r);
    }

    expect(results).toEqual([10, 20, 30]);
  });

  it('external AbortSignal closes the stream', async () => {
    const controller = new AbortController();
    const fn = async (v: number) => v;
    const stream = createLatestStream(fn, { signal: controller.signal });

    stream.push(1); await tick();
    stream.push(2); await tick();

    // At this point both results are buffered; abort closes the stream
    controller.abort();

    const results: number[] = [];
    for await (const r of stream) {
      results.push(r);
    }

    expect(results).toContain(1);
    expect(results).toContain(2);
    expect(stream.isOpen()).toBe(false);
  });

  it('real errors propagate to for await', async () => {
    const boom = new Error('network failure');
    const fn = async (v: number) => {
      if (v === 0) throw boom;
      return v;
    };

    const stream = createLatestStream(fn);
    stream.push(0);

    await expect(async () => {
      for await (const _ of stream) {
        // body intentionally empty
      }
    }).rejects.toThrow('network failure');
  });

  it('stale errors do NOT propagate to for await', async () => {
    // All 5 pushes have the same delay; only the last token survives
    const fn = async (v: number) => {
      await sleep(30);
      return v;
    };

    const stream = createLatestStream(fn);

    for (let i = 0; i < 5; i++) {
      stream.push(i);
    }

    await sleep(80);
    stream.close();

    let caughtError: unknown = null;
    const results: number[] = [];

    try {
      for await (const r of stream) {
        results.push(r);
      }
    } catch (e) {
      caughtError = e;
    }

    expect(caughtError).toBeNull();
    expect(results).toEqual([4]);
  });

  it('pendingCount() reflects buffered item count', async () => {
    const fn = async (v: number) => v;
    const stream = createLatestStream(fn);

    expect(stream.pendingCount()).toBe(0);

    stream.push(1); await tick();
    expect(stream.pendingCount()).toBe(1);

    stream.push(2); await tick();
    expect(stream.pendingCount()).toBe(2);

    stream.close();

    const results: number[] = [];
    for await (const r of stream) {
      results.push(r);
    }

    expect(results).toEqual([1, 2]);
    expect(stream.pendingCount()).toBe(0);
  });

  it('bufferSize limit: drops oldest when buffer is full', async () => {
    const dropped: string[] = [];
    const fn = async (v: string) => v + '!';
    const stream = createLatestStream(fn, {
      bufferSize: 3,
      onDropped: (v) => dropped.push(v),
    });

    // Push sequentially so each resolves before next; buffer fills up
    stream.push('a'); await tick(); // buffer: ['a!']
    stream.push('b'); await tick(); // buffer: ['a!', 'b!']
    stream.push('c'); await tick(); // buffer: ['a!', 'b!', 'c!'] — full
    stream.push('d'); await tick(); // buffer: ['b!', 'c!', 'd!'] (drops 'a!')
    stream.push('e'); await tick(); // buffer: ['c!', 'd!', 'e!'] (drops 'b!')

    stream.close();

    const results: string[] = [];
    for await (const r of stream) {
      results.push(r);
    }

    expect(results).toEqual(['c!', 'd!', 'e!']);
    expect(dropped).toEqual(['a!', 'b!']);
  });

  it('createLatestStream factory function works', async () => {
    const fn = async (x: number) => x ** 2;
    const stream = createLatestStream(fn);

    stream.push(4); await tick();
    stream.close();

    const results: number[] = [];
    for await (const r of stream) {
      results.push(r);
    }

    expect(results).toEqual([16]);
  });

  it('isOpen() returns true initially and false after close()', () => {
    const stream = createLatestStream(async (v: number) => v);
    expect(stream.isOpen()).toBe(true);
    stream.close();
    expect(stream.isOpen()).toBe(false);
  });

  it('push() is a no-op after close()', async () => {
    const fn = vi.fn(async (v: number) => v);
    const stream = createLatestStream(fn);

    stream.close();
    stream.push(1);
    stream.push(2);

    await tick();

    expect(fn).not.toHaveBeenCalled();
  });

  it('fromEvents utility creates stream from event emitter pattern', async () => {
    type Handler = (...args: [string]) => void;
    const listeners = new Map<string, Handler[]>();

    const mockEmitter = {
      on(event: string, handler: Handler) {
        if (!listeners.has(event)) listeners.set(event, []);
        listeners.get(event)!.push(handler);
      },
      off(event: string, handler: Handler) {
        const list = listeners.get(event) ?? [];
        const idx = list.indexOf(handler);
        if (idx >= 0) list.splice(idx, 1);
      },
    };

    const fn = async (value: string) => `processed:${value}`;
    const stream = fromEvents(mockEmitter, 'data', fn);

    const iterPromise = collect(stream, 2);

    // Emit the first event and let it resolve before emitting the second
    for (const h of listeners.get('data') ?? []) h('hello');
    await tick();
    for (const h of listeners.get('data') ?? []) h('world');
    await tick();

    const results = await iterPromise;
    expect(results).toEqual(['processed:hello', 'processed:world']);

    // After close, the listener must be removed
    expect(listeners.get('data')).toHaveLength(0);
  });
});

// ─── LatestStream — real async scenario ──────────────────────────────────────

describe('LatestStream — real async scenario', () => {
  it('search autocomplete: r→re→rea→reac→react, only react appears', async () => {
    // Each query has an inverse delay so "react" resolves first
    const delays: Record<string, number> = {
      r:     200,
      re:    160,
      rea:   120,
      reac:   80,
      react:  10,
    };

    const mockSearch = async (query: string) => {
      await sleep(delays[query] ?? 50);
      return `result:${query}`;
    };

    const stream = createLatestStream(mockSearch);

    // Simulate rapid keystroke progression
    stream.push('r');
    stream.push('re');
    stream.push('rea');
    stream.push('reac');
    stream.push('react');

    // Wait long enough for all promises to settle (slowest = 200ms)
    await sleep(250);
    stream.close();

    const results: string[] = [];
    for await (const r of stream) {
      results.push(r);
    }

    // Only 'react' resolves without being stale — the four earlier pushes are
    // superseded and their results are silently dropped
    expect(results).toHaveLength(1);
    expect(results[0]).toBe('result:react');
  });

  it('bufferSize=0 drops all results when no consumer is waiting', async () => {
    const dropped: number[] = [];
    const fn = async (v: number) => v;
    const stream = createLatestStream(fn, { bufferSize: 0, onDropped: (v) => dropped.push(v) });

    // Resolve before any consumer starts
    stream.push(1); await tick();
    stream.push(2); await tick();

    stream.close();

    const results: number[] = [];
    for await (const r of stream) {
      results.push(r);
    }

    // Nothing buffered → nothing delivered
    expect(results).toHaveLength(0);
    expect(dropped).toEqual([1, 2]);
  });

  it('concurrent multiple iterators share the same buffer queue', async () => {
    const fn = async (v: number) => v;
    const stream = createLatestStream(fn);

    stream.push(1); await tick();
    stream.close();

    // First iterator drains the single buffered value
    const iter1 = stream[Symbol.asyncIterator]();
    const first = await iter1.next();
    expect(first).toEqual({ value: 1, done: false });

    // Second iterator sees the stream as exhausted
    const iter2 = stream[Symbol.asyncIterator]();
    const second = await iter2.next();
    expect(second.done).toBe(true);
  });

  it('breaking out of for await (early return) closes the stream', async () => {
    const fn = vi.fn(async (v: number) => v);
    const stream = createLatestStream(fn);

    stream.push(1); await tick();
    stream.push(2); await tick();
    stream.push(3); await tick();

    // Break after first value
    for await (const _ of stream) {
      break;
    }

    expect(stream.isOpen()).toBe(false);
  });

  it('error does not prevent subsequent next() calls from returning done', async () => {
    const fn = async (v: number) => {
      if (v === 99) throw new Error('oops');
      return v;
    };

    const stream = createLatestStream(fn);
    stream.push(99);

    const iter = stream[Symbol.asyncIterator]();

    // First next() should reject
    await expect(iter.next()).rejects.toThrow('oops');

    // Subsequent next() calls return done (stream closed after error)
    const result = await iter.next();
    expect(result.done).toBe(true);
  });
});
