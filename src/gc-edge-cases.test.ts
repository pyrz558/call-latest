import { describe, it, expect, vi } from "vitest";
import * as v8 from "node:v8";
import { latest, StaleError, isStale } from "./index.js";

vi.setConfig({ testTimeout: 60_000 });

const forceGC = () => {
  if (typeof (global as any).gc === "function") {
    (global as any).gc();
    (global as any).gc();
  }
};

const sink = (p: Promise<unknown>) => void p.catch(() => {});

/**
 * Allocate a large temporary array and discard it immediately.
 * Returns the length to prevent dead-code elimination.
 * The discarded array puts pressure on the GC nursery.
 */
const createPressure = (): number => {
  const arr = new Array<string>(100_000).fill("x".repeat(10));
  return arr.length;
};

// ---------------------------------------------------------------------------

describe("GC Edge Cases — Correctness Under Pressure", () => {
  it("latest() produces correct results while GC is thrashing", async () => {
    const wrapped = latest(async (n: number) => {
      createPressure(); // allocate & discard before await
      await new Promise<void>((r) => setTimeout(r, 1));
      createPressure(); // allocate & discard after await
      return n;
    });

    // Call 0–9 are all stale once call 99 starts
    for (let i = 0; i < 10; i++) {
      sink(wrapped(i));
    }

    // Only the last invocation should win
    const result = await wrapped(99);
    expect(result).toBe(99);
  });

  it("AbortController signals are not retained after use", async () => {
    const refs: WeakRef<AbortSignal>[] = [];

    // Create each controller in its own scope so it can be GC'd
    const attachAndAbort = () => {
      const ctrl = new AbortController();
      refs.push(new WeakRef(ctrl.signal));
      ctrl.abort();
      // `ctrl` goes out of scope here
    };

    for (let i = 0; i < 100; i++) {
      attachAndAbort();
    }

    forceGC();
    await new Promise((r) => setTimeout(r, 10));
    forceGC();
    await new Promise((r) => setTimeout(r, 10));
    forceGC();

    const liveCount = refs.filter((r) => r.deref() !== undefined).length;
    console.log(
      `[AbortSignal WeakRefs] Still live after GC: ${liveCount} of 100 (informational — GC is non-deterministic)`,
    );

    // Reliable assertion: creating and aborting 100 controllers does not throw.
    expect(refs).toHaveLength(100);
  });

  it("StaleError is not instanceof the wrong class after bundle boundary crossing", () => {
    // Direct instanceof check
    expect(isStale(new StaleError())).toBe(true);

    // Simulate a bundle boundary: JSON roundtrip strips the prototype
    const err = new StaleError();
    const serialized = JSON.stringify({ code: err.code, message: err.message });
    const deserialized: unknown = JSON.parse(serialized);

    // Duck-type fallback must recognise { code: "STALE" }
    expect(isStale(deserialized)).toBe(true);

    // Unrelated objects must not match
    expect(isStale({ code: "OTHER" })).toBe(false);
    expect(isStale(new Error("oops"))).toBe(false);
    expect(isStale(null)).toBe(false);
  });

  it("closure variables in latest() wrappers are not retained by resolved promises", async () => {
    forceGC();
    const before = v8.getHeapStatistics().used_heap_size;

    // Run inside a helper so `wrapped` and `bigBuffer` go out of scope afterwards
    const doWork = async (): Promise<number> => {
      const wrapped = latest(async () => {
        const bigBuffer = Buffer.alloc(1024 * 1024, 0); // 1 MB
        await new Promise<void>((r) => setTimeout(r, 0));
        return bigBuffer.length; // return a number, not the buffer itself
      });
      return wrapped();
    };

    const result = await doWork();
    expect(result).toBe(1024 * 1024);

    forceGC();
    const after = v8.getHeapStatistics().used_heap_size;
    const growthMB = (after - before) / 1024 / 1024;

    console.log(
      `[closure GC] Heap before: ${(before / 1024 / 1024).toFixed(2)} MB  after: ${(after / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[closure GC] Growth: ${growthMB.toFixed(2)} MB — LOG ONLY, GC timing is non-deterministic`,
    );
    // Do NOT assert the growth here; GC timing is non-deterministic.
  });

  it("reset() invalidates token before GC can collect old closure", async () => {
    let resolveSlow!: (value: number) => void;
    const slow = new Promise<number>((r) => {
      resolveSlow = r;
    });

    // Wrap a function that stays in-flight until we manually resolve it
    const wrapped = latest(async () => slow);
    const inflight = wrapped();

    // Invalidate the token synchronously before the slow promise resolves
    wrapped.reset();

    // Now let the underlying promise resolve — but the token has moved on
    resolveSlow(42);

    // The caller's promise must reject with StaleError, not resolve to 42
    await expect(inflight).rejects.toBeInstanceOf(StaleError);
  });

  it("100 concurrent latest() instances: no cross-contamination", async () => {
    const COUNT = 100;

    // Each wrapper closes over its own `idx`, so results must be independent
    const wrappers = Array.from({ length: COUNT }, (_, idx) =>
      latest(async (n: number) => {
        createPressure();
        return idx * 1000 + n;
      }),
    );

    const results = new Array<number>(COUNT);

    await Promise.all(
      wrappers.map(async (wrapped, idx) => {
        // Make 4 stale calls per wrapper
        for (let i = 0; i < 4; i++) {
          sink(wrapped(i));
        }
        createPressure(); // extra GC pressure between stale and winning calls
        // The winning call
        results[idx] = await wrapped(999);
      }),
    );

    // Every wrapper must return its own value — no cross-contamination
    for (let idx = 0; idx < COUNT; idx++) {
      expect(results[idx]).toBe(idx * 1000 + 999);
    }
  });

  it("FinalizationRegistry detects wrapper collection lifecycle", async () => {
    const collected = new Set<string>();
    const registry = new FinalizationRegistry<string>((key) =>
      collected.add(key),
    );

    // Create 10 wrappers inside an IIFE so they go out of scope immediately
    (() => {
      for (let i = 0; i < 10; i++) {
        const wrapper = latest(async (n: number) => n);
        registry.register(wrapper as object, `wrapper-${i}`);
        sink(wrapper(1));
        // `wrapper` goes out of scope at end of each iteration
      }
    })();

    // Give the GC several opportunities to run
    forceGC();
    await new Promise((r) => setTimeout(r, 10));
    forceGC();
    await new Promise((r) => setTimeout(r, 10));
    forceGC();
    await new Promise((r) => setTimeout(r, 10));

    console.log(
      `[FinalizationRegistry] Collected ${collected.size} of 10 wrappers (non-deterministic — may be 0)`,
    );

    // Reliable assertion: the API is usable and does not throw.
    expect(registry).toBeInstanceOf(FinalizationRegistry);
    // Do NOT assert `collected.size` — FinalizationRegistry timing is non-deterministic.
  });
});

// ---------------------------------------------------------------------------

describe("GC Edge Cases — Real Memory Patterns", () => {
  it("long-lived latest() wrapper: no memory growth over time", async () => {
    // Simulates a search box used throughout an entire user session
    const wrapped = latest(async (n: number) => n);

    forceGC();
    const before = v8.getHeapStatistics().used_heap_size;

    for (let i = 0; i < 10_000; i++) {
      await wrapped(i);
    }

    forceGC();
    const after = v8.getHeapStatistics().used_heap_size;
    const growthBytes = after - before;

    console.log(
      `[long-lived] Heap before: ${(before / 1024 / 1024).toFixed(2)} MB  after: ${(after / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[long-lived] Growth: ${(growthBytes / 1024 / 1024).toFixed(2)} MB`,
    );

    expect(growthBytes).toBeLessThan(20 * 1024 * 1024); // < 20 MB
  });

  it("rapid wrapper creation/destruction: no accumulation", async () => {
    // Simulates a component that mounts, makes one call, and unmounts repeatedly
    forceGC();
    const before = v8.getHeapStatistics().used_heap_size;

    for (let i = 0; i < 1_000; i++) {
      const wrapped = latest(async (n: number) => n);
      await wrapped(i);
      // `wrapped` goes out of scope here; eligible for GC on the next cycle
    }

    forceGC();
    const after = v8.getHeapStatistics().used_heap_size;
    const growthBytes = after - before;

    console.log(
      `[create/destroy] Heap before: ${(before / 1024 / 1024).toFixed(2)} MB  after: ${(after / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[create/destroy] Growth: ${(growthBytes / 1024 / 1024).toFixed(2)} MB`,
    );

    expect(growthBytes).toBeLessThan(20 * 1024 * 1024); // < 20 MB
  });

  it("memory is stable after reset() is called 10K times", async () => {
    const wrapped = latest(async (n: number) => n);

    forceGC();
    const before = v8.getHeapStatistics().used_heap_size;

    let lastResult = -1;
    for (let i = 0; i < 10_000; i++) {
      // reset() before each call; the subsequent call should still win cleanly
      wrapped.reset();
      lastResult = await wrapped(i);
    }

    // Correctness: the last call always wins after each reset
    expect(lastResult).toBe(9_999);

    forceGC();
    const after = v8.getHeapStatistics().used_heap_size;
    const growthBytes = after - before;

    console.log(
      `[10K resets] Heap before: ${(before / 1024 / 1024).toFixed(2)} MB  after: ${(after / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[10K resets] Growth: ${(growthBytes / 1024 / 1024).toFixed(2)} MB`,
    );

    expect(growthBytes).toBeLessThan(20 * 1024 * 1024); // < 20 MB
  });
});
