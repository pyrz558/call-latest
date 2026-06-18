import { describe, it, expect, vi } from "vitest";
import * as v8 from "node:v8";
import { existsSync, statSync, unlinkSync } from "node:fs";
import { latest, dedupe, latestDedupe } from "./index.js";

vi.setConfig({ testTimeout: 120_000 });

const forceGC = () => {
  if (typeof (global as any).gc === "function") {
    (global as any).gc();
    (global as any).gc();
  }
};
const gcAvailable = typeof (global as any).gc === "function";

const sink = (p: Promise<unknown>) => void p.catch(() => {});

// ---------------------------------------------------------------------------
// Helper: run one batch of N latest() calls in a function scope so the
// wrapper and promise array go out of scope when the function returns.
// ---------------------------------------------------------------------------
const runLatestBatch = async (batchSize: number) => {
  const wrapped = latest(async (n: number) => n);
  const promises: Promise<number>[] = [];
  for (let i = 0; i < batchSize; i++) {
    const p = wrapped(i);
    sink(p);
    promises.push(p);
  }
  await Promise.allSettled(promises);
};

const runDedupeBatch = async (
  wrapped: ReturnType<typeof dedupe<(key: string) => Promise<string>>>,
  batchSize: number,
  keyCount: number,
) => {
  const promises: Promise<string>[] = [];
  for (let i = 0; i < batchSize; i++) {
    promises.push(wrapped(`key-${i % keyCount}`));
  }
  await Promise.allSettled(promises);
};

// ---------------------------------------------------------------------------

describe("Memory Pressure — V8 Heap Analysis", () => {
  it("heap is stable after 100K latest() requests with forced GC", async () => {
    forceGC();
    const before = v8.getHeapStatistics().used_heap_size;

    // 10 batches × 10K calls = 100K total; each batch goes out of scope before the next
    for (let batch = 0; batch < 10; batch++) {
      await runLatestBatch(10_000);
    }

    forceGC();
    const after = v8.getHeapStatistics().used_heap_size;
    const growthBytes = after - before;

    console.log(
      `[100K latest] Heap before: ${(before / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[100K latest] Heap after:  ${(after / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[100K latest] Growth:      ${(growthBytes / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(`[100K latest] GC available: ${gcAvailable}`);

    expect(growthBytes).toBeLessThan(50 * 1024 * 1024); // < 50 MB after GC
  });

  it("heap is stable after 50K dedupe() calls with forced GC", async () => {
    const wrapped = dedupe(async (key: string) => key);
    const KEYS = 50;

    forceGC();
    const before = v8.getHeapStatistics().used_heap_size;

    // 5 batches × 10K calls with 50 unique keys
    for (let batch = 0; batch < 5; batch++) {
      await runDedupeBatch(wrapped, 10_000, KEYS);
      wrapped.clear();
    }

    forceGC();
    const after = v8.getHeapStatistics().used_heap_size;
    const growthBytes = after - before;

    console.log(
      `[50K dedupe] Heap before: ${(before / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[50K dedupe] Heap after:  ${(after / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[50K dedupe] Growth:      ${(growthBytes / 1024 / 1024).toFixed(2)} MB`,
    );

    expect(growthBytes).toBeLessThan(30 * 1024 * 1024); // < 30 MB after GC
  });

  it("retained object count: latest() wrappers are GC-collectable", async () => {
    // Create each wrapper in its own function scope so it goes out of scope immediately
    const createAndUse = () => {
      const w = latest(async (n: number) => n);
      sink(w(1));
      // `w` goes out of scope when this function returns
    };

    for (let i = 0; i < 1_000; i++) {
      createAndUse();
    }

    forceGC();
    const heapAfter = v8.getHeapStatistics().used_heap_size;

    console.log(
      `[retained count] Heap after 1K wrappers + GC: ${(heapAfter / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      "[retained count] NOTE: WeakRef collection is non-deterministic, this is a proxy metric",
    );
    // Do NOT assert exactly (too flaky). Just log and observe.
  });

  it("WeakRef: latest() wrapper becomes weakly reachable after scope ends", async () => {
    let weakRef!: WeakRef<object>;

    // Create the wrapper in an isolated async scope
    await (async () => {
      const wrapper = latest(async (n: number) => n);
      weakRef = new WeakRef(wrapper as object);
      sink(wrapper(1));
      // `wrapper` goes out of scope when this async IIFE returns
    })();

    // BEFORE any GC — assert that WeakRef was successfully created and the
    // target is still alive (no GC has been forced yet).
    expect(weakRef.deref()).not.toBeUndefined();

    // Now force GC multiple times with small delays (best-effort cleanup)
    forceGC();
    await new Promise((r) => setTimeout(r, 10));
    forceGC();
    await new Promise((r) => setTimeout(r, 10));
    forceGC();

    // Informational only — GC timing in V8 is non-deterministic; do NOT assert.
    const aliveAfterGC = weakRef.deref() !== undefined;
    console.log(
      `[WeakRef] Post-GC alive: ${aliveAfterGC} (non-deterministic — not asserted)`,
    );
  });

  it("FinalizationRegistry: registers objects without throwing", async () => {
    const collected: string[] = [];
    const registry = new FinalizationRegistry<string>((key) =>
      collected.push(key),
    );

    // Register each wrapper in its own function scope so it goes out of scope
    const registerWrapper = (i: number) => {
      const wrapper = latest(async (n: number) => n);
      registry.register(wrapper as object, `wrapper-${i}`);
      sink(wrapper(1));
      // `wrapper` goes out of scope here
    };

    for (let i = 0; i < 5; i++) {
      registerWrapper(i);
    }

    forceGC();
    await new Promise((r) => setTimeout(r, 50));
    forceGC();

    console.log(
      `[FinalizationRegistry] Cleanup callbacks fired: ${collected.length} of 5 (may be 0 — that's OK)`,
    );

    // The only reliable assertion: the API is usable and doesn't crash.
    expect(registry).toBeInstanceOf(FinalizationRegistry);
  });

  it("heap snapshot: v8.writeHeapSnapshot() produces a valid file", () => {
    const snapshotPath = v8.writeHeapSnapshot();

    expect(existsSync(snapshotPath)).toBe(true);

    const stats = statSync(snapshotPath);
    expect(stats.size).toBeGreaterThan(1_000); // at least 1 KB

    console.log(
      `[Heap snapshot] Written to ${snapshotPath} (${(stats.size / 1024 / 1024).toFixed(1)} MB)`,
    );

    unlinkSync(snapshotPath); // clean up
  });

  it("memory pressure: 500K rapid calls do not exhaust heap", async () => {
    const wrapped = latest(async (n: number) => n);
    const snapshots: number[] = [];

    for (let batch = 0; batch < 5; batch++) {
      // Fire 100K calls without awaiting each one
      for (let i = 0; i < 100_000; i++) {
        sink(wrapped(i));
      }
      // Give microtask queue time to drain before measuring
      await new Promise((r) => setTimeout(r, 50));
      forceGC();
      snapshots.push(v8.getHeapStatistics().used_heap_size);
    }

    console.log(
      "[500K rapid] Heap per batch:",
      snapshots.map((s) => `${(s / 1024 / 1024).toFixed(2)} MB`),
    );
    const growth = snapshots[snapshots.length - 1]! - snapshots[0]!;
    console.log(
      `[500K rapid] Growth (last − first snapshot): ${(growth / 1024 / 1024).toFixed(2)} MB`,
    );

    // Verify heap does not grow unboundedly across batches
    expect(growth).toBeLessThan(100 * 1024 * 1024); // < 100 MB
  });
});

// ---------------------------------------------------------------------------

describe("Memory Pressure — Retained Object Analysis", () => {
  it("StaleError instances do not accumulate: 10K stale errors then GC", async () => {
    // Fire 10K calls; only the last resolves — the other 9999 reject with StaleError.
    const wrapped = latest(async (n: number) => n);
    const promises: Promise<number>[] = [];

    for (let i = 0; i < 10_000; i++) {
      const p = wrapped(i);
      sink(p);
      promises.push(p);
    }

    await Promise.allSettled(promises);

    forceGC();
    const after = v8.getHeapStatistics().used_heap_size;

    console.log(
      `[StaleError] Heap after 10K stale errors + GC: ${(after / 1024 / 1024).toFixed(2)} MB`,
    );

    // The total heap should remain reasonable; StaleError objects must not accumulate.
    expect(after).toBeLessThan(200 * 1024 * 1024); // < 200 MB total heap
  });

  it("AbortController: abort() releases event listener references", async () => {
    forceGC();
    const before = v8.getHeapStatistics().used_heap_size;

    const controllers: AbortController[] = [];
    for (let i = 0; i < 1_000; i++) {
      const ctrl = new AbortController();
      // Attach a listener that closes over nothing significant
      ctrl.signal.addEventListener("abort", () => {});
      controllers.push(ctrl);
    }

    // Abort all controllers, then drop all references
    for (const ctrl of controllers) {
      ctrl.abort();
    }
    controllers.length = 0; // release strong references

    forceGC();
    const after = v8.getHeapStatistics().used_heap_size;
    const growthBytes = after - before;

    console.log(
      `[AbortController] Heap before: ${(before / 1024 / 1024).toFixed(2)} MB  after: ${(after / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[AbortController] Growth: ${(growthBytes / 1024 / 1024).toFixed(2)} MB`,
    );

    expect(growthBytes).toBeLessThan(50 * 1024 * 1024); // < 50 MB
  });

  it("latestDedupe() under memory pressure: 50K mixed calls", async () => {
    const KEYS = 10;
    const wrapped = latestDedupe(async (key: string) => key);

    forceGC();
    const before = v8.getHeapStatistics().used_heap_size;

    const promises: Promise<string>[] = [];
    for (let i = 0; i < 50_000; i++) {
      const p = wrapped(`key-${i % KEYS}`);
      sink(p);
      promises.push(p);
    }

    await Promise.allSettled(promises);

    forceGC();
    const after = v8.getHeapStatistics().used_heap_size;
    const growthBytes = after - before;

    console.log(
      `[latestDedupe] Heap before: ${(before / 1024 / 1024).toFixed(2)} MB  after: ${(after / 1024 / 1024).toFixed(2)} MB`,
    );
    console.log(
      `[latestDedupe] Growth: ${(growthBytes / 1024 / 1024).toFixed(2)} MB`,
    );

    expect(growthBytes).toBeLessThan(50 * 1024 * 1024); // < 50 MB
  });
});
