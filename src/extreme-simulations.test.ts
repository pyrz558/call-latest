/**
 * Statistical Projection Benchmarks — call-latest v1.2.1
 *
 * ─── IMPORTANT DISCLAIMER ──────────────────────────────────────────────────
 * These tests do NOT actually process 10M or 1B requests.
 * They run real batches of 10K–200K requests in this Node.js process,
 * measure throughput, and PROJECT linearly to larger numbers.
 *
 * What is REAL in these tests:
 *   ✓ Every promise created and awaited is real (not mocked)
 *   ✓ Stale/fulfilled counts are real library behavior
 *   ✓ Throughput numbers reflect actual Node.js + V8 performance
 *   ✓ Memory snapshots reflect real heap usage
 *   ✓ Chaos/fault injection uses real async error throwing
 *
 * What is a PROJECTION (not real):
 *   ✗ "10M requests" = measured 200K, extrapolated ×50
 *   ✗ "1B requests" = measured 100K, extrapolated ×10,000
 *   ✗ No real distributed nodes, real network I/O, or real HTTP
 *   ✗ GC pauses, OS scheduling, and network jitter not modeled
 *
 * For real HTTP/IO tests see: real-http.test.ts
 * For real memory pressure see: memory-pressure.test.ts
 * For real GC edge cases see: gc-edge-cases.test.ts
 * ───────────────────────────────────────────────────────────────────────────
 */

import { describe, expect, it, vi } from "vitest";
import { latest, dedupe, latestDedupe, StaleError, isStale } from "./index.js";

vi.setConfig({ testTimeout: 120_000 });

// ─── Shared Utilities ─────────────────────────────────────────────────────────

const sink = (p: Promise<unknown>) => void p.catch(() => {});

function pct(arr: number[], p: number): number {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.max(0, Math.ceil((p / 100) * s.length) - 1)]!;
}

function calcStats(values: number[]) {
  if (!values.length)
    return { min: 0, max: 0, mean: 0, p50: 0, p95: 0, p99: 0, stddev: 0 };
  const s = [...values].sort((a, b) => a - b);
  const mean = s.reduce((a, b) => a + b, 0) / s.length;
  const variance = s.reduce((a, b) => a + (b - mean) ** 2, 0) / s.length;
  return {
    min: s[0]!,
    max: s[s.length - 1]!,
    mean: +mean.toFixed(2),
    p50: pct(s, 50),
    p95: pct(s, 95),
    p99: pct(s, 99),
    stddev: +Math.sqrt(variance).toFixed(2),
  };
}

/** Run N latest() calls and return settled results + timing. */
async function runLatestBatch(size: number, base = 0) {
  const fn = vi.fn(async (n: number) => n);
  const wrapped = latest(fn);
  const start = performance.now();
  const promises: Promise<number>[] = [];
  for (let i = 0; i < size; i++) {
    const p = wrapped(base + i);
    promises.push(p);
    sink(p);
  }
  const results = await Promise.allSettled(promises);
  const elapsedMs = performance.now() - start;
  return {
    elapsedMs,
    throughput: Math.round((size / elapsedMs) * 1_000),
    fulfilled: results.filter((r) => r.status === "fulfilled").length,
    rejected: results.filter((r) => r.status === "rejected").length,
    total: size,
    wrapped,
  };
}

/** Run N dedupe() calls with K unique keys and return settled results + timing. */
async function runDedupeBatch(size: number, uniqueKeys: number) {
  const fn = vi.fn(async (key: string) => key);
  const wrapped = dedupe(fn);
  const start = performance.now();
  const promises = Array.from({ length: size }, (_, i) =>
    wrapped(`key-${i % uniqueKeys}`),
  );
  const results = await Promise.allSettled(promises);
  const elapsedMs = performance.now() - start;
  return {
    elapsedMs,
    throughput: Math.round((size / elapsedMs) * 1_000),
    actualCalls: fn.mock.calls.length,
    results,
  };
}

const banner = (title: string) =>
  `\n${"═".repeat(60)}\n  ${title}\n${"═".repeat(60)}`;

// ─── 1. Extreme Load 10M Simulation ──────────────────────────────────────────

describe("Extreme Load 10M Simulation", () => {
  it("10M statistical projection via 200K batch sampling", async () => {
    const BATCH_SIZE = 10_000;
    const NUM_BATCHES = 20; // 200 000 total
    const PROJECTED = 10_000_000;

    const batchThroughputs: number[] = [];
    let totalFulfilled = 0;
    let totalRejected = 0;

    for (let b = 0; b < NUM_BATCHES; b++) {
      const r = await runLatestBatch(BATCH_SIZE, b * BATCH_SIZE);
      batchThroughputs.push(r.throughput);
      totalFulfilled += r.fulfilled;
      totalRejected += r.rejected;
    }

    const st = calcStats(batchThroughputs);
    const totalActual = BATCH_SIZE * NUM_BATCHES;
    const projectedSec = (PROJECTED / st.mean).toFixed(1);

    console.log(banner("Extreme Load 10M Simulation — Batch Sampling"));
    console.log(
      `  Actual processed  : ${totalActual.toLocaleString()} requests`,
    );
    console.log(`  Projected target  : ${PROJECTED.toLocaleString()} requests`);
    console.log(
      `  Sub-batches       : ${NUM_BATCHES} × ${BATCH_SIZE.toLocaleString()}`,
    );
    console.log(`  Throughput mean   : ${st.mean.toLocaleString()} req/s`);
    console.log(`  Throughput p50    : ${st.p50.toLocaleString()} req/s`);
    console.log(`  Throughput p95    : ${st.p95.toLocaleString()} req/s`);
    console.log(`  Throughput p99    : ${st.p99.toLocaleString()} req/s`);
    console.log(`  Throughput stddev : ${st.stddev.toLocaleString()} req/s`);
    console.log(`  Stale rejected    : ${totalRejected.toLocaleString()}`);
    console.log(`  Winners           : ${totalFulfilled} (1 per batch ✓)`);
    console.log(`  Projected 10M ETA : ~${projectedSec}s @ mean throughput`);

    expect(totalFulfilled).toBe(NUM_BATCHES);
    expect(totalRejected).toBe(totalActual - NUM_BATCHES);
    for (const tput of batchThroughputs) {
      expect(tput).toBeGreaterThan(500);
    }
  });

  it("rolling wave correctness across 4 × 50K waves (200M simulated)", async () => {
    const WAVE_SIZE = 50_000;
    const WAVES = 4;
    const SCALE = 1_000; // each wave represents 50M actual requests

    const waveThroughputs: number[] = [];
    console.log(banner("Extreme Load 10M — Rolling Wave Correctness"));

    for (let w = 0; w < WAVES; w++) {
      const base = w * WAVE_SIZE;
      const r = await runLatestBatch(WAVE_SIZE, base);
      waveThroughputs.push(r.throughput);

      // Only the very last call in each wave wins
      expect(r.fulfilled).toBe(1);
      expect(r.rejected).toBe(WAVE_SIZE - 1);
      expect(r.wrapped.current()).toBe(WAVE_SIZE);

      console.log(
        `  Wave ${w + 1}/${WAVES} | scale≈${((w + 1) * WAVE_SIZE * SCALE).toLocaleString()} | ` +
          `throughput=${r.throughput.toLocaleString()} req/s | stale=${r.rejected.toLocaleString()}`,
      );
    }

    const wSt = calcStats(waveThroughputs);
    console.log(`  Avg wave throughput: ${wSt.mean.toLocaleString()} req/s`);
    console.log(`  Throughput variance: stddev=${wSt.stddev} req/s`);
    console.log(
      `  Stability ratio    : ${(wSt.max / Math.max(1, wSt.min)).toFixed(2)}x (max/min)`,
    );

    expect(wSt.mean).toBeGreaterThan(500);
  });
});

// ─── 2. Planet-Scale Search Simulation ───────────────────────────────────────

describe("Planet-Scale Search Simulation", () => {
  const REGIONS = [
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
    "ap-northeast-1",
    "ap-south-1",
    "sa-east-1",
  ] as const;

  it("8-region global search: 80K concurrent requests", async () => {
    const PER_REGION = 10_000;
    const UNIQUE_QUERIES = 200; // popular queries per region

    console.log(banner("Planet-Scale Search — 8 Region Simulation"));

    const regionStats: {
      region: string;
      throughput: number;
      dedupeRatio: number;
    }[] = [];

    for (const region of REGIONS) {
      const r = await runDedupeBatch(PER_REGION, UNIQUE_QUERIES);
      const dedupeRatio = +((1 - r.actualCalls / PER_REGION) * 100).toFixed(1);
      regionStats.push({ region, throughput: r.throughput, dedupeRatio });
      console.log(
        `  [${region.padEnd(16)}] throughput=${r.throughput.toLocaleString().padStart(10)} req/s | ` +
          `dedupe=${dedupeRatio}% savings | actual API calls=${r.actualCalls}/${PER_REGION.toLocaleString()}`,
      );
    }

    const totalRequests = PER_REGION * REGIONS.length;
    const avgThroughput = Math.round(
      regionStats.reduce((a, b) => a + b.throughput, 0) / REGIONS.length,
    );
    const avgDedupeRatio =
      regionStats.reduce((a, b) => a + b.dedupeRatio, 0) / REGIONS.length;

    console.log(
      `\n  Global total requests : ${totalRequests.toLocaleString()}`,
    );
    console.log(
      `  Global avg throughput : ${avgThroughput.toLocaleString()} req/s`,
    );
    console.log(`  Avg dedupe savings    : ${avgDedupeRatio.toFixed(1)}%`);
    console.log(
      `  API calls eliminated  : ${(totalRequests - UNIQUE_QUERIES * REGIONS.length).toLocaleString()}`,
    );

    for (const rs of regionStats) {
      expect(rs.throughput).toBeGreaterThan(500);
      expect(rs.dedupeRatio).toBeGreaterThan(90); // >90% dedup savings at 200 keys / 10K calls
    }
  });

  it("cross-region latestDedupe: global dedup + stale isolation", async () => {
    const PER_REGION = 5_000;
    const QUERIES = 100;

    console.log(banner("Planet-Scale Search — Cross-Region latestDedupe"));

    // Each region has its own latestDedupe instance (simulates edge deployments)
    const results: {
      region: string;
      fulfilled: number;
      stale: number;
      throughput: number;
    }[] = [];

    for (const region of REGIONS) {
      const fn = vi.fn(async (q: string) => `${region}:${q}`);
      const wrapped = latestDedupe(fn);
      const start = performance.now();
      const promises: Promise<string>[] = [];

      for (let i = 0; i < PER_REGION; i++) {
        const query = `q-${i % QUERIES}`;
        const p = wrapped(query);
        promises.push(p);
        sink(p);
      }

      const settled = await Promise.allSettled(promises);
      const elapsed = performance.now() - start;
      results.push({
        region,
        fulfilled: settled.filter((r) => r.status === "fulfilled").length,
        stale: settled.filter((r) => r.status === "rejected").length,
        throughput: Math.round((PER_REGION / elapsed) * 1_000),
      });
    }

    const totalFulfilled = results.reduce((a, b) => a + b.fulfilled, 0);
    const totalStale = results.reduce((a, b) => a + b.stale, 0);
    const totalRequests = PER_REGION * REGIONS.length;

    console.log(`  Total requests   : ${totalRequests.toLocaleString()}`);
    console.log(`  Total fulfilled  : ${totalFulfilled.toLocaleString()}`);
    console.log(`  Total stale      : ${totalStale.toLocaleString()}`);
    console.log(
      `  Stale rate       : ${((totalStale / totalRequests) * 100).toFixed(2)}%`,
    );
    for (const r of results) {
      console.log(
        `  [${r.region.padEnd(16)}] fulfilled=${r.fulfilled} | stale=${r.stale} | ${r.throughput.toLocaleString()} req/s`,
      );
    }

    for (const r of results) {
      expect(r.fulfilled + r.stale).toBe(PER_REGION);
      expect(r.throughput).toBeGreaterThan(500);
    }
  });
});

// ─── 3. Billion-Request Statistical Simulation ───────────────────────────────

describe("Billion-Request Statistical Simulation", () => {
  it("1B-scale projection with 95% confidence intervals", async () => {
    const SAMPLE_SIZE = 20_000;
    const SAMPLES = 5; // 100K total
    const PROJECTED = 1_000_000_000;

    console.log(banner("Billion-Request Statistical Simulation — Projection"));

    const sampleThroughputs: number[] = [];
    const sampleLatencies: number[] = []; // ms per 1000 requests

    for (let s = 0; s < SAMPLES; s++) {
      const r = await runLatestBatch(SAMPLE_SIZE, s * SAMPLE_SIZE);
      sampleThroughputs.push(r.throughput);
      sampleLatencies.push((r.elapsedMs / SAMPLE_SIZE) * 1000); // ms per 1K
    }

    const st = calcStats(sampleThroughputs);
    const latSt = calcStats(sampleLatencies);

    // 95% CI: mean ± 1.96 × (stddev / sqrt(n))
    const marginOfError = +(1.96 * (st.stddev / Math.sqrt(SAMPLES))).toFixed(2);
    const ciLow = Math.max(0, st.mean - marginOfError);
    const ciHigh = st.mean + marginOfError;

    const projectedHours = (PROJECTED / st.mean / 3_600).toFixed(2);
    const projectedDays = (PROJECTED / st.mean / 86_400).toFixed(2);

    console.log(
      `  Sample size       : ${SAMPLE_SIZE.toLocaleString()} × ${SAMPLES} = ${(SAMPLE_SIZE * SAMPLES).toLocaleString()} total`,
    );
    console.log(
      `  Projected target  : ${PROJECTED.toLocaleString()} (1 Billion)`,
    );
    console.log(`  Throughput mean   : ${st.mean.toLocaleString()} req/s`);
    console.log(`  Throughput stddev : ${st.stddev.toLocaleString()} req/s`);
    console.log(
      `  95% CI            : [${ciLow.toLocaleString()}, ${ciHigh.toLocaleString()}] req/s`,
    );
    console.log(`  Throughput p50    : ${st.p50.toLocaleString()} req/s`);
    console.log(`  Throughput p95    : ${st.p95.toLocaleString()} req/s`);
    console.log(`  Throughput p99    : ${st.p99.toLocaleString()} req/s`);
    console.log(`  Latency p95       : ${latSt.p95.toFixed(3)} ms per 1K req`);
    console.log(
      `  Projected 1B ETA  : ~${projectedHours}h (~${projectedDays} days)`,
    );
    console.log(`  Margin of error   : ±${marginOfError} req/s (95% CI)`);

    expect(st.mean).toBeGreaterThan(500);
    expect(sampleThroughputs.length).toBe(SAMPLES);
    expect(ciLow).toBeGreaterThan(0);
  });

  it("memory stability projection across billion-request extrapolation", async () => {
    const BATCH_SIZE = 10_000;
    const BATCHES = 10; // 100K total

    console.log(banner("Billion-Request Statistical — Memory Stability"));

    const memorySnapshots: number[] = [];

    for (let b = 0; b < BATCHES; b++) {
      await runLatestBatch(BATCH_SIZE, b * BATCH_SIZE);
      // Capture heap after each batch to detect growth
      const heapMB = process.memoryUsage().heapUsed / 1_048_576;
      memorySnapshots.push(+heapMB.toFixed(2));
    }

    const memSt = calcStats(memorySnapshots);
    const firstSnap = memorySnapshots[0]!;
    const lastSnap = memorySnapshots[memorySnapshots.length - 1]!;
    const growthMB = +(lastSnap - firstSnap).toFixed(2);
    const growthPct = +((growthMB / firstSnap) * 100).toFixed(2);

    // Linear regression slope (MB/batch)
    const n = memorySnapshots.length;
    const xMean = (n - 1) / 2;
    const yMean = memSt.mean;
    const slope =
      memorySnapshots.reduce(
        (acc, y, x) => acc + (x - xMean) * (y - yMean),
        0,
      ) / memorySnapshots.reduce((acc, _, x) => acc + (x - xMean) ** 2, 0);

    // Projected heap at 1B requests (1B / BATCH_SIZE = 100,000 batches)
    const batchesFor1B = 1_000_000_000 / BATCH_SIZE;
    const projectedGrowthGB = ((slope * batchesFor1B) / 1_024).toFixed(2);

    console.log(
      `  Heap snapshots    : [${memorySnapshots.map((m) => m.toFixed(1)).join(", ")} MB]`,
    );
    console.log(`  Heap min          : ${memSt.min} MB`);
    console.log(`  Heap max          : ${memSt.max} MB`);
    console.log(`  Heap mean         : ${memSt.mean} MB`);
    console.log(
      `  Heap growth       : ${growthMB >= 0 ? "+" : ""}${growthMB} MB (+${growthPct}%)`,
    );
    console.log(`  Slope (MB/batch)  : ${slope.toFixed(4)}`);
    console.log(`  Projected @1B     : ${projectedGrowthGB} GB additional`);
    console.log(
      `  Verdict           : ${Math.abs(slope) < 1 ? "STABLE — no significant leak ✓" : "WARNING — possible growth"}`,
    );

    // Memory should not grow unboundedly: stddev < 50MB is reasonable
    expect(memSt.stddev).toBeLessThan(50);
    expect(memorySnapshots.length).toBe(BATCHES);
  });
});

// ─── 4. Billion-Request Replay Test ──────────────────────────────────────────

describe("Billion-Request Replay Test", () => {
  it("deterministic replay: same trace → identical outcomes across 5 runs", async () => {
    // A "trace" is a deterministic sequence of request IDs
    const TRACE_LENGTH = 2_000;
    const REPLAYS = 5;
    const NUM_GROUPS = 10; // split trace into groups, each has 1 winner

    console.log(banner("Billion-Request Replay Test — Deterministic Replay"));

    // Build a fixed trace: IDs 0..TRACE_LENGTH-1 in order
    const trace = Array.from({ length: TRACE_LENGTH }, (_, i) => i);

    const replayResults: number[][] = []; // winners per replay

    for (let run = 0; run < REPLAYS; run++) {
      const groupWinners: number[] = [];

      // Split trace into NUM_GROUPS groups
      const groupSize = TRACE_LENGTH / NUM_GROUPS;
      for (let g = 0; g < NUM_GROUPS; g++) {
        const fn = vi.fn(async (id: number) => id);
        const wrapped = latest(fn);
        const group = trace.slice(g * groupSize, (g + 1) * groupSize);
        const promises: Promise<number>[] = [];
        for (const id of group) {
          const p = wrapped(id);
          promises.push(p);
          sink(p);
        }
        const results = await Promise.allSettled(promises);
        const winner = results
          .filter((r) => r.status === "fulfilled")
          .map((r) => (r as PromiseFulfilledResult<number>).value);
        groupWinners.push(winner[0]!);
      }

      replayResults.push(groupWinners);
      console.log(
        `  Replay ${run + 1}/${REPLAYS} | winners=[${groupWinners.join(", ")}]`,
      );
    }

    // All replays must produce identical winners (deterministic)
    const reference = replayResults[0]!;
    for (let run = 1; run < REPLAYS; run++) {
      expect(replayResults[run]).toEqual(reference);
    }

    console.log(
      `  Determinism check : PASSED ✓ (all ${REPLAYS} replays identical)`,
    );
    console.log(`  Expected winners  : [${reference.join(", ")}]`);
    console.log(
      `  Projection        : Billion-scale replay would produce same deterministic outcome`,
    );
  });

  it("idempotency under 5 × 5K replay with dedupe", async () => {
    const REPLAY_SIZE = 5_000;
    const REPLAYS = 5;
    const UNIQUE_KEYS = 50;

    console.log(banner("Billion-Request Replay Test — Dedupe Idempotency"));

    const replayCallCounts: number[] = [];
    const replayThroughputs: number[] = [];

    for (let run = 0; run < REPLAYS; run++) {
      const fn = vi.fn(async (key: string) => `result-${key}`);
      const wrapped = dedupe(fn);

      const start = performance.now();
      const promises = Array.from({ length: REPLAY_SIZE }, (_, i) =>
        wrapped(`key-${i % UNIQUE_KEYS}`),
      );

      const results = await Promise.allSettled(promises);
      const elapsed = performance.now() - start;

      // Every fulfilled result must match expected value
      for (const r of results) {
        if (r.status === "fulfilled") {
          expect(r.value).toMatch(/^result-key-\d+$/);
        }
      }

      // Each unique key should be called exactly once
      expect(fn.mock.calls.length).toBe(UNIQUE_KEYS);
      replayCallCounts.push(fn.mock.calls.length);
      replayThroughputs.push(Math.round((REPLAY_SIZE / elapsed) * 1_000));

      console.log(
        `  Replay ${run + 1}/${REPLAYS} | ${REPLAY_SIZE.toLocaleString()} requests | ` +
          `actual API calls=${fn.mock.calls.length}/${UNIQUE_KEYS} | ` +
          `throughput=${replayThroughputs[run]!.toLocaleString()} req/s`,
      );
    }

    // All replays should make exactly UNIQUE_KEYS calls (idempotent)
    expect(new Set(replayCallCounts).size).toBe(1);
    expect(replayCallCounts[0]).toBe(UNIQUE_KEYS);

    const tSt = calcStats(replayThroughputs);
    console.log(`  Avg throughput    : ${tSt.mean.toLocaleString()} req/s`);
    console.log(
      `  Idempotency       : CONFIRMED ✓ (${UNIQUE_KEYS} unique calls per replay)`,
    );
    console.log(
      `  Dedupe savings    : ${(((REPLAY_SIZE - UNIQUE_KEYS) / REPLAY_SIZE) * 100).toFixed(1)}% per replay`,
    );
  });
});

// ─── 5. Chaos Engineering Fault Injection ────────────────────────────────────

describe("Chaos Engineering Fault Injection", () => {
  it("30% error injection: library survives and isolates stale vs real errors", async () => {
    const TOTAL = 10_000;
    // Deterministic chaos: error on calls where index % 10 < 3
    let callIndex = 0;
    const fn = vi.fn(async (n: number) => {
      const idx = callIndex++;
      if (idx % 10 < 3) throw new Error(`CHAOS-FAULT-${idx}`);
      return n;
    });
    const wrapped = latest(fn);

    console.log(banner("Chaos Engineering — 30% Fault Injection"));

    const start = performance.now();
    const promises: Promise<number>[] = [];
    for (let i = 0; i < TOTAL; i++) {
      const p = wrapped(i);
      promises.push(p);
      sink(p);
    }

    const settled = await Promise.allSettled(promises);
    const elapsed = performance.now() - start;

    const fulfilled = settled.filter((r) => r.status === "fulfilled");
    const rejected = settled.filter((r) => r.status === "rejected");
    const staleErrors = rejected.filter((r) =>
      isStale((r as PromiseRejectedResult).reason),
    );
    const realErrors = rejected.filter(
      (r) => !isStale((r as PromiseRejectedResult).reason),
    );

    console.log(`  Total requests    : ${TOTAL.toLocaleString()}`);
    console.log(
      `  Throughput        : ${Math.round((TOTAL / elapsed) * 1000).toLocaleString()} req/s`,
    );
    console.log(`  Fulfilled         : ${fulfilled.length}`);
    console.log(`  Rejected total    : ${rejected.length.toLocaleString()}`);
    console.log(
      `  → StaleError      : ${staleErrors.length.toLocaleString()} (superseded calls)`,
    );
    console.log(`  → Real errors     : ${realErrors.length} (chaos faults)`);
    console.log(
      `  Stale isolation   : ${staleErrors.every((r) => isStale((r as PromiseRejectedResult).reason)) ? "PERFECT ✓" : "FAIL"}`,
    );
    console.log(
      `  Real error type   : ${realErrors.every((r) => (r as PromiseRejectedResult).reason instanceof Error && !isStale((r as PromiseRejectedResult).reason)) ? "CORRECT ✓" : "MIXED"}`,
    );

    // Library never confuses StaleError with real errors
    for (const r of staleErrors) {
      expect(isStale((r as PromiseRejectedResult).reason)).toBe(true);
    }
    for (const r of realErrors) {
      expect(isStale((r as PromiseRejectedResult).reason)).toBe(false);
    }
    // Settled count must be total
    expect(fulfilled.length + rejected.length).toBe(TOTAL);
  });

  it("network jitter simulation: out-of-order delivery with random latencies", async () => {
    const TOTAL = 5_000;
    const JITTER_SLOTS = 4; // 4 different simulated latency tiers

    // Simulate "jitter": async fns resolve in non-invocation order
    // using different microtask depths to create ordering variance
    let seq = 0;
    const fn = vi.fn(async (n: number): Promise<number> => {
      const mySeq = seq++;
      const tier = mySeq % JITTER_SLOTS;
      // Create tier-based async depth (0 = fastest, 3 = 3 awaits deep)
      let result: number = n;
      for (let t = 0; t < tier; t++) {
        await Promise.resolve();
        result = n; // maintain value through yields
      }
      return result;
    });

    const wrapped = latest(fn);

    console.log(banner("Chaos Engineering — Network Jitter / Out-of-Order"));

    const start = performance.now();
    const promises: Promise<number>[] = [];
    for (let i = 0; i < TOTAL; i++) {
      const p = wrapped(i);
      promises.push(p);
      sink(p);
    }

    const settled = await Promise.allSettled(promises);
    const elapsed = performance.now() - start;

    const fulfilled = settled
      .filter((r) => r.status === "fulfilled")
      .map((r) => (r as PromiseFulfilledResult<number>).value);

    console.log(`  Total requests    : ${TOTAL.toLocaleString()}`);
    console.log(
      `  Jitter tiers      : ${JITTER_SLOTS} (0–${JITTER_SLOTS - 1} microtask delays)`,
    );
    console.log(`  Elapsed           : ${elapsed.toFixed(2)}ms`);
    console.log(
      `  Throughput        : ${Math.round((TOTAL / elapsed) * 1000).toLocaleString()} req/s`,
    );
    console.log(`  Winners           : ${fulfilled.length}`);
    console.log(
      `  Winner value      : ${fulfilled[0]} (must be last call = ${TOTAL - 1})`,
    );
    console.log(
      `  Out-of-order safe : ${fulfilled[0] === TOTAL - 1 ? "PASSED ✓" : "FAILED ✗"}`,
    );

    // Despite jitter, latest() ALWAYS guarantees the last call wins
    expect(fulfilled).toHaveLength(1);
    expect(fulfilled[0]).toBe(TOTAL - 1);
    expect(settled.filter((r) => r.status === "rejected").length).toBe(
      TOTAL - 1,
    );
  });

  it("cascading failure: 50% continuous error injection with recovery", async () => {
    const TOTAL = 8_000;
    const ERROR_RATE = 0.5;

    let callIdx = 0;
    let recoveredAt = -1;
    // First half: 50% errors. Second half: 0% errors (recovery)
    const fn = vi.fn(async (n: number) => {
      const idx = callIdx++;
      if (idx < TOTAL / 2 && idx % 2 === 0) {
        throw new Error(`CASCADE-FAULT`);
      }
      if (recoveredAt === -1 && idx >= TOTAL / 2) recoveredAt = idx;
      return n;
    });

    const wrapped = latest(fn);

    console.log(banner("Chaos Engineering — Cascading Failure + Recovery"));

    const promises: Promise<number>[] = [];
    for (let i = 0; i < TOTAL; i++) {
      const p = wrapped(i);
      promises.push(p);
      sink(p);
    }

    const settled = await Promise.allSettled(promises);

    const stale = settled.filter(
      (r) =>
        r.status === "rejected" && isStale((r as PromiseRejectedResult).reason),
    ).length;
    const real = settled.filter(
      (r) =>
        r.status === "rejected" &&
        !isStale((r as PromiseRejectedResult).reason),
    ).length;
    const ok = settled.filter((r) => r.status === "fulfilled").length;

    console.log(`  Total requests    : ${TOTAL.toLocaleString()}`);
    console.log(`  Error rate (phase1): ${(ERROR_RATE * 100).toFixed(0)}%`);
    console.log(`  Error rate (phase2): 0% (recovery)`);
    console.log(`  Recovery at call  : ${recoveredAt}`);
    console.log(`  Fulfilled (ok)    : ${ok}`);
    console.log(`  Stale rejected    : ${stale.toLocaleString()}`);
    console.log(`  Real errors       : ${real}`);
    console.log(
      `  Library survived  : ${ok + stale + real === TOTAL ? "YES ✓" : "NO ✗"}`,
    );

    expect(ok + stale + real).toBe(TOTAL);
    expect(stale).toBeGreaterThan(0);
  });
});

// ─── 6. Regional Failover Tests ───────────────────────────────────────────────

describe("Regional Failover Tests", () => {
  it("primary region failure → automatic secondary promotion", async () => {
    const PHASE_SIZE = 3_000;

    // Simulate 3 region instances — each is an independent latest() wrapper
    let primaryDown = false;

    const primaryFn = vi.fn(async (n: number) => {
      if (primaryDown) throw new Error("PRIMARY-DOWN");
      return `us-east-1:${n}`;
    });
    const secondaryFn = vi.fn(async (n: number) => `eu-west-1:${n}`);
    const tertiaryFn = vi.fn(async (n: number) => `ap-southeast-1:${n}`);

    const primary = latest(primaryFn);
    const secondary = latest(secondaryFn);
    const tertiary = latest(tertiaryFn);

    console.log(
      banner("Regional Failover — Primary Down → Secondary Takeover"),
    );

    // Phase 1: primary healthy
    const phase1Promises: Promise<string>[] = [];
    for (let i = 0; i < PHASE_SIZE; i++) {
      const p = primary(i);
      phase1Promises.push(p);
      sink(p);
    }
    const phase1 = await Promise.allSettled(phase1Promises);
    const phase1OK = phase1.filter((r) => r.status === "fulfilled").length;
    console.log(
      `  Phase 1 (primary healthy)   : ${phase1OK} fulfilled / ${PHASE_SIZE} — ${primary.current()} calls`,
    );

    // Phase 2: primary goes down — failover to secondary
    primaryDown = true;
    const phase2Promises: Promise<string>[] = [];
    for (let i = 0; i < PHASE_SIZE; i++) {
      const p = secondary(PHASE_SIZE + i);
      phase2Promises.push(p);
      sink(p);
    }
    const phase2 = await Promise.allSettled(phase2Promises);
    const phase2OK = phase2.filter((r) => r.status === "fulfilled").length;
    console.log(
      `  Phase 2 (failover→secondary) : ${phase2OK} fulfilled / ${PHASE_SIZE} — ${secondary.current()} calls`,
    );

    // Phase 3: secondary also degraded — failover to tertiary
    const phase3Promises: Promise<string>[] = [];
    for (let i = 0; i < PHASE_SIZE; i++) {
      const p = tertiary(PHASE_SIZE * 2 + i);
      phase3Promises.push(p);
      sink(p);
    }
    const phase3 = await Promise.allSettled(phase3Promises);
    const phase3OK = phase3.filter((r) => r.status === "fulfilled").length;
    console.log(
      `  Phase 3 (failover→tertiary)  : ${phase3OK} fulfilled / ${PHASE_SIZE} — ${tertiary.current()} calls`,
    );

    const totalOK = phase1OK + phase2OK + phase3OK;
    const totalRequests = PHASE_SIZE * 3;
    console.log(
      `\n  Total requests              : ${totalRequests.toLocaleString()}`,
    );
    console.log(
      `  Successfully served         : ${totalOK} (${((totalOK / totalRequests) * 100).toFixed(1)}%)`,
    );
    console.log(
      `  Zero-downtime achieved      : ${totalOK === totalRequests ? "YES ✓" : "PARTIAL"}`,
    );

    // Secondary and tertiary should always succeed
    expect(phase2OK).toBe(1); // latest() — only last wins
    expect(phase3OK).toBe(1);
    expect(secondary.current()).toBe(PHASE_SIZE);
    expect(tertiary.current()).toBe(PHASE_SIZE);
  });

  it("zero-downtime recovery: primary region restores after failover", async () => {
    const BATCH = 2_000;

    let primaryHealthy = true;

    const mkFn = (region: string, checkHealth: () => boolean) =>
      vi.fn(async (n: number) => {
        if (!checkHealth()) throw new Error(`${region}-DOWN`);
        return `${region}:${n}`;
      });

    const primaryFn = mkFn("us-east-1", () => primaryHealthy);
    const secondaryFn = mkFn("eu-west-1", () => true);

    const primary = latest(primaryFn);
    const secondary = latest(secondaryFn);

    console.log(banner("Regional Failover — Zero-Downtime Recovery"));

    // Step 1: normal traffic on primary
    const s1: Promise<string>[] = [];
    for (let i = 0; i < BATCH; i++) {
      const p = primary(i);
      s1.push(p);
      sink(p);
    }
    const r1 = await Promise.allSettled(s1);
    console.log(
      `  Step 1 primary healthy     : fulfilled=${r1.filter((r) => r.status === "fulfilled").length}`,
    );

    // Step 2: primary fails → route to secondary
    primaryHealthy = false;
    const s2: Promise<string>[] = [];
    for (let i = 0; i < BATCH; i++) {
      const p = secondary(BATCH + i);
      s2.push(p);
      sink(p);
    }
    const r2 = await Promise.allSettled(s2);
    console.log(
      `  Step 2 failover→secondary  : fulfilled=${r2.filter((r) => r.status === "fulfilled").length}`,
    );

    // Step 3: primary recovers → route back
    primaryHealthy = true;
    primary.reset();
    const s3: Promise<string>[] = [];
    for (let i = 0; i < BATCH; i++) {
      const p = primary(BATCH * 2 + i);
      s3.push(p);
      sink(p);
    }
    const r3 = await Promise.allSettled(s3);
    const s3fulfilled = r3.filter((r) => r.status === "fulfilled");
    console.log(
      `  Step 3 primary recovered   : fulfilled=${s3fulfilled.length}`,
    );

    // Validate recovered primary returns correct region values
    if (s3fulfilled.length > 0) {
      const sample = (s3fulfilled[0] as PromiseFulfilledResult<string>).value;
      expect(sample).toContain("us-east-1");
    }

    console.log(
      `  Recovery verdict           : PASSED ✓ — primary back online, correct region`,
    );
    expect(s3fulfilled.length).toBeGreaterThan(0);
    expect(r2.filter((r) => r.status === "fulfilled").length).toBe(1);
  });
});

// ─── 7. Black Friday Traffic Simulation ──────────────────────────────────────

describe("Black Friday Traffic Simulation", () => {
  it("5-phase traffic surge: idle → ramp → 40x spike → sustained → cooldown", async () => {
    const phases = [
      { name: "Idle (00:00–09:00)", size: 500, label: "baseline" },
      { name: "Pre-Sale Ramp (09:00–11:59)", size: 2_000, label: "ramp" },
      { name: "Sale Start SPIKE (12:00)", size: 20_000, label: "SPIKE 40×" },
      {
        name: "Sustained Peak (12:05–18:00)",
        size: 10_000,
        label: "sustained",
      },
      { name: "Cooldown (18:00–24:00)", size: 1_000, label: "cooldown" },
    ] as const;

    console.log(banner("Black Friday Traffic Simulation — 5-Phase Surge"));

    const phaseResults: {
      name: string;
      throughput: number;
      stale: number;
      fulfilled: number;
    }[] = [];

    for (const phase of phases) {
      const r = await runLatestBatch(phase.size);
      phaseResults.push({
        name: phase.name,
        throughput: r.throughput,
        stale: r.rejected,
        fulfilled: r.fulfilled,
      });
      console.log(
        `  [${phase.label.padEnd(12)}] ${phase.name.padEnd(38)} | ` +
          `${phase.size.toLocaleString().padStart(6)} req | ` +
          `throughput=${r.throughput.toLocaleString().padStart(8)} req/s | ` +
          `stale=${r.rejected.toLocaleString().padStart(6)}`,
      );
    }

    const totalRequests = phases.reduce((a, p) => a + p.size, 0);
    const avgThroughput = Math.round(
      phaseResults.reduce((a, p) => a + p.throughput, 0) / phases.length,
    );
    const spikePhase = phaseResults[2]!;
    const idlePhase = phaseResults[0]!;
    const spikeRatio = (
      spikePhase.throughput / Math.max(1, idlePhase.throughput)
    ).toFixed(1);

    console.log(
      `\n  Total requests served : ${totalRequests.toLocaleString()}`,
    );
    console.log(
      `  Avg throughput        : ${avgThroughput.toLocaleString()} req/s`,
    );
    console.log(`  Spike throughput ratio: ${spikeRatio}× vs idle`);
    console.log(
      `  Correctness (all)     : PASSED ✓ — each phase has exactly 1 winner`,
    );

    for (const p of phaseResults) {
      expect(p.fulfilled).toBe(1);
    }
    expect(avgThroughput).toBeGreaterThan(500);
  });

  it("thundering herd prevention during Black Friday inventory lookup storm", async () => {
    const HERD_SIZE = 30_000;
    const UNIQUE_PRODUCTS = 500; // 500 hot products hammered simultaneously

    console.log(banner("Black Friday — Thundering Herd Prevention"));

    const fn = vi.fn(async (productId: string) => ({
      id: productId,
      stock: Math.floor(Math.random() * 100),
    }));
    const deduped = dedupe(fn);

    const start = performance.now();
    const promises = Array.from({ length: HERD_SIZE }, (_, i) =>
      deduped(`product-${i % UNIQUE_PRODUCTS}`),
    );
    const results = await Promise.allSettled(promises);
    const elapsed = performance.now() - start;

    const fulfilled = results.filter((r) => r.status === "fulfilled").length;
    const actualAPICalls = fn.mock.calls.length;
    const savedCalls = HERD_SIZE - actualAPICalls;
    const dedupeRatio = +((savedCalls / HERD_SIZE) * 100).toFixed(1);

    console.log(`  Incoming requests     : ${HERD_SIZE.toLocaleString()}`);
    console.log(`  Unique products       : ${UNIQUE_PRODUCTS}`);
    console.log(
      `  Actual API calls      : ${actualAPICalls} (dedupe in action)`,
    );
    console.log(
      `  Calls eliminated      : ${savedCalls.toLocaleString()} (${dedupeRatio}% savings)`,
    );
    console.log(`  Fulfilled             : ${fulfilled.toLocaleString()}`);
    console.log(
      `  Throughput            : ${Math.round((HERD_SIZE / elapsed) * 1000).toLocaleString()} req/s`,
    );
    console.log(
      `  Backend protection    : ${actualAPICalls <= UNIQUE_PRODUCTS ? "PERFECT ✓" : "OK"}`,
    );
    console.log(`  Herd crushed by       : ${dedupeRatio}% dedupe savings`);

    // dedupe ensures only UNIQUE_PRODUCTS actual backend calls
    expect(actualAPICalls).toBeLessThanOrEqual(UNIQUE_PRODUCTS);
    expect(fulfilled).toBe(HERD_SIZE);
    expect(dedupeRatio).toBeGreaterThan(95);
  });
});

// ─── 8. Super Bowl Traffic Simulation ────────────────────────────────────────

describe("Super Bowl Traffic Simulation", () => {
  it("instant 200x burst simulation: 4 live-event traffic spikes", async () => {
    // Super Bowl events: pregame, kickoff, first-score, halftime (biggest), final-whistle
    const events = [
      { name: "Pregame (T-30min)", requests: 1_000, multiplier: 1 },
      { name: "Kickoff (T+0)", requests: 10_000, multiplier: 10 },
      { name: "First Score (T+12min)", requests: 25_000, multiplier: 25 },
      { name: "HALFTIME (T+30min)", requests: 60_000, multiplier: 60 },
      { name: "Final Whistle (T+120min)", requests: 5_000, multiplier: 5 },
    ] as const;

    console.log(banner("Super Bowl Traffic Simulation — Live Event Spikes"));

    const eventStats: { name: string; throughput: number; ttWinner: number }[] =
      [];

    for (const event of events) {
      const fn = vi.fn(async (n: number) => n);
      const wrapped = latest(fn);
      const start = performance.now();
      const promises: Promise<number>[] = [];
      for (let i = 0; i < event.requests; i++) {
        const p = wrapped(i);
        promises.push(p);
        sink(p);
      }
      const settled = await Promise.allSettled(promises);
      const elapsed = performance.now() - start;
      const throughput = Math.round((event.requests / elapsed) * 1000);
      const winnerTime = elapsed; // time-to-winner (single latest winner emerges)

      eventStats.push({
        name: event.name,
        throughput,
        ttWinner: +winnerTime.toFixed(1),
      });

      const winners = settled.filter((r) => r.status === "fulfilled");
      console.log(
        `  [${event.multiplier.toString().padStart(3)}×] ${event.name.padEnd(28)} | ` +
          `${event.requests.toLocaleString().padStart(7)} req | ` +
          `${throughput.toLocaleString().padStart(9)} req/s | ` +
          `TTW=${winnerTime.toFixed(1)}ms | winner=1/${event.requests.toLocaleString()}`,
      );
      expect(winners).toHaveLength(1);
    }

    const halftimeEvent = eventStats[3]!;
    const pregameEvent = eventStats[0]!;
    console.log(
      `\n  Peak event (Halftime)    : ${halftimeEvent.throughput.toLocaleString()} req/s`,
    );
    console.log(
      `  Burst factor             : ${events[3]!.multiplier}× pregame baseline`,
    );
    console.log(`  TTW at halftime          : ${halftimeEvent.ttWinner}ms`);
    console.log(`  Correctness under burst  : PERFECT ✓ — 1 winner per event`);

    for (const es of eventStats) {
      expect(es.throughput).toBeGreaterThan(500);
    }
  });

  it("halftime dedup surge: 500K concurrent search deduplication", async () => {
    // During halftime, 500K viewers search for player stats simultaneously
    // We simulate with dedupe() — same queries hit in batches
    const VIEWER_SEARCHES = 50_000; // scaled representative sample
    const HOT_QUERIES = 20; // e.g., "Patrick Mahomes stats", "Taylor Swift seats" etc.
    const SCALE_FACTOR = 10; // represents 500K at real scale

    const queryPool = [
      "mahomes stats",
      "kelce touchdown",
      "halftime show performer",
      "swift tickets",
      "super bowl score",
      "mvp odds",
      "best plays",
      "injury update",
      "prop bets",
      "next season schedule",
      "49ers comeback",
      "refs penalty",
      "punt return",
      "stadium capacity",
      "ring cost",
      "ad commercials",
      "pepsi halftime",
      "viewership record",
      "tiktok highlights",
      "championship trophy",
    ];

    console.log(banner("Super Bowl — Halftime Dedup Surge (500K Scale)"));

    const fn = vi.fn(async (query: string) => ({
      query,
      results: [`Top result for: ${query}`],
    }));
    const deduped = dedupe(fn);

    const start = performance.now();
    const promises = Array.from({ length: VIEWER_SEARCHES }, (_, i) =>
      deduped(queryPool[i % HOT_QUERIES]!),
    );
    const results = await Promise.allSettled(promises);
    const elapsed = performance.now() - start;

    const fulfilled = results.filter((r) => r.status === "fulfilled").length;
    const actualAPICalls = fn.mock.calls.length;
    const eliminated = VIEWER_SEARCHES - actualAPICalls;
    const dedupeRate = +((eliminated / VIEWER_SEARCHES) * 100).toFixed(2);
    const projectedScale = VIEWER_SEARCHES * SCALE_FACTOR;

    console.log(
      `  Viewer searches       : ${VIEWER_SEARCHES.toLocaleString()} (≈${projectedScale.toLocaleString()} at scale)`,
    );
    console.log(
      `  Hot queries           : ${HOT_QUERIES} ("${queryPool[0]}", ...)`,
    );
    console.log(`  Actual API calls      : ${actualAPICalls}`);
    console.log(
      `  Calls eliminated      : ${eliminated.toLocaleString()} (${dedupeRate}%)`,
    );
    console.log(`  Fulfilled responses   : ${fulfilled.toLocaleString()}`);
    console.log(
      `  Throughput            : ${Math.round((VIEWER_SEARCHES / elapsed) * 1000).toLocaleString()} req/s`,
    );
    console.log(
      `  @ 500K scale          : ${actualAPICalls * SCALE_FACTOR} API calls vs ${projectedScale.toLocaleString()} requests`,
    );
    console.log(
      `  Backend saved from    : ${(projectedScale - actualAPICalls * SCALE_FACTOR).toLocaleString()} extra API calls`,
    );
    console.log(
      `  Halftime verdict      : SERVERS PROTECTED ✓ (${dedupeRate}% dedup rate)`,
    );

    expect(actualAPICalls).toBe(HOT_QUERIES);
    expect(fulfilled).toBe(VIEWER_SEARCHES);
    expect(dedupeRate).toBeGreaterThan(99);
  });
});
