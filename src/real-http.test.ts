/**
 * Real HTTP integration tests for call-latest.
 *
 * Uses a genuine node:http server and real fetch() calls.
 * No vi.fn() mocks for the network layer.
 */

import { createServer } from "node:http";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { dedupe, isStale, latest, type LatestContext } from "./index.js";

vi.setConfig({ testTimeout: 30_000 });

// ---------------------------------------------------------------------------
// Shared test server
// ---------------------------------------------------------------------------

interface RequestRecord {
  query: string;
  callId: string;
  delay: number;
  aborted: boolean;
}

const requestLog: RequestRecord[] = [];
let port: number;
let serverAddress: string;

const server = createServer((req, res) => {
  const url = new URL(req.url ?? "/", "http://127.0.0.1");
  const q = url.searchParams.get("q") ?? "";
  const delay = Number(url.searchParams.get("delay") ?? "0");
  const callId = url.searchParams.get("id") ?? "";

  const record: RequestRecord = { query: q, callId, delay, aborted: false };
  requestLog.push(record);

  let settled = false;

  req.on("close", () => {
    if (!settled) {
      record.aborted = true;
    }
  });

  const timer = setTimeout(() => {
    settled = true;
    if (res.destroyed || res.writableEnded) return;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ query: q, callId, delay }));
  }, delay);

  // Clean up the timer if the connection is torn down before the delay fires
  res.on("close", () => clearTimeout(timer));
});

beforeAll(
  () =>
    new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", () => {
        const addr = server.address() as { port: number };
        port = addr.port;
        serverAddress = `http://127.0.0.1:${port}`;
        resolve();
      });
    }),
);

afterAll(
  () =>
    new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    }),
);

// ---------------------------------------------------------------------------
// Real HTTP Integration — latest()
// ---------------------------------------------------------------------------

describe("Real HTTP Integration — latest()", () => {
  beforeEach(() => {
    requestLog.length = 0;
  });

  it("stale race: out-of-order HTTP responses, only latest result survives", async () => {
    // fn signature includes LatestContext as last param (required for abort: true)
    const wrapped = latest(
      async (
        q: string,
        delayMs: number,
        ctx: LatestContext,
      ): Promise<{ query: string }> => {
        const res = await fetch(
          `${serverAddress}/?q=${q}&delay=${delayMs}&id=${ctx.callId}`,
          { signal: ctx.signal },
        );
        return res.json() as Promise<{ query: string }>;
      },
      { abort: true },
    );

    // q1 = 270 ms (slowest), q10 = 0 ms (fastest)
    const queries = Array.from({ length: 10 }, (_, i) => ({
      q: `q${i + 1}`,
      delay: (10 - (i + 1)) * 30,
    }));

    const results = await Promise.allSettled(
      queries.map(({ q, delay }) => wrapped(q, delay)),
    );

    // First nine should all be stale
    for (let i = 0; i < 9; i++) {
      const r = results[i];
      expect(r.status, `q${i + 1} expected to be rejected`).toBe("rejected");
      if (r.status === "rejected") {
        expect(isStale(r.reason), `q${i + 1} reason should be StaleError`).toBe(
          true,
        );
      }
    }

    // Only q10 (last invoked) should fulfill
    const winner = results[9];
    expect(winner.status).toBe("fulfilled");
    if (winner.status === "fulfilled") {
      expect(winner.value.query).toBe("q10");
    }
  });

  it("real AbortSignal: aborting in-flight fetch throws and is caught as StaleError", async () => {
    const wrapped = latest(
      async (
        q: string,
        delayMs: number,
        ctx: LatestContext,
      ): Promise<{ query: string }> => {
        const res = await fetch(
          `${serverAddress}/?q=${q}&delay=${delayMs}&id=${ctx.callId}`,
          { signal: ctx.signal },
        );
        return res.json() as Promise<{ query: string }>;
      },
      { abort: true },
    );

    // p1 starts first but takes 300 ms; p2 starts immediately and wins at 10 ms
    const p1 = wrapped("slow", 300);
    const p2 = wrapped("fast", 10);

    const [r1, r2] = await Promise.allSettled([p1, p2]);

    expect(r1.status).toBe("rejected");
    if (r1.status === "rejected") {
      expect(isStale(r1.reason)).toBe(true);
    }

    expect(r2.status).toBe("fulfilled");
    if (r2.status === "fulfilled") {
      expect(r2.value.query).toBe("fast");
    }
  });

  it("real rapid-typing: r→re→rea→reac→react over real HTTP", async () => {
    const wrapped = latest(
      async (
        q: string,
        delayMs: number,
        ctx: LatestContext,
      ): Promise<{ query: string }> => {
        const res = await fetch(
          `${serverAddress}/?q=${q}&delay=${delayMs}&id=${ctx.callId}`,
          { signal: ctx.signal },
        );
        return res.json() as Promise<{ query: string }>;
      },
      { abort: true },
    );

    // Shorter query → higher latency: 'r' = 200 ms, 'react' = 40 ms
    // So 'r' would resolve last but was invoked first → stale
    const queries = ["r", "re", "rea", "reac", "react"];
    const results = await Promise.allSettled(
      queries.map((q) => wrapped(q, (6 - q.length) * 40)),
    );

    // All but the last invocation should be stale
    for (let i = 0; i < queries.length - 1; i++) {
      const r = results[i];
      expect(r.status, `"${queries[i]}" expected to be rejected`).toBe(
        "rejected",
      );
      if (r.status === "rejected") {
        expect(
          isStale(r.reason),
          `"${queries[i]}" reason should be StaleError`,
        ).toBe(true);
      }
    }

    // Only "react" (last invoked) resolves
    const winner = results[results.length - 1];
    expect(winner.status).toBe("fulfilled");
    if (winner.status === "fulfilled") {
      expect(winner.value.query).toBe("react");
    }
  });
});

// ---------------------------------------------------------------------------
// Real HTTP Integration — dedupe()
// ---------------------------------------------------------------------------

describe("Real HTTP Integration — dedupe()", () => {
  beforeEach(() => {
    requestLog.length = 0;
  });

  it("100 concurrent identical requests: server hit count is minimal", async () => {
    const fetcher = dedupe(async (q: string): Promise<{ query: string }> => {
      const res = await fetch(`${serverAddress}/?q=${q}&delay=10`);
      return res.json() as Promise<{ query: string }>;
    });

    // All 100 created synchronously in the same tick so dedupe coalesces them
    const promises = Array.from({ length: 100 }, () => fetcher("hello"));
    const results = await Promise.all(promises);

    // Every result should carry the right query name
    for (const r of results) {
      expect(r.query).toBe("hello");
    }

    // All promises are the same reference → all resolved values are identical
    for (const r of results) {
      expect(r).toBe(results[0]);
    }

    // Server should have received at most a handful of requests (ideally exactly 1)
    const hits = requestLog.filter((rec) => rec.query === "hello").length;
    expect(hits).toBeLessThanOrEqual(5);
  });

  it("different queries each get independent server requests", async () => {
    const fetcher = dedupe(async (q: string): Promise<{ query: string }> => {
      const res = await fetch(`${serverAddress}/?q=${q}&delay=10`);
      return res.json() as Promise<{ query: string }>;
    });

    const queries = ["alpha", "beta", "gamma", "delta", "epsilon"];
    const results = await Promise.all(queries.map((q) => fetcher(q)));

    // Each result maps to the correct query
    for (let i = 0; i < queries.length; i++) {
      expect(results[i].query).toBe(queries[i]);
    }

    // Each distinct query should have hit the server at least once
    for (const q of queries) {
      const hits = requestLog.filter((rec) => rec.query === q).length;
      expect(hits).toBeGreaterThanOrEqual(1);
    }
  });
});

// ---------------------------------------------------------------------------
// Real HTTP Integration — error scenarios
// ---------------------------------------------------------------------------

describe("Real HTTP Integration — error scenarios", () => {
  it("real connection refused propagates as non-stale Error", async () => {
    const fetcher = latest(async (): Promise<unknown> => {
      const res = await fetch("http://127.0.0.1:1/");
      return res.json();
    });

    let caught: unknown;
    try {
      await fetcher();
      expect.fail("Expected a network error to be thrown");
    } catch (e) {
      caught = e;
    }

    expect(caught).toBeInstanceOf(Error);
    expect(isStale(caught)).toBe(false);
  });

  it("request timeout via AbortSignal fires within budget", async () => {
    // Server delay is 500 ms; client aborts after 100 ms
    const ac = new AbortController();
    const timeoutId = setTimeout(() => ac.abort(), 100);

    const start = Date.now();
    let caught: unknown;
    try {
      await fetch(`${serverAddress}/?q=timeout-test&delay=500`, {
        signal: ac.signal,
      });
      clearTimeout(timeoutId);
      expect.fail("Expected the request to be aborted");
    } catch (e) {
      clearTimeout(timeoutId);
      caught = e;
    }

    const elapsed = Date.now() - start;

    expect(caught).toBeInstanceOf(Error);
    // Should have aborted well within 300 ms, not waited the full 500 ms
    expect(elapsed).toBeLessThan(300);
  });
});
