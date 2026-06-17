import { describe, expect, it, vi } from "vitest";
import {
  createFetchSearchAdapter,
  createSmartSearch,
  dispatchCancelSignal,
  dedupe,
  isStale,
  latest,
  latestDedupe,
  StaleError,
  type SearchResponse,
} from "./index.js";

const tick = () => new Promise((r) => setTimeout(r, 0));

/** Attach a no-op handler so stale rejections never become unhandled. */
const sink = (promise: Promise<unknown>) => {
  void promise.catch(() => {});
};

describe("latest", () => {
  it("returns only the latest result", async () => {
    let resolveFirst!: (v: number) => void;
    const first = new Promise<number>((r) => {
      resolveFirst = r;
    });

    const fn = vi.fn(async (n: number) => {
      if (n === 1) return first;
      return n;
    });

    const wrapped = latest(fn);
    const p1 = wrapped(1);
    const p2 = wrapped(2);
    const stale = expect(p1).rejects.toBeInstanceOf(StaleError);
    const fresh = expect(p2).resolves.toBe(2);

    resolveFirst(999);
    await stale;
    await fresh;
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it("rejects stale calls when the latest throws", async () => {
    let resolveSlow!: () => void;
    const slow = new Promise<number>((r) => {
      resolveSlow = () => r(1);
    });

    const wrapped = latest(async (n: number) => {
      if (n === 1) return slow;
      throw new Error("boom");
    });

    const p1 = wrapped(1);
    const p2 = wrapped(2);
    const stale = expect(p1).rejects.toBeInstanceOf(StaleError);
    const fresh = expect(p2).rejects.toThrow("boom");

    resolveSlow();
    await stale;
    await fresh;
  });

  it("aborts previous work when abort: true", async () => {
    const signals: AbortSignal[] = [];

    const wrapped = latest(
      async (n: number, ctx: { signal: AbortSignal }) => {
        signals.push(ctx.signal);
        await tick();
        return n;
      },
      { abort: true },
    );

    sink(wrapped(1));
    await wrapped(2);

    expect(signals).toHaveLength(2);
    expect(signals[0]!.aborted).toBe(true);
    expect(signals[1]!.aborted).toBe(false);
  });

  it("calls onStale for superseded invocations", async () => {
    let resolveSlow!: () => void;
    const slow = new Promise((r) => {
      resolveSlow = () => r(1);
    });
    const onStale = vi.fn();

    const wrapped = latest(
      async (n: number) => {
        if (n === 1) return slow;
        return n;
      },
      { onStale },
    );

    const p1 = wrapped(1);
    sink(wrapped(2));
    const stale = expect(p1).rejects.toBeInstanceOf(StaleError);

    resolveSlow();
    await stale;
    expect(onStale).toHaveBeenCalledWith([1]);
  });

  it("reset invalidates in-flight calls", async () => {
    let resolveSlow!: () => void;
    const slow = new Promise((r) => {
      resolveSlow = () => r(1);
    });

    const wrapped = latest(async () => slow);
    const p = wrapped();
    const assertion = expect(p).rejects.toBeInstanceOf(StaleError);
    wrapped.reset();
    resolveSlow();
    await assertion;
    expect(wrapped.current()).toBe(1);
  });
});

describe("dedupe", () => {
  it("shares one promise for concurrent identical calls", async () => {
    const fn = vi.fn(async (id: string) => id);
    const wrapped = dedupe(fn);

    const [a, b, c] = await Promise.all([
      wrapped("x"),
      wrapped("x"),
      wrapped("x"),
    ]);

    expect(a).toBe("x");
    expect(b).toBe("x");
    expect(c).toBe("x");
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it("runs separate calls for different keys", async () => {
    const fn = vi.fn(async (id: string) => id);
    const wrapped = dedupe(fn);

    await Promise.all([wrapped("a"), wrapped("b")]);
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it("allows a new call after the first settles", async () => {
    const fn = vi.fn(async (id: string) => id);
    const wrapped = dedupe(fn);

    await wrapped("x");
    await wrapped("x");
    expect(fn).toHaveBeenCalledTimes(2);
  });
});

describe("latestDedupe", () => {
  it("dedupes identical concurrent calls and drops stale distinct ones", async () => {
    const resolvers = new Map<number, (v: number) => void>();
    const fn = vi.fn((n: number) => {
      return new Promise<number>((r) => resolvers.set(n, r));
    });

    const wrapped = latestDedupe(fn);
    const p1 = wrapped(1);
    const p1b = wrapped(1);
    const p2 = wrapped(2);
    const stale1 = expect(p1).rejects.toBeInstanceOf(StaleError);
    const stale1b = expect(p1b).rejects.toBeInstanceOf(StaleError);
    const fresh = expect(p2).resolves.toBe(20);

    resolvers.get(1)!(10);
    resolvers.get(2)!(20);

    await stale1;
    await stale1b;
    await fresh;
    expect(fn).toHaveBeenCalledTimes(2);
  });
});

describe("isStale", () => {
  it("detects StaleError across shapes", () => {
    expect(isStale(new StaleError())).toBe(true);
    expect(isStale({ code: "STALE" })).toBe(true);
    expect(isStale(new Error())).toBe(false);
  });
});

describe("createSmartSearch", () => {
  it("uses backtrack cache for repeated query", async () => {
    const runSearch = vi.fn(async (query: string): Promise<SearchResponse<string>> => {
      return { items: [query] };
    });

    const smart = createSmartSearch(runSearch, {
      baseDebounceMs: 0,
      minDebounceMs: 0,
      swr: false,
    });
    await smart.search("iphone");
    await smart.search("iphon");
    await smart.search("iphone");

    expect(runSearch).toHaveBeenCalledTimes(2);
  });

  it("triggers distributed cancellation when new query starts", async () => {
    const canceled: number[] = [];
    const runSearch = vi.fn(async (query: string): Promise<SearchResponse<string>> => {
      return { items: [query] };
    });
    const smart = createSmartSearch(runSearch, {
      baseDebounceMs: 0,
      minDebounceMs: 0,
      onDistributedCancel: (id) => canceled.push(id),
    });

    await smart.search("iph");
    await smart.search("ipho");

    expect(canceled).toEqual([1]);
  });

  it("switches to conserve mode with bad metrics", async () => {
    const runSearch = vi.fn(async (query: string): Promise<SearchResponse<string>> => {
      return { items: [query] };
    });
    const smart = createSmartSearch(runSearch, {
      baseDebounceMs: 0,
      minDebounceMs: 0,
      metrics: () => ({ rttMs: 900, errorRate: 0.01, status503Rate: 0.01 }),
    });

    await smart.search("x");
    expect(smart.mode()).toBe("conserve");
    expect(smart.currentDebounce()).toBe(800);
  });

  it("evicts old cache entries with LRU max size", async () => {
    const runSearch = vi.fn(async (query: string): Promise<SearchResponse<string>> => {
      return { items: [query] };
    });
    const smart = createSmartSearch(runSearch, {
      baseDebounceMs: 0,
      minDebounceMs: 0,
      cacheMaxEntries: 2,
    });
    await smart.search("a");
    await smart.search("b");
    await smart.search("c");
    await smart.search("a");
    expect(runSearch).toHaveBeenCalledTimes(4);
  });

  it("does swr revalidation after cache hit", async () => {
    const swr = vi.fn();
    let calls = 0;
    const runSearch = vi.fn(async (query: string): Promise<SearchResponse<string>> => {
      calls++;
      return { items: [`${query}-${calls}`] };
    });
    const smart = createSmartSearch(runSearch, {
      baseDebounceMs: 0,
      minDebounceMs: 0,
      swr: true,
      onSWRUpdate: swr,
    });
    await smart.search("iphone");
    await smart.search("iphone");
    await tick();
    expect(swr).toHaveBeenCalled();
  });
});

describe("fetch adapter", () => {
  it("adds If-None-Match and call headers", async () => {
    const fetcher = vi.fn(async () => {
      return new Response(JSON.stringify({ add: [], removeIds: [] }), {
        status: 200,
        headers: { ETag: "v2" },
      });
    });
    const adapter = createFetchSearchAdapter<string>({
      endpoint: "https://x.test/search",
      fetcher: fetcher as unknown as typeof fetch,
    });
    await adapter(
      "iph",
      { query: "iph", version: "v1", items: ["a"] },
      { callId: 7, mode: "normal", signal: neverSignal, cancelRemote: () => {} },
    );
    const args = fetcher.mock.calls[0]![1] as RequestInit;
    const headers = args.headers as Record<string, string>;
    expect(headers["If-None-Match"]).toBe("v1");
    expect(headers["X-Call-Latest-Id"]).toBe("7");
  });

  it("dispatchCancelSignal falls back to fetch keepalive", async () => {
    const fetcher = vi.fn(async () => new Response("{}"));
    dispatchCancelSignal("https://x.test/cancel", 11, undefined, fetcher as unknown as typeof fetch);
    await tick();
    expect(fetcher).toHaveBeenCalled();
  });
});

const neverSignal = new AbortController().signal;
