import { describe, it, expect, vi } from "vitest";
import { observe, createEventBus, type ObservableEvent } from "./observable.js";
import { latest } from "./index.js";

// ─── Helpers ────────────────────────────────────────────────────────────────

/** Resolves after one macrotask — lets all pending microtasks flush first. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

// ─── observe() ──────────────────────────────────────────────────────────────

describe("observe()", () => {
  it("emits CALL_START on every call", async () => {
    const events: ObservableEvent[] = [];
    const fn = vi.fn(async (x: number) => x * 2);
    const wrapped = observe(fn, { onEvent: (e) => events.push(e) });

    await wrapped(1);
    await wrapped(2);

    const starts = events.filter((e) => e.type === "CALL_START");
    expect(starts).toHaveLength(2);
    expect(starts[0]).toMatchObject({
      type: "CALL_START",
      callId: 1,
      args: [1],
    });
    expect(starts[1]).toMatchObject({
      type: "CALL_START",
      callId: 2,
      args: [2],
    });
  });

  it("emits RESOLVED with correct latencyMs on success", async () => {
    const events: ObservableEvent[] = [];
    const fn = async () => {
      await new Promise((r) => setTimeout(r, 30));
      return "done";
    };
    const wrapped = observe(fn, { onEvent: (e) => events.push(e) });

    await wrapped();

    const resolved = events.find((e) => e.type === "RESOLVED");
    expect(resolved).toBeDefined();
    expect(resolved).toMatchObject({ type: "RESOLVED", callId: 1 });
    // latencyMs should be at least ~25 ms (allow timing jitter in CI)
    expect(
      (resolved as Extract<ObservableEvent, { type: "RESOLVED" }>).latencyMs,
    ).toBeGreaterThanOrEqual(20);
  });

  it("emits STALE_ABORT (not REJECTED) when isStale", async () => {
    let resolveFirst!: (v: string) => void;

    const innerFn = async (query: string) => {
      if (query === "slow") {
        return new Promise<string>((r) => {
          resolveFirst = r;
        });
      }
      return `result:${query}`;
    };

    const latestFn = latest(innerFn);
    const events: ObservableEvent[] = [];
    const wrapped = observe(latestFn, { onEvent: (e) => events.push(e) });

    const p1 = wrapped("slow");
    const p2 = wrapped("fast");

    // Unblock the slow call — it will be stale because 'fast' already set token=2
    resolveFirst("slow-result");

    await Promise.allSettled([p1, p2]);

    const staleAborts = events.filter((e) => e.type === "STALE_ABORT");
    const rejecteds = events.filter((e) => e.type === "REJECTED");

    expect(staleAborts).toHaveLength(1);
    expect(staleAborts[0]).toMatchObject({ type: "STALE_ABORT", callId: 1 });
    expect(rejecteds).toHaveLength(0);
  });

  it("emits REJECTED with error on real failure", async () => {
    const events: ObservableEvent[] = [];
    const boom = new Error("boom");
    const fn = async () => {
      throw boom;
    };
    const wrapped = observe(fn, { onEvent: (e) => events.push(e) });

    await wrapped().catch(() => {
      /* expected */
    });

    const rejected = events.find((e) => e.type === "REJECTED");
    expect(rejected).toBeDefined();
    expect(rejected).toMatchObject({ type: "REJECTED", callId: 1 });
    expect(
      (rejected as Extract<ObservableEvent, { type: "REJECTED" }>).error,
    ).toBe(boom);
  });

  it("emits RESET when .reset() is called", () => {
    const fn = vi.fn(async (x: number) => x);
    const latestFn = latest(fn);
    const events: ObservableEvent[] = [];
    const wrapped = observe(latestFn, { onEvent: (e) => events.push(e) });

    expect(wrapped.reset).toBeDefined();
    wrapped.reset!();

    const resets = events.filter((e) => e.type === "RESET");
    expect(resets).toHaveLength(1);
    expect(resets[0].timestamp).toBeTypeOf("number");
  });

  it("filter option only emits matching events", async () => {
    const events: ObservableEvent[] = [];
    const fn = async (x: number) => x;
    const wrapped = observe(fn, {
      onEvent: (e) => events.push(e),
      filter: (e) => e.type === "RESOLVED",
    });

    await wrapped(1);
    await wrapped(2);

    // Only RESOLVED events pass the filter; CALL_START events are suppressed
    expect(events.every((e) => e.type === "RESOLVED")).toBe(true);
    expect(events).toHaveLength(2);
  });

  it("sampleRate=0 emits no events", async () => {
    const events: ObservableEvent[] = [];
    const fn = async (x: number) => x;
    const wrapped = observe(fn, {
      onEvent: (e) => events.push(e),
      sampleRate: 0,
    });

    await wrapped(1);
    await wrapped(2);

    expect(events).toHaveLength(0);
  });

  it("sampleRate=1 emits all events", async () => {
    const events: ObservableEvent[] = [];
    const fn = async (x: number) => x;
    const wrapped = observe(fn, {
      onEvent: (e) => events.push(e),
      sampleRate: 1,
    });

    await wrapped(1); // CALL_START + RESOLVED = 2 events

    expect(events).toHaveLength(2);
    expect(events[0].type).toBe("CALL_START");
    expect(events[1].type).toBe("RESOLVED");
  });

  it("bufferSize maintains ring buffer of last N events", async () => {
    const fn = async (x: number) => x;
    const wrapped = observe(fn, {
      onEvent: () => {
        /* sink */
      },
      bufferSize: 4,
    });

    // 3 calls × 2 events each = 6 events total; buffer keeps last 4
    await wrapped(1); // events 1–2
    await wrapped(2); // events 3–4
    await wrapped(3); // events 5–6 → oldest 2 evicted

    const buf = wrapped.getBuffer();
    expect(buf).toHaveLength(4);

    // Eviction order: 6 events total, buffer keeps last 4
    // Evicted: CALL_START(1), RESOLVED(1)
    // Remaining: CALL_START(2), RESOLVED(2), CALL_START(3), RESOLVED(3)
    expect(buf[0]).toMatchObject({ type: "CALL_START", callId: 2 });
    expect(buf[1]).toMatchObject({ type: "RESOLVED", callId: 2 });
    expect(buf[2]).toMatchObject({ type: "CALL_START", callId: 3 });
    expect(buf[3]).toMatchObject({ type: "RESOLVED", callId: 3 });
  });

  it("getBuffer returns empty array when bufferSize is 0 (default)", async () => {
    const fn = async (x: number) => x;
    const wrapped = observe(fn, {
      onEvent: () => {
        /* sink */
      },
    });

    await wrapped(1);

    expect(wrapped.getBuffer()).toEqual([]);
  });

  it("onEvent throwing does not propagate", async () => {
    const fn = async (x: number) => x;
    const wrapped = observe(fn, {
      onEvent: () => {
        throw new Error("handler exploded");
      },
    });

    await expect(wrapped(42)).resolves.toBe(42);
  });

  it("callId increments monotonically", async () => {
    const fn = async (x: number) => x;
    const events: ObservableEvent[] = [];
    const wrapped = observe(fn, { onEvent: (e) => events.push(e) });

    await wrapped(1);
    await wrapped(2);
    await wrapped(3);

    const callStarts = events.filter((e) => e.type === "CALL_START");
    expect(callStarts).toHaveLength(3);
    expect(callStarts[0].callId).toBe(1);
    expect(callStarts[1].callId).toBe(2);
    expect(callStarts[2].callId).toBe(3);
  });

  it("wraps latest(fn) and tracks stale events correctly", async () => {
    let resolveFirst!: (v: string) => void;

    const innerFn = vi.fn(async (query: string) => {
      if (query === "slow") {
        return new Promise<string>((r) => {
          resolveFirst = r;
        });
      }
      return `result:${query}`;
    });

    const latestFn = latest(innerFn);
    const events: ObservableEvent[] = [];
    const wrapped = observe(latestFn, { onEvent: (e) => events.push(e) });

    const p1 = wrapped("slow");
    const p2 = wrapped("fast");

    // Resolve slow after fast has made slow stale
    resolveFirst("slow-result");

    await Promise.allSettled([p1, p2]);

    expect(events.some((e) => e.type === "STALE_ABORT" && e.callId === 1)).toBe(
      true,
    );
    expect(events.some((e) => e.type === "RESOLVED" && e.callId === 2)).toBe(
      true,
    );
    expect(events.some((e) => e.type === "REJECTED")).toBe(false);
  });

  it("forwards current() when wrapped fn has it", async () => {
    const fn = vi.fn(async (x: number) => x);
    const latestFn = latest(fn);
    const wrapped = observe(latestFn, {
      onEvent: () => {
        /* sink */
      },
    });

    expect(wrapped.current).toBeDefined();

    await wrapped(1);
    await wrapped(2);

    // latest() increments its own call counter
    expect(wrapped.current!()).toBe(2);
  });
});

// ─── createEventBus() ───────────────────────────────────────────────────────

describe("createEventBus()", () => {
  const makeResetEvent = (): ObservableEvent => ({
    type: "RESET",
    timestamp: Date.now(),
  });

  it("subscribe receives all emitted events", () => {
    const bus = createEventBus();
    const received: ObservableEvent[] = [];
    bus.subscribe((e) => received.push(e));

    const e1 = makeResetEvent();
    const e2 = makeResetEvent();
    bus.emit(e1);
    bus.emit(e2);

    expect(received).toHaveLength(2);
    expect(received[0]).toBe(e1);
    expect(received[1]).toBe(e2);
  });

  it("unsubscribe stops receiving events", () => {
    const bus = createEventBus();
    const received: ObservableEvent[] = [];
    const unsub = bus.subscribe((e) => received.push(e));

    bus.emit(makeResetEvent());
    unsub();
    bus.emit(makeResetEvent());

    expect(received).toHaveLength(1);
  });

  it("subscribeOnce fires exactly once", () => {
    const bus = createEventBus();
    let count = 0;
    bus.subscribeOnce(() => count++);

    bus.emit(makeResetEvent());
    bus.emit(makeResetEvent());
    bus.emit(makeResetEvent());

    expect(count).toBe(1);
  });

  it("subscribeOnce returns an unsubscribe function that cancels before first fire", () => {
    const bus = createEventBus();
    let count = 0;
    const unsub = bus.subscribeOnce(() => count++);

    unsub(); // Cancel before any event fires
    bus.emit(makeResetEvent());

    expect(count).toBe(0);
  });

  it("multiple subscribers all receive events", () => {
    const bus = createEventBus();
    const counts = [0, 0, 0];
    bus.subscribe(() => counts[0]++);
    bus.subscribe(() => counts[1]++);
    bus.subscribe(() => counts[2]++);

    bus.emit(makeResetEvent());

    expect(counts).toEqual([1, 1, 1]);
  });

  it("clear() removes all subscribers", () => {
    const bus = createEventBus();
    let count = 0;
    bus.subscribe(() => count++);
    bus.subscribe(() => count++);

    bus.clear();
    bus.emit(makeResetEvent());

    expect(count).toBe(0);
  });

  it("history() returns all stored events", () => {
    const bus = createEventBus();

    const e1 = makeResetEvent();
    const e2: ObservableEvent = {
      type: "BATCH_FLUSH",
      batchSize: 5,
      timestamp: Date.now(),
    };
    bus.emit(e1);
    bus.emit(e2);

    expect(bus.history()).toHaveLength(2);
    expect(bus.history()[0]).toBe(e1);
    expect(bus.history()[1]).toBe(e2);
  });

  it("history(limit) returns last N events", () => {
    const bus = createEventBus();

    for (let i = 0; i < 5; i++) {
      bus.emit(makeResetEvent());
    }

    const last2 = bus.history(2);
    expect(last2).toHaveLength(2);
  });

  it("emit is safe even when a subscriber throws", () => {
    const bus = createEventBus();
    const received: ObservableEvent[] = [];

    bus.subscribe(() => {
      throw new Error("subscriber error");
    });
    bus.subscribe((e) => received.push(e));

    expect(() => bus.emit(makeResetEvent())).not.toThrow();
    expect(received).toHaveLength(1);
  });

  it("tick — subscribe and unsubscribe do not interfere with ongoing iteration", () => {
    const bus = createEventBus();
    const log: string[] = [];

    const unsub1 = bus.subscribe(() => log.push("a"));
    bus.subscribe(() => {
      // This subscriber removes 'a' mid-iteration; 'a' has already been called
      unsub1();
      log.push("b");
    });

    bus.emit(makeResetEvent());

    // Both fire on the first emission (snapshot taken before loop)
    expect(log).toEqual(["a", "b"]);

    log.length = 0;
    bus.emit(makeResetEvent());

    // 'a' was unsubscribed so only 'b' fires now
    expect(log).toEqual(["b"]);
  });

  it("history returns a snapshot (mutations do not affect internal storage)", () => {
    const bus = createEventBus();
    bus.emit(makeResetEvent());

    const snap = bus.history();
    snap.push(makeResetEvent()); // mutate returned array

    expect(bus.history()).toHaveLength(1); // internal storage unchanged
  });

  it("tick — uses observe() onEvent to pipe into event bus", async () => {
    const bus = createEventBus();
    const received: ObservableEvent[] = [];
    bus.subscribe((e) => received.push(e));

    const fn = async (x: number) => x * 3;
    const wrapped = observe(fn, { onEvent: (e) => bus.emit(e) });

    await wrapped(7);

    expect(received.some((e) => e.type === "CALL_START")).toBe(true);
    expect(received.some((e) => e.type === "RESOLVED")).toBe(true);
  });

  it("tick — tick helper resolves on next macrotask", async () => {
    let done = false;
    void tick().then(() => {
      done = true;
    });
    expect(done).toBe(false);
    await tick();
    expect(done).toBe(true);
  });
});
