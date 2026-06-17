// Example Cloudflare Worker implementing search + cancel endpoint
// for `call-latest`'s distributed cancellation + delta/ETag protocol.
//
// This file is NOT bundled by the library itself; it's a usage example.

export interface Env {
  SEARCH_DB: D1Database; // or any backing store you like
}

type InFlight = {
  controller: AbortController;
};

const inflight = new Map<number, InFlight>();

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/api/search/cancel" && request.method === "POST") {
      return handleCancel(request);
    }

    if (url.pathname === "/api/search" && request.method === "GET") {
      return handleSearch(request, env, ctx);
    }

    return new Response("Not found", { status: 404 });
  },
};

async function handleCancel(request: Request): Promise<Response> {
  try {
    const body = (await request.json().catch(() => ({}))) as { callId?: number };
    const callId = typeof body.callId === "number" ? body.callId : undefined;
    if (callId === undefined) {
      return new Response("missing callId", { status: 400 });
    }
    const current = inflight.get(callId);
    if (current) {
      current.controller.abort("cancelled-by-client");
      inflight.delete(callId);
    }
  } catch {
    // best effort: ignore parse errors
  }
  return new Response(null, { status: 204 });
}

async function handleSearch(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const url = new URL(request.url);
  const query = url.searchParams.get("q") ?? "";
  const callIdHeader = request.headers.get("X-Call-Latest-Id");
  const callId = callIdHeader ? Number(callIdHeader) : NaN;
  const mode = request.headers.get("X-Call-Latest-Mode") ?? "normal";

  const ifNoneMatch = request.headers.get("If-None-Match") ?? undefined;

  const controller = new AbortController();
  const signal = controller.signal;

  if (!Number.isNaN(callId)) {
    inflight.set(callId, { controller });
    ctx.waitUntil(
      Promise.resolve().then(() => {
        // Clean up when the request finishes or aborts.
        signal.addEventListener(
          "abort",
          () => {
            inflight.delete(callId);
          },
          { once: true },
        );
      }),
    );
  }

  // Example: simple D1 search. Replace with your own logic.
  if (signal.aborted) {
    return new Response("aborted", { status: 499 });
  }

  // Compute a data version (ETag) – could be a hash of result set, updated_at, etc.
  const version = await computeVersion(env, query);

  if (ifNoneMatch && ifNoneMatch === version) {
    // Client already has this version: respond with 304 and no body.
    return new Response(null, {
      status: 304,
      headers: {
        ETag: version,
        "Cache-Control": "private, max-age=0, must-revalidate",
      },
    });
  }

  const results = await searchDb(env, query, signal, mode);

  const body = JSON.stringify({
    items: results,
    version,
  });

  return new Response(body, {
    status: 200,
    headers: {
      "content-type": "application/json",
      ETag: version,
      "Cache-Control": "private, max-age=0, must-revalidate",
    },
  });
}

async function computeVersion(env: Env, query: string): Promise<string> {
  // For demo purposes we just use a stable hash based on query + a global version.
  // In a real system, use last-updated timestamps or a materialized view version.
  const base = `v1:${query}`;
  const digest = await crypto.subtle.digest("SHA-1", new TextEncoder().encode(base));
  const bytes = Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return `"${bytes.slice(0, 12)}"`; // ETag format with quotes
}

async function searchDb(
  env: Env,
  query: string,
  signal: AbortSignal,
  mode: string,
): Promise<Array<{ id: string; title: string }>> {
  // Example: respect mode to simplify query under high load.
  const limit = mode === "conserve" ? 5 : 20;
  // Abort-aware DB work: your real implementation should periodically check `signal.aborted`.
  const stmt = await env.SEARCH_DB.prepare(
    "SELECT id, title FROM documents WHERE title LIKE ? LIMIT ?",
  ).bind(`%${query}%`, limit);

  const rows = await stmt.all<Array<{ id: string; title: string }>>();
  if (signal.aborted) {
    throw new Error("search aborted");
  }
  return rows.results ?? [];
}

