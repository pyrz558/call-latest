import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    streaming: "src/streaming.ts",
    priority: "src/priority.ts",
    "request-budget": "src/request-budget.ts",
    "distributed-dedupe": "src/distributed-dedupe.ts",
    "multi-level-cache": "src/multi-level-cache.ts",
    "persistent-cache": "src/persistent-cache.ts",
    "cross-tab": "src/cross-tab.ts",
    telemetry: "src/telemetry.ts",
    ssr: "src/ssr.ts",
    edge: "src/edge.ts",
    "adapters/react": "src/adapters/react.ts",
    "adapters/vue": "src/adapters/vue.ts",
    "adapters/svelte": "src/adapters/svelte.ts",
    "adapters/solid": "src/adapters/solid.ts",
  },
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  sourcemap: true,
  treeshake: true,
  minify: true,
  splitting: false,
  target: "es2022",
});
