import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    pool: "forks",
    poolOptions: {
      forks: {
        // expose global.gc() for memory-pressure and gc-edge-cases tests
        execArgv: ["--expose-gc"],
      },
    },
    testTimeout: 60_000,
    hookTimeout: 60_000,
    teardownTimeout: 60_000,
    include: ["src/**/*.test.ts"],
    exclude: ["src/jest.test.ts", "e2e/**/*"],
  },
});
