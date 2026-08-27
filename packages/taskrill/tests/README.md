# Test organization

- `src/*.test.ts` verifies isolated scheduler, gate, queue, emitter, and runtime
  invariants against internal APIs.
- `contracts/*.contract.test.ts` verifies one narrow lifecycle guarantee through
  the exported `Runtime` API. These tests intentionally use the smallest graph
  that can expose an ordering or state-transition defect.
- `graphs/*.e2e.test.ts` models application-owned state, routing, retries,
  fan-out, fan-in, cycles, concurrency, and shutdown across complete workflows.

Contract tests should make failures easy to localize. Graph tests should prove
that several contracts compose correctly under realistic orchestration.
