# taskrill

## Unreleased

- Replaced finite singles and groups with reusable task nodes.
- Replaced aggregate settlements and joins with synchronous lifecycle events and runtime idleness.
- Task submissions now return their runtime-wide task ID, or `undefined` when the runtime no longer accepts work.
- Added graceful runtime closure and an awaitable `closed` lifecycle that does not aggregate or reject on task failures.

## 0.1.0

### Minor Changes

- e807365: feat: concurrency limits for task groups
