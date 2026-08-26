# taskrill

A lightweight in-process scheduler for event-driven task graphs with global and
per-node concurrency control.

Task nodes are reusable and may form fan-out, fan-in, conditional edges, and
cycles. Taskrill reports execution facts; application state decides what those
facts mean and which work they unlock.

## Quickstart

```sh
npm install taskrill
```

```ts
import { Runtime } from 'taskrill'

interface ImageTask {
  batchId: string
  file: string
}

const runtime = new Runtime({ concurrency: 8 })

const Resize = runtime.node<ImageTask>(
  async ({ file }, { signal }) => {
    await resizeImage(file, { signal })
  },
  { name: 'Resize', concurrency: 4 },
)

const Thumbnail = runtime.node<ImageTask>(
  async ({ file }, { signal }) => {
    await makeThumbnail(file, { signal })
  },
  { name: 'Thumbnail', concurrency: 2 },
)

const Report = runtime.node<{ batchId: string }>(async ({ batchId }) => {
  await createReport(batchId)
})

Resize.on('task:complete', ({ input }) => {
  state.completeResize(input.batchId, input.file)
  advance(input.batchId)
})

Thumbnail.on('task:complete', ({ input }) => {
  state.completeThumbnail(input.batchId, input.file)
  advance(input.batchId)
})

function advance(batchId: string) {
  if (!state.takeReportReady(batchId)) return
  Report.submit({ batchId })
}

runtime.on('task:failure', ({ node, input, error }) => {
  console.error(`Task failed in ${node.name}`, input, error)
})

runtime.on('idle', () => {
  if (!state.isComplete()) state.showAvailableRecoveryActions()
})

for (const file of await listFiles()) {
  const input = { batchId: 'images-2026-08-27', file }
  Resize.submit(input)
  Thumbnail.submit(input)
}
```

`state.takeReportReady()` owns the fan-in decision and atomically consumes it,
preventing duplicate report submissions. Taskrill does not guess how tasks from
different nodes are correlated or which outcomes satisfy a dependency.

## API

| API | Description |
| --- | --- |
| `new Runtime(options)` | Creates a scheduler with a required global `concurrency` limit and optional `AbortSignal`. |
| `runtime.node(handler, options?)` | Creates a reusable typed task node. `options.concurrency` can further limit that node. |
| `node.submit(input)` | Accepts a task for deferred execution. It is non-blocking and returns no result. |
| `node.on(event, listener)` | Observes typed submit, start, completion, failure, and cancellation facts synchronously. |
| `runtime.on('task:failure', listener)` | Observes failures from every node for logging or runtime-wide policy. |
| `runtime.on('idle', listener)` | Observes multi-shot transitions to zero pending tasks. Idleness is not graph completion. |

Queued tasks are cancelled when the runtime signal aborts. Running handlers are
not forcibly stopped; they receive the same signal and settle normally.

## License

MIT
