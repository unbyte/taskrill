# taskrill

A lightweight in-process task runtime for expressing fine-grained asynchronous pipelines with dynamic fan-out.

## Quickstart

```sh
npm install taskrill
```

```ts
import { Runtime, join } from 'taskrill'

const runtime = new Runtime({ concurrency: 4 })

// A single: runs exactly once, after both groups have fully settled.
const Report = runtime.single(async () => {
  console.log('all images processed')
})

// Two groups: dynamically growing sets of tasks, each started as a slot frees up.
const Resize = runtime.group<string>(async (file, { signal }) => {
  await resizeImage(file, { signal })
})

const Thumbnail = runtime.group<string>(async (file, { signal }) => {
  await makeThumbnail(file, { signal })
})

// Wire the seal graph: run Report once both groups have settled.
join(Resize, Thumbnail).then((s) => (s.ok ? Report.submit() : Report.skip()))

for (const file of await listFiles()) {
  Resize.submit(file)
  Thumbnail.submit(file)
}
Resize.seal() // no more files are coming
Thumbnail.seal()

await Report.done
```

## API

| API | Description |
| --- | --- |
| `new Runtime(options)` | One in-process scheduling environment: task queue, global `concurrency` limit, execution, and failure reporting. |
| `runtime.group(handler, options?)` | Creates a dynamically growing set of tasks. `submit(input)` adds work (non-blocking); `seal()` declares the set final. |
| `runtime.single(handler, options?)` | A group capped at one task with auto-sealing. `submit()` runs it once; `skip()` seals it empty. |
| `join(...units)` | Awaits several units and reduces their settlements into one, so a downstream unit can be sealed after multiple upstreams settle. |
| `unit` | A unit is a group or a single — the two kinds of task set a runtime creates. |
| `unit.done` | A `Promise<Settlement>` that resolves — never rejects — once the unit is sealed and every task is terminal. |

## License

MIT
