/** FIFO storage that releases consumed references without shifting on every read. */
export class FifoQueue<T extends object> {
  private readonly items: Array<T | undefined> = []
  private head = 0

  get size() {
    return this.items.length - this.head
  }

  push(item: T) {
    this.items.push(item)
  }

  shift(): T | undefined {
    const item = this.items[this.head]
    if (!item) return undefined

    this.items[this.head] = undefined
    this.head++
    this.compact()
    return item
  }

  drain() {
    const drained: T[] = []
    let item = this.shift()
    while (item) {
      drained.push(item)
      item = this.shift()
    }
    return drained
  }

  private compact() {
    if (this.head === 0) return
    if (this.head >= this.items.length) {
      this.items.length = 0
      this.head = 0
    } else if (this.head > 1024) {
      this.items.splice(0, this.head)
      this.head = 0
    }
  }
}
