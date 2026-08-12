/**
 * Serial task queue, concurrency fixed at 1.
 *
 * Constraint (do not relax): parallel headless Claude invocations on this
 * machine have cross-contaminated content between sessions before
 * (documented 2026-04-26). All runtime sessions execute one at a time.
 * Follow-up prompts for a session that is already running are appended to
 * that session's pending prompts rather than queued as separate tasks.
 */
export class SerialQueue {
  /**
   * Tail of the serial chain. Always settles (fulfilled), even when a task
   * rejects, so one failing task never stalls the tasks queued behind it.
   */
  private tail: Promise<void> = Promise.resolve();
  private waiting = 0;
  private running = 0;

  /** Enqueue a task; resolves when the task itself completes. */
  enqueue<T>(task: () => Promise<T>): Promise<T> {
    this.waiting++;

    const runPromise = this.tail.then(async () => {
      this.waiting--;
      this.running++;
      try {
        return await task();
      } finally {
        this.running--;
      }
    });

    this.tail = runPromise.then(
      () => undefined,
      () => undefined,
    );

    return runPromise;
  }

  /** Number of tasks waiting or running. */
  get size(): number {
    return this.waiting + this.running;
  }
}
