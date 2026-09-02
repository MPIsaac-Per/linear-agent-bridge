/**
 * Serial lanes keyed by Linear agent-session id.
 *
 * Turns in one Linear session run FIFO because they share a Claude
 * conversation. Distinct sessions run concurrently because their SDK
 * conversations are isolated by session UUID.
 *
 * Retired constraint: MPI-682 imported host-wide concurrency 1 from a
 * 2026-04-26 `claude -p` file-write incident. That incident did not apply to
 * the Claude Agent SDK's session-isolated conversations.
 */
interface Lane {
  tail: Promise<void>;
  size: number;
}

export class SessionLanes {
  private readonly lanes = new Map<string, Lane>();

  /** Enqueue a task in one session's FIFO lane. */
  enqueue<T>(sessionId: string, task: () => Promise<T>): Promise<T> {
    let lane = this.lanes.get(sessionId);
    if (lane === undefined) {
      lane = { tail: Promise.resolve(), size: 0 };
      this.lanes.set(sessionId, lane);
    }
    lane.size += 1;

    const runPromise = lane.tail.then(task);
    lane.tail = runPromise.then(
      () => this.finish(sessionId, lane),
      () => this.finish(sessionId, lane),
    );
    return runPromise;
  }

  /** Number of tasks waiting or running in one session's lane. */
  size(sessionId: string): number {
    return this.lanes.get(sessionId)?.size ?? 0;
  }

  /** Settle after every lane tail present at call time settles. */
  async drain(): Promise<void> {
    await Promise.all([...this.lanes.values()].map((lane) => lane.tail));
  }

  private finish(sessionId: string, lane: Lane): void {
    lane.size -= 1;
    if (lane.size === 0 && this.lanes.get(sessionId) === lane) {
      this.lanes.delete(sessionId);
    }
  }
}
