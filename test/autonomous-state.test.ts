import { promises as fs } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { JsonBridgeStateStore } from "../src/state/store.js";
import { createIngressRecoveryKeyring } from "../src/state/recovery-envelope.js";

const ISSUE_ID = "123e4567-e89b-42d3-a456-426614174000";

describe("autonomous goal durable state", () => {
  const tempDirs: string[] = [];

  afterEach(async () => {
    await Promise.all(
      tempDirs.map((dir) => fs.rm(dir, { recursive: true, force: true })),
    );
    tempDirs.length = 0;
  });

  async function store(ownerId = "goal-owner"): Promise<JsonBridgeStateStore> {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "goal-state-"));
    tempDirs.push(dir);
    return new JsonBridgeStateStore(path.join(dir, "bridge-state.json"), {
      ownerId,
      recoveryKeyring: createIngressRecoveryKeyring("A".repeat(43)),
    });
  }

  it("persists authorization, bounded step transitions, blocking, and resume", async () => {
    const state = await store();
    await state.prepareAutonomousGoal({
      linearSessionId: "session-1",
      issueId: ISSUE_ID,
      issueIdentifier: "LIN-1",
      runtime: "claude",
    });
    await state.activateAutonomousGoal("session-1");

    const started = await state.beginAutonomousGoalStep("session-1");
    expect(started).toMatchObject({
      disposition: "started",
      goal: { status: "running", step: 1, stepsSinceGuidance: 1 },
    });
    await state.blockAutonomousGoal("session-1", "goal-step-1-blocked");
    expect(await state.getAutonomousGoal("session-1")).toMatchObject({
      status: "blocked",
      pendingNotice: {
        kind: "elicitation",
        activityKey: "goal-step-1-blocked",
      },
    });

    await state.clearAutonomousGoalPendingNotice(
      "session-1",
      "goal-step-1-blocked",
    );
    await state.resumeAutonomousGoal("session-1");
    expect(await state.getAutonomousGoal("session-1")).toMatchObject({
      status: "active",
      stepsSinceGuidance: 0,
    });
  });

  it("allows only one concurrent step to cross the durable running fence", async () => {
    const state = await store();
    await state.prepareAutonomousGoal({
      linearSessionId: "session-concurrent",
      issueId: ISSUE_ID,
      runtime: "codex",
    });
    await state.activateAutonomousGoal("session-concurrent");

    const results = await Promise.all([
      state.beginAutonomousGoalStep("session-concurrent"),
      state.beginAutonomousGoalStep("session-concurrent"),
    ]);

    expect(results.filter((result) => result.disposition === "started")).toHaveLength(
      1,
    );
    expect(
      results.filter((result) => result.disposition === "not_active"),
    ).toHaveLength(1);
  });

  it("survives restart without transferring ownership of an in-flight step", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "goal-restart-"));
    tempDirs.push(dir);
    const statePath = path.join(dir, "bridge-state.json");
    const first = new JsonBridgeStateStore(statePath, { ownerId: "first" });
    await first.prepareAutonomousGoal({
      linearSessionId: "session-restart",
      issueId: ISSUE_ID,
      runtime: "claude",
    });
    await first.activateAutonomousGoal("session-restart");
    await first.beginAutonomousGoalStep("session-restart");

    const restarted = new JsonBridgeStateStore(statePath, { ownerId: "second" });
    expect(await restarted.beginAutonomousGoalStep("session-restart")).toMatchObject({
      disposition: "not_active",
      goal: { status: "running", runningOwnerId: "first" },
    });
  });

  it("atomically stops a goal when a Linear stop activity is claimed", async () => {
    const state = await store();
    await state.prepareAutonomousGoal({
      linearSessionId: "session-stop",
      issueId: ISSUE_ID,
      runtime: "claude",
    });
    await state.activateAutonomousGoal("session-stop");
    await state.beginAutonomousGoalStep("session-stop");

    const cursor = { createdAt: "2026-09-18T12:00:00.000Z", id: "stop-1" };
    await state.claimStopEvent(
      {
        webhookId: "delivery-stop-1",
        executionId: "stop-1",
        linearSessionId: "session-stop",
        action: "prompted",
      },
      cursor,
    );

    expect(await state.getAutonomousGoal("session-stop")).toMatchObject({
      status: "stopped",
    });
  });

  it("records guidance with its ingress claim until that FIFO turn consumes it", async () => {
    const state = await store();
    await state.prepareAutonomousGoal({
      linearSessionId: "session-guidance",
      issueId: ISSUE_ID,
      runtime: "claude",
    });
    await state.activateAutonomousGoal("session-guidance");
    await state.beginAutonomousGoalStep("session-guidance");

    await state.claimEvent(
      {
        webhookId: "delivery-guidance-1",
        executionId: "guidance-1",
        linearSessionId: "session-guidance",
        action: "prompted",
      },
      {
        action: "prompted",
        prompt: "Apply the correction.",
        occurredAt: "2026-09-18T12:00:00.000Z",
        stop: false,
      },
    );

    expect(await state.getAutonomousGoal("session-guidance")).toMatchObject({
      status: "running",
      pendingGuidanceIds: ["guidance-1"],
    });
    await state.continueAutonomousGoal("session-guidance");
    await state.resumeAutonomousGoal("session-guidance", "guidance-1");
    expect(await state.getAutonomousGoal("session-guidance")).toMatchObject({
      status: "active",
      pendingGuidanceIds: [],
      stepsSinceGuidance: 0,
    });
  });
});
