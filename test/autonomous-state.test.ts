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
      openingRecoverySequence: 1,
      objective: "Complete session 1.",
    });
    await state.activateAutonomousGoal("session-1");

    const started = await state.beginAutonomousGoalStep("session-1");
    expect(started).toMatchObject({
      disposition: "started",
      goal: { status: "running", step: 1, stepsSinceGuidance: 1 },
    });
    await state.blockAutonomousGoal(
      "session-1",
      "goal-step-1-blocked",
      "Which environment should I use?",
    );
    expect(await state.getAutonomousGoal("session-1")).toMatchObject({
      status: "blocked",
      pendingNotice: {
        kind: "elicitation",
        activityKey: "goal-step-1-blocked",
        envelope: expect.any(Object),
      },
    });
    await expect(
      state.getAutonomousGoalPendingNoticeBody(
        "session-1",
        "goal-step-1-blocked",
      ),
    ).resolves.toBe("Which environment should I use?");

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
      openingRecoverySequence: 1,
      objective: "Complete the concurrent session.",
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
    const first = new JsonBridgeStateStore(statePath, {
      ownerId: "first",
      recoveryKeyring: createIngressRecoveryKeyring("A".repeat(43)),
    });
    await first.prepareAutonomousGoal({
      linearSessionId: "session-restart",
      issueId: ISSUE_ID,
      runtime: "claude",
      openingRecoverySequence: 1,
      objective: "Complete the restarted session.",
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
      openingRecoverySequence: 1,
      objective: "Complete the stoppable session.",
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
      openingRecoverySequence: 1,
      objective: "Complete the guided session.",
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
    await expect(
      state.beginAutonomousGoalStep("session-guidance"),
    ).resolves.toMatchObject({
      disposition: "guidance_pending",
      goal: { status: "active", pendingGuidanceIds: ["guidance-1"] },
    });
    await state.resumeAutonomousGoal("session-guidance", "guidance-1");
    expect(await state.getAutonomousGoal("session-guidance")).toMatchObject({
      status: "active",
      pendingGuidanceIds: [],
      stepsSinceGuidance: 0,
    });
  });

  it("adopts guidance claimed after the opening event but before goal preparation", async () => {
    const state = await store();
    const opening = await state.claimEvent(
      {
        webhookId: "delivery-opening-race",
        executionId: "created:session-opening-race",
        linearSessionId: "session-opening-race",
        action: "created",
      },
      {
        action: "created",
        prompt: "Start the goal.",
        occurredAt: "2026-09-18T12:00:00.000Z",
        issueIdentifier: "LIN-1",
      },
    );
    expect(opening.disposition).toBe("claimed");
    const openingSequence = opening.receipt.recoverySequence!;

    await state.claimEvent(
      {
        webhookId: "delivery-guidance-race",
        executionId: "guidance-race",
        linearSessionId: "session-opening-race",
        action: "prompted",
      },
      {
        action: "prompted",
        prompt: "Apply this before starting.",
        occurredAt: "2026-09-18T12:00:01.000Z",
        stop: false,
      },
    );

    await state.prepareAutonomousGoal({
      linearSessionId: "session-opening-race",
      issueId: ISSUE_ID,
      runtime: "claude",
      openingRecoverySequence: openingSequence,
      objective: "Start the goal.",
    });

    expect(await state.getAutonomousGoal("session-opening-race")).toMatchObject({
      status: "authorizing",
      pendingGuidanceIds: ["guidance-race"],
    });
  });

  it("persists a terminal goal when a stop fence wins before preparation", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "goal-stop-prepare-"));
    tempDirs.push(dir);
    const statePath = path.join(dir, "bridge-state.json");
    const recoveryKeyring = createIngressRecoveryKeyring("A".repeat(43));
    const state = new JsonBridgeStateStore(statePath, {
      ownerId: "goal-stop-prepare-first",
      recoveryKeyring,
    });
    const opening = await state.claimEvent(
      {
        webhookId: "delivery-stop-prepare-opening",
        executionId: "created:session-stop-prepare",
        linearSessionId: "session-stop-prepare",
        action: "created",
      },
      {
        action: "created",
        prompt: "Start the goal.",
        occurredAt: "2026-09-18T12:00:00.000Z",
        issueIdentifier: "LIN-1",
      },
    );
    const stopCursor = {
      createdAt: "2026-09-18T12:00:01.000Z",
      id: "stop-before-prepare",
    };
    await state.claimStopEvent(
      {
        webhookId: "delivery-stop-before-prepare",
        executionId: stopCursor.id,
        linearSessionId: "session-stop-prepare",
        action: "prompted",
      },
      stopCursor,
      {
        action: "prompted",
        prompt: "stop",
        occurredAt: stopCursor.createdAt,
        stop: true,
      },
    );

    await state.prepareAutonomousGoal({
      linearSessionId: "session-stop-prepare",
      issueId: ISSUE_ID,
      runtime: "claude",
      openingRecoverySequence: opening.receipt.recoverySequence!,
      objective: "Start the goal unless stopped.",
    });

    const reopened = new JsonBridgeStateStore(statePath, {
      ownerId: "goal-stop-prepare-reopened",
      recoveryKeyring,
    });
    expect(await reopened.getAutonomousGoal("session-stop-prepare")).toMatchObject(
      {
        status: "stopped",
        pendingGuidanceIds: [],
      },
    );
    await expect(reopened.listRecoverableAutonomousGoals()).resolves.toEqual([]);
  });
});
