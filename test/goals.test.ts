import { describe, expect, it } from "vitest";
import {
  autonomousGoalPrompt,
  parseAutonomousGoalDecision,
} from "../src/goals/protocol.js";

describe("autonomous goal protocol", () => {
  it("adds a bounded provider-neutral contract without command syntax", () => {
    const prompt = autonomousGoalPrompt("Implement the issue", {
      step: 2,
      maxSteps: 6,
      continuation: false,
    });

    expect(prompt).toContain("Implement the issue");
    expect(prompt).toContain("bounded step 2 of at most 6");
    expect(prompt).not.toContain("/goal");
    expect(prompt).not.toContain("model");
  });

  it("parses continue and blocked decisions", () => {
    expect(
      parseAutonomousGoalDecision(
        '<linear_autonomous_result>{"status":"continue","message":"Tests added."}</linear_autonomous_result>',
      ),
    ).toEqual({ status: "continue", message: "Tests added." });
    expect(
      parseAutonomousGoalDecision(
        '<linear_autonomous_result>{"status":"blocked","message":"Which account should I use?"}</linear_autonomous_result>',
      ),
    ).toEqual({
      status: "blocked",
      message: "Which account should I use?",
    });
  });

  it("requires verification for completion", () => {
    expect(
      parseAutonomousGoalDecision(
        '<linear_autonomous_result>{"status":"completed","message":"Done.","verification":"Typecheck and 12 tests passed."}</linear_autonomous_result>',
      ),
    ).toEqual({
      status: "completed",
      message: "Done.",
      verification: "Typecheck and 12 tests passed.",
    });
    expect(
      parseAutonomousGoalDecision(
        '<linear_autonomous_result>{"status":"completed","message":"Done."}</linear_autonomous_result>',
      ),
    ).toBeUndefined();
  });

  it("fails closed on prose, malformed JSON, and trailing output", () => {
    expect(parseAutonomousGoalDecision("Done.")).toBeUndefined();
    expect(
      parseAutonomousGoalDecision(
        "<linear_autonomous_result>{bad}</linear_autonomous_result>",
      ),
    ).toBeUndefined();
    expect(
      parseAutonomousGoalDecision(
        '<linear_autonomous_result>{"status":"continue","message":"More"}</linear_autonomous_result> trailing',
      ),
    ).toBeUndefined();
  });
});
