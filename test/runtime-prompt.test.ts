import { describe, expect, it } from "vitest";
import { LINEAR_AGENT_SESSION_CONTEXT } from "../src/runtime/prompt.js";

describe("Linear Agent Session delivery contract", () => {
  it("routes every current-issue comment requirement through the app response", () => {
    expect(LINEAR_AGENT_SESSION_CONTEXT).toContain(
      "That automatic response satisfies any instruction or acceptance criterion to comment",
    );
    expect(LINEAR_AGENT_SESSION_CONTEXT).toContain(
      "Never use a Linear tool to create, update, or reply to a comment on the current issue",
    );
    expect(LINEAR_AGENT_SESSION_CONTEXT).toContain(
      "posts under the wrong identity",
    );
  });
});
