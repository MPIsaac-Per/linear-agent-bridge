export type AutonomousGoalDecision =
  | {
      status: "continue";
      message: string;
    }
  | {
      status: "blocked";
      message: string;
    }
  | {
      status: "completed";
      message: string;
      verification: string;
    };

const RESULT_OPEN = "<linear_autonomous_result>";
const RESULT_CLOSE = "</linear_autonomous_result>";
const MAX_RESULT_FIELD_LENGTH = 20_000;

export function autonomousGoalPrompt(
  prompt: string,
  options: { step: number; maxSteps: number; continuation: boolean },
): string {
  const direction = options.continuation
    ? "Continue the same assigned issue from the current provider session. Use the existing context and keep making concrete progress."
    : prompt;
  return `${direction}

AUTONOMOUS EXECUTION CONTRACT
This issue carries the Linear label that explicitly authorizes autonomous execution. Work on the issue now. Do not ask for permission to take ordinary in-scope implementation steps.

This is bounded step ${options.step} of at most ${options.maxSteps} steps since the last human message. At the end of this turn, return exactly one machine-readable result and no text outside it:

${RESULT_OPEN}
{"status":"continue|blocked|completed","message":"A concise user-visible progress update, question, or final result","verification":"Required only for completed: the concrete checks or evidence that prove the work is finished"}
${RESULT_CLOSE}

Use "continue" only when another provider turn can make useful progress without user input. Use "blocked" only when a specific user answer or external state change is required; message must ask the actionable question. Use "completed" only after the requested work is actually finished and you have run appropriate verification. Never claim completion merely because a step limit is near.`;
}

export function parseAutonomousGoalDecision(
  response: string,
): AutonomousGoalDecision | undefined {
  const trimmed = response.trim();
  if (!trimmed.startsWith(RESULT_OPEN) || !trimmed.endsWith(RESULT_CLOSE)) {
    return undefined;
  }
  const json = trimmed.slice(RESULT_OPEN.length, -RESULT_CLOSE.length).trim();
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch {
    return undefined;
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return undefined;
  }
  const record = parsed as Record<string, unknown>;
  if (
    (record.status !== "continue" &&
      record.status !== "blocked" &&
      record.status !== "completed") ||
    typeof record.message !== "string" ||
    record.message.trim() === "" ||
    record.message.length > MAX_RESULT_FIELD_LENGTH
  ) {
    return undefined;
  }
  if (record.status === "completed") {
    if (
      typeof record.verification !== "string" ||
      record.verification.trim() === "" ||
      record.verification.length > MAX_RESULT_FIELD_LENGTH
    ) {
      return undefined;
    }
    return {
      status: "completed",
      message: record.message.trim(),
      verification: record.verification.trim(),
    };
  }
  return { status: record.status, message: record.message.trim() };
}
