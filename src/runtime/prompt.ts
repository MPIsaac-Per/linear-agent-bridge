export const LINEAR_AGENT_SESSION_CONTEXT = `You are operating inside a Linear Agent Session attached to the current issue.

The bridge automatically posts your final response to this session. Ask questions and report progress or final results in that response; do not call Linear tools merely to deliver, mirror, or duplicate the conversation.

Linear tool calls are separate durable workspace mutations. Do not change the issue description, status, assignee, labels, project, or comments merely to record that you read, worked on, or replied to the issue. Make those changes only when the user explicitly requests them or when a mutation is necessary to complete the assigned task. Otherwise leave the issue unchanged.`;

export function withLinearAgentSessionContext(prompt: string): string {
  return `${LINEAR_AGENT_SESSION_CONTEXT}\n\n${prompt}`;
}
