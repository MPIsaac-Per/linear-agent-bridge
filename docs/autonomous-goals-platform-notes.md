# Autonomous-goal platform notes

Research date: 2026-09-18. This note separates documented interface facts from
the bridge policy recommended from those facts. It intentionally contains no
prompt, token, or runtime-transcript contents.

## Linear: documented behavior

- An app must request `app:assignable` to appear as an issue delegate and
  `app:mentionable` to appear in mentions. Assigning an app sets
  `Issue.delegate`, rather than the human `assignee`; Linear presents this as
  delegation. [Linear: Getting started for agents](https://linear.app/developers/agents#mention--assign-scopes)
- Linear automatically creates an Agent Session when the app is either
  mentioned or delegated. Its own onboarding guide calls delegation—the user
  assigning the issue to the app—the common entry point. [Linear: Agent
  lifecycle](https://linear.app/developers/agents#agent-session-lifecycle)
- The Agent Session webhook has only two documented `action` values:
  `created` (a session created by *either* mention or delegation) and
  `prompted` (a new user message in an existing session). Linear directs an
  app to start a new loop for `created` and to insert a `prompted` activity
  into that conversation. [Linear: Agent Session
  webhooks](https://linear.app/developers/agent-interaction#session-webhooks)
- A `created` payload contains `agentSession`, optional `agentActivity`,
  `previousComments`, `promptContext`, and metadata. The schema describes
  `previousComments` as present only for a created session started by a mention
  in a child comment; it does **not** provide an enum or boolean for the
  initiating interaction. The session payload has `sourceCommentId`, but that
  describes the source comment when one exists, not a delegation/mention
  discriminator. [Linear raw SDL: `AgentSessionEventWebhookPayload`](https://raw.githubusercontent.com/linear/linear/refs/heads/master/packages/sdk/src/schema.graphql)
- `Issue.delegate`/`delegateId` exist and are specifically the agent delegated
  to work on the issue. An `Issue` data-change webhook includes the current
  `delegateId`, labels, and `labelIds`; data-change envelopes include
  `updatedFrom`, the previous values of changed fields. [Linear raw SDL:
  `IssueWebhookPayload`](https://raw.githubusercontent.com/linear/linear/refs/heads/master/packages/sdk/src/schema.graphql)
- Inbox-notification webhooks separately list `issueAssignedToYou`,
  `issueUnassignedFromYou`, `issueMention`, and `issueCommentMention` among
  useful actions. The raw schema also has distinct notification payload types
  for assignment and mentions. [Linear: interaction best
  practices](https://linear.app/developers/agent-best-practices#inbox-notifications-webhooks)
- Linear documents no correlation identifier that links an `Issue` update or
  inbox-notification delivery to the particular Agent Session `created`
  delivery it caused. Webhooks may retry after failures and must be answered
  within five seconds, so arrival order is not a safe correlation mechanism.
  [Linear: webhooks and retries](https://linear.app/developers/webhooks#how-does-a-webhook-work)
- Agent Activities are the durable, immutable conversation record that Linear
  recommends reading instead of editable comments. `response` means completed
  work; `elicitation` or `error` communicates needed user action or failure.
  Linear derives session UI state from emitted activities, including
  `awaitingInput` and `complete`. [Linear: agent interaction](https://linear.app/developers/agent-interaction#activity-content-payload),
  [Linear: best practices](https://linear.app/developers/agent-best-practices#agent-activities)
- A human `prompt` activity can carry the `stop` signal. Linear requires the
  agent to halt immediately—no further mutations or API calls—and then emit a
  final `response` or `error`. [Linear: stop
  signal](https://linear.app/developers/agent-signals#stop)
- A label is a fully native issue property: `IssueUpdateInput` accepts
  `addedLabelIds`, `removedLabelIds`, and `labelIds`; an app may create an
  Agent Session on a chosen issue with `agentSessionCreateOnIssue`. [Linear raw
  SDL: `IssueUpdateInput`](https://raw.githubusercontent.com/linear/linear/refs/heads/master/packages/sdk/src/schema.graphql),
  [Linear raw SDL: `AgentSessionCreateOnIssue`](https://raw.githubusercontent.com/linear/linear/refs/heads/master/packages/sdk/src/schema.graphql)

## Trigger decision

### Fact: assignment cannot be proved from AgentSessionEvent alone

`action: "created"` deliberately merges delegation and mention. A present
`previousComments` is evidence of one particular child-comment mention path,
but its absence is not evidence of delegation. Current delegation state is
also insufficient: a user can mention an app after it was already delegated.
Neither the Agent Session schema nor the public documentation exposes a
delivery-to-delivery cause/correlation key.

Joining an `issueAssignedToYou` notification or an Issue webhook to the nearest
`created` session by issue ID and time is therefore an inference, not a
reliable authorization record. It would be vulnerable to delayed/redelivered
webhooks, sessions created by both interactions, and concurrent activity.

### Recommendation: a configured, visible issue label is the autonomous-goal grant

Use a configured label ID (for example, a workspace label displayed as
**Autonomous**) as the v1 autonomous-goal authorization. Adding that label is a
discoverable, explicit Linear-native interaction; it contains no magic text in
the conversation. The user applies the label before delegating or mentioning
the app. When Linear creates the Agent Session, the bridge reads the issue's
current labels and persists a goal only when the configured ID is present.

This standing-grant design deliberately leaves session creation to Linear.
Although `agentSessionCreateOnIssue` is available, its documented input has no
idempotency key and the returned session ID is learned only after the mutation.
A process crash between that mutation and local persistence could therefore
create another session on retry. Checking a visible label on Linear's own
session-created event preserves the bridge's existing durable ingress identity
and avoids a new non-idempotent crash boundary. It also removes the need to
correlate differently ordered Issue and Agent Session webhooks.

Delegation remains the recommended way to state that the app is responsible
for the issue. It does not by itself select autonomous mode. A mention can open
the session too, but only the label grants the additional continuation and
issue-completion authority. After a goal starts, ordinary session messages are
guidance or answers.

Migration/default policy: autonomous mode is disabled unless the configured
label ID is set. Existing mention/delegation behavior stays one turn per
Agent-Session webhook. The label must be present before a new session opens;
an already recorded non-autonomous session is not silently upgraded. No Issue
data-change webhook subscription is required.

### Native alternatives and their limits

| Control | Documented strength | Limitation |
| --- | --- | --- |
| Delegate the issue to the app | Intended Linear agent workflow; app is visibly `Issue.delegate` | `created` does not say it was caused by delegation; no causal correlation to a separate assignment event. |
| Mention the app | Creates an Agent Session | Same `created` action as delegation; should remain a conversational turn, not a goal grant. |
| Inbox `issueAssignedToYou` | Distinct notification type | No documented relation to a session ID; it cannot safely choose among same-issue `created` sessions. |
| Issue webhook `delegateId` transition | Explicit issue-property change | No documented relation to a session ID; webhook timing cannot repair that. |
| Configured issue label checked on Linear's session creation | Explicit, inspectable issue property; uses the bridge's existing durable session ingress | Requires a label ID/configuration and the label to be applied before opening the session. Recommended. |
| Configured issue label + `agentSessionCreateOnIssue` | Explicit property and a bridge-created session | The mutation has no documented idempotency key; a crash before persisting the returned session ID can create duplicates. |

## Lifecycle constraints derived from Linear

The following is bridge policy informed by the facts above, not behavior
Linear supplies automatically:

1. **Authorizing:** after the Agent Session ingress claim reaches its durable
   dispatch boundary, persist the session/issue/runtime goal record, read the
   issue, and activate only if the configured label is present and the issue is
   not already completed. Preserve the opening objective only in a
   recovery-key AES-GCM envelope so guidance that races goal preparation cannot
   replace it; never store prompt or runtime-response text in plaintext.
2. **Active:** start one provider turn for that session. Emit a prompt-safe
   thought within Linear's ten-second `created` liveness window. Persist every
   runtime-session mapping and lifecycle transition before scheduling the next
   side effect.
3. **Continue:** after a successful, nonterminal turn, persist `active` before
   scheduling the next explicit provider turn in the same FIFO lane. Recheck
   the label before every turn. Record guidance activity IDs in the same durable
   ingress claim, then consume each ID only when its turn reaches the lane. If
   guidance is already claimed, yield to it before another continuation or
   completion. Stop after the configured count since the last human message
   and emit an elicitation rather than retrying without bound.
4. **Blocked:** persist `blocked`, emit one `elicitation`, and schedule no
   continuation. The next user `prompted` activity is the only normal resume
   trigger; it must clear the blocked state durably before it enters the
   per-session FIFO lane.
5. **Stopped:** claim the stop and set the goal's terminal state in the same
   durable state mutation, then abort active and older queued turns. No provider
   continuation or issue-completion mutation may begin after that fence.
6. **Completing:** require a structured `completed` result with a nonempty
   verification summary, recheck the label and issue state, persist the target
   completed workflow state, pending response key, and exact response body in
   an authenticated recovery envelope, then apply the idempotent issue-state
   update. Before emitting the final response, query the stable caller-generated
   activity ID; this reconciles a crash between the two Linear writes without
   duplicating the response. A separate durable completion dispatch boundary
   after the label query prevents a stop that already won its state transaction
   from being followed by `issueUpdate`.
7. **Restart:** resume `active`, `authorizing`, and `completing` boundaries.
   Recover pending activities by stable ID and decrypt the exact notice only
   when Linear does not already contain that activity. A goal found `running`
   belonged to an interrupted provider turn with unknown side effects, so
   persist its blocked state and encrypted elicitation before reconciling
   downtime ingress. Emit the elicitation before processing any recovered
   guidance instead of replaying the interrupted turn. A provider mismatch is
   handled the same way. Before accepted ingress recovery, any recovery turn,
   or a completion update can restart autonomous work, reconcile that session's
   activities so a stop or guidance message sent during downtime wins. If a
   goal-session preflight cannot read Linear activity, keep startup unready
   rather than dispatching accepted work through an unverified downtime window.

The existing bridge's durable ingress claims, per-session FIFO lanes, stop
fences, provider-session guard, and restart recovery remain the required
boundaries for every transition above. Do not put raw Linear prompt text into
logs or durable diagnostics.

## Provider continuation facts and implications

### Claude Agent SDK

- `query()` runs the autonomous tool loop within one call. Its `resume` option
  resumes a specific saved session; the session ID is available on result
  messages even for error results. The TypeScript `continue: true` option finds
  the most recent session and is unsuitable for a multi-session bridge; use
  the recorded ID instead. [Anthropic: work with
  sessions](https://code.claude.com/docs/en/agent-sdk/sessions)
- Claude sessions persist conversation history, not filesystem state. They are
  local to the originating machine unless transcripts are mirrored with a
  `SessionStore` adapter or copied deliberately. [Anthropic: session
  persistence](https://code.claude.com/docs/en/agent-sdk/sessions#resume-across-hosts)
- There is no documented Claude Agent SDK persistent *goal* primitive matching
  Codex's app-server goal API. The bridge must therefore own the portable goal
  state and use `resume` only to preserve provider conversation context.

### Codex

- The current TypeScript Codex SDK treats a `Thread` as conversation state and
  a `run()`/`runStreamed()` call as one turn. Calling it repeatedly on the same
  thread continues the conversation; after process loss, `resumeThread(id)`
  restores a thread persisted under `~/.codex/sessions`. `runStreamed()` is the
  appropriate boundary for progress and completion handling. [OpenAI Codex
  TypeScript SDK](https://github.com/openai/codex/blob/main/sdk/typescript/README.md)
- Codex app-server (a different integration surface from the bridge's current
  TypeScript SDK) has `thread/goal/set|get|clear`, plus durable
  `thread/resume`, `turn/start`, and `turn/interrupt`. App-server completion
  notifications report `completed`, `interrupted`, or `failed`. [OpenAI Codex
  app-server: threads and goals](https://developers.openai.com/codex/app-server/#manage-a-thread-goal),
  [turn cancellation](https://developers.openai.com/codex/app-server/#interrupt-a-turn)
- Therefore, do not make Codex app-server goal state the bridge source of
  truth in this slice: it is unavailable through the bridge's current SDK
  adapter and lacks Claude parity. The bridge's durable goal record controls
  scheduling, stop, blocked, and completion; each provider's saved session ID
  only controls conversational continuity.

## Short conclusion

Linear supports visible delegation, but its Agent Session webhook intentionally
does not reliably distinguish delegation from mention. A configured issue label
checked on Linear's own Agent Session creation provides explicit, durable,
Linear-native authorization without `/goal` or other magic message syntax. It
also avoids a non-idempotent session-creation mutation. Provider sessions can
safely resume individual bounded turns; the cross-provider autonomous lifecycle
is bridge-owned and durably fenced.
