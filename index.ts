import type { AgentSessionEvent, AgentToolResult, ExtensionAPI, ExtensionContext } from "@mariozechner/pi-coding-agent";
import { randomUUID } from "crypto";
import { Type } from "typebox";
import { Text } from "@mariozechner/pi-tui";
import { defineTool, sessionCompactFailuresReachExtensions, sessionInfoChangesReachExtensions, StringEnum } from "./pi-compat.ts";
import { ParleyClient, type SendResult } from "./broker/client.ts";
import { spawnBrokerIfNeeded } from "./broker/spawn.ts";
import { SessionListOverlay } from "./ui/session-list.ts";
import { ComposeOverlay, type ComposeResult } from "./ui/compose.ts";
import { InlineMessageComponent } from "./ui/inline-message.ts";
import { getAskTimeoutMs, loadConfig, type ParleyConfig } from "./config.ts";
import { COMPACTION_AWARENESS_FEATURE, EXTENSION_BUS_FEATURE, SESSION_PROFILE_FEATURE } from "./types.ts";
import type { Attachment, BrokerMessage, Message, MessageControl, MessageReceipt, MessageReceiptStatus, PeerCompactionNotice, SessionInfo, SessionRegistration } from "./types.ts";
import {
  PARLEY_EXTENSION_REGISTER_EVENT,
  PARLEY_EXTENSION_REGISTRY_READY_EVENT,
  PARLEY_OUTBOX_REQUEST_EVENT,
  PARLEY_OUTBOX_RESULT_EVENT,
  type ParleyExtensionChannel,
  type ParleyExtensionEvent,
  type ParleyExtensionOwner,
  type ParleyExtensionRegistration,
  type ParleyExtensionState,
  type ParleyOutboxRequestV1,
  type ParleyOutboxResultCode,
  type ParleyOutboxResultStatus,
  type ParleyOutboxResultV1,
} from "./extension-api.ts";
import { ReplyTracker, type ParleyContext } from "./reply-tracker.ts";
import { normalizeEntryType, restoreConversationHistory, messageControlKey, type OutstandingAsk } from "./conversation-history.ts";
import { parleyEnv } from "./env-compat.ts";
import { resolve as resolvePath } from "node:path";
import { sameCwd } from "./cwd.ts";
import { formatContextUsage } from "./format-context.ts";
import { formatPeerCompactionNotice } from "./compaction-awareness.ts";
import { formatCancellationResult, formatDeliveryResult } from "./message-results.ts";
import {
  openProjectPane,
  ProjectLaunchError,
  projectLaunchRequestText,
  resolveProjectLauncherCommand,
  resolveTargetInCwd,
  waitForProjectSession,
  type ProjectPaneLaunch,
} from "./project-agent.ts";
import { isValidSessionDescription, isValidSessionName, normalizeSelfProfileUpdate, type SelfProfileUpdate } from "./session-profile.ts";

type SessionInfoChangedEvent = Extract<AgentSessionEvent, { type: "session_info_changed" }>;

const PARLEY_TOOL_NAME = "parley";
const SUBAGENT_CONTROL_PARLEY_EVENT = "subagent:control-parley";
const SUBAGENT_RESULT_PARLEY_EVENT = "subagent:result-parley";
const SUBAGENT_RESULT_PARLEY_DELIVERY_EVENT = "subagent:result-parley-delivery";
const INBOUND_MESSAGE_DEDUPE_MAX = 1000;
const INBOUND_MESSAGE_DEDUPE_RETENTION_MS = 60 * 60 * 1000;
const MAX_EXPLICIT_SEND_TARGETS = 32;
const SEND_FANOUT_CONCURRENCY = 8;
const COMPACTION_STATUS_FAILSAFE_MS = 15 * 60 * 1000;
const DEFAULT_UNNAMED_SESSION_ALIAS_PREFIX = "session";
const SUBAGENT_ORCHESTRATOR_TARGET_ENV = "PI_SUBAGENT_ORCHESTRATOR_TARGET";
const SUBAGENT_ORCHESTRATOR_SESSION_ID_ENV = "PI_SUBAGENT_ORCHESTRATOR_SESSION_ID";
const PARLEY_SESSION_ID_ENV = "PI_PARLEY_SESSION_ID";
const STABLE_PARLEY_SESSION_ID_ENV = "PI_PARLEY_STABLE_ID";
const SUBAGENT_RUN_ID_ENV = "PI_SUBAGENT_RUN_ID";
const SUBAGENT_CHILD_AGENT_ENV = "PI_SUBAGENT_CHILD_AGENT";
const SUBAGENT_CHILD_INDEX_ENV = "PI_SUBAGENT_CHILD_INDEX";
const SUBAGENT_PARLEY_SESSION_NAME_ENV = "PI_SUBAGENT_PARLEY_SESSION_NAME";
/** pi-subagents sets this before its own 1:1 rename lands; reading both keeps
 * child session-name targeting working during a mixed-version rollout. */
const SUBAGENT_LEGACY_SESSION_NAME_ENV = "PI_SUBAGENT_INTERCOM_SESSION_NAME";
const SUBAGENT_SUPERVISOR_CHANNEL_DIR_ENV = "PI_SUBAGENT_SUPERVISOR_CHANNEL_DIR";

interface ChildOrchestratorMetadata {
  orchestratorTarget: string;
  orchestratorSessionId?: string;
  runId: string;
  agent: string;
  index: string;
  sessionName?: string;
}

interface InboundMessageEntry {
  from: SessionInfo;
  message: Message;
  replyCommand?: string;
  bodyText: string;
}

interface DeliveryTarget {
  id: string;
  label: string;
  projectPane?: ProjectPaneLaunch;
  session?: SessionInfo;
}

interface OutboxTarget {
  id: string;
  label: string;
}

interface BatchDeliveryTarget {
  requested: string;
  label: string;
  session?: SessionInfo;
  resolutionError?: string;
  resolutionCode?: string;
}

interface BatchDeliveryOutcome {
  to: string;
  targetId?: string;
  messageId?: string;
  delivered: boolean;
  delivery: "socket_delivered" | "queued" | "failed" | "unknown";
  retryable: boolean;
  outcomeKnown: boolean;
  code?: string;
  reason?: string;
  peerCompaction?: PeerCompactionNotice;
}

interface OutboxRequestTrace {
  requestId: string;
  extensionId?: string;
  extensionName?: string;
  to?: string;
  message?: string;
}

interface PendingOutboxRequest {
  generation: number;
  request: OutboxRequestTrace;
}

type ContactSupervisorReason = "need_decision" | "progress_update" | "interview_request";

interface SupervisorInterviewQuestion extends Record<string, unknown> {
  id: string;
  type: "single" | "multi" | "text" | "image" | "info";
  question: string;
  options?: unknown[];
}

interface SupervisorInterviewRequest extends Record<string, unknown> {
  title?: string;
  description?: string;
  questions: SupervisorInterviewQuestion[];
}

interface SupervisorInterviewReply {
  responses: Array<{ id: string; value: unknown }>;
}

function getErrorMessage(error: unknown): string {
  if (error instanceof ProjectLaunchError) {
    const launch = error.launch;
    const receipt = launch ? `\nLaunch in ${launch.projectRoot}: ${launch.outcome}${launch.requestMessageId ? ` (request ${launch.requestMessageId})` : ""}.` : "";
    const observed = error.session ? `\nObserved session: ${error.session.name || error.session.id} (${error.session.id}).` : "";
    return `${error.message}\nStopped at ${error.stage}.${receipt}${observed}`;
  }
  return error instanceof Error ? error.message : String(error);
}

// Fork: a queued send to a disconnected session used to report plain
// "Message sent", which reads as live delivery. Make the mailbox state explicit
// so senders immediately know the target is offline and may never return.
function queuedDeliveryNote(targetDisplay: string): string {
  return `Queued for offline session "${targetDisplay}" for up to 24h while this broker remains running. The peer has not received it; this mailbox is not a durable handoff.`;
}

function deliveryDetails(result: SendResult): Record<string, unknown> {
  return {
    messageId: result.id,
    delivered: result.delivered,
    delivery: result.delivery,
    retryable: result.retryable,
    outcomeKnown: result.outcomeKnown,
    ...(result.code ? { code: result.code } : {}),
    ...(result.reason ? { reason: result.reason } : {}),
    ...(result.peerCompaction ? { peerCompaction: result.peerCompaction } : {}),
    ...(result.recipient ? { recipient: result.recipient } : {}),
    ...(result.cancellation ? { cancellation: result.cancellation } : {}),
  };
}

async function mapWithConcurrency<T, R>(
  values: readonly T[],
  concurrency: number,
  mapper: (value: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(values.length);
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(concurrency, values.length) }, async () => {
    while (nextIndex < values.length) {
      const index = nextIndex++;
      results[index] = await mapper(values[index]!, index);
    }
  });
  await Promise.all(workers);
  return results;
}

function batchSendToolResult(options: {
  batchId: string;
  outcomes: BatchDeliveryOutcome[];
  requestedTargetCount: number;
  duplicateCount: number;
  broadcast: boolean;
  sender: string;
  excludedRemoteCount?: number;
}) {
  const acceptedCount = options.outcomes.filter((outcome) => outcome.delivered).length;
  const unknownCount = options.outcomes.filter((outcome) => !outcome.outcomeKnown || outcome.delivery === "unknown").length;
  const failedCount = options.outcomes.length - acceptedCount - unknownCount;
  const noun = options.broadcast
    ? `visible session${options.outcomes.length === 1 ? "" : "s"}`
    : `target${options.outcomes.length === 1 ? "" : "s"}`;
  const heading = options.broadcast
    ? `Broadcast accepted for ${acceptedCount} of ${options.outcomes.length} ${noun}.`
    : `Message accepted for ${acceptedCount} of ${options.outcomes.length} ${noun}.`;
  const lines = options.outcomes.map((outcome) => {
    if (outcome.delivered) {
      const state = outcome.delivery === "queued" ? "queued for offline delivery (up to 24h while this broker remains running)" : "sent";
      const deliveryLine = `- ✓ ${outcome.to}: ${state}${outcome.messageId ? ` (${outcome.messageId})` : ""}`;
      return outcome.peerCompaction
        ? `${deliveryLine}\n  ${formatPeerCompactionNotice(outcome.to, outcome.peerCompaction, outcome.targetId)}`
        : deliveryLine;
    }
    const unknown = !outcome.outcomeKnown || outcome.delivery === "unknown";
    return `- ${unknown ? "?" : "✗"} ${outcome.to}: ${unknown ? "outcome unknown; repeating may duplicate delivery — " : ""}${outcome.reason ?? "delivery failed"}${outcome.messageId ? ` (messageId ${outcome.messageId})` : " (no message created)"}`;
  });
  if (options.duplicateCount > 0) {
    lines.push(`- Skipped ${options.duplicateCount} duplicate target${options.duplicateCount === 1 ? "" : "s"}.`);
  }
  if (options.broadcast) {
    lines.push("", `Host-local broadcast; ${options.excludedRemoteCount ?? 0} visible remote peer(s) were not included.`);
  }
  return {
    content: [{ type: "text" as const, text: `${heading} Sent as ${options.sender}.\n${lines.join("\n")}` }],
    details: {
      ...(acceptedCount === 0 ? { error: true } : {}),
      batch: true,
      broadcast: options.broadcast,
      batchId: options.batchId,
      requestedTargetCount: options.requestedTargetCount,
      recipientCount: options.outcomes.length,
      acceptedCount,
      failedCount,
      unknownCount,
      duplicateCount: options.duplicateCount,
      allAccepted: acceptedCount === options.outcomes.length,
      outcomes: options.outcomes,
    },
  };
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function formatAttachments(attachments: Attachment[]): string {
  let text = "";
  for (const att of attachments) {
    const label = `Attachment snapshot: ${att.name} (${att.type})`;
    if (att.language) {
      const runs = att.content.match(/~+/g) ?? [];
      const fence = "~".repeat(Math.max(3, ...runs.map((run) => run.length + 1)));
      text += `\n\n---\n${label}\n${fence}${att.language}\n${att.content}\n${fence}`;
    } else {
      text += `\n\n---\n${label}\n${att.content}`;
    }
  }
  return text;
}
function readChildOrchestratorMetadata(): ChildOrchestratorMetadata | null {
  const orchestratorTarget = process.env[SUBAGENT_ORCHESTRATOR_TARGET_ENV]?.trim();
  const orchestratorSessionId = process.env[SUBAGENT_ORCHESTRATOR_SESSION_ID_ENV]?.trim()
    || parleyEnv(PARLEY_SESSION_ID_ENV)?.trim();
  const runId = process.env[SUBAGENT_RUN_ID_ENV]?.trim();
  const agent = process.env[SUBAGENT_CHILD_AGENT_ENV]?.trim();
  const index = process.env[SUBAGENT_CHILD_INDEX_ENV]?.trim();
  if (!orchestratorTarget || !runId || !agent || !index) {
    return null;
  }
  const sessionName = process.env[SUBAGENT_PARLEY_SESSION_NAME_ENV]?.trim() ?? process.env[SUBAGENT_LEGACY_SESSION_NAME_ENV]?.trim();
  return {
    orchestratorTarget,
    ...(orchestratorSessionId ? { orchestratorSessionId } : {}),
    runId,
    agent,
    index,
    ...(sessionName ? { sessionName } : {}),
  };
}
function formatChildOrchestratorMessage(kind: "ask" | "update" | "interview", metadata: ChildOrchestratorMetadata, message: string): string {
  const heading = kind === "ask"
    ? "Subagent needs a supervisor decision."
    : kind === "interview"
      ? "Subagent requests a structured supervisor interview."
      : "Subagent progress update.";
  return [
    heading,
    `Run: ${metadata.runId}`,
    `Agent: ${metadata.agent}`,
    `Child index: ${metadata.index}`,
    metadata.sessionName ? `Child parley target: ${metadata.sessionName}` : undefined,
    "",
    message,
  ].filter((line): line is string => line !== undefined).join("\n");
}

function validateSupervisorInterviewRequest(input: unknown): { ok: true; interview: SupervisorInterviewRequest } | { ok: false; error: string } {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return { ok: false, error: "interview must be an object with a questions array" };
  }

  const raw = input as Record<string, unknown>;
  if (raw.title !== undefined && typeof raw.title !== "string") {
    return { ok: false, error: "interview.title must be a string when provided" };
  }
  if (raw.description !== undefined && typeof raw.description !== "string") {
    return { ok: false, error: "interview.description must be a string when provided" };
  }
  if (!Array.isArray(raw.questions) || raw.questions.length === 0) {
    return { ok: false, error: "interview.questions must be a non-empty array" };
  }

  const validTypes = new Set(["single", "multi", "text", "image", "info"]);
  const ids = new Set<string>();
  const questions: SupervisorInterviewQuestion[] = [];

  for (let index = 0; index < raw.questions.length; index++) {
    const questionInput = raw.questions[index];
    if (!questionInput || typeof questionInput !== "object" || Array.isArray(questionInput)) {
      return { ok: false, error: `interview.questions[${index}] must be an object` };
    }
    const question = questionInput as Record<string, unknown>;
    if (typeof question.id !== "string" || question.id.trim() === "") {
      return { ok: false, error: `interview.questions[${index}].id must be a non-empty string` };
    }
    const id = question.id.trim();
    if (ids.has(id)) {
      return { ok: false, error: `interview question id must be unique: ${id}` };
    }
    ids.add(id);

    if (typeof question.type !== "string" || !validTypes.has(question.type)) {
      return { ok: false, error: `interview.questions[${index}].type must be one of: single, multi, text, image, info` };
    }
    if (typeof question.question !== "string" || question.question.trim() === "") {
      return { ok: false, error: `interview.questions[${index}].question must be a non-empty string` };
    }
    if (question.context !== undefined && typeof question.context !== "string") {
      return { ok: false, error: `interview.questions[${index}].context must be a string when provided` };
    }
    let options: unknown[] | undefined;
    if (question.options !== undefined) {
      if (!Array.isArray(question.options)) {
        return { ok: false, error: `interview.questions[${index}].options must be an array when provided` };
      }
      options = [];
      for (let optionIndex = 0; optionIndex < question.options.length; optionIndex++) {
        const option = question.options[optionIndex];
        if (typeof option === "string") {
          const label = option.trim();
          if (!label) {
            return { ok: false, error: `interview.questions[${index}].options[${optionIndex}] must not be empty` };
          }
          options.push(label);
        } else if (!option || typeof option !== "object" || Array.isArray(option) || typeof (option as { label?: unknown }).label !== "string" || (option as { label: string }).label.trim() === "") {
          return { ok: false, error: `interview.questions[${index}].options[${optionIndex}] must be a non-empty string or an object with a non-empty label` };
        } else {
          options.push({ ...option, label: (option as { label: string }).label.trim() });
        }
      }
    }
    if ((question.type === "single" || question.type === "multi") && (!options || options.length === 0)) {
      return { ok: false, error: `interview.questions[${index}].options must be a non-empty array for ${question.type} questions` };
    }
    if (question.type !== "single" && question.type !== "multi" && options) {
      return { ok: false, error: `interview.questions[${index}].options is only valid for single and multi questions` };
    }

    questions.push({
      ...question,
      id,
      type: question.type as SupervisorInterviewQuestion["type"],
      question: question.question.trim(),
      ...(options ? { options } : {}),
    });
  }

  return {
    ok: true,
    interview: {
      ...raw,
      ...(typeof raw.title === "string" ? { title: raw.title.trim() } : {}),
      ...(typeof raw.description === "string" ? { description: raw.description.trim() } : {}),
      questions,
    },
  };
}

function interviewOptionLabel(option: unknown): string {
  return typeof option === "string" ? option : (option as { label: string }).label;
}

function interviewExampleValue(question: SupervisorInterviewQuestion): unknown {
  if (question.type === "multi") {
    return ["<selected option label>"];
  }
  if (question.type === "single") {
    return "<selected option label>";
  }
  if (question.type === "image") {
    return "image/file reference or description";
  }
  return "answer text";
}

function formatSupervisorInterviewRequest(interview: SupervisorInterviewRequest, message?: string): string {
  const lines: string[] = [];
  const title = interview.title?.trim();
  if (title) lines.push(`Interview: ${title}`);
  const description = interview.description?.trim();
  if (description) lines.push(description);
  const note = message?.trim();
  if (note) lines.push(`Child note: ${note}`);
  if (lines.length > 0) lines.push("");

  lines.push("Questions:");
  interview.questions.forEach((question, index) => {
    lines.push(`${index + 1}. [${question.id}] (${question.type}) ${question.question}`);
    if (typeof question.context === "string" && question.context.trim()) {
      lines.push(`   Context: ${question.context.trim()}`);
    }
    if (question.options?.length) {
      lines.push("   Options:");
      for (const option of question.options) {
        lines.push(`   - ${interviewOptionLabel(option)}`);
      }
    }
  });

  const responseExample = {
    responses: interview.questions
      .filter((question) => question.type !== "info")
      .map((question) => ({
        id: question.id,
        value: interviewExampleValue(question),
      })),
  };

  lines.push(
    "",
    "Answer format:",
    "Answers are parsed by question id: one option label for single, a list of labels for multi, and text for text/image. Info entries are context only.",
    "",
    "```json",
    JSON.stringify(responseExample, null, 2),
    "```",
  );

  return lines.join("\n");
}

function validateSupervisorInterviewReply(value: unknown, interview: SupervisorInterviewRequest): SupervisorInterviewReply {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("reply JSON must be an object with a responses array");
  }

  const responsesInput = (value as Record<string, unknown>).responses;
  if (!Array.isArray(responsesInput)) {
    throw new Error("reply JSON must include a responses array");
  }

  const questionById = new Map(interview.questions
    .filter((question) => question.type !== "info")
    .map((question) => [question.id, question]));
  const seenIds = new Set<string>();
  const responses: SupervisorInterviewReply["responses"] = [];

  for (let index = 0; index < responsesInput.length; index++) {
    const response = responsesInput[index];
    if (!response || typeof response !== "object" || Array.isArray(response)) {
      throw new Error(`responses[${index}] must be an object`);
    }

    const raw = response as Record<string, unknown>;
    if (typeof raw.id !== "string" || raw.id.trim() === "") {
      throw new Error(`responses[${index}].id must be a non-empty string`);
    }
    const id = raw.id.trim();
    const question = questionById.get(id);
    if (!question) {
      throw new Error(`responses[${index}].id must match a non-info interview question id`);
    }
    if (seenIds.has(id)) {
      throw new Error(`responses[${index}].id is duplicated: ${id}`);
    }
    seenIds.add(id);
    if (!Object.hasOwn(raw, "value")) {
      throw new Error(`responses[${index}].value is required`);
    }

    const value = raw.value;
    if (question.type === "single") {
      if (typeof value !== "string") throw new Error(`responses[${index}].value must be a string for single questions`);
      const optionLabels = new Set(question.options?.map(interviewOptionLabel));
      if (!optionLabels.has(value.trim())) throw new Error(`responses[${index}].value must match one of the question options`);
      responses.push({ id, value: value.trim() });
      continue;
    }

    if (question.type === "multi") {
      if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
        throw new Error(`responses[${index}].value must be an array of strings for multi questions`);
      }
      const optionLabels = new Set(question.options?.map(interviewOptionLabel));
      const selected = value.map((item) => item.trim());
      const invalid = selected.find((item) => !optionLabels.has(item));
      if (invalid) throw new Error(`responses[${index}].value contains an option that is not in the question options: ${invalid}`);
      responses.push({ id, value: selected });
      continue;
    }

    if (typeof value !== "string") {
      throw new Error(`responses[${index}].value must be a string for ${question.type} questions`);
    }
    responses.push({ id, value });
  }

  return { responses };
}

function parseStructuredSupervisorReply(text: string, interview: SupervisorInterviewRequest): { value?: SupervisorInterviewReply; error?: string } | undefined {
  const fencedMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = (fencedMatch?.[1] ?? text).trim();
  if (!candidate.startsWith("{") && !candidate.startsWith("[")) {
    return undefined;
  }
  try {
    return { value: validateSupervisorInterviewReply(JSON.parse(candidate), interview) };
  } catch (error) {
    return { error: getErrorMessage(error) };
  }
}
function duplicateSessionNames(sessions: SessionInfo[]): Set<string> {
  return new Set(
    sessions
      .map(s => s.name?.toLowerCase())
      .filter((name): name is string => Boolean(name))
      .filter((name, index, names) => names.indexOf(name) !== index)
  );
}
export function sessionIdPrefixes(sessions: SessionInfo[]): Map<string, string> {
  const prefixes = new Map<string, string>();
  for (const session of sessions) {
    let longestSharedPrefix = 0;
    for (const other of sessions) {
      if (other.id === session.id) {
        continue;
      }
      let length = 0;
      while (length < session.id.length && session.id[length] === other.id[length]) {
        length += 1;
      }
      longestSharedPrefix = Math.max(longestSharedPrefix, length);
    }
    const minimumLength = Math.max(8, longestSharedPrefix + 1);
    // Prefer a clean segment boundary over slicing mid-segment, and never
    // truncate the final segment: ids like "mistfall-remote:game:t226" whose
    // unique tail follows the last separator would otherwise display as "t2".
    let groupBoundary = -1;
    for (const separator of ["-", ":"]) {
      const boundary = session.id.indexOf(separator, minimumLength);
      if (boundary !== -1 && (groupBoundary === -1 || boundary < groupBoundary)) {
        groupBoundary = boundary;
      }
    }
    const length = groupBoundary === -1 ? session.id.length : groupBoundary;
    prefixes.set(session.id, session.id.slice(0, length));
  }
  return prefixes;
}
function parseSubagentParleyPayload(payload: unknown): { to: string; message: string; requestId?: string } | null {
  if (typeof payload !== "object" || payload === null) {
    return null;
  }
  const record = payload as Record<string, unknown>;
  if (typeof record.to !== "string" || typeof record.message !== "string") {
    return null;
  }
  const requestId = typeof record.requestId === "string" ? record.requestId : undefined;
  return { to: record.to, message: record.message, ...(requestId ? { requestId } : {}) };
}
function parseOutboxRequestPayload(payload: unknown): { ok: true; request: ParleyOutboxRequestV1 } | { ok: false; requestId?: string; extensionId?: string; extensionName?: string; detail: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, detail: "request must be an object" };
  }
  const record = payload as Record<string, unknown>;
  const requestId = typeof record.requestId === "string" && record.requestId.trim() ? record.requestId : undefined;
  const extensionId = typeof record.extensionId === "string" && record.extensionId.trim() ? record.extensionId.trim() : undefined;
  const extensionName = typeof record.extensionName === "string" && record.extensionName.trim() ? record.extensionName.trim() : undefined;
  if (record.version !== 1) {
    return { ok: false, requestId, extensionId, extensionName, detail: "version must be 1" };
  }
  if (!requestId) {
    return { ok: false, extensionId, extensionName, detail: "requestId is required" };
  }
  if (!extensionId) {
    return { ok: false, requestId, extensionName, detail: "extensionId is required" };
  }
  if (!extensionName) {
    return { ok: false, requestId, extensionId, detail: "extensionName is required" };
  }
  if (typeof record.to !== "string" || !record.to.trim()) {
    return { ok: false, requestId, extensionId, extensionName, detail: "to is required" };
  }
  if (typeof record.message !== "string" || !record.message.trim()) {
    return { ok: false, requestId, extensionId, extensionName, detail: "message is required" };
  }
  return {
    ok: true,
    request: {
      version: 1,
      requestId,
      extensionId,
      extensionName,
      to: record.to.trim(),
      message: record.message,
    },
  };
}
function resolveParleyPresenceName(sessionName: string | undefined, sessionId: string): string {
  const trimmedName = sessionName?.trim();
  if (trimmedName) {
    return trimmedName;
  }
  const normalizedSessionId = sessionId.startsWith("session-") ? sessionId.slice("session-".length) : sessionId;
  return `${DEFAULT_UNNAMED_SESSION_ALIAS_PREFIX}-${normalizedSessionId.slice(0, 18)}`;
}
function buildPresenceIdentity(pi: ExtensionAPI, sessionId: string): { name: string; runtimeFallbackAlias: boolean } {
  const sessionName = pi.getSessionName();
  return {
    name: resolveParleyPresenceName(sessionName, sessionId),
    runtimeFallbackAlias: !sessionName?.trim(),
  };
}
function resolveConfiguredParleySessionId(piSessionId: string, config: ParleyConfig): string {
  return parleyEnv(STABLE_PARLEY_SESSION_ID_ENV)?.trim() || config.stableId || piSessionId;
}
// The tmux pane id (e.g. "%212") the session was launched in. $TMUX_PANE is
// inherited at process start and immutable for the lifetime — moving the pane
// between windows keeps its id — so it is a stable join key a peer can use to
// live-resolve the current window via tmux. Absent outside tmux.
function currentTmuxPane(): string | undefined {
  const pane = process.env.TMUX_PANE?.trim();
  return pane ? pane : undefined;
}
function formatParleyContactSnippet(sessionId: string): string {
  return `Pi parley target: ${sessionId}`;
}
function formatSessionLabel(session: SessionInfo, duplicates: Set<string>): string {
  if (!session.name) {
    return session.id;
  }
  return duplicates.has(session.name.toLowerCase())
    ? `${session.name} (${session.id.slice(0, 8)})`
    : session.name;
}
function formatSessionListRow(session: SessionInfo, currentCwd: string, isSelf: boolean, idPrefix: string): string {
  const name = session.name || "Unnamed session";
  const remote = session.federation
    ? `remote:${session.federation.originLabel ?? session.federation.originId}; text sends only when supported, no asks/replies/attachments`
    : undefined;
  const tags = [isSelf ? "self" : session.cwd === currentCwd ? "same cwd" : undefined, remote, session.status]
    .filter((tag): tag is string => Boolean(tag));
  const suffix = tags.length ? ` [${tags.join(", ")}]` : "";
  const pane = session.tmuxPane ? ` · tmux ${session.tmuxPane}` : "";
  const description = session.description ? ` — ${session.description}` : "";
  return `• ${name} (${idPrefix})${description} — ${session.cwd} (${session.model}${formatContextUsage(session)}${pane})${suffix}`;
}
function previewText(value: unknown, maxLength = 72): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return undefined;
  }
  return normalized.length > maxLength ? `${normalized.slice(0, maxLength - 1)}…` : normalized;
}
function firstTextContent(result: { content?: Array<{ type: string; text?: string }> }): string {
  return result.content?.find((item) => item.type === "text" && typeof item.text === "string")?.text?.replace(/\*\*/g, "") ?? "";
}
function formatMessageTimestamp(timestamp: number | undefined): string | undefined {
  return typeof timestamp === "number" && Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : undefined;
}
function formatInboundDeliveryMetadata(message: Message): string {
  const parts = [`Message: ${message.id}`];
  if (message.replyTo) parts.push(`Reply to: ${message.replyTo}`);
  if (message.supersedes) parts.push(`Supersedes: ${message.supersedes}`);
  if (message.retryOf) parts.push(`Retry of: ${message.retryOf}`);
  if (message.provenance) {
    parts.push(`Via extension ${message.provenance.extensionName} (${message.provenance.extensionId}); request ${message.provenance.requestId}`);
  }
  // Transport timestamps remain in details. A long mailbox delay changes the meaning of the text.
  if (Date.now() - message.timestamp > 60_000) parts.push(`Originally sent: ${formatMessageTimestamp(message.timestamp)}`);
  return parts.join("\n");
}
export default function piParleyExtension(pi: ExtensionAPI) {
  let client: ParleyClient | null = null;
  const config: ParleyConfig = loadConfig();
  const askTimeoutMs = getAskTimeoutMs();
  const compactionPresenceSupported = sessionCompactFailuresReachExtensions();
  const localExtensions = new Map<string, {
    registration: ParleyExtensionRegistration;
    channel: ParleyExtensionChannel;
    owner?: ParleyExtensionOwner;
    state?: ParleyExtensionState;
  }>();
  let runtimeContext: ExtensionContext | null = null;
  let currentSessionId: string | null = null;
  let currentParleySessionId: string | null = null;
  // ACL fork: fires the quiet, one-time supervisor notice the first time this
  // subagent successfully advertises itself. Re-advertising under a new name
  // later in the same session does not repeat the notice.
  let hasNotifiedSupervisorOfAdvertise = false;
  let currentModel = "unknown";
  let sessionStartedAt: number | null = null;
  let reconnectTimer: NodeJS.Timeout | null = null;
  const previousParleySessionId = parleyEnv(PARLEY_SESSION_ID_ENV);
  let reconnectPromise: Promise<ParleyClient> | null = null;
  let reconnectPromiseGeneration: number | null = null;
  let startupConnectTimer: NodeJS.Timeout | null = null;
  let sessionNameCompatibilityTimer: NodeJS.Timeout | null = null;
  let observedSessionName: string | undefined;
  let currentSessionDescription: string | undefined;
  let profileManagedName: string | undefined;
  let currentAdvertisedName: string | undefined;
  let lastPresenceRequestedName: string | undefined;
  let profileNameMutationTarget: string | undefined;
  let profileOwnershipRevocationPending = false;
  let reconnectAttempt = 0;
  let shuttingDown = false;
  let disposed = true;
  let runtimeStarted = false;
  let runtimeGeneration = 0;
  let agentRunning = false;
  let compactionRunning = false;
  let compactionStatusGeneration = 0;
  let compactionStatusTimer: NodeJS.Timeout | null = null;
  const pendingCompactionReports = new Map<string, number>();
  const pendingReceiverBaselineTokens = new Set<string>();
  let compactionReportFlush: Promise<void> | null = null;
  let compactionReportRetryTimer: NodeJS.Timeout | null = null;
  let receiverBaselineRetryTimer: NodeJS.Timeout | null = null;
  const activeTools = new Map<string, string>();
  const replyTracker = new ReplyTracker();
  let conversationPersistenceWarning: string | undefined;

  function recordConversationEntry(type: string, data: unknown): void {
    try {
      pi.appendEntry(type, data);
    } catch (error) {
      // History is recovery support, not the acceptance boundary for live conversation.
      conversationPersistenceWarning = `Parley history was not fully persisted (${type}: ${previewText(getErrorMessage(error), 160) ?? "history write failed"}). Messages and conversation updates remain available in this running session, but recovery after restart may be incomplete.`;
    }
  }

  const seenInboundMessages = new Map<string, number>();
  const latestOutboundReceipts = new Map<string, { status: MessageReceiptStatus; timestamp: number; detail?: string }>();
  const outboxRequestIds = new Set<string>();
  const pendingOutboxRequests = new Map<string, PendingOutboxRequest>();
  function dismissIncomingAsk(messageId: string): void {
    replyTracker.dismissPendingAsk(messageId);
    recordConversationEntry("parley_inbound_settled", { messageId, timestamp: Date.now() });
  }
  function hasSeenInboundMessage(from: SessionInfo, message: Message, now = Date.now()): boolean {
    for (const [key, seenAt] of seenInboundMessages) {
      if (now - seenAt > INBOUND_MESSAGE_DEDUPE_RETENTION_MS) {
        seenInboundMessages.delete(key);
      }
    }
    const key = `${from.id}\0${message.id}`;
    if (seenInboundMessages.has(key)) {
      return true;
    }
    seenInboundMessages.set(key, now);
    while (seenInboundMessages.size > INBOUND_MESSAGE_DEDUPE_MAX) {
      const oldestKey = seenInboundMessages.keys().next().value;
      if (typeof oldestKey !== "string") break;
      seenInboundMessages.delete(oldestKey);
    }
    return false;
  }
  function emitMessageReceipt(messageId: string, status: MessageReceiptStatus, detail?: string): void {
    try {
      client?.sendMessageReceipt({
        messageId,
        status,
        timestamp: Date.now(),
        ...(detail ? { detail } : {}),
      });
    } catch {
      // Receipts are diagnostics; message handling should not fail when the sender disconnects.
    }
  }
  function handleMessageControl(from: SessionInfo, control: MessageControl, restored = false): void {
    if (!restored) recordConversationEntry("parley_inbound_control", { from, control });
    replyTracker.setDisposition(control.messageId, {
      state: control.action === "cancel" ? "withdrawn" : "superseded",
      ...(control.supersededBy ? { replacementId: control.supersededBy } : {}),
    });
    deferredInboundMessages.delete(control.messageId);
    pendingHostEnvelopes.delete(`message:${control.messageId}`);
    contextFallbackEnvelopes.delete(`message:${control.messageId}`);
    freshModelContexts.delete(control.messageId);
    const original = replyTracker.getMessage(control.messageId);
    const topic = original ? `\nOriginal message: ${JSON.stringify(previewText(original.message.content.text, 180))}` : "";
    const content = control.action === "cancel"
      ? `**Parley withdrawal from ${from.name || from.id}** (${from.id})\n\nMessage ${control.messageId} was withdrawn by its sender.${topic}\nThe sender no longer requests this work. Earlier delivery or work may already have happened; withdrawal does not undo it.`
      : `**Parley update from ${from.name || from.id}** (${from.id})\n\nMessage ${control.messageId} was superseded${control.supersededBy ? ` by ${control.supersededBy}` : ""}.${topic}\nThe earlier message is no longer the current request; prior work is not undone.`;
    const key = messageControlKey(control);
    deferredInboundControls.set(key, { from, control, content });
    flushInboundControl(key);
  }
  function flushInboundControl(key: string): void {
    const entry = deferredInboundControls.get(key);
    const ctx = getLiveContext();
    if (!entry || !ctx) return;
    const envelope = { customType: "parley_message_control", content: entry.content, display: true, details: { from: entry.from, control: entry.control } };
    pendingHostEnvelopes.set(`control:${key}`, envelope);
    try {
      pi.sendMessage(envelope, ctx.isIdle() ? { triggerTurn: true } : { deliverAs: "steer" });
    } catch {
      // The control remains pending in memory; context injection and idle retry share this queue.
    }
    confirmPersistedInbound();
    scheduleInboundRetry();
  }
  function latestDeliveryState(messageId: string | null, fallback: string): string {
    if (!messageId) {
      return fallback;
    }
    const receipt = latestOutboundReceipts.get(messageId);
    return receipt ? receipt.status : fallback;
  }
  let replyWaiter: {
    from: string;
    replyTo: string;
    resolve: (message: Message) => void;
    reject: (error: Error) => void;
  } | null = null;
  /** Non-blocking asks awaiting replies. Resolved by replyTo correlation on
   * inbound messages, by cancellation, or surfaced with age by 'status'; the
   * broker's own ask-edge timeout keeps the authoritative lifecycle bounded. */
  const outstandingAsks = new Map<string, OutstandingAsk>();
  const deferredInboundMessages = new Map<string, InboundMessageEntry>();
  const deferredInboundControls = new Map<string, { from: SessionInfo; control: MessageControl; content: string }>();
  let inboundRetryTimer: NodeJS.Timeout | null = null;
  const pendingHostEnvelopes = new Map<string, { customType: string; content: string; display: boolean; details: unknown }>();
  const contextFallbackEnvelopes = new Map<string, { customType: string; content: string; display: boolean; details: unknown }>();
  const freshModelContexts = new Map<string, ParleyContext>();

  function scheduleInboundRetry(): void {
    if (inboundRetryTimer || (!deferredInboundMessages.size && !deferredInboundControls.size)) return;
    const generation = runtimeGeneration;
    inboundRetryTimer = setTimeout(() => {
      inboundRetryTimer = null;
      const ctx = getLiveContext(runtimeContext, generation);
      if (!ctx) return;
      confirmPersistedInbound();
      // Busy queues are recovered by the context event, not repeated steering copies.
      if (ctx.isIdle()) {
        for (const entry of deferredInboundMessages.values()) sendIncomingMessage(entry, "trigger", generation);
        for (const key of deferredInboundControls.keys()) flushInboundControl(key);
      }
      scheduleInboundRetry();
    }, 1_000);
    inboundRetryTimer.unref?.();
  }

  function confirmPersistedInbound(): void {
    const ctx = getLiveContext();
    if (!ctx || (pendingHostEnvelopes.size === 0 && contextFallbackEnvelopes.size === 0)) return;
    for (const entry of ctx.sessionManager.getEntries()) {
      if (entry.type !== "custom_message") continue;
      const key = inboundEnvelopeKey(entry);
      if (key) {
        contextFallbackEnvelopes.delete(key);
        confirmInboundEnvelope(key);
      }
    }
  }

  function inboundEnvelopeKey(value: { customType?: string; details?: unknown }): string | undefined {
    const details = value.details as { message?: Message; control?: MessageControl } | undefined;
    if (normalizeEntryType(value.customType) === "parley_message" && details?.message?.id) return `message:${details.message.id}`;
    if (normalizeEntryType(value.customType) === "parley_message_control" && details?.control?.messageId) return `control:${messageControlKey(details.control)}`;
    return undefined;
  }

  function confirmInboundEnvelope(key: string): void {
    if (!pendingHostEnvelopes.delete(key)) return;
    if (key.startsWith("message:")) {
      const id = key.slice("message:".length);
      const entry = deferredInboundMessages.get(id);
      if (!entry) return;
      recordConversationEntry("parley_inbound_visible", { messageId: id, timestamp: Date.now() });
      deferredInboundMessages.delete(id);
      freshModelContexts.set(id, { from: entry.from, message: entry.message, receivedAt: entry.message.receiverReceivedAt ?? Date.now() });
      emitMessageReceipt(id, "injected", "observed in host conversation/model context; not proof of processing");
      settleOutgoingReply(entry.from, entry.message);
      acknowledgeInboundMessageContact(client, entry.message);
    } else {
      const controlId = key.slice("control:".length);
      const entry = deferredInboundControls.get(controlId);
      if (!entry) return;
      recordConversationEntry("parley_control_visible", { key: controlId, timestamp: Date.now() });
      deferredInboundControls.delete(controlId);
      emitMessageReceipt(entry.control.messageId, entry.control.action === "cancel" ? "cancellation_requested" : "superseded", "withdrawal/update observed in host conversation/model context; earlier work may have happened");
    }
  }

  function settleOutgoingReply(from: SessionInfo, message: Message): void {
    if (!message.replyTo || message.completesAsk === false || message.expectsReply) return;
    const ask = outstandingAsks.get(message.replyTo);
    if (!ask || ask.to !== from.id) return;
    recordConversationEntry("parley_ask_settled", { messageId: message.replyTo, reason: "reply accepted by host", timestamp: Date.now() });
    outstandingAsks.delete(message.replyTo);
  }
  /** The effective parley identity for an outgoing message: the broker's
   * collision-resolved projection of this session when available, else the
   * canonical Pi name. Captured at send time so receipts keep the identity
   * actually used, even after a later rename. */
  function currentSendIdentity(client: ParleyClient): string {
    return client.getSelfSession()?.name?.trim() || pi.getSessionName()?.trim() || "unnamed session";
  }
  function waitForReply(from: string, replyTo: string, signal?: AbortSignal, cancelOnAbort?: () => void, getDeliveryState: () => string = () => "unknown"): Promise<Message> {
    if (replyWaiter) {
      return Promise.reject(new Error("Already waiting for a reply"));
    }
    if (signal?.aborted) {
      return Promise.reject(new Error("Cancelled"));
    }
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const timeoutDescription = askTimeoutMs % 60000 === 0 ? `${askTimeoutMs / 60000} minutes` : `${askTimeoutMs}ms`;
        rejectReplyWaiter(new Error(`No reply from "${from}" for message ${replyTo} within ${timeoutDescription}. Last known delivery state: ${getDeliveryState()}. This waiter timeout is not cancellation; the delivered message may still be queued or actionable in the recipient session.`));
      }, askTimeoutMs);
      const cleanup = () => {
        clearTimeout(timeout);
        signal?.removeEventListener("abort", onAbort);
        if (replyWaiter?.replyTo === replyTo) {
          replyWaiter = null;
        }
      };
      const onAbort = () => {
        cancelOnAbort?.();
        cleanup();
        reject(new Error("Cancelled"));
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      replyWaiter = {
        from,
        replyTo,
        resolve: (message) => {
          cleanup();
          resolve(message);
        },
        reject: (error) => {
          cleanup();
          reject(error);
        },
      };
    });
  }
  function rejectOwnedReplyWaiter(messageId: string | null, error: Error): void {
    if (messageId && replyWaiter?.replyTo === messageId) rejectReplyWaiter(error);
  }
  function rejectReplyWaiter(error: Error): void {
    replyWaiter?.reject(error);
  }
  function clearReconnectTimer(): void {
    if (!reconnectTimer) {
      return;
    }
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  function clearStartupConnectTimer(): void {
    if (!startupConnectTimer) {
      return;
    }
    clearTimeout(startupConnectTimer);
    startupConnectTimer = null;
  }
  function clearSessionNameCompatibilityTimer(): void {
    if (!sessionNameCompatibilityTimer) return;
    clearInterval(sessionNameCompatibilityTimer);
    sessionNameCompatibilityTimer = null;
  }
  function clearCompactionStatusTimer(): void {
    if (!compactionStatusTimer) return;
    clearTimeout(compactionStatusTimer);
    compactionStatusTimer = null;
  }
  function resetCompactionStatus(): void {
    compactionStatusGeneration += 1;
    compactionRunning = false;
    clearCompactionStatusTimer();
  }
  function startSessionNameCompatibilityTimer(): void {
    clearSessionNameCompatibilityTimer();
    observedSessionName = pi.getSessionName()?.trim() || undefined;
    if (sessionInfoChangesReachExtensions()) return;
    sessionNameCompatibilityTimer = setInterval(() => {
      if (!currentSessionId || !getLiveContext()) return;
      const currentName = pi.getSessionName()?.trim() || undefined;
      if (currentName === observedSessionName) return;
      observedSessionName = currentName;
      syncPresenceIdentity(currentSessionId);
    }, 1_000);
    sessionNameCompatibilityTimer.unref?.();
  }
  function getLiveContext(ctx: ExtensionContext | null = runtimeContext, generation = runtimeGeneration): ExtensionContext | null {
    if (disposed || shuttingDown || generation !== runtimeGeneration || !ctx) {
      return null;
    }
    try {
      if (currentSessionId && ctx.sessionManager.getSessionId() !== currentSessionId) {
        return null;
      }
      void ctx.hasUI;
      return ctx;
    } catch {
      // A context that throws while reading session/UI state is no longer usable.
      return null;
    }
  }
  function notifyIfLive(ctx: ExtensionContext, message: string, level: "info" | "warning" | "error", generation = runtimeGeneration): void {
    const liveContext = getLiveContext(ctx, generation);
    if (!liveContext?.hasUI) {
      return;
    }
    try {
      liveContext.ui.notify(message, level);
    } catch {
      // The UI can disappear during session shutdown/reload while async overlay work is settling.
    }
  }
  function notifyAliasCommand(ctx: ExtensionContext, message: string, level: "info" | "warning" | "error", generation = runtimeGeneration): void {
    const liveContext = getLiveContext(ctx, generation);
    if (!liveContext) return;
    if (!liveContext.hasUI) {
      // Command handlers return void and print mode supplies a no-op UI. Keep
      // alias guidance visible without injecting a synthetic Pi message.
      console.error(message);
      return;
    }
    notifyIfLive(liveContext, message, level, generation);
  }
  function getReconnectDelayMs(): number {
    const backoffMs = [1000, 2000, 5000, 10000, 30000];
    return backoffMs[Math.min(reconnectAttempt, backoffMs.length - 1)]!;
  }
  function currentStatus(): string {
    const activeToolName = activeTools.values().next().value;
    const lifecycleStatus = compactionRunning
      ? "compacting"
      : activeToolName
        ? `tool:${activeToolName}`
        : agentRunning ? "thinking" : "idle";
    return config.status ? `${lifecycleStatus} · ${config.status}` : lifecycleStatus;
  }
  function emitLocalExtensionEvent(namespace: string, event: ParleyExtensionEvent): void {
    try {
      localExtensions.get(namespace)?.registration.onEvent(event);
    } catch {
      // One local extension must not break parley or other extension channels.
    }
  }
  function createExtensionChannel(namespace: string): ParleyExtensionChannel {
    return {
      namespace,
      snapshot() {
        const extension = localExtensions.get(namespace);
        return {
          connected: Boolean(client?.isConnected()),
          supported: Boolean(client?.supportsFeature(EXTENSION_BUS_FEATURE)),
          ...(extension?.owner ? { owner: extension.owner } : {}),
          ...(extension?.state ? { state: extension.state } : {}),
        };
      },
      publish(payload, options = {}) {
        const activeClient = client;
        if (!activeClient?.isConnected()) throw new Error("Parley is not connected");
        const extension = localExtensions.get(namespace);
        const ownerOnly = options.ownerOnly ?? false;
        const ownerEpoch = ownerOnly ? extension?.owner?.epoch : undefined;
        if (ownerOnly && !ownerEpoch) throw new Error(`No owner is available for ${namespace}`);
        activeClient.sendExtensionMessage({
          type: "extension_publish",
          namespace,
          audience: options.audience ?? "owner",
          ...(ownerOnly ? { ownerOnly: true, ownerEpoch } : {}),
          payload,
        });
      },
      commitState(payload, expectedRevision) {
        const activeClient = client;
        if (!activeClient?.isConnected()) throw new Error("Parley is not connected");
        const extension = localExtensions.get(namespace);
        const ownerEpoch = extension?.owner?.epoch;
        if (!ownerEpoch || extension.owner?.sessionId !== activeClient.sessionId) {
          throw new Error(`Current session is not the owner of ${namespace}`);
        }
        activeClient.sendExtensionMessage({
          type: "extension_state_commit",
          namespace,
          ownerEpoch,
          expectedRevision: expectedRevision ?? extension.state?.revision ?? 0,
          payload,
        });
      },
      async listSessions() {
        const activeClient = client;
        if (!activeClient?.isConnected()) throw new Error("Parley is not connected");
        return activeClient.listSessions();
      },
    };
  }
  function currentExtensionCapabilities() {
    return [...localExtensions.values()].map(({ registration }) => ({
      namespace: registration.namespace,
      ownerEligible: registration.ownerEligible,
    }));
  }
  function registerLocalExtension(registration: ParleyExtensionRegistration): void {
    if (!/^[a-z0-9][a-z0-9._/-]{0,63}$/.test(registration.namespace)) {
      throw new Error(`Invalid parley extension namespace: ${registration.namespace}`);
    }
    if (localExtensions.has(registration.namespace)) {
      throw new Error(`Parley extension namespace already registered: ${registration.namespace}`);
    }
    const channel = createExtensionChannel(registration.namespace);
    localExtensions.set(registration.namespace, { registration, channel });
    const activeClient = client;
    const connected = Boolean(activeClient?.isConnected());
    const supported = Boolean(activeClient?.supportsFeature(EXTENSION_BUS_FEATURE));
    // Write the capability update before exposing a connected channel. Socket
    // framing preserves this order if onReady publishes synchronously.
    if (activeClient && connected && supported) {
      activeClient.updateExtensionCapabilities(currentExtensionCapabilities());
    }
    registration.onReady(channel);
    if (connected) {
      emitLocalExtensionEvent(registration.namespace, { type: "connection", connected: true, supported });
    }
  }
  function buildRegistration(): SessionRegistration {
    const liveContext = getLiveContext();
    if (!liveContext || !currentSessionId || sessionStartedAt === null) {
      throw new Error("Parley runtime not initialized");
    }

    const identity = buildPresenceIdentity(pi, currentParleySessionId ?? currentSessionId);
    const tmuxPane = currentTmuxPane();
    return {
      ...identity,
      ...(currentSessionDescription ? { description: currentSessionDescription } : {}),
      cwd: liveContext.cwd,
      model: currentModel,
      pid: process.pid,
      startedAt: sessionStartedAt,
      lastActivity: Date.now(),
      status: currentStatus(),
      ...(tmuxPane ? { tmuxPane } : {}),
      ...(localExtensions.size > 0
        ? {
            extensions: currentExtensionCapabilities(),
          }
        : {}),
      ...subagentAclFields(),
    };
  }

  // ACL fork: label this session as a subagent child and record its
  // supervisor identity so the broker can scope visibility. Uses the same
  // gating (orchestratorTarget + runId + agent + index) as the
  // contact_supervisor bridge, so a session is only ever tagged as a
  // subagent when pi-subagents actually supplied bridge metadata.
  function subagentAclFields(): { isSubagent?: boolean; supervisorSessionId?: string; supervisorName?: string } {
    const metadata = readChildOrchestratorMetadata();
    if (!metadata) {
      return {};
    }
    return {
      isSubagent: true,
      supervisorName: metadata.orchestratorTarget,
      ...(metadata.orchestratorSessionId ? { supervisorSessionId: metadata.orchestratorSessionId } : {}),
    };
  }
  // Snapshot the live session's context-window usage for presence. getContextUsage()
  // (stock SDK) reports { tokens, contextWindow, percent }, with tokens/percent null
  // right after a compaction (before the next assistant response). We emit null in
  // that case to CLEAR a peer's stale value rather than freeze the old percentage.
  // Feature-detected so an older runtime without getContextUsage() just omits it.
  function currentContextUsage(): { contextPct?: number | null; contextTokens?: number | null; contextWindow?: number } {
    const usage = getLiveContext()?.getContextUsage?.();
    if (!usage) {
      return {};
    }
    const result: { contextPct?: number | null; contextTokens?: number | null; contextWindow?: number } = {
      contextPct: typeof usage.percent === "number" && Number.isFinite(usage.percent) ? Math.round(usage.percent) : null,
      contextTokens: typeof usage.tokens === "number" && Number.isFinite(usage.tokens) ? usage.tokens : null,
    };
    if (typeof usage.contextWindow === "number" && usage.contextWindow > 0) {
      result.contextWindow = usage.contextWindow;
    }
    return result;
  }

  function persistProfileOwnershipRevocation(): void {
    if (!profileOwnershipRevocationPending) return;
    try {
      pi.appendEntry("parley_profile_updated", {
        managedName: null,
        description: currentSessionDescription ?? null,
        timestamp: Date.now(),
      });
      profileOwnershipRevocationPending = false;
    } catch {
      // Retry on the next lifecycle or tool-driven presence reconciliation.
    }
  }

  function syncPresenceIdentity(sessionId: string): void {
    if (!getLiveContext()) return;
    const identity = buildPresenceIdentity(pi, currentParleySessionId ?? sessionId);
    if (
      profileManagedName
      && !identity.runtimeFallbackAlias
      && identity.name !== profileManagedName
      && identity.name !== profileNameMutationTarget
    ) {
      profileManagedName = undefined;
      profileOwnershipRevocationPending = true;
    }
    persistProfileOwnershipRevocation();
    if (!client) return;
    if (lastPresenceRequestedName !== undefined && lastPresenceRequestedName !== identity.name) {
      client.invalidateSelfSessionProjection();
    }
    lastPresenceRequestedName = identity.name;
    client.updatePresence({
      ...identity,
      ...(client.supportsFeature(SESSION_PROFILE_FEATURE)
        ? { description: currentSessionDescription ?? null }
        : {}),
      status: currentStatus(),
      ...currentContextUsage(),
    });
  }
  function publishParleySessionId(sessionId: string): void {
    process.env[PARLEY_SESSION_ID_ENV] = sessionId;
  }
  function restoreParleySessionId(): void {
    if (previousParleySessionId === undefined) {
      delete process.env[PARLEY_SESSION_ID_ENV];
      return;
    }
    process.env[PARLEY_SESSION_ID_ENV] = previousParleySessionId;
  }
  function syncPresenceStatus(): void {
    if (!client || !currentSessionId || !getLiveContext()) {
      return;
    }
    // context% rides the status heartbeat so peers see live usage at turn boundaries.
    client.updatePresence({ status: currentStatus(), ...currentContextUsage() });
  }
  function finishCompactionStatus(ctx: ExtensionContext, expectedGeneration?: number): void {
    if (!getLiveContext(ctx)) {
      return;
    }
    if (expectedGeneration !== undefined && expectedGeneration !== compactionStatusGeneration) {
      return;
    }
    resetCompactionStatus();
    syncPresenceStatus();
  }
  function clearCompactionReportRetryTimer(): void {
    if (!compactionReportRetryTimer) return;
    clearTimeout(compactionReportRetryTimer);
    compactionReportRetryTimer = null;
  }

  function scheduleCompactionReportRetry(expectedGeneration: number): void {
    if (compactionReportRetryTimer || pendingCompactionReports.size === 0) return;
    compactionReportRetryTimer = setTimeout(() => {
      compactionReportRetryTimer = null;
      if (expectedGeneration === runtimeGeneration && getLiveContext()) {
        flushPendingCompactionReports();
      }
    }, 1_000);
    compactionReportRetryTimer.unref?.();
  }

  function restorePendingCompactionReports(ctx: ExtensionContext): void {
    pendingCompactionReports.clear();
    pendingReceiverBaselineTokens.clear();
    currentSessionDescription = undefined;
    profileManagedName = undefined;
    currentAdvertisedName = undefined;
    lastPresenceRequestedName = undefined;
    profileNameMutationTarget = undefined;
    profileOwnershipRevocationPending = false;
    const sessionManager = ctx.sessionManager as typeof ctx.sessionManager & { getEntries?: () => ReturnType<typeof ctx.sessionManager.getEntries> };
    const entries = sessionManager.getEntries?.() ?? [];
    type PersistedProfileState = {
      description?: string | null;
      managedName?: string | null;
      requiredName?: string;
      index: number;
    };
    const pendingProfiles = new Map<string, PersistedProfileState>();
    let lastAppliedProfileIndex = -1;
    const parseProfileState = (data: {
      description?: unknown;
      managedName?: unknown;
      requiredName?: unknown;
    }, index: number): PersistedProfileState | undefined => {
      if (data.description !== undefined && data.description !== null && !isValidSessionDescription(data.description)) return undefined;
      if (data.managedName !== undefined && data.managedName !== null && !isValidSessionName(data.managedName)) return undefined;
      if (data.requiredName !== undefined && !isValidSessionName(data.requiredName)) return undefined;
      return {
        ...(data.description === null || isValidSessionDescription(data.description) ? { description: data.description } : {}),
        ...(data.managedName === null || typeof data.managedName === "string" ? { managedName: data.managedName } : {}),
        ...(typeof data.requiredName === "string" ? { requiredName: data.requiredName } : {}),
        index,
      };
    };
    const applyProfileState = (state: PersistedProfileState): void => {
      if (state.description === null) currentSessionDescription = undefined;
      else if (state.description !== undefined) currentSessionDescription = state.description;
      if (state.managedName === null) profileManagedName = undefined;
      else if (state.managedName) profileManagedName = state.managedName;
      lastAppliedProfileIndex = state.index;
    };
    for (const [entryIndex, entry] of entries.entries()) {
      if (entry.type === "custom_message" && typeof entry.details === "object" && entry.details !== null) {
        const details = entry.details as { message?: { contactToken?: unknown; contactBaseline?: unknown }; contactToken?: unknown; contactBaseline?: unknown };
        const delivered = details.message ?? details;
        if (delivered.contactBaseline === true && typeof delivered.contactToken === "string") {
          pendingReceiverBaselineTokens.add(delivered.contactToken);
        }
        continue;
      }
      if (entry.type !== "custom" || typeof entry.data !== "object" || entry.data === null) continue;
      const data = entry.data as {
        eventId?: unknown;
        compactedAt?: unknown;
        token?: unknown;
        description?: unknown;
        managedName?: unknown;
        requiredName?: unknown;
        updateId?: unknown;
      };
      if (normalizeEntryType(entry.customType) === "parley_profile_pending" && typeof data.updateId === "string") {
        const state = parseProfileState(data, entryIndex);
        if (state) pendingProfiles.set(data.updateId, state);
      } else if (normalizeEntryType(entry.customType) === "parley_profile_abandoned" && typeof data.updateId === "string") {
        pendingProfiles.delete(data.updateId);
      } else if (normalizeEntryType(entry.customType) === "parley_profile_updated") {
        const state = parseProfileState(data, entryIndex);
        if (state) {
          applyProfileState(state);
          if (typeof data.updateId === "string") pendingProfiles.delete(data.updateId);
        }
      }
      if (normalizeEntryType(entry.customType) === "parley_receiver_baseline_pending" && typeof data.token === "string") {
        pendingReceiverBaselineTokens.add(data.token);
      } else if (
        (normalizeEntryType(entry.customType) === "parley_receiver_baseline_recorded"
          || normalizeEntryType(entry.customType) === "parley_receiver_baseline_abandoned")
        && typeof data.token === "string"
      ) {
        pendingReceiverBaselineTokens.delete(data.token);
      }
      if (typeof data.eventId !== "string" || data.eventId.length === 0) continue;
      if (normalizeEntryType(entry.customType) === "parley_compaction_pending") {
        pendingCompactionReports.set(
          data.eventId,
          typeof data.compactedAt === "number" ? data.compactedAt : 0,
        );
      } else if (normalizeEntryType(entry.customType) === "parley_compaction_recorded") {
        pendingCompactionReports.delete(data.eventId);
      }
    }
    const currentName = pi.getSessionName()?.trim();
    const recoverablePending = [...pendingProfiles.values()]
      .filter((state) => state.index > lastAppliedProfileIndex && state.requiredName === currentName)
      .sort((left, right) => right.index - left.index)[0];
    if (recoverablePending) applyProfileState(recoverablePending);
    if (profileManagedName !== currentName) profileManagedName = undefined;
  }

  function flushPendingCompactionReports(): void {
    if (compactionReportFlush || pendingCompactionReports.size === 0) return;
    const activeClient = client;
    const expectedGeneration = runtimeGeneration;
    if (!activeClient?.isConnected() || !activeClient.supportsFeature(COMPACTION_AWARENESS_FEATURE)) return;

    clearCompactionReportRetryTimer();
    compactionReportFlush = (async () => {
      while (
        expectedGeneration === runtimeGeneration
        && activeClient === client
        && activeClient.isConnected()
        && pendingCompactionReports.size > 0
      ) {
        const next = pendingCompactionReports.entries().next().value as [string, number] | undefined;
        if (!next) return;
        const [eventId, compactedAt] = next;
        try {
          const recorded = await activeClient.reportCompactionCompleted(eventId);
          if (expectedGeneration !== runtimeGeneration || activeClient !== client) return;
          pi.appendEntry("parley_compaction_recorded", {
            eventId,
            compactedAt,
            generation: recorded.generation,
            recordedAt: recorded.compactedAt,
          });
          pendingCompactionReports.delete(eventId);
        } catch {
          scheduleCompactionReportRetry(expectedGeneration);
          return;
        }
      }
    })().finally(() => {
      if (expectedGeneration !== runtimeGeneration) return;
      compactionReportFlush = null;
      if (pendingCompactionReports.size > 0) {
        scheduleCompactionReportRetry(expectedGeneration);
      }
    });
  }

  function beginCompactionStatus(ctx: ExtensionContext, signal?: AbortSignal): void {
    if (!compactionPresenceSupported || !getLiveContext(ctx)) {
      return;
    }
    resetCompactionStatus();
    compactionRunning = true;
    const generation = compactionStatusGeneration;
    compactionStatusTimer = setTimeout(() => {
      if (generation !== compactionStatusGeneration || !compactionRunning) {
        return;
      }
      finishCompactionStatus(ctx, generation);
    }, COMPACTION_STATUS_FAILSAFE_MS);
    compactionStatusTimer.unref?.();
    if (signal?.aborted) {
      finishCompactionStatus(ctx, generation);
      return;
    }
    signal?.addEventListener("abort", () => finishCompactionStatus(ctx, generation), { once: true });
    syncPresenceStatus();
  }
  function currentSessionTargetMatches(to: string, resolvedTo?: string | null, activeClient?: ParleyClient): boolean {
    const targets = new Set<string>();
    const addTarget = (target: string | undefined | null) => {
      const trimmed = target?.trim();
      if (trimmed) targets.add(trimmed.toLowerCase());
    };
    addTarget(currentSessionId);
    addTarget(currentParleySessionId);
    addTarget(activeClient?.sessionId);
    addTarget(pi.getSessionName());
    if (currentSessionId) addTarget(buildPresenceIdentity(pi, currentParleySessionId ?? currentSessionId).name);
    return Boolean(resolvedTo && activeClient?.sessionId && resolvedTo === activeClient.sessionId)
      || targets.has(to.trim().toLowerCase());
  }
  function buildOutboxResult(request: OutboxRequestTrace, status: ParleyOutboxResultStatus, options: {
    code?: ParleyOutboxResultCode;
    detail?: string;
    messageId?: string;
  } = {}): ParleyOutboxResultV1 {
    return {
      version: 1,
      requestId: request.requestId,
      status,
      ...(options.code ? { code: options.code } : {}),
      ...(request.extensionId ? { extensionId: request.extensionId } : {}),
      ...(request.extensionName ? { extensionName: request.extensionName } : {}),
      ...(options.messageId ? { messageId: options.messageId } : {}),
      ...(options.detail ? { detail: options.detail } : {}),
    };
  }
  function emitOutboxResult(result: ParleyOutboxResultV1, request: OutboxRequestTrace): void {
    recordConversationEntry("parley_outbox_result", {
      ...result,
      ...(request.to ? { to: request.to } : {}),
      ...(request.message ? { message: { text: request.message } } : {}),
      timestamp: Date.now(),
    });
    pi.events.emit(PARLEY_OUTBOX_RESULT_EVENT, result);
  }
  function settleOutboxRequest(requestId: string, status: ParleyOutboxResultStatus, options: {
    code?: ParleyOutboxResultCode;
    detail?: string;
    messageId?: string;
  } = {}): boolean {
    const pending = pendingOutboxRequests.get(requestId);
    if (!pending) {
      return false;
    }
    pendingOutboxRequests.delete(requestId);
    emitOutboxResult(buildOutboxResult(pending.request, status, options), pending.request);
    return true;
  }
  function failPendingOutboxRequests(generation: number, code: ParleyOutboxResultCode, detail: string): void {
    for (const [requestId, pending] of [...pendingOutboxRequests]) {
      if (pending.generation === generation) {
        settleOutboxRequest(requestId, "failed", { code, detail });
      }
    }
  }
  function resolveOutboxTarget(sessions: SessionInfo[], currentId: string, to: string): { ok: true; target: OutboxTarget } | { ok: false; code: "target_not_found" | "target_ambiguous" | "self_target"; detail: string } {
    const byId = sessions.find((session) => session.id === to);
    const lowerName = to.toLowerCase();
    const byName = byId ? [] : sessions.filter((session) => session.name?.toLowerCase() === lowerName);
    const byPrefix = byId || byName.length > 0 ? [] : sessions.filter((session) => session.id.startsWith(to));
    const matches = byId ? [byId] : byName.length > 0 ? byName : byPrefix;
    if (matches.length === 0) {
      return { ok: false, code: "target_not_found", detail: `Session "${to}" is not currently connected.` };
    }
    if (matches.length > 1) {
      return { ok: false, code: "target_ambiguous", detail: `Multiple sessions match "${to}".` };
    }
    const target = matches[0]!;
    if (target.id === currentId) {
      return { ok: false, code: "self_target", detail: "Cannot message the current session." };
    }
    return { ok: true, target: { id: target.id, label: target.name || target.id } };
  }
  function handleOutboxRequest(payload: unknown): void {
    const parsed = parseOutboxRequestPayload(payload);
    if (parsed.ok === false) {
      if (parsed.requestId) {
        const trace: OutboxRequestTrace = {
          requestId: parsed.requestId,
          ...(parsed.extensionId ? { extensionId: parsed.extensionId } : {}),
          ...(parsed.extensionName ? { extensionName: parsed.extensionName } : {}),
        };
        emitOutboxResult(buildOutboxResult(trace, "rejected", { code: "invalid_request", detail: parsed.detail }), trace);
      }
      return;
    }

    const request = parsed.request;
    const trace: OutboxRequestTrace = {
      requestId: request.requestId,
      extensionId: request.extensionId,
      extensionName: request.extensionName,
      to: request.to,
      message: request.message,
    };
    if (outboxRequestIds.has(request.requestId)) {
      emitOutboxResult(buildOutboxResult(trace, "rejected", { code: "duplicate_request", detail: "requestId has already been used in this session runtime" }), trace);
      return;
    }
    outboxRequestIds.add(request.requestId);

    const outboxGeneration = runtimeGeneration;
    pendingOutboxRequests.set(request.requestId, { generation: outboxGeneration, request: trace });

    void (async () => {
      const liveContext = getLiveContext(runtimeContext, outboxGeneration);
      if (!liveContext) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_unavailable", detail: "Parley session is not active" });
        return;
      }
      if (config.confirmSend && !liveContext.hasUI) {
        settleOutboxRequest(request.requestId, "blocked", { code: "confirmation_unavailable", detail: "confirmSend is enabled but no UI is available" });
        return;
      }

      let activeClient: ParleyClient;
      try {
        activeClient = await ensureConnected("background");
      } catch (error) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_unavailable", detail: getErrorMessage(error) });
        return;
      }
      if (!getLiveContext(liveContext, outboxGeneration)) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended before target resolution" });
        return;
      }

      let target: OutboxTarget;
      try {
        const currentClientSessionId = activeClient.sessionId;
        const sessions = await activeClient.listSessions();
        if (!currentClientSessionId) {
          settleOutboxRequest(request.requestId, "failed", { code: "session_unavailable", detail: "Current session is not registered with parley" });
          return;
        }
        const resolved = resolveOutboxTarget(sessions, currentClientSessionId, request.to);
        if (resolved.ok === false) {
          settleOutboxRequest(request.requestId, "blocked", { code: resolved.code, detail: resolved.detail });
          return;
        }
        target = resolved.target;
      } catch (error) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_unavailable", detail: getErrorMessage(error) });
        return;
      }
      if (!getLiveContext(liveContext, outboxGeneration)) {
        settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended before confirmation" });
        return;
      }

      if (config.confirmSend) {
        let confirmed = false;
        try {
          confirmed = await liveContext.ui.confirm(
            "Send extension message",
            `Allow ${request.extensionName} (${request.extensionId}) to send to "${target.label}":\n\n${request.message}`,
          );
        } catch (error) {
          settleOutboxRequest(request.requestId, "blocked", { code: "confirmation_unavailable", detail: getErrorMessage(error) });
          return;
        }
        if (!getLiveContext(liveContext, outboxGeneration)) {
          settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended during confirmation" });
          return;
        }
        if (!confirmed) {
          settleOutboxRequest(request.requestId, "rejected", { code: "user_cancelled", detail: "User cancelled the outbox request" });
          return;
        }
      }

      try {
        if (!getLiveContext(liveContext, outboxGeneration) || client !== activeClient || !activeClient.isConnected()) {
          settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended before delivery" });
          return;
        }
        const result = await activeClient.send(target.id, {
          messageId: request.requestId,
          text: request.message,
          provenance: {
            type: "extension_outbox",
            extensionId: request.extensionId,
            extensionName: request.extensionName,
            requestId: request.requestId,
          },
        });
        if (!getLiveContext(liveContext, outboxGeneration)) {
          settleOutboxRequest(request.requestId, "failed", { code: "session_ended", detail: "Session ended during delivery" });
          return;
        }
        if (!result.delivered) {
          settleOutboxRequest(request.requestId, "failed", { code: "delivery_failed", messageId: result.id, detail: result.reason ?? "Delivery failed" });
          return;
        }
        surfaceBackgroundPeerCompaction(activeClient, target.label, result, outboxGeneration, target.id);
        recordConversationEntry("parley_sent", {
          to: target.label,
          targetId: result.recipient?.id ?? target.id,
          message: { text: request.message },
          messageId: result.id,
          timestamp: Date.now(),
          extension: { id: request.extensionId, name: request.extensionName, requestId: request.requestId },
        });
        settleOutboxRequest(request.requestId, "sent", { messageId: result.id });
      } catch (error) {
        const live = getLiveContext(liveContext, outboxGeneration);
        settleOutboxRequest(request.requestId, "failed", {
          code: live ? "session_unavailable" : "session_ended",
          detail: getErrorMessage(error),
        });
      }
    })();
  }
  function shouldTriggerInboundMessage(entry: InboundMessageEntry, forceTrigger = false): boolean {
    if (forceTrigger) {
      return true;
    }
    if (config.inboundTrigger === "always") {
      return true;
    }
    if (config.inboundTrigger === "replies") {
      return Boolean(entry.message.replyTo);
    }
    return false;
  }
  // Fork: when the broker expires (or evicts) a queued mailbox message we sent,
  // surface it to the session instead of silently updating the delivery record.
  // Without this, a "Message sent" from up to a day ago can quietly die with
  // the sender none the wiser.
  function handleExpiredOutboundReceipt(from: SessionInfo, receipt: MessageReceipt): void {
    if (!runtimeStarted || !getLiveContext()) {
      return;
    }
    const targetDisplay = from.name || from.id.slice(0, 8);
    const original = findOutgoingTopic(receipt.messageId);
    const topic = original ? `\nOriginal message: ${JSON.stringify(previewText(original.message?.text ?? original.preview, 240))}` : "";
    pi.appendEntry("parley_delivery_failed", {
      to: targetDisplay,
      messageId: receipt.messageId,
      detail: receipt.detail ?? "Mailbox delivery expired",
      timestamp: receipt.timestamp,
    });
    pi.sendMessage(
      {
        customType: "parley_delivery_notice",
        content: `**Parley delivery failed:** queued message to ${targetDisplay} (${from.cwd}) expired before delivery.\nMessage ID: ${receipt.messageId}${topic}\nReason: ${receipt.detail ?? "mailbox entry expired"}.`,
        display: true,
        details: { to: targetDisplay, messageId: receipt.messageId, expired: true, ...(receipt.detail ? { detail: receipt.detail } : {}) },
      },
      { triggerTurn: true },
    );
  }

  function surfaceBackgroundPeerCompaction(
    sourceClient: ParleyClient,
    peerDisplay: string,
    result: Pick<SendResult, "id" | "peerCompaction" | "contactToken">,
    generation = runtimeGeneration,
    expectedPeerSessionId?: string,
  ): void {
    if (!result.peerCompaction || (runtimeStarted && !getLiveContext(runtimeContext, generation))) return;
    const notice = formatPeerCompactionNotice(peerDisplay, result.peerCompaction, expectedPeerSessionId);
    pi.sendMessage(
      {
        customType: "parley_compaction_awareness",
        content: `**Parley compaction awareness**\n\n${notice}`,
        display: true,
        details: { messageId: result.id, peerCompaction: result.peerCompaction },
      },
      { triggerTurn: false },
    );
    sourceClient.acknowledgeSendContact(result);
  }

  function clearReceiverBaselineRetryTimer(): void {
    if (!receiverBaselineRetryTimer) return;
    clearTimeout(receiverBaselineRetryTimer);
    receiverBaselineRetryTimer = null;
  }

  function scheduleReceiverBaselineRetry(expectedGeneration = runtimeGeneration): void {
    if (receiverBaselineRetryTimer || pendingReceiverBaselineTokens.size === 0) return;
    receiverBaselineRetryTimer = setTimeout(() => {
      receiverBaselineRetryTimer = null;
      const activeClient = client;
      if (expectedGeneration !== runtimeGeneration || !activeClient?.isConnected()) return;
      for (const token of pendingReceiverBaselineTokens) activeClient.acknowledgeContactToken(token);
      scheduleReceiverBaselineRetry(expectedGeneration);
    }, 1_000);
    receiverBaselineRetryTimer.unref?.();
  }

  function acknowledgeInboundMessageContact(sourceClient: ParleyClient | null, message: Message): void {
    if (!message.contactToken || !sourceClient) return;
    if (message.contactBaseline) {
      recordConversationEntry("parley_receiver_baseline_pending", {
        token: message.contactToken,
        messageId: message.id,
        timestamp: Date.now(),
      });
      pendingReceiverBaselineTokens.add(message.contactToken);
      scheduleReceiverBaselineRetry();
    }
    sourceClient.acknowledgeMessageContact(message);
  }

  function findOutgoingTopic(messageId: string): { to: string; preview: string; message?: Message["content"] } | undefined {
    const entries = getLiveContext()?.sessionManager.getEntries() ?? [];
    for (let index = entries.length - 1; index >= 0; index--) {
      const entry = entries[index];
      if (entry?.type !== "custom" || (normalizeEntryType(entry.customType) !== "parley_ask_pending" && normalizeEntryType(entry.customType) !== "parley_sent")) continue;
      const data = entry.data as { messageId?: string; to?: string; targetId?: string; message?: Message["content"] } | undefined;
      const recipient = data?.targetId ?? data?.to;
      if (data?.messageId === messageId && recipient && data.message) return { to: recipient, preview: data.message.text, message: data.message };
    }
    return undefined;
  }
  function sendIncomingMessage(entry: InboundMessageEntry, delivery: "trigger" | "steer", generation = runtimeGeneration, forceTrigger = false): void {
    if (runtimeStarted && !getLiveContext(runtimeContext, generation)) return;
    const injectedMessage = { ...entry.message, injectedAt: Date.now() };
    const deliveredEntry = { ...entry, message: injectedMessage };
    const senderDisplay = entry.from.name || entry.from.id;
    const focus = entry.from.description ? ` — ${entry.from.description}` : "";
    const origin = entry.from.federation
      ? `\nRemote origin: ${entry.from.federation.originLabel || entry.from.federation.originId}; scope ${entry.from.federation.remoteScopeAlias}`
      : "";
    const outgoing = injectedMessage.replyTo ? outstandingAsks.get(injectedMessage.replyTo) ?? findOutgoingTopic(injectedMessage.replyTo) : undefined;
    const topic = outgoing && outgoing.to === entry.from.id
      ? `\nReply to your message: ${JSON.stringify(previewText(outgoing.message?.text ?? outgoing.preview, 240))}`
      : "";
    const elapsed = injectedMessage.replyDeadline !== undefined && injectedMessage.replyDeadline < Date.now();
    const waiting = injectedMessage.senderWaitMode === "blocking" ? " · blocking ask"
      : injectedMessage.senderWaitMode === "nonblocking" ? " · async ask" : "";
    const request = injectedMessage.expectsReply
      ? `\nReply requested${waiting}.${elapsed ? " The original wait window has elapsed." : ""}`
      : "";
    const compactionNotice = injectedMessage.peerCompaction
      ? `\n\n${formatPeerCompactionNotice(senderDisplay, injectedMessage.peerCompaction, entry.from.id)}`
      : "";
    const envelope = {
      customType: "parley_message",
      content: `**From ${senderDisplay}**${focus}${request}${topic}\n\n${entry.bodyText}\n\n${formatInboundDeliveryMetadata(injectedMessage)}\nSession: ${entry.from.id} · ${entry.from.cwd}${origin}${compactionNotice}`,
      display: true,
      details: deliveredEntry,
    };
    deferredInboundMessages.set(entry.message.id, entry);
    pendingHostEnvelopes.set(`message:${entry.message.id}`, envelope);
    try {
      pi.sendMessage(envelope,
        delivery === "trigger" && shouldTriggerInboundMessage(entry, forceTrigger || Boolean(outgoing && outgoing.to === entry.from.id))
          ? { triggerTurn: true } : { deliverAs: "steer" });
    } catch {
      emitMessageReceipt(entry.message.id, "queued", "retained locally; host injection will be retried");
    }
    // sendMessage is fire-and-forget on supported hosts. Returning void is not delivery confirmation.
    confirmPersistedInbound();
    scheduleInboundRetry();
  }
  function surfaceInboundCompactionOnly(from: SessionInfo, message: Message, generation: number): void {
    if (!message.peerCompaction || !getLiveContext(runtimeContext, generation)) return;
    const senderDisplay = from.name || from.id.slice(0, 8);
    pi.sendMessage(
      {
        customType: "parley_compaction_awareness",
        content: `**Parley compaction awareness**\n\n${formatPeerCompactionNotice(senderDisplay, message.peerCompaction, from.id)}`,
        display: true,
        details: { messageId: message.id, peerCompaction: message.peerCompaction },
      },
      { triggerTurn: false },
    );
    acknowledgeInboundMessageContact(client, message);
  }
  function sendIncomingBrokerMessage(entry: InboundMessageEntry, delivery: "trigger" | "steer", generation = runtimeGeneration): void {
    sendIncomingMessage(entry, delivery, generation);
  }
  function handleIncomingMessage(ctx: ExtensionContext, from: SessionInfo, message: Message): void {
    const messageGeneration = runtimeGeneration;
    const liveContext = getLiveContext(ctx, messageGeneration);
    if (!liveContext) {
      return;
    }
    const receiverReceivedAt = Date.now();
    if (hasSeenInboundMessage(from, message, receiverReceivedAt)) {
      surfaceInboundCompactionOnly(from, message, messageGeneration);
      emitMessageReceipt(message.id, "acknowledged", "duplicate message id suppressed");
      return;
    }
    const receivedMessage = { ...message, receiverReceivedAt };
    emitMessageReceipt(receivedMessage.id, "receiver_received");
    const waiter = replyWaiter;
    const fromMatches = waiter && ((from.name || from.id).toLowerCase() === waiter.from.toLowerCase() || from.id === waiter.from);
    const matchedWaiter = fromMatches && receivedMessage.replyTo === waiter.replyTo && receivedMessage.completesAsk !== false && !receivedMessage.expectsReply;
    replyTracker.recordIncomingMessage(from, receivedMessage, receiverReceivedAt);
    recordConversationEntry("parley_inbound_received", {
      from, message: receivedMessage, receivedAt: receiverReceivedAt,
      ...(matchedWaiter ? { delivery: "tool_result" } : {}),
    });
    if (matchedWaiter) {
      emitMessageReceipt(receivedMessage.id, "acknowledged", "matched reply waiter");
      waiter.resolve(receivedMessage);
      return;
    }
    const attachmentText = receivedMessage.content.attachments?.length
      ? formatAttachments(receivedMessage.content.attachments)
      : "";
    const bodyText = `${receivedMessage.content.text}${attachmentText}`;
    const replyCommand = config.replyHint && receivedMessage.expectsReply
      ? `parley({ action: "reply", message: "..." })`
      : undefined;
    emitMessageReceipt(receivedMessage.id, "acknowledged", "accepted by receiver");
    const entry = { from, message: receivedMessage, replyCommand, bodyText };
    void (async () => {
      const activeContext = getLiveContext(liveContext, messageGeneration);
      if (!activeContext) {
        return;
      }
      if (!activeContext.isIdle()) {
        sendIncomingBrokerMessage(entry, "steer");
        return;
      }
      if (getLiveContext(liveContext, messageGeneration)) {
        sendIncomingBrokerMessage(entry, "trigger", messageGeneration);
      }
    })();
  }
  function attachClientHandlers(nextClient: ParleyClient): void {
    nextClient.onBrokerMessage((message: BrokerMessage) => {
      if (client !== nextClient) return;
      switch (message.type) {
        case "registered": {
          const supported = message.features?.includes(EXTENSION_BUS_FEATURE) ?? false;
          if (supported && localExtensions.size > 0) {
            nextClient.updateExtensionCapabilities(currentExtensionCapabilities());
          }
          for (const namespace of localExtensions.keys()) {
            emitLocalExtensionEvent(namespace, { type: "connection", connected: true, supported });
          }
          flushPendingCompactionReports();
          for (const token of pendingReceiverBaselineTokens) {
            nextClient.acknowledgeContactToken(token);
          }
          scheduleReceiverBaselineRetry();
          break;
        }
        case "direct_contact_recorded":
          if (pendingReceiverBaselineTokens.delete(message.token)) {
            if (pendingReceiverBaselineTokens.size === 0) clearReceiverBaselineRetryTimer();
            recordConversationEntry("parley_receiver_baseline_recorded", {
              token: message.token,
              timestamp: Date.now(),
            });
          }
          break;
        case "direct_contact_unknown":
          if (pendingReceiverBaselineTokens.delete(message.token)) {
            if (pendingReceiverBaselineTokens.size === 0) clearReceiverBaselineRetryTimer();
            recordConversationEntry("parley_receiver_baseline_abandoned", {
              token: message.token,
              timestamp: Date.now(),
              reason: "Broker no longer retains the bounded staged baseline token",
            });
          }
          break;
        case "extension_owner": {
          const extension = localExtensions.get(message.namespace);
          if (!extension) break;
          extension.owner = message.ownerId && message.ownerEpoch
            ? { sessionId: message.ownerId, epoch: message.ownerEpoch }
            : undefined;
          emitLocalExtensionEvent(message.namespace, { type: "owner", ...(extension.owner ? { owner: extension.owner } : {}) });
          break;
        }
        case "extension_message": {
          const extension = localExtensions.get(message.namespace);
          if (!extension) break;
          emitLocalExtensionEvent(message.namespace, {
            type: "message",
            fromSessionId: message.fromSessionId,
            ...(message.ownerId && message.ownerEpoch
              ? { owner: { sessionId: message.ownerId, epoch: message.ownerEpoch } }
              : {}),
            payload: message.payload,
          });
          break;
        }
        case "extension_state": {
          const extension = localExtensions.get(message.namespace);
          if (!extension) break;
          extension.state = { revision: message.revision, payload: message.payload };
          emitLocalExtensionEvent(message.namespace, { type: "state", state: extension.state });
          break;
        }
        case "extension_state_result":
          emitLocalExtensionEvent(message.namespace, {
            type: "state_result",
            committed: message.committed,
            revision: message.revision,
            ...(message.reason ? { reason: message.reason } : {}),
          });
          break;
        case "message_receipt":
          latestOutboundReceipts.set(message.receipt.messageId, {
            status: message.receipt.status,
            timestamp: message.receipt.timestamp,
            ...(message.receipt.detail ? { detail: message.receipt.detail } : {}),
          });
          if (message.receipt.status === "expired") {
            handleExpiredOutboundReceipt(message.from, message.receipt);
          }
          break;
        case "message_control":
          handleMessageControl(message.from, message.control);
          break;
        case "session_joined":
          for (const namespace of localExtensions.keys()) {
            emitLocalExtensionEvent(namespace, { type: "session_joined", session: message.session });
          }
          break;
        case "session_left":
          for (const namespace of localExtensions.keys()) {
            emitLocalExtensionEvent(namespace, { type: "session_left", sessionId: message.sessionId });
          }
          break;
        case "presence_update":
          for (const namespace of localExtensions.keys()) {
            emitLocalExtensionEvent(namespace, { type: "presence_update", session: message.session });
          }
          break;
      }
    });
    nextClient.on("message", (from, message) => {
      const liveContext = getLiveContext();
      if (client !== nextClient || !liveContext) {
        return;
      }
      handleIncomingMessage(liveContext, from, message);
    });
    nextClient.on("disconnected", (error: Error) => {
      if (client !== nextClient) {
        return;
      }
      rejectReplyWaiter(new Error(`Disconnected while waiting for reply: ${error.message}`, { cause: error }));
      for (const [namespace, extension] of localExtensions) {
        extension.owner = undefined;
        emitLocalExtensionEvent(namespace, { type: "connection", connected: false, supported: false });
        emitLocalExtensionEvent(namespace, { type: "owner" });
      }
      currentAdvertisedName = undefined;
      lastPresenceRequestedName = undefined;
      client = null;
      if (!shuttingDown && !disposed) {
        clearReconnectTimer();
        scheduleReconnect();
      }
    });
    nextClient.on("error", () => {
      // Keep broker/socket noise out of the TUI. Reconnect logic runs from the disconnect path.
    });
  }
  function scheduleReconnect(): void {
    if (disposed || shuttingDown || reconnectTimer || reconnectPromise || !getLiveContext()) {
      return;
    }
    const scheduledGeneration = runtimeGeneration;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (scheduledGeneration !== runtimeGeneration || !getLiveContext()) {
        return;
      }
      reconnectAttempt += 1;
      void ensureConnected("background").catch(() => {
        // ensureConnected("background") already queued the next retry.
      });
    }, getReconnectDelayMs());
  }
  async function ensureConnected(reason: "startup" | "background" | "tool" | "overlay"): Promise<ParleyClient> {
    if (!config.enabled) {
      throw new Error("Parley disabled");
    }
    if (disposed || shuttingDown) {
      throw new Error("Parley shutting down");
    }
    if (client && client.isConnected()) {
      return client;
    }
    const contextAtStart = getLiveContext();
    const generationAtStart = runtimeGeneration;
    if (!contextAtStart || !currentSessionId || sessionStartedAt === null) {
      throw new Error("Parley runtime not initialized");
    }
    clearReconnectTimer();
    if (reconnectPromise && reconnectPromiseGeneration === generationAtStart) {
      return reconnectPromise;
    }
    let nextReconnectPromise!: Promise<ParleyClient>;
    let retryAfterFailure = false
    nextReconnectPromise = (async () => {
      const nextClient = new ParleyClient();
      client = nextClient;
      attachClientHandlers(nextClient);
      try {
        await spawnBrokerIfNeeded(config.brokerCommand, config.brokerArgs);
        await nextClient.connect(buildRegistration(), currentParleySessionId ?? currentSessionId);
        if (!getLiveContext(contextAtStart, generationAtStart)) {
          await nextClient.disconnect();
          throw new Error("Parley runtime no longer active");
        }
        client = nextClient;
        reconnectAttempt = 0;
        // Registration snapshots identity before awaiting the broker ACK. A Pi
        // name event can land in that window while updatePresence is not yet
        // writable; replay the live identity once the session id is assigned.
        syncPresenceIdentity(currentSessionId);
        return nextClient;
      } catch (error) {
        if (client === nextClient) {
          client = null;
        }
        retryAfterFailure = getLiveContext(contextAtStart, generationAtStart) !== null;
        throw toError(error);
      } finally {
        if (reconnectPromise === nextReconnectPromise) {
          reconnectPromise = null;
          reconnectPromiseGeneration = null;
        }
        // Schedule only after the owned promise is cleared: scheduleReconnect
        // drops calls made while a reconnect attempt is still in flight, and
        // a legacy-broker cutover must keep retrying after it idles out.
        if (retryAfterFailure) {
          scheduleReconnect();
        }
      }
    })();
    reconnectPromise = nextReconnectPromise;
    reconnectPromiseGeneration = generationAtStart;
    return nextReconnectPromise;
  }
  function resolveSessionFromRoster(sessions: SessionInfo[], nameOrId: string): SessionInfo | null {
    const byId = sessions.find(s => s.id === nameOrId);
    if (byId) {
      return byId;
    }
    const lowerName = nameOrId.toLowerCase();
    const byName = sessions.filter(s => s.name?.toLowerCase() === lowerName);
    if (byName.length > 1) {
      const prefixes = sessionIdPrefixes(sessions);
      const ids = byName.map((session) => prefixes.get(session.id)!).join(", ");
      throw new Error(`Multiple sessions named "${nameOrId}" are connected. Address one by the id shown in parentheses by "list" (${ids}).`);
    }
    if (byName.length === 1) {
      return byName[0]!;
    }

    const byIdPrefix = sessions.filter(s => s.id.startsWith(nameOrId));
    if (byIdPrefix.length === 1) {
      return byIdPrefix[0]!;
    }
    if (byIdPrefix.length > 1) {
      throw new Error(`Multiple sessions match ID prefix "${nameOrId}". Use a longer session ID prefix.`);
    }
    return null;
  }
  async function resolveSessionTarget(activeClient: ParleyClient, nameOrId: string): Promise<string | null> {
    return resolveSessionFromRoster(await activeClient.listSessions(), nameOrId)?.id ?? null;
  }
  async function resolveSupervisorTarget(activeClient: ParleyClient, metadata: ChildOrchestratorMetadata): Promise<string | null> {
    if (metadata.orchestratorSessionId) {
      const bySessionId = await resolveSessionTarget(activeClient, metadata.orchestratorSessionId);
      if (bySessionId) {
        return bySessionId;
      }
    }
    return resolveSessionTarget(activeClient, metadata.orchestratorTarget);
  }
  async function resolveCwdDeliveryTarget(activeClient: ParleyClient, options: {
    to?: string;
    cwd: string;
    openProjectPaneIfMissing?: boolean;
    focus?: boolean;
    signal?: AbortSignal;
  }): Promise<DeliveryTarget> {
    const sessions = await activeClient.listSessions();
    const currentSessionId = activeClient.sessionId;
    if (!currentSessionId) {
      throw new Error("Current session is not registered with parley.");
    }
    const currentSession = sessions.find((session) => session.id === currentSessionId);
    if (!currentSession) {
      throw new Error("Current session is missing from parley session list.");
    }

    const targetCwd = options.cwd && options.cwd !== "."
      ? resolvePath(currentSession.cwd, options.cwd)
      : currentSession.cwd;
    const existing = resolveTargetInCwd({
      sessions,
      currentSessionId,
      targetCwd,
      ...(options.to ? { to: options.to } : {}),
    });
    if (existing.kind === "found" && existing.session) {
      return { id: existing.session.id, label: options.to || existing.session.name || existing.session.id };
    }
    if (!options.openProjectPaneIfMissing) {
      throw new Error(`${existing.reason ?? `No parley session is connected in ${targetCwd}.`} No session was launched and no message was sent.`);
    }

    const beforeSessionIds = new Set(sessions.map((session) => session.id));
    const projectPane = await openProjectPane({
      cwd: targetCwd,
      focus: options.focus,
      sessions,
      currentSessionId,
      launcherCommand: resolveProjectLauncherCommand(process.env, config.projectLauncher),
      sendRequest: (provider, request) => activeClient.send(provider.id, {
        text: projectLaunchRequestText(request),
        signal: options.signal,
      }).then((result) => ({ delivered: result.delivered, id: result.id, outcomeKnown: result.outcomeKnown, ...(result.reason ? { reason: result.reason } : {}) })),
      signal: options.signal,
    });
    const session = await waitForProjectSession(activeClient, {
      projectRoot: projectPane.projectRoot,
      currentSessionId,
      beforeSessionIds,
      launch: projectPane,
      signal: options.signal,
    });
    return { id: session.id, label: session.name || session.id, projectPane, session };
  }
  function projectObservation(target: DeliveryTarget): string {
    if (!target.projectPane) return "";
    const launch = target.projectPane;
    return `\nProject launch: ${launch.outcome} in ${launch.projectRoot}${launch.requestMessageId ? ` (request ${launch.requestMessageId})` : ""}.\nObserved local session: ${target.label} (${target.id}). Registration does not prove which launch created it.`;
  }

  async function sendBatchMessages(
    activeClient: ParleyClient,
    targets: BatchDeliveryTarget[],
    options: {
      message: string;
      attachments?: Attachment[];
      batchId: string;
      broadcast: boolean;
      signal?: AbortSignal;
      allowRosterMailboxFallback?: boolean;
    },
  ): Promise<BatchDeliveryOutcome[]> {
    return mapWithConcurrency(targets, SEND_FANOUT_CONCURRENCY, async (target) => {
      if (options.signal?.aborted) {
        return {
          to: target.label,
          ...(target.session ? { targetId: target.session.id } : {}),
          delivered: false,
          delivery: "failed",
          retryable: true,
          outcomeKnown: true,
          code: "E_CANCELLED",
          reason: "Send cancelled before this recipient was attempted",
        };
      }
      if (target.resolutionError) {
        return {
          to: target.label,
          delivered: false,
          delivery: "failed",
          retryable: false,
          outcomeKnown: true,
          code: target.resolutionCode ?? "E_TARGET_RESOLUTION",
          reason: target.resolutionError,
        };
      }
      try {
        const sendOptions = {
          text: options.message,
          attachments: options.attachments,
          signal: options.signal,
          contactKind: options.broadcast ? "broadcast" as const : "direct" as const,
        };
        let result = target.session
          ? await activeClient.sendToSession(target.session, sendOptions)
          : await activeClient.send(target.requested, sendOptions);
        // Exact delivery to a roster peer can lose a disconnect race before the
        // frame reaches the broker. For an unconfirmed explicit group, retry
        // that known not-delivered outcome through ordinary ID routing so the
        // existing disconnected mailbox can accept it. Confirmed sends preserve
        // their displayed endpoint snapshot; broadcast is live-only. Neither may
        // turn a departed roster entry into mail for a rebound identity.
        if (
          !options.broadcast
          && options.allowRosterMailboxFallback !== false
          && target.session
          && result.code === "E_TARGET_NOT_FOUND"
        ) {
          if (options.signal?.aborted) {
            return {
              to: target.label,
              targetId: target.session.id,
              messageId: result.id,
              delivered: false,
              delivery: "failed",
              retryable: true,
              outcomeKnown: true,
              code: "E_CANCELLED",
              reason: "Send cancelled before offline delivery was attempted",
            };
          }
          result = await activeClient.send(target.session.id, sendOptions);
        }
        const outcome: BatchDeliveryOutcome = {
          to: result.recipient?.name || target.label,
          ...(result.recipient ? { targetId: result.recipient.id } : target.session ? { targetId: target.session.id } : {}),
          messageId: result.id,
          delivered: result.delivered,
          delivery: result.delivery,
          retryable: result.retryable,
          outcomeKnown: result.outcomeKnown,
          ...(result.code ? { code: result.code } : {}),
          ...(result.reason ? { reason: result.reason } : {}),
          ...(result.peerCompaction ? { peerCompaction: result.peerCompaction } : {}),
        };
        if (result.delivered) {
          recordConversationEntry("parley_sent", {
            to: target.label,
            targetId: result.recipient?.id ?? target.session?.id,
            message: { text: options.message, attachments: options.attachments },
            messageId: result.id,
            batchId: options.batchId,
            broadcast: options.broadcast,
            ...(result.peerCompaction ? { peerCompaction: result.peerCompaction } : {}),
            timestamp: Date.now(),
          });
        }
        activeClient.acknowledgeSendContact(result);
        return outcome;
      } catch (error) {
        return {
          to: target.label,
          ...(target.session ? { targetId: target.session.id } : {}),
          delivered: false,
          delivery: "unknown",
          retryable: true,
          outcomeKnown: false,
          reason: getErrorMessage(error),
        };
      }
    });
  }
  function deliverLocalSubagentRelayMessage(sender: "subagent-control" | "subagent-result", status: string, messageText: string): void {
    const liveContext = getLiveContext();
    const now = Date.now();
    sendIncomingMessage({
      from: {
        id: sender,
        name: sender,
        cwd: liveContext?.cwd ?? "",
        model: sender,
        pid: process.pid,
        startedAt: now,
        lastActivity: now,
        status,
      },
      message: {
        id: randomUUID(),
        timestamp: now,
        content: { text: messageText },
      },
      bodyText: messageText,
    }, "trigger", runtimeGeneration, true);
  }
  function recordSubagentDeliveryError(entryType: string, to: string, message: string, error: unknown): void {
    pi.appendEntry(entryType, {
      to,
      message,
      error: getErrorMessage(error),
      timestamp: Date.now(),
    });
  }
  function restoreConversations(ctx: ExtensionContext): void {
    if (inboundRetryTimer) clearTimeout(inboundRetryTimer);
    inboundRetryTimer = null;
    deferredInboundMessages.clear();
    deferredInboundControls.clear();
    pendingHostEnvelopes.clear();
    contextFallbackEnvelopes.clear();
    freshModelContexts.clear();
    outstandingAsks.clear();
    seenInboundMessages.clear();
    latestOutboundReceipts.clear();
    conversationPersistenceWarning = undefined;
    const history = restoreConversationHistory(ctx.sessionManager.getEntries());
    for (const [id, ask] of history.outgoing) outstandingAsks.set(id, ask);
    for (const context of history.incoming.values()) {
      replyTracker.recordIncomingMessage(context.from, context.message, context.receivedAt);
      hasSeenInboundMessage(context.from, context.message);
      if (history.settledIncoming.has(context.message.id)) {
        replyTracker.dismissPendingAsk(context.message.id);
      } else if (!history.persistedIncoming.has(context.message.id)) {
        deferredInboundMessages.set(context.message.id, {
          from: context.from, message: context.message,
          bodyText: context.message.content.text + (context.message.content.attachments?.length ? formatAttachments(context.message.content.attachments) : ""),
        });
      }
    }
    for (const [key, value] of history.controls) {
      replyTracker.setDisposition(value.control.messageId, {
        state: value.control.action === "cancel" ? "withdrawn" : "superseded",
        ...(value.control.supersededBy ? { replacementId: value.control.supersededBy } : {}),
      });
      if (!history.persistedControls.has(key)) handleMessageControl(value.from, value.control, true);
    }
    for (const entry of deferredInboundMessages.values()) sendIncomingMessage(entry, ctx.isIdle() ? "trigger" : "steer");
    if (deferredInboundMessages.size || deferredInboundControls.size) scheduleInboundRetry();
  }
  function startSessionRuntime(ctx: ExtensionContext): void {
    const previousClient = client;
    failPendingOutboxRequests(runtimeGeneration, "session_ended", "Session replaced");
    shuttingDown = false;
    disposed = false;
    runtimeStarted = true;
    runtimeGeneration += 1;
    compactionReportFlush = null;
    clearCompactionReportRetryTimer();
    clearReceiverBaselineRetryTimer();
    restorePendingCompactionReports(ctx);
    outboxRequestIds.clear();
    reconnectAttempt = 0;
    clearReconnectTimer();
    clearStartupConnectTimer();
    rejectReplyWaiter(new Error("Session replaced"));
    replyTracker.reset();
    if (previousClient) {
      client = null;
      void previousClient.disconnect().catch(() => undefined);
    }
    runtimeContext = ctx;
    currentSessionId = ctx.sessionManager.getSessionId();
    currentParleySessionId = resolveConfiguredParleySessionId(currentSessionId, config);
    publishParleySessionId(currentParleySessionId);
    currentModel = ctx.model?.id ?? "unknown";
    sessionStartedAt = Date.now();
    restoreConversations(ctx);
    agentRunning = false;
    activeTools.clear();
    resetCompactionStatus();
    startSessionNameCompatibilityTimer();
    const startupGeneration = runtimeGeneration;
    startupConnectTimer = setTimeout(() => {
      startupConnectTimer = null;
      if (!getLiveContext(ctx, startupGeneration)) {
        return;
      }
      void ensureConnected("startup").catch(() => {
        if (!getLiveContext(ctx, startupGeneration)) {
          return;
        }
        client = null;
        scheduleReconnect();
      });
    }, 0);
  }
  function emitResultDelivery(requestId: string | undefined, delivered: boolean, error?: unknown): void {
    if (!requestId) return;
    pi.events.emit(SUBAGENT_RESULT_PARLEY_DELIVERY_EVENT, {
      requestId,
      delivered,
      ...(error ? { error: getErrorMessage(error) } : {}),
    });
  }
  function relaySubagentParleyPayload(payload: unknown, options: {
    sender: "subagent-control" | "subagent-result";
    status: string;
    errorEntryType: string;
    acknowledge?: boolean;
  }): void {
    const parsed = parseSubagentParleyPayload(payload);
    if (!parsed) return;

    const relayGeneration = runtimeGeneration;
    void (async () => {
      const relayStillLive = () => !runtimeStarted || Boolean(getLiveContext(runtimeContext, relayGeneration));
      if (!relayStillLive()) {
        return;
      }
      if (currentSessionTargetMatches(parsed.to)) {
        deliverLocalSubagentRelayMessage(options.sender, options.status, parsed.message);
        if (options.acknowledge) emitResultDelivery(parsed.requestId, true);
        return;
      }

      let activeClient: ParleyClient;
      let target: string;
      try {
        activeClient = await ensureConnected("background");
        target = await resolveSessionTarget(activeClient, parsed.to) ?? parsed.to;
      } catch (error) {
        if (!relayStillLive()) return;
        recordSubagentDeliveryError(options.errorEntryType, parsed.to, parsed.message, error);
        if (options.acknowledge) emitResultDelivery(parsed.requestId, false, error);
        return;
      }

      if (!relayStillLive()) {
        return;
      }
      if (currentSessionTargetMatches(parsed.to, target, activeClient)) {
        deliverLocalSubagentRelayMessage(options.sender, options.status, parsed.message);
        if (options.acknowledge) emitResultDelivery(parsed.requestId, true);
        return;
      }

      try {
        const result = await activeClient.send(target, { text: parsed.message });
        if (!relayStillLive()) return;
        if (!result.delivered) {
          const error = new Error(result.reason ?? "Session may not exist or has disconnected.");
          recordSubagentDeliveryError(options.errorEntryType, parsed.to, parsed.message, error);
          if (options.acknowledge) emitResultDelivery(parsed.requestId, false, error);
          return;
        }
        surfaceBackgroundPeerCompaction(activeClient, parsed.to, result, relayGeneration, target);
        if (options.acknowledge) emitResultDelivery(parsed.requestId, true);
      } catch (error) {
        if (!relayStillLive()) return;
        recordSubagentDeliveryError(options.errorEntryType, parsed.to, parsed.message, error);
        if (options.acknowledge) emitResultDelivery(parsed.requestId, false, error);
      }
    })();
  }
  const unsubscribeExtensionRegister = pi.events.on(PARLEY_EXTENSION_REGISTER_EVENT, (payload) => {
    if (!payload || typeof payload !== "object") return;
    const registration = payload as Partial<ParleyExtensionRegistration>;
    if (
      typeof registration.namespace !== "string"
      || typeof registration.ownerEligible !== "boolean"
      || typeof registration.onEvent !== "function"
      || typeof registration.onReady !== "function"
    ) {
      return;
    }
    registerLocalExtension(registration as ParleyExtensionRegistration);
  });
  pi.events.emit(PARLEY_EXTENSION_REGISTRY_READY_EVENT, { version: 1 });
  const unsubscribeSubagentControlParley = pi.events.on(SUBAGENT_CONTROL_PARLEY_EVENT, (payload) => {
    relaySubagentParleyPayload(payload, {
      sender: "subagent-control",
      status: "needs_attention",
      errorEntryType: "parley_control_error",
    });
  });
  const unsubscribeSubagentResultParley = pi.events.on(SUBAGENT_RESULT_PARLEY_EVENT, (payload) => {
    relaySubagentParleyPayload(payload, {
      sender: "subagent-result",
      status: "result",
      errorEntryType: "parley_result_error",
      acknowledge: true,
    });
  });
  const unsubscribeOutboxRequest = pi.events.on(PARLEY_OUTBOX_REQUEST_EVENT, handleOutboxRequest);
  pi.on("session_start", (_event, ctx) => {
    if (!config.enabled) {
      return;
    }
    startSessionRuntime(ctx);
  });
  
  pi.on("session_shutdown", async () => {
    unsubscribeExtensionRegister();
    unsubscribeSubagentControlParley();
    unsubscribeSubagentResultParley();
    unsubscribeOutboxRequest();
    shuttingDown = true;
    disposed = true;
    failPendingOutboxRequests(runtimeGeneration, "session_ended", "Session shutting down");
    runtimeGeneration += 1;
    compactionReportFlush = null;
    clearCompactionReportRetryTimer();
    clearReceiverBaselineRetryTimer();
    clearStartupConnectTimer();
    clearReconnectTimer();
    clearSessionNameCompatibilityTimer();
    restoreParleySessionId();
    rejectReplyWaiter(new Error("Session shutting down"));
    replyTracker.reset();
    outstandingAsks.clear();
    deferredInboundMessages.clear();
    deferredInboundControls.clear();
    pendingHostEnvelopes.clear();
    contextFallbackEnvelopes.clear();
    freshModelContexts.clear();
    if (inboundRetryTimer) clearTimeout(inboundRetryTimer);
    inboundRetryTimer = null;
    agentRunning = false;
    activeTools.clear();
    resetCompactionStatus();
    if (client) {
      await client.disconnect();
      client = null;
    }
    runtimeContext = null;
    currentSessionId = null;
    currentParleySessionId = null;
    currentSessionDescription = undefined;
    profileManagedName = undefined;
    currentAdvertisedName = undefined;
    lastPresenceRequestedName = undefined;
    profileNameMutationTarget = undefined;
    profileOwnershipRevocationPending = false;
    sessionStartedAt = null;
  });
  // Upstream 0.73.1 emits this event but omitted it from ExtensionAPI's event
  // overloads. Compile against the common runtime contract shared by both Pi
  // distributions instead of requiring the fork's broader declaration file.
  const onSessionInfoChanged = pi.on as unknown as (
    event: "session_info_changed",
    handler: (event: SessionInfoChangedEvent, ctx: ExtensionContext) => void,
  ) => void;
  onSessionInfoChanged("session_info_changed", (event, ctx) => {
    if (!currentSessionId || !getLiveContext(ctx)) {
      return;
    }
    observedSessionName = event.name?.trim() || undefined;
    syncPresenceIdentity(currentSessionId);
  });
  pi.on("session_before_compact", (event, ctx) => {
    beginCompactionStatus(ctx, event.signal);
  });
  pi.on("session_compact", (_event, ctx) => {
    finishCompactionStatus(ctx);
    if (getLiveContext(ctx)) {
      const eventId = randomUUID();
      const compactedAt = Date.now();
      pi.appendEntry("parley_compaction_pending", { eventId, compactedAt });
      pendingCompactionReports.set(eventId, compactedAt);
      flushPendingCompactionReports();
    }
  });
  // Earendil Pi 0.85+ reports aborts and failures explicitly. Older hosts only
  // declare before/success, so compaction presence stays disabled there rather
  // than risking stale status after an ordinary provider failure. Registering
  // the additional event remains harmless on those hosts.
  const onSessionCompactFailed = pi.on as unknown as (
    event: "session_compact_failed",
    handler: (event: unknown, ctx: ExtensionContext) => void,
  ) => void;
  onSessionCompactFailed("session_compact_failed", (_event, ctx) => {
    finishCompactionStatus(ctx);
  });
  pi.on("context", (event) => {
    if (!getLiveContext()) return;
    // This is the actual model boundary, including hosts whose asynchronous sendMessage failed.
    // Duplicate host retries are collapsed by actionable message ID, not by identical body text.
    const seen = new Set<string>();
    const messages = event.messages.filter((message) => {
      if (message.role !== "custom") return true;
      if (normalizeEntryType(message.customType) === "parley_persistence_notice") return !conversationPersistenceWarning;
      const key = inboundEnvelopeKey(message);
      if (!key) return true;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    for (const [key, envelope] of pendingHostEnvelopes) {
      if (!seen.has(key)) contextFallbackEnvelopes.set(key, envelope);
      confirmInboundEnvelope(key);
    }
    for (const [key, envelope] of contextFallbackEnvelopes) {
      if (!seen.has(key)) messages.push({ role: "custom", ...envelope, timestamp: Date.now() });
      else contextFallbackEnvelopes.delete(key);
    }
    if (conversationPersistenceWarning) messages.push({
      role: "custom", customType: "parley_persistence_notice", content: conversationPersistenceWarning, display: true, timestamp: Date.now(),
    });
    replyTracker.activateContexts([...freshModelContexts.values()]);
    freshModelContexts.clear();
    return { messages };
  });
  pi.on("agent_start", () => {
    if (!getLiveContext()) {
      return;
    }
    agentRunning = true;
    activeTools.clear();
    syncPresenceStatus();
  });
  pi.on("tool_execution_start", (event) => {
    if (!getLiveContext()) {
      return;
    }
    activeTools.set(event.toolCallId, event.toolName);
    syncPresenceStatus();
  });
  pi.on("tool_execution_end", (event) => {
    if (!getLiveContext()) {
      return;
    }
    activeTools.delete(event.toolCallId);
    syncPresenceStatus();
  });
  pi.on("agent_end", () => {
    if (!getLiveContext()) {
      return;
    }
    // A turn is only one model/tool iteration. End the implicit conversation
    // with the run, so inspecting state does not discard the reply target.
    replyTracker.clearActiveContexts();
    agentRunning = false;
    activeTools.clear();
    syncPresenceStatus();
  });
  pi.on("turn_start", (_event, ctx) => {
    const sessionId = ctx.sessionManager.getSessionId();
    if (!currentSessionId || sessionId !== currentSessionId) {
      if (!config.enabled) {
        return;
      }
      startSessionRuntime(ctx);
      return;
    }
    if (!getLiveContext(ctx)) {
      return;
    }
    syncPresenceIdentity(sessionId);
  });
  pi.on("model_select", (event, ctx) => {
    if (!getLiveContext(ctx)) {
      return;
    }
    currentModel = event.model.id;
    if (client) {
      client.updatePresence({
        ...buildPresenceIdentity(pi, currentParleySessionId ?? ctx.sessionManager.getSessionId()),
        model: event.model.id,
        status: currentStatus(),
      });
    }
  });

  pi.registerMessageRenderer("parley_message", (message, options, theme) => {
    const details = message.details as { from: SessionInfo; message: Message; replyCommand?: string; bodyText?: string } | undefined;
    if (!details) return undefined;
    return new InlineMessageComponent(details.from, details.message, theme, details.replyCommand, details.bodyText, !options.expanded);
  });

  pi.on("tool_result", (event) => {
    if (event.toolName !== PARLEY_TOOL_NAME && event.toolName !== "contact_supervisor") {
      return;
    }
    if (!event.details || typeof event.details !== "object") {
      return;
    }

    const details = event.details as { error?: unknown; delivered?: unknown };
    if (details.error === true || details.delivered === false) {
      return { isError: true };
    }
  });

  const childOrchestratorMetadata = readChildOrchestratorMetadata();
  const nativeSupervisorChannelAvailable = Boolean(process.env[SUBAGENT_SUPERVISOR_CHANNEL_DIR_ENV]?.trim());
  if (childOrchestratorMetadata && !nativeSupervisorChannelAvailable) {
    pi.registerTool(defineTool({
      name: "contact_supervisor",
      label: "Contact Supervisor",
      description: "Conversation with the supervisor who delegated this task. need_decision waits for a reply; interview_request waits for structured answers; progress_update returns a delivery receipt without waiting. Task completion has its own return channel.",
      promptSnippet: "Conversation with the delegating supervisor.",
      parameters: Type.Object({
        reason: StringEnum(["need_decision", "progress_update", "interview_request"] as const, {
          description: "Contact reason: 'need_decision' waits for a reply; 'interview_request' sends structured questions and waits for a reply; 'progress_update' sends a non-blocking update",
        }),
        message: Type.Optional(Type.String({
          description: "Decision request, optional interview note, or meaningful progress update for the supervisor",
        })),
        interview: Type.Optional(Type.Object({
          title: Type.Optional(Type.String()),
          description: Type.Optional(Type.String()),
          questions: Type.Array(Type.Object({
            id: Type.String(),
            type: StringEnum(["single", "multi", "text", "image", "info"] as const, {
              description: "Question type: single, multi, text, image, or info",
            }),
            question: Type.String(),
            options: Type.Optional(Type.Array(Type.Any())),
            context: Type.Optional(Type.String()),
          })),
        }, { description: "Structured interview request for reason='interview_request'" })),
      }),
      async execute(_toolCallId, params, signal, _onUpdate, ctx) {
        const historyWarnings: string[] = [];
        const recordActionEntry = (type: string, data: unknown): void => {
          try { pi.appendEntry(type, data); }
          catch (error) { historyWarnings.push(getErrorMessage(error)); }
        };
        const historyNote = () => historyWarnings.length ? `\nLocal history was not fully persisted; recovery may be incomplete. ${historyWarnings.join("; ")}` : "";
        const reason = params.reason as ContactSupervisorReason;
        if (reason !== "need_decision" && reason !== "progress_update" && reason !== "interview_request") {
          return {
            content: [{ type: "text", text: "Invalid reason. Use 'need_decision', 'interview_request', or 'progress_update'." }],
            details: { error: true },
          };
        }
        if ((reason === "need_decision" || reason === "progress_update") && typeof params.message !== "string") {
          return {
            content: [{ type: "text", text: `Missing 'message' parameter for reason '${reason}'.` }],
            details: { error: true },
          };
        }
        const interviewValidation = reason === "interview_request"
          ? validateSupervisorInterviewRequest(params.interview)
          : undefined;
        if (interviewValidation?.ok === false) {
          return {
            content: [{ type: "text", text: `Invalid interview request: ${interviewValidation.error}` }],
            details: { error: true },
          };
        }
        const supervisorInterview = interviewValidation?.ok === true ? interviewValidation.interview : undefined;

        let connectedClient: ParleyClient;
        try {
          connectedClient = await ensureConnected("tool");
        } catch (error) {
          return {
            content: [{ type: "text", text: `Parley not connected: ${getErrorMessage(error)}` }],
            details: { error: true },
          };
        }

        syncPresenceIdentity(ctx.sessionManager.getSessionId());

        if (signal?.aborted) {
          return {
            content: [{ type: "text", text: "Cancelled" }],
            details: { error: true },
          };
        }

        const metadata = childOrchestratorMetadata;
        let resolvedSupervisor: string | null;
        try {
          resolvedSupervisor = await resolveSupervisorTarget(connectedClient, metadata);
        } catch (error) {
          return {
            content: [{ type: "text", text: `Failed to resolve supervisor target: ${getErrorMessage(error)}` }],
            details: { error: true },
          };
        }
        if (!resolvedSupervisor && reason !== "progress_update") {
          return {
            content: [{ type: "text", text: `Supervisor "${metadata.orchestratorTarget}" is not connected. No question was sent.` }],
            details: { error: true },
          };
        }
        const sendTo = resolvedSupervisor ?? metadata.orchestratorTarget;
        const senderIdentity = currentSendIdentity(connectedClient);
        if (signal?.aborted) {
          return {
            content: [{ type: "text", text: "Cancelled" }],
            details: { error: true },
          };
        }
        if (sendTo === connectedClient.sessionId) {
          return {
            content: [{ type: "text", text: "Cannot message the current session" }],
            details: { error: true },
          };
        }

        if (reason === "progress_update") {
          const message = params.message as string;
          try {
            const result = await connectedClient.send(sendTo, {
              text: formatChildOrchestratorMessage("update", metadata, message),
              completesAsk: false,
            });
            if (!result.delivered) {
              const errorText = result.reason ?? "Session may not exist or has disconnected.";
              return {
                content: [{ type: "text", text: formatDeliveryResult(result, { kind: "Progress update", sender: senderIdentity, target: metadata.orchestratorTarget }) }],
                details: deliveryDetails(result),
              };
            }
            recordActionEntry("parley_sent", {
              to: metadata.orchestratorTarget,
              targetId: result.recipient?.id ?? sendTo,
              as: senderIdentity,
              message: { text: message, reason, completesAsk: false },
              messageId: result.id,
              timestamp: Date.now(),
              subagent: { runId: metadata.runId, agent: metadata.agent, index: metadata.index },
            });
            const awareness = result.peerCompaction
              ? `\n\n${formatPeerCompactionNotice(metadata.orchestratorTarget, result.peerCompaction, sendTo)}`
              : "";
            connectedClient.acknowledgeSendContact(result);
            return {
              content: [{ type: "text", text: formatDeliveryResult(result, { kind: "Progress update", sender: senderIdentity, target: metadata.orchestratorTarget }) + historyNote() + (replyTracker.formatConversationContext() ? `\n\n${replyTracker.formatConversationContext()}` : "") }],
              details: deliveryDetails(result),
            };
          } catch (error) {
            return {
              content: [{ type: "text", text: `Failed to send progress update: ${getErrorMessage(error)}` }],
              details: { error: true },
            };
          }
        }

        if (replyWaiter) {
          return {
            content: [{ type: "text", text: "Already waiting for a reply" }],
            details: { error: true },
          };
        }

        let replyPromise: Promise<Message> | null = null;
        let deliveryState = "created";
        let questionId: string | null = null;
        let requestSendResult: SendResult | undefined;
        try {
          questionId = randomUUID();
          replyPromise = waitForReply(sendTo, questionId, signal, () => connectedClient.cancelAsk(questionId!), () => latestDeliveryState(questionId, deliveryState));
          replyPromise.catch(() => undefined);
          if (signal?.aborted) {
            rejectReplyWaiter(new Error("Cancelled"));
            try {
              await replyPromise;
            } catch {
              // The waiter was intentionally rejected above; the tool result reports cancellation.
            }
            return {
              content: [{ type: "text", text: "Cancelled" }],
              details: { error: true },
            };
          }
          const requestText = reason === "interview_request"
            ? formatChildOrchestratorMessage("interview", metadata, formatSupervisorInterviewRequest(supervisorInterview!, typeof params.message === "string" ? params.message : undefined))
            : formatChildOrchestratorMessage("ask", metadata, params.message as string);
          const sendResult = await connectedClient.send(sendTo, {
            messageId: questionId,
            text: requestText,
            expectsReply: true,
            completesAsk: false,
            senderWaitMode: "blocking",
          });
          requestSendResult = sendResult;
          deliveryState = sendResult.delivery;
          if (!sendResult.delivered) {
            const errorText = sendResult.reason ?? "Session may not exist or has disconnected.";
            rejectOwnedReplyWaiter(questionId, new Error(`Message to "${metadata.orchestratorTarget}" was not delivered: ${errorText}`));
            if (replyPromise) {
              try {
                await replyPromise;
              } catch {
                // The waiter was already rejected above. Keep the delivery failure as the only error here.
              }
            }
            return {
              content: [{ type: "text", text: formatDeliveryResult(sendResult, { kind: "Ask", sender: senderIdentity, target: metadata.orchestratorTarget }) }],
              details: { error: true, ...deliveryDetails(sendResult) },
            };
          }
          recordActionEntry("parley_sent", {
            to: metadata.orchestratorTarget,
            targetId: sendResult.recipient?.id ?? sendTo,
            as: senderIdentity,
            message: {
              text: reason === "interview_request" ? requestText : params.message,
              reason,
              ...(reason === "interview_request" ? { interview: supervisorInterview } : {}),
            },
            messageId: sendResult.id,
            timestamp: Date.now(),
            subagent: { runId: metadata.runId, agent: metadata.agent, index: metadata.index },
          });
          const requestCompaction = sendResult.peerCompaction;
          const replyMessage = await replyPromise;
          const replyText = replyMessage.content.text;
          const replyAttachments = replyMessage.content.attachments?.length
            ? formatAttachments(replyMessage.content.attachments)
            : "";
          const structuredReply = reason === "interview_request" ? parseStructuredSupervisorReply(replyText, supervisorInterview!) : undefined;
          recordActionEntry("parley_received", {
            from: metadata.orchestratorTarget,
            message: { text: replyText, attachments: replyMessage.content.attachments },
            messageId: replyMessage.id,
            timestamp: replyMessage.timestamp,
            subagent: { runId: metadata.runId, agent: metadata.agent, index: metadata.index },
          });
          const awareness = [
            requestCompaction
              ? formatPeerCompactionNotice(metadata.orchestratorTarget, requestCompaction, sendTo)
              : undefined,
            replyMessage.peerCompaction
              ? formatPeerCompactionNotice(metadata.orchestratorTarget, replyMessage.peerCompaction, sendTo)
              : undefined,
          ].filter((notice): notice is string => Boolean(notice));
          connectedClient.acknowledgeSendContact(sendResult);
          acknowledgeInboundMessageContact(connectedClient, replyMessage);
          return {
            content: [{
              type: "text",
              text: `${awareness.length ? `${awareness.join("\n\n")}\n\n` : ""}**Reply from supervisor:**\nQuestion message ID: ${questionId}\nReply message ID: ${replyMessage.id}\n\n${replyText}${replyAttachments}${historyNote()}${structuredReply?.error ? `\n\nThe structured answer could not be validated: ${structuredReply.error}` : ""}`,
            }],
            details: {
              replyMessageId: replyMessage.id,
              ...(structuredReply
                ? structuredReply.value !== undefined
                  ? { structuredReply: structuredReply.value }
                  : { error: true, structuredReplyParseError: structuredReply.error }
                : {}),
              ...(requestCompaction ? { requestPeerCompaction: requestCompaction } : {}),
              ...(replyMessage.peerCompaction ? { replyPeerCompaction: replyMessage.peerCompaction } : {}),
            },
          };
        } catch (error) {
          rejectOwnedReplyWaiter(questionId, toError(error));
          if (replyPromise) {
            try {
              await replyPromise;
            } catch {
              // The waiter is cleanup-only on this path. The real failure is the one from the outer catch.
            }
          }
          const requestAwareness = requestSendResult?.peerCompaction
            ? `\n\n${formatPeerCompactionNotice(metadata.orchestratorTarget, requestSendResult.peerCompaction, sendTo)}`
            : "";
          if (requestSendResult) connectedClient.acknowledgeSendContact(requestSendResult);
          return {
            content: [{ type: "text", text: `Failed: ${getErrorMessage(error)}${requestAwareness}` }],
            details: {
              error: true,
              ...(questionId ? { messageId: questionId, deliveryState: latestDeliveryState(questionId, deliveryState) } : {}),
              ...(requestSendResult?.peerCompaction ? { peerCompaction: requestSendResult.peerCompaction } : {}),
            },
          };
        }
      },
      renderCall(args, theme) {
        const reason = typeof args.reason === "string" ? args.reason : "contact";
        const messagePreview = previewText(args.message, 96);
        const interview = args.interview && typeof args.interview === "object" ? args.interview as { title?: unknown } : undefined;
        let text = theme.fg("toolTitle", theme.bold("contact_supervisor "));
        text += theme.fg(reason === "need_decision" ? "warning" : reason === "progress_update" ? "muted" : "accent", reason);
        if (typeof interview?.title === "string" && interview.title.trim()) {
          text += " " + theme.fg("accent", interview.title.trim());
        }
        if (messagePreview) {
          text += "\n  " + theme.fg("dim", messagePreview);
        }
        return new Text(text, 0, 0);
      },
      renderResult(result, { isPartial }, theme, context) {
        if (isPartial) {
          return new Text(theme.fg("warning", "Waiting for supervisor..."), 0, 0);
        }
        const details = result.details as { delivered?: boolean; error?: boolean; messageId?: string; reason?: string; structuredReplyParseError?: string } | undefined;
        const textContent = firstTextContent(result);
        const failed = Boolean(context.isError || details?.error === true || details?.delivered === false);
        const parseWarning = typeof details?.structuredReplyParseError === "string";
        let text = failed
          ? theme.fg("error", "✗ ")
          : parseWarning
            ? theme.fg("warning", "⚠ ")
            : theme.fg("success", "✓ ");
        text += theme.fg(failed ? "error" : "text", textContent);
        if (parseWarning) {
          text += "\n" + theme.fg("warning", `Structured reply parse issue: ${details.structuredReplyParseError}`);
        }
        return new Text(text, 0, 0);
      },
    }));
  }

  function normalizeToolProfilePlaceholders(profile: SelfProfileUpdate | undefined): SelfProfileUpdate | undefined {
    if (!profile) return undefined;
    const name = typeof profile.name === "string" && profile.name.trim() ? profile.name : undefined;
    const description = profile.description === null
      ? null
      : typeof profile.description === "string" && profile.description.trim()
        ? profile.description
        : undefined;
    if (name === undefined && description === undefined) return undefined;
    return {
      ...(name !== undefined ? { name } : {}),
      ...(description !== undefined ? { description } : {}),
    };
  }

  function applySelfProfile(profile: SelfProfileUpdate | undefined, ctx: ExtensionContext): string | undefined {
    if (!profile) return undefined;
    persistProfileOwnershipRevocation();
    const normalized = normalizeSelfProfileUpdate(profile);
    if (!normalized.ok) return normalized.error;
    if (normalized.profile.name === undefined && normalized.profile.description === undefined) {
      return "profile requires a name, a description, or both.";
    }

    const currentName = pi.getSessionName()?.trim();
    const requestedName = normalized.profile.name;
    const nameChanges = requestedName !== undefined && currentName !== requestedName;
    if (requestedName !== undefined) {
      const effectiveSession = client?.getSelfSession();
      if (
        nameChanges
        && (currentAdvertisedName !== undefined || effectiveSession?.advertised === true)
      ) {
        return `profile.name cannot rename an advertised subagent (current parley name: "${currentAdvertisedName ?? effectiveSession?.name ?? currentName ?? "unknown"}").`;
      }
      if (currentName && nameChanges && currentName !== profileManagedName) {
        return `profile.name cannot replace the explicit session name "${currentName}". Omit it and update only the description.`;
      }
    }

    const nextDescription = normalized.profile.description === undefined
      ? currentSessionDescription
      : normalized.profile.description ?? undefined;
    const descriptionChanges = nextDescription !== currentSessionDescription;
    if (!nameChanges && !descriptionChanges) {
      syncPresenceIdentity(ctx.sessionManager.getSessionId());
      return undefined;
    }

    const nextManagedName = nameChanges
      ? requestedName
      : profileOwnershipRevocationPending
        ? null
        : profileManagedName;
    const profileState = {
      ...(nextManagedName === null
        ? { managedName: null }
        : nextManagedName
          ? { managedName: nextManagedName }
          : {}),
      description: nextDescription ?? null,
      timestamp: Date.now(),
    };

    if (nameChanges && requestedName) {
      const updateId = randomUUID();
      try {
        // Stage durable intent before changing Pi's canonical name. Recovery
        // promotes it only when Pi persisted the same required name.
        pi.appendEntry("parley_profile_pending", {
          ...profileState,
          updateId,
          requiredName: requestedName,
        });
      } catch (error) {
        return `Unable to persist self profile; no changes were applied: ${getErrorMessage(error)}`;
      }
      profileNameMutationTarget = requestedName;
      try {
        pi.setSessionName(requestedName);
      } catch (error) {
        profileNameMutationTarget = undefined;
        try {
          pi.appendEntry("parley_profile_abandoned", { updateId, timestamp: Date.now() });
        } catch {
          // The pending entry is conditional on Pi having persisted the new
          // canonical name, so it remains inert if abandonment cannot journal.
        }
        return `Unable to update profile name: ${getErrorMessage(error)}`;
      }
      profileNameMutationTarget = undefined;
      observedSessionName = requestedName;
      profileManagedName = requestedName;
      currentSessionDescription = nextDescription;
      try {
        pi.appendEntry("parley_profile_updated", { ...profileState, updateId });
        profileOwnershipRevocationPending = false;
      } catch {
        // The synchronously durable pending entry plus Pi's matching canonical
        // name is sufficient for restart recovery; the commit is a cleanup aid.
      }
    } else {
      try {
        // Description-only changes are journaled before publication so a throw
        // cannot leave an undurable focus visible to peers.
        pi.appendEntry("parley_profile_updated", profileState);
        profileOwnershipRevocationPending = false;
      } catch (error) {
        return `Unable to persist self profile; no changes were applied: ${getErrorMessage(error)}`;
      }
      currentSessionDescription = nextDescription;
    }

    syncPresenceIdentity(ctx.sessionManager.getSessionId());
    return undefined;
  }

  function currentSelfProfile(): {
    name: string;
    description?: string;
    parleyName?: string;
    descriptionPublished: boolean;
  } {
    const requestedName = pi.getSessionName()?.trim() || observedSessionName;
    const fallbackId = currentParleySessionId ?? currentSessionId;
    const name = requestedName || (fallbackId ? resolveParleyPresenceName(undefined, fallbackId) : "unnamed");
    const effectiveName = currentAdvertisedName ?? client?.getSelfSession()?.name;
    return {
      name,
      ...(currentSessionDescription ? { description: currentSessionDescription } : {}),
      ...(effectiveName ? { parleyName: effectiveName } : {}),
      descriptionPublished: currentSessionDescription === undefined || client?.supportsFeature(SESSION_PROFILE_FEATURE) === true,
    };
  }

  function attachSelfProfile(result: AgentToolResult<unknown>, profileUpdated = false): AgentToolResult<unknown> {
    const profile = currentSelfProfile();
    const existingDetails = typeof result.details === "object" && result.details !== null && !Array.isArray(result.details)
      ? result.details as Record<string, unknown>
      : {};
    const identityAlreadyShown = !profileUpdated && existingDetails.delivered === true
      && existingDetails.senderIdentity === profile.name && profile.parleyName === profile.name
      && profile.descriptionPublished;
    const parleyProjection = profile.parleyName && profile.parleyName !== profile.name
      ? ` (parley: ${profile.parleyName})`
      : profile.parleyName
        ? ""
        : " (parley name awaiting broker confirmation)";
    const publication = profile.description && !profile.descriptionPublished
      ? " [description local only: broker upgrade required]"
      : "";
    if (!identityAlreadyShown) result.content.push({
      type: "text",
      text: `Self profile: ${profile.name}${parleyProjection}${profile.description ? ` — ${profile.description}` : ""}${publication}`,
    });
    result.details = { ...existingDetails, selfProfile: profile };
    return result;
  }

  function formatOutstandingAsks(limit = 3): string {
    const entries = [...outstandingAsks.entries()].sort((a, b) => a[1].sentAt - b[1].sentAt);
    if (!entries.length) return "";
    const now = Date.now();
    const lines = entries.slice(0, limit).map(([id, ask]) => {
      const ageMs = Math.max(0, now - ask.sentAt);
      const age = ageMs < 60_000 ? `${Math.round(ageMs / 1000)}s` : `${Math.round(ageMs / 60_000)}m`;
      const elapsed = ageMs > askTimeoutMs ? "; local wait window elapsed" : "";
      return `- ${ask.targetDisplay} · messageId ${id} · ${age}${elapsed} · ${ask.preview}`;
    });
    if (entries.length > limit) lines.push(`… ${entries.length - limit} more outstanding questions.`);
    return `Outstanding asks (${entries.length}, local tracking):\n${lines.join("\n")}`;
  }

  pi.registerTool(defineTool({
    name: PARLEY_TOOL_NAME,
    label: "Parley",
    description: `Conversation with other Pi sessions visible in the current scope.

• list / list-cwd: Connected peers, their identity, focus, location, and activity.
• send: A notification to one peer or an explicit group; it does not answer a pending question.
• ask: A question whose answer returns here. Waits by default; blocking: false delivers the answer later in the conversation.
• reply: Responds to a message, answering its question when applicable.
• pending / read: Unanswered questions, or a retained message's full content.
• status: Connection state and outstanding questions.
• cancel: Removes offline mail or communicates withdrawal; work already done is unchanged.
• broadcast: Independent messages to visible local peers, not remote peers.
• advertise: Subagent public visibility within the current scope.
• rename: Changes this session's name.

Receipts include sender/recipient identity, exact message IDs, delivery state, and nearby conversation context. Endpoint acceptance is not an acknowledgement from the colleague.`,
    promptSnippet: "Communicate with other Pi sessions.",
    promptGuidelines: [
      "Parley messages can wake colleagues; broadcasts reach every visible local peer.",
    ],

    parameters: Type.Object({
      action: StringEnum(["list", "list-cwd", "send", "broadcast", "ask", "reply", "pending", "status", "cancel", "advertise", "rename", "read"] as const, {
        description: "Parley operation.",
      }),
      profile: Type.Optional(Type.Object({
        name: Type.Optional(Type.String({
          description: "Name for an unnamed or profile-managed session; does not replace a user-set name.",
        })),
        description: Type.Optional(Type.Union([
          Type.String(),
          Type.Null(),
        ], {
          description: "Current focus (5-9 words), or null to clear. Display metadata, not routing identity.",
        })),
      }, {
        description: "Optional self-profile update.",
      })),
      to: Type.Optional(Type.String({
        description: "One target session: name, full session ID, or the short id shown in parentheses by 'list' (a leading ID prefix resolves). For send/ask with cwd, omit to target the sole live session in that cwd or the newly opened project-pane session. For 'reply', disambiguates the pending ask.",
      })),
      targets: Type.Optional(Type.Array(Type.String(), {
        minItems: 1,
        maxItems: MAX_EXPLICIT_SEND_TARGETS,
        description: "For 'send', several explicit target names or IDs. Each receives an independent message and outcome. Cannot be combined with 'to', cwd targeting, replyTo, supersedes, or retryOf.",
      })),
      message: Type.Optional(Type.String({
        description: "Message to send (for 'send', 'broadcast', 'ask', or 'reply' action)",
      })),
      attachments: Type.Optional(Type.Array(Type.Object({
        type: StringEnum(["file", "snippet", "context"] as const),
        name: Type.String(),
        content: Type.String(),
        language: Type.Optional(Type.String()),
      }), { description: "Inline text snapshots; no files are created in the receiving workspace." })),
      replyTo: Type.Optional(Type.String({
        description: "Message ID to reply to (for threading or responding to an 'ask')",
      })),
      messageId: Type.Optional(Type.String({
        description: "Exact message ID for read or cancel; session-ID prefix matching does not apply.",
      })),
      supersedes: Type.Optional(Type.String({
        description: "Previous message ID this send/ask explicitly supersedes. Only works for the same sender and receiver.",
      })),
      retryOf: Type.Optional(Type.String({
        description: "Previous message ID this send/ask is a user-authored retry of. Retries always send a new message ID.",
      })),
      cwd: Type.Optional(Type.String({
        description: "Working directory filter for 'list-cwd'. For send/ask, scopes target lookup to that directory; omit 'to' to target the sole live peer there. Absolute, or relative to the current session's cwd; '.' means the current cwd.",
      })),
      blocking: Type.Optional(Type.Boolean({
        description: "For 'ask', true waits for the answer in this tool call (default); false returns immediately and delivers the answer as an incoming message.",
      })),
      openProjectPaneIfMissing: Type.Optional(Type.Boolean({
        description: "For send/ask with cwd, launch Pi in that project through a registered generic project launcher when no matching live session exists.",
      })),
      focus: Type.Optional(Type.Boolean({
        description: "For openProjectPaneIfMissing, focus the new terminal when the launcher supports it. Defaults to true.",
      })),
      name: Type.Optional(Type.String({
        description: "For 'advertise': the public name this subagent claims. For 'rename': the new canonical name for this session.",
      })),
    }),

    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      let sendIdentity: string | undefined;
      const historyWarnings: string[] = [];
      const recordActionEntry = (type: string, data: unknown): void => {
        try { pi.appendEntry(type, data); }
        catch (error) { historyWarnings.push(`${type}: ${getErrorMessage(error)}`); }
      };
      const settleSupersededQuestion = (): void => {
        if (!params.supersedes) return;
        outstandingAsks.delete(params.supersedes);
        rejectOwnedReplyWaiter(params.supersedes, new Error(`Request ${params.supersedes} was superseded.`));
        recordActionEntry("parley_ask_settled", { messageId: params.supersedes, reason: "superseded", timestamp: Date.now() });
      };
      const toolResult = await (async (): Promise<AgentToolResult<unknown>> => {
      const profile = normalizeToolProfilePlaceholders(params.profile);
      const profileError = applySelfProfile(profile, ctx);
      if (profileError) {
        return {
          content: [{ type: "text" as const, text: `Action ${params.action} was not performed. ${profileError}` }],
          details: { error: true },
        };
      }

      let connectedClient: ParleyClient;
      try {
        connectedClient = await ensureConnected("tool");
      } catch (error) {
        return {
          content: [{ type: "text", text: `Parley not connected: ${getErrorMessage(error)}` }],
          details: { error: true },
        };
      }

      syncPresenceIdentity(ctx.sessionManager.getSessionId());
      const requestedPresenceName = buildPresenceIdentity(
        pi,
        currentParleySessionId ?? ctx.sessionManager.getSessionId(),
      ).name;
      const selfProjection = connectedClient.getSelfSession();
      const projectionMayChange = currentAdvertisedName === undefined
        && (selfProjection === undefined || selfProjection.name !== requestedPresenceName);
      if (profile?.name !== undefined || projectionMayChange) {
        try {
          // Presence and list share one ordered local socket. The response gives
          // us the broker-owned collision-resolved name without guessing it and
          // observes later collision self-healing before metadata is returned.
          await connectedClient.listSessions({ timeoutMs: 1_000 });
        } catch {
          // The action can still proceed. Result metadata distinguishes a
          // missing effective projection from the canonical Pi name.
        }
      }

      const captureSendIdentity = (): void => {
        sendIdentity = currentSendIdentity(connectedClient);
        _onUpdate?.({ content: [{ type: "text", text: `Communicating as ${sendIdentity}…` }], details: { senderIdentity: sendIdentity } });
      };
      if (["send", "ask", "reply", "broadcast"].includes(params.action)) captureSendIdentity();
      const {
        action,
        to,
        message,
        attachments,
        replyTo,
        messageId,
        supersedes,
        retryOf,
        cwd,
        openProjectPaneIfMissing,
        focus,
        blocking,
        name,
      } = params;
      // Some tool-schema adapters materialize optional arrays as [""]. Treat
      // an all-blank placeholder as omitted while preserving errors for mixed
      // real/blank recipient lists.
      const allBlankTargets = params.targets?.every((target) => !target.trim()) ?? false;
      // Some adapters also duplicate the recipient into both optional fields;
      // a single target identical to `to` is the same singular delivery
      // intent (multicast already delivers same-session aliases once), so it
      // is treated as `to` alone. Genuinely different or multi-element lists
      // still conflict.
      const duplicatedSingularTarget = params.targets !== undefined
        && to !== undefined
        && params.targets.length === 1
        && params.targets[0]?.trim() === to.trim();
      const targets = params.targets === undefined || allBlankTargets || duplicatedSingularTarget
        ? undefined
        : params.targets;

      if (messageId && action !== "cancel" && action !== "read") {
        return {
          content: [{ type: "text", text: "messageId identifies a retained message for read or cancel; sends and asks create a new message ID." }],
          details: { error: true },
        };
      }

      switch (action) {
        case "advertise": {
          const requestedName = name?.trim();
          if (!requestedName) {
            return {
              content: [{ type: "text", text: "advertise requires a non-empty 'name'." }],
              details: { error: true },
            };
          }

          const metadata = readChildOrchestratorMetadata();
          let result;
          try {
            result = await connectedClient.advertise(requestedName);
          } catch (error) {
            return {
              content: [{ type: "text", text: `Advertise failed: ${getErrorMessage(error)}` }],
              details: { error: true },
            };
          }

          if (!result.ok) {
            return {
              content: [{ type: "text", text: `Advertise failed: ${result.error ?? "unknown error"}` }],
              details: { error: true },
            };
          }
          currentAdvertisedName = result.name;

          // Best-effort, non-blocking notice to the supervisor -- same ordinary
          // message pipeline as any other parley send, delivered exactly once
          // per session regardless of how many times this child re-advertises
          // under a different name afterward.
          if (metadata && !hasNotifiedSupervisorOfAdvertise) {
            hasNotifiedSupervisorOfAdvertise = true;
            try {
              const resolvedSupervisor = await resolveSupervisorTarget(connectedClient, metadata);
              const sendTarget = resolvedSupervisor ?? metadata.orchestratorTarget;
              const advertiseNotice = await connectedClient.send(sendTarget, {
                text: formatChildOrchestratorMessage(
                  "update",
                  metadata,
                  `This subagent has advertised itself as "${result.name}" and is now fully visible to and reachable by every session on the parley mesh, not just you.`,
                ),
                expectsReply: false,
              });
              if (advertiseNotice.delivered) {
                surfaceBackgroundPeerCompaction(connectedClient, metadata.orchestratorTarget, advertiseNotice, runtimeGeneration, sendTarget);
              }
            } catch {
              // Best-effort only -- a failed notice must never fail the advertise
              // call itself; the promotion already succeeded on the broker.
            }
          }

          return {
            content: [{ type: "text", text: `Advertised as "${result.name}" within this session's scope.` }],
            details: {},
          };
        }

        case "list": {
          try {
            const mySessionId = connectedClient.sessionId;
            const sessions = await connectedClient.listSessions();
            const currentSession = sessions.find(s => s.id === mySessionId);
            const otherSessions = sessions.filter(s => s.id !== mySessionId);

            if (!currentSession) {
              return {
                content: [{ type: "text", text: "Current session is missing from parley session list." }],
                details: { error: true },
              };
            }

            const prefixes = sessionIdPrefixes(sessions);
            const currentSection = `**Current session:**\n${formatSessionListRow(currentSession, currentSession.cwd, true, prefixes.get(currentSession.id)!)}`;
            const otherSection = otherSessions.length === 0
              ? "**Other sessions:**\nNo other sessions connected."
              : `**Other sessions:**\n${otherSessions.map((session) => formatSessionListRow(session, currentSession.cwd, false, prefixes.get(session.id)!)).join("\n")}`;

            return {
              content: [{ type: "text", text: `${currentSection}\n\n${otherSection}` }],
              details: {},
            };
          } catch (error) {
            return {
              content: [{ type: "text", text: `Failed to list sessions: ${getErrorMessage(error)}` }],
              details: { error: true },
            };
          }
        }

        case "list-cwd": {
          try {
            const mySessionId = connectedClient.sessionId;
            const sessions = await connectedClient.listSessions();
            const currentSession = sessions.find(s => s.id === mySessionId);

            if (!currentSession) {
              return {
                content: [{ type: "text", text: "Current session is missing from parley session list." }],
                details: { error: true },
              };
            }

            // Default to the current session's cwd; an explicit `cwd` overrides
            // (relative paths resolved against it, "." meaning the current cwd).
            const filterCwd = cwd && cwd !== "."
              ? resolvePath(currentSession.cwd, cwd)
              : currentSession.cwd;

            const otherSessions = sessions.filter(
              s => s.id !== mySessionId && sameCwd(s.cwd, filterCwd),
            );

            // Fail loud: filtering by a directory with no peers while the
            // session's OWN cwd has some otherwise reads as a misleading empty
            // result (common when a caller passes a guessed parent cwd).
            let emptyNote = "No other sessions in this directory.";
            if (otherSessions.length === 0 && !sameCwd(filterCwd, currentSession.cwd)) {
              const here = sessions.filter(
                s => s.id !== mySessionId && sameCwd(s.cwd, currentSession.cwd),
              ).length;
              if (here > 0) {
                emptyNote += ` Your session's cwd is ${currentSession.cwd} (${here} peer${here === 1 ? "" : "s"} there) — call list-cwd without a cwd argument to list them.`;
              }
            }

            const prefixes = sessionIdPrefixes(sessions);
            const currentSection = `**Current session:**\n${formatSessionListRow(currentSession, currentSession.cwd, true, prefixes.get(currentSession.id)!)}`;
            const otherSection = otherSessions.length === 0
              ? `**Other sessions (cwd: ${filterCwd}):**\n${emptyNote}`
              : `**Other sessions (cwd: ${filterCwd}):**\n${otherSessions.map((session) => formatSessionListRow(session, currentSession.cwd, false, prefixes.get(session.id)!)).join("\n")}`;

            return {
              content: [{ type: "text", text: `${currentSection}\n\n${otherSection}` }],
              details: {},
            };
          } catch (error) {
            return {
              content: [{ type: "text", text: `Failed to list sessions: ${getErrorMessage(error)}` }],
              details: { error: true },
            };
          }
        }

        case "cancel": {
          if (!messageId) {
            return {
              content: [{ type: "text", text: "Missing 'messageId' parameter" }],
              details: { error: true },
            };
          }
          try {
            const result = await connectedClient.cancelMessage(messageId);
            if (result.delivered && result.outcomeKnown) {
              outstandingAsks.delete(messageId);
              rejectOwnedReplyWaiter(messageId, new Error(`Request ${messageId} was withdrawn.`));
              recordActionEntry("parley_ask_settled", { messageId, reason: "withdrawn", timestamp: Date.now() });
            }
            return {
              content: [{ type: "text", text: formatCancellationResult(result) }],
              details: deliveryDetails(result),
            };
          } catch (error) {
            return {
              content: [{ type: "text", text: `Failed to cancel message: ${getErrorMessage(error)}` }],
              details: { error: true, messageId },
            };
          }
        }

        case "rename": {
          const requestedName = name?.trim();
          if (!requestedName) {
            return {
              content: [{ type: "text", text: "rename requires a non-empty 'name'." }],
              details: { error: true },
            };
          }
          if (!isValidSessionName(requestedName)) {
            return {
              content: [{ type: "text", text: `"${requestedName}" cannot be used as a session name.` }],
              details: { error: true },
            };
          }
          try {
            pi.setSessionName(requestedName);
          } catch (error) {
            return {
              content: [{ type: "text", text: `Unable to set the session name: ${getErrorMessage(error)}` }],
              details: { error: true },
            };
          }
          // session_info_changed is the canonical identity contract; this
          // direct push is an idempotent fast path so the broker roster shows
          // the new name before the call returns.
          syncPresenceIdentity(ctx.sessionManager.getSessionId());
          let publication = "";
          try { await connectedClient.listSessions({ timeoutMs: 1_000 }); }
          catch { publication = " Broker publication has not been confirmed."; }
          return {
            content: [{ type: "text", text: `Session name set to "${requestedName}".${publication}` }],
            details: {},
          };
        }

        case "broadcast": {
          if (!message) {
            return {
              content: [{ type: "text", text: "Missing 'message' parameter" }],
              details: { error: true },
            };
          }
          if (to || targets || cwd || openProjectPaneIfMissing) {
            return {
              content: [{ type: "text", text: "broadcast does not accept 'to', 'targets', 'cwd', or openProjectPaneIfMissing; it uses the current visible live-session roster." }],
              details: { error: true },
            };
          }
          if (replyTo || supersedes || retryOf) {
            return {
              content: [{ type: "text", text: "broadcast cannot use replyTo, supersedes, or retryOf because each recipient receives an independent message." }],
              details: { error: true },
            };
          }
          try {
            const sessions = await connectedClient.listSessions();
            const currentSessionId = connectedClient.sessionId;
            const prefixes = sessionIdPrefixes(sessions);
            const recipients: BatchDeliveryTarget[] = sessions
              .filter((session) => session.id !== currentSessionId && !session.federation)
              .sort((left, right) => left.id.localeCompare(right.id))
              .map((session) => ({
                requested: session.id,
                label: session.name
                  ? `${session.name} (${prefixes.get(session.id) ?? session.id.slice(0, 8)})`
                  : prefixes.get(session.id) ?? session.id,
                session,
              }));
            if (recipients.length === 0) {
              return {
                content: [{ type: "text", text: "No other visible local sessions are connected; nothing was broadcast." }],
                details: { error: true, broadcast: true, recipientCount: 0 },
              };
            }
            const attachmentText = attachments?.length ? formatAttachments(attachments) : "";
            if (config.confirmSend && ctx.hasUI) {
              const confirmed = await ctx.ui.confirm(
                "Broadcast message",
                `Broadcast to ${recipients.length} visible live session${recipients.length === 1 ? "" : "s"}:\n\n${message}${attachmentText}`,
              );
              if (!confirmed) {
                return {
                  content: [{ type: "text", text: "Broadcast cancelled by user" }],
                  details: {},
                };
              }
            }
            const batchId = randomUUID();
            const outcomes = await sendBatchMessages(connectedClient, recipients, {
              message,
              attachments,
              batchId,
              broadcast: true,
              signal: _signal,
            });
            return batchSendToolResult({
              batchId,
              outcomes,
              requestedTargetCount: recipients.length,
              duplicateCount: 0,
              broadcast: true,
              sender: sendIdentity!,
              excludedRemoteCount: sessions.filter((session) => Boolean(session.federation)).length,
            });
          } catch (error) {
            return {
              content: [{ type: "text", text: `Failed to broadcast: ${getErrorMessage(error)}` }],
              details: { error: true },
            };
          }
        }

        case "send": {
          if (to && targets) {
            return {
              content: [{ type: "text", text: "Use either 'to' or 'targets' for send, not both." }],
              details: { error: true },
            };
          }
          if (targets) {
            if (!message) {
              return {
                content: [{ type: "text", text: "Missing 'message' parameter" }],
                details: { error: true },
              };
            }
            if (cwd || openProjectPaneIfMissing) {
              return {
                content: [{ type: "text", text: "Multi-target send does not accept cwd or openProjectPaneIfMissing; identify each live session explicitly." }],
                details: { error: true },
              };
            }
            if (replyTo || supersedes || retryOf) {
              return {
                content: [{ type: "text", text: "Multi-target send cannot use replyTo, supersedes, or retryOf because each recipient receives an independent message." }],
                details: { error: true },
              };
            }
            if (targets.length === 0 || targets.length > MAX_EXPLICIT_SEND_TARGETS || targets.some((target) => !target.trim())) {
              return {
                content: [{ type: "text", text: `targets must contain 1-${MAX_EXPLICIT_SEND_TARGETS} non-empty session names or IDs.` }],
                details: { error: true },
              };
            }
            try {
              const requestedTargets = targets.map((target) => target.trim());
              const sessions = await connectedClient.listSessions();
              const recipients: BatchDeliveryTarget[] = [];
              const seenSessionIds = new Set<string>();
              const seenUnresolved = new Set<string>();
              let duplicateCount = 0;
              for (const requested of requestedTargets) {
                try {
                  const session = resolveSessionFromRoster(sessions, requested);
                  if (session) {
                    if (seenSessionIds.has(session.id)) {
                      duplicateCount += 1;
                      continue;
                    }
                    seenSessionIds.add(session.id);
                    recipients.push({
                      requested,
                      label: requested,
                      session,
                      ...(session.id === connectedClient.sessionId
                        ? { resolutionError: "Cannot message the current session", resolutionCode: "E_SELF_TARGET" }
                        : {}),
                    });
                    continue;
                  }
                  if (seenUnresolved.has(requested)) {
                    duplicateCount += 1;
                    continue;
                  }
                  seenUnresolved.add(requested);
                  recipients.push({ requested, label: requested });
                } catch (error) {
                  recipients.push({
                    requested,
                    label: requested,
                    resolutionError: getErrorMessage(error),
                    resolutionCode: "E_TARGET_RESOLUTION",
                  });
                }
              }

              let confirmedSnapshot = false;
              if (config.confirmSend && ctx.hasUI) {
                const attachmentText = attachments?.length ? formatAttachments(attachments) : "";
                const recipientLines = recipients.map((recipient) => {
                  if (recipient.session) {
                    const name = recipient.session.name ? `${recipient.session.name} ` : "";
                    return `- ${recipient.requested} → ${name}(${recipient.session.id})`;
                  }
                  if (recipient.resolutionError) {
                    return `- ${recipient.requested} → will fail: ${recipient.resolutionError}`;
                  }
                  return `- ${recipient.requested} → not currently live; ordinary offline delivery may apply`;
                });
                if (duplicateCount > 0) {
                  recipientLines.push(`- ${duplicateCount} duplicate target${duplicateCount === 1 ? "" : "s"} omitted`);
                }
                const confirmed = await ctx.ui.confirm(
                  "Send message",
                  `Send independently to this resolved recipient snapshot:\n${recipientLines.join("\n")}\n\n${message}${attachmentText}`,
                );
                if (!confirmed) {
                  return {
                    content: [{ type: "text", text: "Message cancelled by user" }],
                    details: {},
                  };
                }
                confirmedSnapshot = true;
              }

              const batchId = randomUUID();
              const outcomes = await sendBatchMessages(connectedClient, recipients, {
                message,
                attachments,
                batchId,
                broadcast: false,
                signal: _signal,
                allowRosterMailboxFallback: !confirmedSnapshot,
              });
              return batchSendToolResult({
                batchId,
                outcomes,
                requestedTargetCount: requestedTargets.length,
                duplicateCount,
                broadcast: false,
                sender: sendIdentity!,
              });
            } catch (error) {
              return {
                content: [{ type: "text", text: `Failed to send: ${getErrorMessage(error)}` }],
                details: { error: true },
              };
            }
          }
          if ((!to && !cwd) || !message) {
            return {
              content: [{ type: "text", text: "Missing 'to' or 'cwd', or missing 'message' parameter" }],
              details: { error: true },
            };
          }
          try {
            if (openProjectPaneIfMissing && !cwd) {
              return {
                content: [{ type: "text", text: "openProjectPaneIfMissing requires a target cwd." }],
                details: { error: true },
              };
            }
            const confirmSend = !replyTo && config.confirmSend && ctx.hasUI;
            const attachmentText = attachments?.length ? formatAttachments(attachments) : "";
            if (confirmSend && cwd && openProjectPaneIfMissing) {
              const confirmed = await ctx.ui.confirm(
                "Send message",
                `Send to "${to ?? cwd}":\n\n${message}${attachmentText}`,
              );
              if (!confirmed) {
                return {
                  content: [{ type: "text", text: "Message cancelled by user" }],
                  details: {},
                };
              }
            }
            const target: DeliveryTarget = cwd
              ? await resolveCwdDeliveryTarget(connectedClient, { to, cwd, openProjectPaneIfMissing, focus, signal: _signal })
              : { id: await resolveSessionTarget(connectedClient, to!) ?? to!, label: to! };
            const sendTo = target.id;
            const targetDisplay = target.projectPane ? target.label : to ?? target.label;
            if (sendTo === connectedClient.sessionId) {
              return {
                content: [{ type: "text", text: "Cannot message the current session" }],
                details: { error: true },
              };
            }
            if (confirmSend && !(cwd && openProjectPaneIfMissing)) {
              const confirmed = await ctx.ui.confirm(
                "Send message",
                `Send to "${targetDisplay}":\n\n${message}${attachmentText}`,
              );
              if (!confirmed) {
                return {
                  content: [{ type: "text", text: "Message cancelled by user" }],
                  details: {},
                };
              }
            }
            captureSendIdentity();
            const result = await connectedClient.send(sendTo, {
              text: message,
              attachments,
              replyTo,
              completesAsk: false,
              supersedes,
              retryOf,
              signal: _signal,
              contactKind: "direct",
            });
            if (!result.delivered) {
              return {
                content: [{ type: "text", text: formatDeliveryResult(result, { kind: "Message", sender: sendIdentity!, target: targetDisplay }) + projectObservation(target) }],
                details: { ...deliveryDetails(result), ...(target.projectPane ? { projectLaunch: target.projectPane } : {}) },
              };
            }
            settleSupersededQuestion();
            recordActionEntry("parley_sent", {
              to: targetDisplay,
              targetId: result.recipient?.id ?? sendTo,
              as: sendIdentity!,
              toolCallId: _toolCallId,
              message: { text: message, attachments, replyTo, completesAsk: false, supersedes, retryOf },
              messageId: result.id,
              ...(result.peerCompaction ? { peerCompaction: result.peerCompaction } : {}),
              timestamp: Date.now(),
            });
            const awarenessText = formatDeliveryResult(result, { kind: "Message", sender: sendIdentity!, target: targetDisplay }) + projectObservation(target);
            connectedClient.acknowledgeSendContact(result);
            return {
              content: [{
                type: "text",
                text: awarenessText,
              }],
              details: {
                ...deliveryDetails(result),
                ...(replyTo ? { replyTo } : {}),
                ...(target.projectPane ? { openedProjectPane: true, projectRoot: target.projectPane.projectRoot, projectLauncher: target.projectPane.provider.kind === "session" ? target.projectPane.provider.name : "configured-command" } : {}),
              },
            };
          } catch (error) {
            return {
              content: [{ type: "text", text: `Failed to send: ${getErrorMessage(error)}` }],
              details: { error: true },
            };
          }
        }

        case "ask": {
          if (targets) {
            return {
              content: [{ type: "text", text: "ask accepts one recipient through 'to' or cwd targeting; use separate asks when each recipient needs to reply." }],
              details: { error: true },
            };
          }
          if ((!to && !cwd) || !message) {
            return {
              content: [{ type: "text", text: "Missing 'to' or 'cwd', or missing 'message' parameter" }],
              details: { error: true },
            };
          }

          if (replyWaiter && blocking !== false) {
            return {
              content: [{ type: "text", text: "Already waiting for a reply" }],
              details: { error: true },
            };
          }

          if (_signal?.aborted) {
            return {
              content: [{ type: "text", text: "Cancelled" }],
              details: { error: true },
            };
          }
          let replyPromise: Promise<Message> | null = null;
          let deliveryState = "created";
          let questionId: string | null = null;
          let questionCompaction: PeerCompactionNotice | undefined;
          let questionSendResult: SendResult | undefined;
          let questionTargetDisplay = to ?? cwd ?? "peer";
          let questionTargetId: string | undefined;
          const rememberQuestion = (id: string, target: string, label: string): void => {
            const sentAt = Date.now();
            outstandingAsks.set(id, { to: target, targetDisplay: label, preview: previewText(message, 120) ?? message, sentAt, message: { text: message, attachments } });
            recordActionEntry("parley_ask_pending", { messageId: id, to: target, targetDisplay: label, message: { text: message, attachments }, sentAt });
          };
          const settleQuestion = (id: string, reason: string): void => {
            outstandingAsks.delete(id);
            recordActionEntry("parley_ask_settled", { messageId: id, reason, timestamp: Date.now() });
          };

          try {
            if (openProjectPaneIfMissing && !cwd) {
              return {
                content: [{ type: "text", text: "openProjectPaneIfMissing requires a target cwd." }],
                details: { error: true },
              };
            }
            let target: DeliveryTarget;
            if (cwd) {
              target = await resolveCwdDeliveryTarget(connectedClient, { to, cwd, openProjectPaneIfMissing, focus, signal: _signal });
            } else {
              const resolved = await resolveSessionTarget(connectedClient, to!);
              if (!resolved) {
                return {
                  content: [{ type: "text", text: `Session "${to}" is not currently connected. Questions require a connected peer; no question was sent.` }],
                  details: { error: true },
                };
              }
              target = { id: resolved, label: to! };
            }
            const sendTo = target.id;
            const targetDisplay = target.projectPane ? target.label : to ?? target.label;
            questionTargetDisplay = targetDisplay;
            questionTargetId = sendTo;
            if (_signal?.aborted) {
              return {
                content: [{ type: "text", text: "Cancelled" }],
                details: { error: true },
              };
            }
            if (sendTo === connectedClient.sessionId) {
              return {
                content: [{ type: "text", text: "Cannot message the current session" }],
                details: { error: true },
              };
            }
            if (replyWaiter && blocking !== false) {
              return {
                content: [{ type: "text", text: "Already waiting for a reply" }],
                details: { error: true },
              };
            }
            if (blocking === false) {
              const askId = randomUUID();
              rememberQuestion(askId, sendTo, targetDisplay);
              captureSendIdentity();
              const sendResult = await connectedClient.send(sendTo, {
                messageId: askId,
                text: message,
                attachments,
                replyTo,
                expectsReply: true,
                completesAsk: false,
                senderWaitMode: "nonblocking",
                supersedes,
                retryOf,
                signal: _signal,
                contactKind: "direct",
              });
              if (!sendResult.delivered) {
                if (sendResult.outcomeKnown) {
                  settleQuestion(askId, "not-delivered");
                }
                return {
                  content: [{ type: "text", text: formatDeliveryResult(sendResult, { kind: "Ask", sender: sendIdentity!, target: targetDisplay }) + projectObservation(target) }],
                  details: { error: true, ...deliveryDetails(sendResult) },
                };
              }
              settleSupersededQuestion();
              recordActionEntry("parley_sent", {
                to: targetDisplay,
                targetId: sendResult.recipient?.id ?? sendTo,
                as: sendIdentity!,
                toolCallId: _toolCallId,
                message: { text: message, attachments, replyTo, completesAsk: false, supersedes, retryOf },
                messageId: sendResult.id,
                ...(sendResult.peerCompaction ? { peerCompaction: sendResult.peerCompaction } : {}),
                timestamp: Date.now(),
              });
              connectedClient.acknowledgeSendContact(sendResult);
              return {
                content: [{
                  type: "text",
                  text: `${formatDeliveryResult(sendResult, { kind: "Ask", sender: sendIdentity!, target: targetDisplay }) + projectObservation(target)}\nNon-blocking: the answer arrives in this conversation.`,
                }],
                details: {
                  nonBlocking: true,
                  messageId: askId,
                  ...deliveryDetails(sendResult),
                  ...(target.projectPane ? { openedProjectPane: true, projectRoot: target.projectPane.projectRoot, projectLauncher: target.projectPane.provider.kind === "session" ? target.projectPane.provider.name : "configured-command" } : {}),
                },
              };
            }
            questionId = randomUUID();
            rememberQuestion(questionId, sendTo, targetDisplay);
            replyPromise = waitForReply(sendTo, questionId, _signal, () => connectedClient.cancelAsk(questionId!), () => latestDeliveryState(questionId, deliveryState));
            replyPromise.catch(() => undefined);
            captureSendIdentity();
            const sendResult = await connectedClient.send(sendTo, {
              messageId: questionId,
              text: message,
              attachments,
              replyTo,
              expectsReply: true,
              completesAsk: false,
              senderWaitMode: "blocking",
              supersedes,
              retryOf,
              signal: _signal,
              contactKind: "direct",
            });

            questionSendResult = sendResult;
            deliveryState = sendResult.delivery;
            questionCompaction = sendResult.peerCompaction;
            if (!sendResult.delivered) {
              if (sendResult.outcomeKnown) settleQuestion(questionId, "not-delivered");
              const errorText = sendResult.reason ?? "Session may not exist or has disconnected.";
              rejectOwnedReplyWaiter(questionId, new Error(`Message to "${targetDisplay}" was not delivered: ${errorText}`));
              if (replyPromise) {
                try {
                  await replyPromise;
                } catch {
                  // The waiter was already rejected above. Keep the delivery failure as the only error here.
                }
              }
              return {
                content: [{ type: "text", text: formatDeliveryResult(sendResult, { kind: "Ask", sender: sendIdentity!, target: targetDisplay }) + projectObservation(target) }],
                details: { error: true, ...deliveryDetails(sendResult) },
              };
            }
            settleSupersededQuestion();
            recordActionEntry("parley_sent", {
              to: targetDisplay,
              targetId: sendResult.recipient?.id ?? sendTo,
              as: sendIdentity!,
              toolCallId: _toolCallId,
              message: { text: message, attachments, replyTo, completesAsk: false, supersedes, retryOf },
              messageId: sendResult.id,
              ...(sendResult.peerCompaction ? { peerCompaction: sendResult.peerCompaction } : {}),
              timestamp: Date.now(),
            });
            const replyMessage = await replyPromise;
            settleQuestion(questionId, "answer returned");
            const replyText = replyMessage.content.text;
            const replyAttachments = replyMessage.content.attachments?.length
              ? formatAttachments(replyMessage.content.attachments)
              : "";
            recordActionEntry("parley_received", {
              from: targetDisplay,
              message: { text: replyText, attachments: replyMessage.content.attachments },
              messageId: replyMessage.id,
              ...(replyMessage.peerCompaction ? { peerCompaction: replyMessage.peerCompaction } : {}),
              timestamp: replyMessage.timestamp,
            });
            const latestCompaction = replyMessage.peerCompaction && (
              !questionCompaction || replyMessage.peerCompaction.generation > questionCompaction.generation
            ) ? replyMessage.peerCompaction : questionCompaction;
            const awarenessText = latestCompaction
              ? `${formatPeerCompactionNotice(targetDisplay, latestCompaction, sendTo)}\n\n`
              : "";
            connectedClient.acknowledgeSendContact(sendResult);
            acknowledgeInboundMessageContact(connectedClient, replyMessage);
            return {
              content: [{ type: "text", text: `${awarenessText}**Reply from ${targetDisplay}** (asked as ${sendIdentity!}):\nQuestion message ID: ${questionId}\nReply message ID: ${replyMessage.id}\n\n${replyText}${replyAttachments}` }],
              details: {
                ...deliveryDetails(sendResult),
                replyMessageId: replyMessage.id,
                ...(latestCompaction ? { peerCompaction: latestCompaction } : {}),
                ...(target.projectPane ? { openedProjectPane: true, projectRoot: target.projectPane.projectRoot, projectLauncher: target.projectPane.provider.kind === "session" ? target.projectPane.provider.name : "configured-command" } : {}),
              },
            };
          } catch (error) {
            rejectOwnedReplyWaiter(questionId, toError(error));
            if (replyPromise) {
              try {
                await replyPromise;
              } catch {
                // The waiter is cleanup-only on this path. The real failure is the one from the outer catch.
              }
            }
            const failureText = `Failed: ${getErrorMessage(error)}`;
            if (questionSendResult) connectedClient.acknowledgeSendContact(questionSendResult);
            return {
              content: [{
                type: "text",
                text: questionCompaction
                  ? `${failureText}\n\n${formatPeerCompactionNotice(questionTargetDisplay, questionCompaction, questionTargetId)}`
                  : failureText,
              }],
              details: {
                error: true,
                ...(questionId ? { messageId: questionId, deliveryState: latestDeliveryState(questionId, deliveryState) } : {}),
                ...(questionCompaction ? { peerCompaction: questionCompaction } : {}),
              },
            };
          }
        }

        case "reply": {
          if (targets) {
            return {
              content: [{ type: "text", text: "reply accepts one pending conversation; 'targets' is only available for send." }],
              details: { error: true },
            };
          }
          if (!message) {
            return {
              content: [{ type: "text", text: "Missing 'message' parameter" }],
              details: { error: true },
            };
          }

          try {
            const target = replyTracker.resolveReplyTarget({ to, replyTo });
            if (target.from.id === connectedClient.sessionId) {
              return {
                content: [{ type: "text", text: "Cannot message the current session" }],
                details: { error: true },
              };
            }
            captureSendIdentity();
            const result = await connectedClient.send(target.from.id, {
              text: message,
              attachments,
              replyTo: target.message.id,
              completesAsk: true,
              signal: _signal,
              contactKind: "direct",
            });
            if (!result.delivered) {
              return {
                content: [{ type: "text", text: formatDeliveryResult(result, { kind: "Reply", sender: sendIdentity!, target: target.from.name || target.from.id }) }],
                details: deliveryDetails(result),
              };
            }
            dismissIncomingAsk(target.message.id);
            recordActionEntry("parley_sent", {
              to: target.from.name || target.from.id,
              targetId: result.recipient?.id ?? target.from.id,
              as: sendIdentity!,
              toolCallId: _toolCallId,
              message: { text: message, attachments, replyTo: target.message.id, completesAsk: true },
              messageId: result.id,
              ...(result.peerCompaction ? { peerCompaction: result.peerCompaction } : {}),
              timestamp: Date.now(),
            });
            const targetDisplay = target.from.name || target.from.id;
            const awarenessText = formatDeliveryResult(result, { kind: "Reply", sender: sendIdentity!, target: targetDisplay });
            connectedClient.acknowledgeSendContact(result);
            return {
              content: [{
                type: "text",
                text: awarenessText,
              }],
              details: { ...deliveryDetails(result), replyTo: target.message.id },
            };
          } catch (error) {
            return {
              content: [{ type: "text", text: `Failed to reply: ${getErrorMessage(error)}` }],
              details: { error: true },
            };
          }
        }

        case "read": {
          const retained = messageId ? replyTracker.getMessage(messageId) : undefined;
          if (!retained) {
            return { content: [{ type: "text", text: messageId ? `Message ${messageId} is not retained in this session.` : "Missing messageId." }], details: { error: true } };
          }
          const { from, message: original, disposition } = retained;
          const state = disposition ? `\nStatus: ${disposition.state}${disposition.replacementId ? ` by message ${disposition.replacementId}` : ""}` : "";
          const attachmentsText = original.content.attachments?.length ? formatAttachments(original.content.attachments) : "";
          const origin = from.federation ? `\nRemote origin: ${from.federation.originLabel || from.federation.originId}` : "";
          return {
            content: [{ type: "text", text: `From ${from.name || from.id}${from.description ? ` — ${from.description}` : ""} (${from.cwd})${origin}\n${formatInboundDeliveryMetadata(original)}${state}\n\n${original.content.text}${attachmentsText}` }],
            details: { messageId: original.id },
          };
        }

        case "pending": {
          const conversation = replyTracker.formatConversationContext({ limit: Infinity, previewLength: 180 });
          return {
            content: [{ type: "text", text: conversation || "No unresolved inbound asks." }],
            details: {},
          };
        }

        case "status": {
          try {
            const mySessionId = connectedClient.sessionId;
            const sessions = await connectedClient.listSessions();
            const outstandingText = `\n${formatOutstandingAsks(20) || "Outstanding asks: none"}`;
            return {
              content: [{
                type: "text",
                text: `**Parley Status:**\nConnected: Yes\nSession ID: ${mySessionId}\nVisible connected sessions: ${sessions.length} (including this session; scope and permissions apply)${outstandingText}`,
              }],
              details: {
                outstandingAsks: [...outstandingAsks.entries()].map(([id, ask]) => ({
                  messageId: id,
                  to: ask.targetDisplay,
                  sentAt: ask.sentAt,
                })),
              },
            };
          } catch (error) {
            return {
              content: [{ type: "text", text: `Failed to get status: ${getErrorMessage(error)}` }],
              details: { error: true },
            };
          }
        }

        default:
          return {
            content: [{ type: "text", text: `Unknown action: ${action}` }],
            details: { error: true },
          };
      }
      })();
      if (sendIdentity && ["send", "ask", "reply", "broadcast"].includes(params.action)) {
        toolResult.details = { ...(toolResult.details as Record<string, unknown> ?? {}), senderIdentity: sendIdentity };
      }
      if (["send", "ask", "reply", "cancel", "broadcast"].includes(params.action)) {
        const conversation = replyTracker.formatConversationContext();
        if (conversation) toolResult.content.push({ type: "text", text: conversation });
        const outstanding = formatOutstandingAsks();
        if (outstanding) toolResult.content.push({ type: "text", text: outstanding });
      }
      if (conversationPersistenceWarning) toolResult.content.push({ type: "text", text: conversationPersistenceWarning });
      if (historyWarnings.length) {
        toolResult.content.push({ type: "text", text: `Local history was not fully persisted; recovery may be incomplete. ${historyWarnings.join("; ")}` });
      }
      return attachSelfProfile(toolResult, normalizeToolProfilePlaceholders(params.profile) !== undefined);
    },
    renderCall(args, theme, context) {
      const action = typeof args.action === "string" ? args.action : PARLEY_TOOL_NAME;
      const target = typeof args.to === "string" && args.to.trim() ? args.to.trim() : undefined;
      const targets = Array.isArray(args.targets)
        ? args.targets.filter((value): value is string => typeof value === "string" && Boolean(value.trim()))
        : [];
      const messagePreview = previewText(args.message, 96);
      const attachmentCount = Array.isArray(args.attachments) ? args.attachments.length : 0;
      let text = theme.fg("toolTitle", theme.bold("parley "));
      text += theme.fg(action === "ask" || action === "broadcast" ? "warning" : action === "reply" ? "success" : "accent", action);
      const senderIdentity = (context?.state as { senderIdentity?: string } | undefined)?.senderIdentity;
      if (senderIdentity && ["send", "ask", "reply", "broadcast"].includes(action)) {
        text += " " + theme.fg("muted", `as ${senderIdentity}`);
      }
      if (target) {
        text += " " + theme.fg("muted", "→") + " " + theme.fg("accent", target);
      } else if (targets.length > 0) {
        const targetSummary = targets.length <= 3 ? targets.join(", ") : `${targets.slice(0, 3).join(", ")} +${targets.length - 3}`;
        text += " " + theme.fg("muted", "→") + " " + theme.fg("accent", targetSummary);
      } else if (action === "broadcast") {
        text += " " + theme.fg("muted", "→ visible local sessions");
      }
      if (attachmentCount > 0) {
        text += " " + theme.fg("dim", `(${attachmentCount} attachment${attachmentCount === 1 ? "" : "s"})`);
      }
      if (messagePreview) {
        text += "\n  " + theme.fg("dim", messagePreview);
      }
      return new Text(text, 0, 0);
    },
    renderResult(result, { isPartial }, theme, context) {
      const details = result.details as { delivered?: boolean; error?: boolean; messageId?: string; reason?: string; senderIdentity?: string; outcomeKnown?: boolean; unknownCount?: number } | undefined;
      if (details?.senderIdentity) {
        const state = (context.state ?? {}) as { senderIdentity?: string };
        if (state.senderIdentity !== details.senderIdentity) {
          state.senderIdentity = details.senderIdentity;
          context.state = state;
          context.invalidate?.();
        }
      }
      if (isPartial) {
        return new Text(theme.fg("warning", details?.senderIdentity ? `Parley working as ${details.senderIdentity}…` : "Parley working..."), 0, 0);
      }
      const failed = Boolean(context.isError || details?.error === true || details?.delivered === false);
      const uncertain = details?.outcomeKnown === false || (details?.unknownCount ?? 0) > 0;
      let text = uncertain ? theme.fg("warning", "? ") : failed ? theme.fg("error", "✗ ") : theme.fg("success", "✓ ");
      text += theme.fg(uncertain ? "warning" : failed ? "error" : "text", result.content.filter((item) => item.type === "text").map((item) => item.text).join("\n"));
      if (details?.messageId && !context.expanded) {
        text += theme.fg("dim", ` (${details.messageId.slice(0, 8)})`);
      }
      if (details?.reason && context.expanded) {
        text += "\n" + theme.fg("dim", `Reason: ${details.reason}`);
      }
      return new Text(text, 0, 0);
    },
  }));

  function insertIntoEditor(ctx: ExtensionContext, text: string): boolean {
    if (!ctx.hasUI) return false;
    const ui = ctx.ui as { getEditorText?: () => string; setEditorText?: (text: string) => void };
    if (typeof ui.setEditorText !== "function") return false;
    const existing = typeof ui.getEditorText === "function" ? ui.getEditorText() : "";
    ui.setEditorText(existing.trim() ? `${existing.trimEnd()}\n\n${text}` : text);
    return true;
  }

  async function insertParleyId(ctx: ExtensionContext): Promise<void> {
    const commandGeneration = runtimeGeneration;
    const liveContext = getLiveContext(ctx, commandGeneration);
    if (!liveContext) return;
    let contactClient: ParleyClient;
    try {
      contactClient = await ensureConnected("tool");
    } catch (error) {
      notifyIfLive(ctx, `Parley unavailable: ${getErrorMessage(error)}`, "error", commandGeneration);
      return;
    }
    const sessionId = contactClient.sessionId;
    if (!sessionId || !getLiveContext(liveContext, commandGeneration)) return;
    const snippet = formatParleyContactSnippet(sessionId);
    if (insertIntoEditor(liveContext, snippet)) {
      notifyIfLive(liveContext, `Inserted parley contact target: ${sessionId}`, "info", commandGeneration);
      return;
    }
    notifyIfLive(liveContext, `Parley contact target: ${sessionId}`, "info", commandGeneration);
  }

  async function setParleyAlias(args: string, ctx: ExtensionContext): Promise<void> {
    const commandGeneration = runtimeGeneration;
    const liveContext = getLiveContext(ctx, commandGeneration);
    if (!liveContext) return;

    let alias = args.trim();
    const opensAliasInput = !alias || alias.toLowerCase() === "menu";
    if (opensAliasInput) {
      if (!liveContext.hasUI) {
        const currentAlias = pi.getSessionName()?.trim();
        notifyAliasCommand(
          liveContext,
          alias ? "The alias menu requires an interactive UI; use /alias <name>." : currentAlias ? `Session alias: ${currentAlias}` : "No session alias set. Use /alias <name>.",
          alias ? "warning" : "info",
          commandGeneration,
        );
        return;
      }

      const currentAlias = pi.getSessionName()?.trim();
      let entered: string | undefined;
      try {
        entered = await liveContext.ui.input(
          "Set session alias",
          currentAlias ? `Current alias: ${currentAlias}` : "Enter an alias",
        );
      } catch (error) {
        notifyAliasCommand(liveContext, `Unable to set session alias: ${getErrorMessage(error)}`, "error", commandGeneration);
        return;
      }
      if (entered === undefined) return;
      alias = entered.trim();
      if (!alias) {
        notifyAliasCommand(liveContext, "Session alias cannot be empty.", "warning", commandGeneration);
        return;
      }
    }

    if (!getLiveContext(liveContext, commandGeneration)) return;
    try {
      pi.setSessionName(alias);
    } catch (error) {
      notifyAliasCommand(liveContext, `Unable to set session alias: ${getErrorMessage(error)}`, "error", commandGeneration);
      return;
    }

    // session_info_changed is the canonical identity contract where the host
    // forwards it to extensions. Keep this direct push as an idempotent fast
    // path so /alias has completed broker presence synchronization before the
    // command reports success.
    syncPresenceIdentity(liveContext.sessionManager.getSessionId());
    notifyAliasCommand(liveContext, `Session alias set: ${alias}`, "info", commandGeneration);
  }

  async function openParleyOverlay(ctx: ExtensionContext): Promise<void> {
    const overlayGeneration = runtimeGeneration;
    const liveContext = getLiveContext(ctx, overlayGeneration);
    if (!liveContext?.hasUI || (liveContext as ExtensionContext & { mode?: string }).mode !== "tui") return;

    let overlayClient: ParleyClient;
    try {
      overlayClient = await ensureConnected("overlay");
    } catch (error) {
      notifyIfLive(ctx, `Parley unavailable: ${getErrorMessage(error)}`, "error", overlayGeneration);
      return;
    }
    if (!getLiveContext(ctx, overlayGeneration)) return;

    syncPresenceIdentity(ctx.sessionManager.getSessionId());

    let currentSession: SessionInfo;
    let sessions: SessionInfo[];
    let duplicates: Set<string>;
    try {
      const mySessionId = overlayClient.sessionId;
      const allSessions = await overlayClient.listSessions();
      if (!getLiveContext(ctx, overlayGeneration)) return;
      const foundCurrentSession = allSessions.find(s => s.id === mySessionId);
      if (!foundCurrentSession) {
        notifyIfLive(ctx, "Current session is missing from parley session list", "error", overlayGeneration);
        return;
      }
      currentSession = foundCurrentSession;
      duplicates = duplicateSessionNames(allSessions);
      sessions = allSessions.filter(s => s.id !== mySessionId);
    } catch (error) {
      notifyIfLive(ctx, `Failed to list sessions: ${getErrorMessage(error)}`, "error", overlayGeneration);
      return;
    }

    const selectedSession = await ctx.ui.custom<SessionInfo | undefined>(
      (_tui, theme, keybindings, done) => new SessionListOverlay(theme, keybindings, currentSession, sessions, done),
      { overlay: true, overlayOptions: { width: 88 } }
    ).catch(() => undefined);

    if (!selectedSession || !getLiveContext(ctx, overlayGeneration)) return;

    try {
      overlayClient = await ensureConnected("overlay");
    } catch (error) {
      notifyIfLive(ctx, `Parley unavailable: ${getErrorMessage(error)}`, "error", overlayGeneration);
      return;
    }
    if (!getLiveContext(ctx, overlayGeneration)) return;

    const targetLabel = formatSessionLabel(selectedSession, duplicates);

    const result = await ctx.ui.custom<ComposeResult>(
      (tui, theme, keybindings, done) => new ComposeOverlay(tui, theme, keybindings, selectedSession, targetLabel, overlayClient, done),
      { overlay: true, overlayOptions: { width: 72 } }
    ).catch(() => undefined);

    if (result?.sent && result.messageId && result.text && getLiveContext(ctx, overlayGeneration)) {
      recordConversationEntry("parley_sent", {
        to: selectedSession.name || selectedSession.id,
        targetId: selectedSession.id,
        as: currentSendIdentity(overlayClient),
        message: { text: result.text },
        messageId: result.messageId,
        ...(result.peerCompaction ? { peerCompaction: result.peerCompaction } : {}),
        timestamp: Date.now(),
      });
      const deliveryNotice = result.delivery === "queued"
        ? `Message queued for offline session ${targetLabel} — delivered only if it reconnects within 24h`
        : `Message sent to ${targetLabel} (as ${currentSendIdentity(overlayClient)})`;
      notifyIfLive(
        ctx,
        result.peerCompaction
          ? `${deliveryNotice}\n\n${formatPeerCompactionNotice(targetLabel, result.peerCompaction, selectedSession.id)}`
          : deliveryNotice,
        "info",
        overlayGeneration,
      );
      overlayClient.acknowledgeSendContact(result);
    }
  }

  pi.registerCommand("parley", {
    description: "Open session parley overlay to browse active sessions and send messages",
    handler: async (_args, ctx) => openParleyOverlay(ctx),
  });

  pi.registerCommand("parley-id", {
    description: "Insert a stable parley contact target snippet for this session into the editor",
    handler: async (_args, ctx) => insertParleyId(ctx),
  });

  pi.registerCommand("alias", {
    description: "Set or inspect this session's alias (usage: /alias <name> or /alias menu)",
    handler: async (args, ctx) => setParleyAlias(args, ctx),
  });

  pi.registerShortcut("alt+m", {
    description: "Open session parley",
    handler: async (ctx) => openParleyOverlay(ctx),
  });
}
