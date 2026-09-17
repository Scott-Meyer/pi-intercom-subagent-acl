import { getAskTimeoutMs } from "./config.ts";
import type { Message, SessionInfo } from "./types.ts";

export interface IntercomContext {
  from: SessionInfo;
  message: Message;
  receivedAt: number;
  disposition?: { state: "withdrawn" | "superseded"; replacementId?: string };
}

function senderMatchPriority(context: IntercomContext, to: string): number {
  if (context.from.id === to) return 0;
  if (context.from.name?.toLowerCase() === to.toLowerCase()) return 1;
  if (context.from.id.startsWith(to)) return 2;
  return 3;
}

function matchesPendingSender(context: IntercomContext, to: string): boolean {
  return senderMatchPriority(context, to) < 3;
}

function resolvePendingSender(pending: IntercomContext[], to: string): IntercomContext {
  const exactIdMatches = pending.filter((context) => senderMatchPriority(context, to) === 0);
  if (exactIdMatches.length === 1) {
    return exactIdMatches[0]!;
  }
  if (exactIdMatches.length > 1) {
    throw new Error(`Multiple pending asks from session ID "${to}" — specify \`replyTo\``);
  }

  const exactNameMatches = pending.filter((context) => senderMatchPriority(context, to) === 1);
  if (exactNameMatches.length === 1) {
    return exactNameMatches[0]!;
  }
  if (exactNameMatches.length > 1) {
    throw new Error(`Multiple pending asks match sender name "${to}" — specify a full session ID or \`replyTo\``);
  }

  const idPrefixMatches = pending.filter((context) => senderMatchPriority(context, to) === 2);
  if (idPrefixMatches.length === 1) {
    return idPrefixMatches[0]!;
  }
  if (idPrefixMatches.length > 1) {
    throw new Error(`Multiple pending asks match ID prefix "${to}" — use a longer session ID prefix or specify \`replyTo\``);
  }

  throw new Error(`No pending ask from "${to}"`);
}

export class ReplyTracker {
  private readonly messages = new Map<string, IntercomContext>();
  private readonly pendingAsks = new Map<string, IntercomContext>();
  private activeContexts: readonly IntercomContext[] = [];

  constructor(private readonly askTimeoutMs = getAskTimeoutMs()) {}

  recordIncomingMessage(from: SessionInfo, message: Message, receivedAt = Date.now()): IntercomContext {
    const context = { from, message, receivedAt };
    this.messages.set(message.id, context);
    // Pending requests retain their full context until an explicit settlement.
    for (const id of this.messages.keys()) {
      if (this.messages.size <= 200) break;
      if (!this.pendingAsks.has(id) && id !== message.id) this.messages.delete(id);
    }
    if (message.expectsReply) {
      this.pendingAsks.set(message.id, context);
    }
    return context;
  }

  /** Newly surfaced messages replace the active conversation; tool-only iterations retain it.
   * Keep simultaneous candidates distinct from an absence of context. */
  activateContexts(contexts: readonly IntercomContext[]): void {
    if (contexts.length > 0) this.activeContexts = [...contexts];
  }

  clearActiveContexts(): void {
    this.activeContexts = [];
  }

  reset(): void {
    this.messages.clear();
    this.pendingAsks.clear();
    this.clearActiveContexts();
  }

  resolveReplyTarget(options: { to?: string; replyTo?: string }, now = Date.now()): IntercomContext {

    if (options.replyTo) {
      const target = this.messages.get(options.replyTo);
      if (!target) {
        throw new Error(`No retained message with ID "${options.replyTo}"`);
      }
      if (options.to && !matchesPendingSender(target, options.to)) {
        throw new Error(`Pending ask "${options.replyTo}" is not from "${options.to}"`);
      }
      return target;
    }

    const pending = Array.from(this.pendingAsks.values());
    if (options.to) {
      const candidates = [...this.activeContexts, ...pending];
      const priority = candidates.reduce((best, context) => Math.min(best, senderMatchPriority(context, options.to!)), 3);
      const matches = candidates.filter((context) => priority < 3 && senderMatchPriority(context, options.to!) === priority);
      const activeMatches = this.activeContexts.filter((context) => matches.includes(context));
      if (activeMatches.length > 0 && new Set(matches.map((context) => context.from.id)).size > 1) {
        throw new Error(`Multiple senders match "${options.to}" — specify a full session ID or \`replyTo\`.`);
      }
      if (activeMatches.length > 1) {
        throw new Error(`Multiple active messages match "${options.to}" — specify \`replyTo\`.`);
      }
      // Naming the sender narrows the conversation; it must not redirect an
      // acknowledgment of a fresh note into an answer to an older question.
      if (activeMatches.length === 1) return activeMatches[0]!;
      if (pending.some((context) => matchesPendingSender(context, options.to!))) {
        try { return resolvePendingSender(pending, options.to); } catch (error) {
          throw new Error(`${(error as Error).message}\n${this.formatConversationContext({ now })}`);
        }
      }
      throw new Error(`No pending ask from "${options.to}". Exact replyTo can identify a retained ordinary message.`);
    }

    if (this.activeContexts.length > 1) {
      throw new Error("Multiple active conversations — specify `to` or `replyTo`.");
    }
    if (this.activeContexts.length === 1) {
      return this.activeContexts[0]!;
    }

    if (pending.length === 1) {
      return pending[0]!;
    }
    if (pending.length === 0) {
      throw new Error("No active intercom context to reply to");
    }

    throw new Error(`Multiple pending asks — specify \`to\` or \`replyTo\`.\n${this.formatConversationContext({ now })}`);
  }

  findUniquePendingAskFrom(to: string, now = Date.now()): IntercomContext | null {
    const candidates = Array.from(this.pendingAsks.values()).filter((context) => {
      if (now - context.receivedAt > this.askTimeoutMs) {
        return false;
      }
      return context.from.id === to || context.from.name?.toLowerCase() === to.toLowerCase();
    });
    return candidates.length === 1 ? candidates[0]! : null;
  }

  getActiveReplyTarget(now = Date.now()): IntercomContext | null {
    const active = this.activeContexts.length === 1 ? this.activeContexts[0] : undefined;
    return active?.message.expectsReply ? active : null;
  }

  findActiveReplyTargetMismatch(to: string, now = Date.now()): IntercomContext | null {
    const activeReplyTarget = this.getActiveReplyTarget(now);
    if (!activeReplyTarget) {
      return null;
    }
    return activeReplyTarget.from.id === to ? null : activeReplyTarget;
  }

  markReplied(replyTo: string): void {
    this.dismissPendingAsk(replyTo);
  }

  dismissPendingAsk(replyTo: string): void {
    this.pendingAsks.delete(replyTo);
    this.activeContexts = this.activeContexts.filter((context) => context.message.id !== replyTo);
  }

  listPending(now = Date.now()): IntercomContext[] {
    return Array.from(this.pendingAsks.values()).sort((a, b) => a.receivedAt - b.receivedAt);
  }

  /** Retained text keeps its later withdrawal/replacement context. */
  setDisposition(messageId: string, disposition: NonNullable<IntercomContext["disposition"]>): void {
    const context = this.messages.get(messageId);
    if (context) this.messages.set(messageId, { ...context, disposition });
    this.dismissPendingAsk(messageId);
  }

  /** Full retained snapshot for recovery or explicit conversation threading. */
  getMessage(messageId: string): IntercomContext | undefined {
    return this.messages.get(messageId);
  }

  /** A response timeout is a waiting-window boundary, not completed or withdrawn work. */
  replyWindowElapsed(context: IntercomContext, now = Date.now()): boolean {
    return now > (context.message.replyDeadline ?? context.receivedAt + this.askTimeoutMs);
  }

  /** Bounded adjacent context for action results; never selects or answers a request. */
  formatConversationContext(options: { limit?: number; previewLength?: number; now?: number } = {}): string {
    const now = options.now ?? Date.now();
    const limit = Math.max(1, Math.floor(options.limit ?? 3));
    const previewLength = Math.max(20, Math.min(300, options.previewLength ?? 120));
    const pending = this.listPending(now);
    const activeIds = new Set(this.activeContexts.map((context) => context.message.id));
    const contexts = [...this.activeContexts, ...pending.filter((item) => !activeIds.has(item.message.id))];
    if (contexts.length === 0) return "";
    const lines = contexts.slice(0, limit).map((context) => {
      const preview = context.message.content.text.replace(/\s+/g, " ").trim();
      const status = this.pendingAsks.has(context.message.id)
        ? this.replyWindowElapsed(context, now) ? "unanswered; reply window elapsed, not withdrawn" : "awaiting your reply"
        : "active conversation";
      const attachments = context.message.content.attachments?.length;
      return `- ${context.from.name || context.from.id} (${context.from.id}), message ${context.message.id} — ${status}: ${JSON.stringify(preview.length > previewLength ? `${preview.slice(0, previewLength - 1)}…` : preview)}${attachments ? ` [${attachments} attachment snapshot(s)]` : ""}${context.message.peerCompaction ? " [sender compacted since prior direct contact (notice at message arrival)]" : ""}`;
    });
    if (contexts.length > limit) lines.push(`- ${contexts.length - limit} more conversation(s); pending has the complete request list.`);
    return `Conversation context (${pending.length} unanswered request${pending.length === 1 ? "" : "s"}):\n${lines.join("\n")}`;
  }
}
