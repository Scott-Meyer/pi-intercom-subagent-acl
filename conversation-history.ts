import type { ParleyContext } from "./reply-tracker.ts";
import type { Message, MessageControl, SessionInfo } from "./types.ts";

export interface OutstandingAsk {
  to: string;
  targetDisplay: string;
  preview: string;
  sentAt: number;
  message?: Message["content"];
}

export interface ConversationHistory {
  incoming: Map<string, ParleyContext>;
  settledIncoming: Set<string>;
  persistedIncoming: Set<string>;
  controls: Map<string, { from: SessionInfo; control: MessageControl }>;
  persistedControls: Set<string>;
  outgoing: Map<string, OutstandingAsk>;
}

export function messageControlKey(control: MessageControl): string {
  return JSON.stringify([control.messageId, control.action, control.supersededBy ?? null]);
}

/** Reconstruct conversational state from session records, not from local age or broker assumptions.
 * Host-persisted custom messages distinguish delivered context from a lost in-memory steering queue.
 * Entries include all branches: sending/withdrawal are external effects, not undone by tree navigation.
 */
export function restoreConversationHistory(entries: readonly unknown[]): ConversationHistory {
  const state: ConversationHistory = {
    incoming: new Map(), settledIncoming: new Set(), persistedIncoming: new Set(),
    controls: new Map(), persistedControls: new Set(), outgoing: new Map(),
  };
  for (const raw of entries) {
    if (!raw || typeof raw !== "object") continue;
    const entry = raw as { type?: string; customType?: string; data?: unknown; details?: unknown; message?: { role?: string; details?: { replyMessageId?: string } } };
    const data = entry.data as Record<string, unknown> | undefined;
    if (entry.type === "message" && entry.message?.role === "toolResult") {
      const replyId = entry.message.details?.replyMessageId;
      if (typeof replyId === "string") state.persistedIncoming.add(replyId);
      continue;
    }
    if (entry.type === "custom_message") {
      if (entry.customType === "parley_message") {
        const details = entry.details as Partial<ParleyContext> | undefined;
        if (details?.from?.id && details.message?.id && details.message.content) {
          state.incoming.set(details.message.id, { from: details.from, message: details.message, receivedAt: details.message.receiverReceivedAt ?? details.message.timestamp });
          state.persistedIncoming.add(details.message.id);
        }
      } else if (entry.customType === "parley_message_control") {
        const details = entry.details as { from?: SessionInfo; control?: MessageControl } | undefined;
        if (details?.control?.messageId) {
          const key = messageControlKey(details.control);
          state.persistedControls.add(key);
          state.settledIncoming.add(details.control.messageId);
          if (details.from?.id) state.controls.set(key, { from: details.from, control: details.control });
        }
      }
      continue;
    }
    if (entry.type !== "custom" || !data) continue;
    switch (entry.customType) {
      case "parley_inbound_received": {
        const context = data as unknown as ParleyContext;
        if (context.from?.id && context.message?.id && context.message.content) {
          state.incoming.set(context.message.id, context);
        }
        break;
      }
      case "parley_inbound_settled":
        if (typeof data.messageId === "string") state.settledIncoming.add(data.messageId);
        break;
      case "parley_inbound_control": {
        const value = data as unknown as { from: SessionInfo; control: MessageControl };
        if (value.from?.id && value.control?.messageId) {
          state.controls.set(messageControlKey(value.control), value);
          state.settledIncoming.add(value.control.messageId);
        }
        break;
      }
      case "parley_sent": {
        const message = data.message as Partial<Message> | undefined;
        if (typeof message?.replyTo === "string" && message.completesAsk !== false && !message.expectsReply) state.settledIncoming.add(message.replyTo);
        break;
      }
      case "parley_ask_pending":
        if (typeof data.messageId === "string" && typeof data.to === "string" && typeof data.sentAt === "number") {
          const message = data.message as Message["content"] | undefined;
          state.outgoing.set(data.messageId, {
            to: data.to, targetDisplay: typeof data.targetDisplay === "string" ? data.targetDisplay : data.to,
            sentAt: data.sentAt, preview: message?.text?.replace(/\s+/g, " ").slice(0, 120) ?? "",
            ...(message ? { message } : {}),
          });
        }
        break;
      case "parley_ask_settled":
        if (typeof data.messageId === "string") state.outgoing.delete(data.messageId);
        break;
    }
  }
  // A crash may occur between host persistence and our settlement entry.
  // Reconcile only answers actually present in a host message/tool result, not merely received.
  for (const id of state.persistedIncoming) {
    const context = state.incoming.get(id);
    if (!context) continue;
    const message = context.message;
    if (!message.replyTo || message.completesAsk === false || message.expectsReply) continue;
    if (state.outgoing.get(message.replyTo)?.to === context.from.id) state.outgoing.delete(message.replyTo);
  }
  return state;
}
