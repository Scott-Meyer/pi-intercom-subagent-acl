import type { SendResult } from "./broker/client.ts";
import { formatPeerCompactionNotice } from "./compaction-awareness.ts";

/** A receipt describes observed delivery, not whether a colleague read or acted on it. */
export function formatDeliveryResult(result: SendResult, context: {
  kind: "Message" | "Ask" | "Reply" | "Progress update";
  sender: string;
  target: string;
}): string {
  const recipient = result.recipient;
  const actualTarget = recipient
    ? `${recipient.name || recipient.id} (${recipient.id})`
    : context.target;
  const identity = `as ${context.sender} to ${actualTarget}`;
  const lines: string[] = [];
  if (!result.outcomeKnown || result.delivery === "unknown") {
    lines.push(`${context.kind} delivery outcome unknown ${identity}.`,
      "The message may have arrived; sending it again could repeat it.");
  } else if (!result.delivered) {
    lines.push(`${context.kind} not delivered ${identity}.`);
  } else if (result.delivery === "queued") {
    lines.push(`${context.kind} queued ${identity}.`,
      "Recipient offline; queued for up to 24 hours while this broker remains running.");
  } else {
    lines.push(`${context.kind} sent ${identity}.`);
  }
  lines.push(`Message ID: ${result.id}${result.delivered && result.delivery === "socket_delivered" ? " · endpoint accepted" : ""}`);
  if (result.reason) lines.push(`Reason: ${result.reason}`);
  if (result.code) lines.push(`Outcome code: ${result.code}`);
  if (result.code === "E_REPLY_TARGET") {
    lines.push("The broker cannot authorize this thread. Its relationship may have expired or been lost on restart; the local message can still be retained.");
  }
  if (result.peerCompaction) {
    lines.push(formatPeerCompactionNotice(context.target, result.peerCompaction, recipient?.id));
  }
  return lines.join("\n");
}

export function formatCancellationResult(result: SendResult): string {
  if (!result.outcomeKnown || result.delivery === "unknown") {
    return `Cancellation outcome unknown for ${result.id}. The request may still be actionable.${result.reason ? ` ${result.reason}` : ""}`;
  }
  if (!result.delivered) {
    return `Cancellation was not accepted for ${result.id}.${result.reason ? ` ${result.reason}` : ""}`;
  }
  if (result.cancellation === "removed_from_mailbox") {
    return `Cancelled ${result.id}: removed from the offline mailbox before delivery.`;
  }
  if (result.cancellation === "not_delivered") {
    return `Cancelled ${result.id}: the original message was not delivered.`;
  }
  return `Withdrawal requested for ${result.id}. Work may already have happened; this does not undo it.`;
}
