import type {
  Attachment,
  Message,
  MessageControl,
  MessageProvenance,
  MessageReceipt,
  MessageReceiptStatus,
  PeerCompactionNotice,
  SessionInfo,
  SessionRegistration,
} from "../types.ts";
import { isValidSessionDescription, isValidSessionName } from "../session-profile.ts";
import {
  encodeOriginQualifiedSessionIdentity,
  isCanonicalFederationOriginId,
  isCanonicalFederationScopeAlias,
} from "./federation-protocol.ts";

import { PARLEY_PROTOCOL_NAME, PARLEY_PROTOCOL_VERSION } from "./paths.ts";

/** Validate the wire identity and correlation of a health response.
 * Build metadata is diagnostic and does not decide wire compatibility. */
export function isBrokerHealthOkMessage(message: unknown, requestId: string): boolean {
  if (!isRecord(message)) return false;
  return message.type === "health_ok"
    && message.requestId === requestId
    && message.protocol === PARLEY_PROTOCOL_NAME
    && message.version === PARLEY_PROTOCOL_VERSION;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isMessageReceiptStatus(value: unknown): value is MessageReceiptStatus {
  return value === "receiver_received"
    || value === "queued"
    || value === "injected"
    || value === "acknowledged"
    || value === "expired"
    || value === "cancelled"
    || value === "superseded"
    || value === "cancellation_requested";
}

export function isMessageReceipt(value: unknown): value is MessageReceipt {
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.messageId !== "string" || !isMessageReceiptStatus(value.status) || typeof value.timestamp !== "number") {
    return false;
  }
  return value.detail === undefined || typeof value.detail === "string";
}

export function isMessageControl(value: unknown): value is MessageControl {
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.messageId !== "string" || typeof value.timestamp !== "number") {
    return false;
  }
  if (value.action !== "cancel" && value.action !== "supersede") {
    return false;
  }
  if (value.supersededBy !== undefined && typeof value.supersededBy !== "string") {
    return false;
  }
  return value.detail === undefined || typeof value.detail === "string";
}

function isAttachment(value: unknown): value is Attachment {
  if (!isRecord(value)) {
    return false;
  }

  if (
    value.type !== "file"
    && value.type !== "snippet"
    && value.type !== "context"
  ) {
    return false;
  }

  if (typeof value.name !== "string" || typeof value.content !== "string") {
    return false;
  }

  return value.language === undefined || typeof value.language === "string";
}

function isMessageProvenance(value: unknown): value is MessageProvenance {
  if (!isRecord(value)) {
    return false;
  }
  return value.type === "extension_outbox"
    && typeof value.extensionId === "string"
    && typeof value.extensionName === "string"
    && typeof value.requestId === "string";
}

export function isPeerCompactionNotice(value: unknown): value is PeerCompactionNotice {
  if (!isRecord(value)) return false;
  if (
    typeof value.peerSessionId !== "string"
    || (value.peerName !== undefined && typeof value.peerName !== "string")
    || (value.requestedPeerSessionId !== undefined && typeof value.requestedPeerSessionId !== "string")
    || !Number.isSafeInteger(value.generation)
    || (value.generation as number) < 1
    || !Number.isSafeInteger(value.previousGeneration)
    || (value.previousGeneration as number) < 0
    || (value.previousGeneration as number) >= (value.generation as number)
    || !Number.isSafeInteger(value.compactedAt)
    || (value.compactedAt as number) < 0
  ) {
    return false;
  }
  return value.contextPct === undefined || typeof value.contextPct === "number";
}

export function isMessage(value: unknown): value is Message {
  if (!isRecord(value)) {
    return false;
  }

  if (typeof value.id !== "string" || typeof value.timestamp !== "number") {
    return false;
  }

  for (const key of ["senderSequence", "brokerReceivedAt", "brokerDeliveredAt", "receiverReceivedAt", "injectedAt"] as const) {
    if (value[key] !== undefined && typeof value[key] !== "number") {
      return false;
    }
  }

  if (value.supersedes !== undefined && typeof value.supersedes !== "string") {
    return false;
  }

  if (value.retryOf !== undefined && typeof value.retryOf !== "string") {
    return false;
  }

  if (value.replyTo !== undefined && typeof value.replyTo !== "string") {
    return false;
  }

  if (value.expectsReply !== undefined && typeof value.expectsReply !== "boolean") {
    return false;
  }
  if (value.completesAsk !== undefined && typeof value.completesAsk !== "boolean") return false;
  if (value.completesAsk === true && !value.replyTo) return false;
  if (value.senderWaitMode !== undefined && value.senderWaitMode !== "blocking" && value.senderWaitMode !== "nonblocking") return false;
  if (value.replyDeadline !== undefined && (!Number.isSafeInteger(value.replyDeadline) || (value.replyDeadline as number) < 0)) return false;

  if (value.provenance !== undefined && !isMessageProvenance(value.provenance)) {
    return false;
  }

  if (value.peerCompaction !== undefined && !isPeerCompactionNotice(value.peerCompaction)) {
    return false;
  }
  if (value.contactToken !== undefined && typeof value.contactToken !== "string") {
    return false;
  }
  if (value.contactBaseline !== undefined && typeof value.contactBaseline !== "boolean") {
    return false;
  }
  if (value.contactBaseline === true && typeof value.contactToken !== "string") {
    return false;
  }

  if (!isRecord(value.content) || typeof value.content.text !== "string") {
    return false;
  }

  return value.content.attachments === undefined
    || (Array.isArray(value.content.attachments) && value.content.attachments.every(isAttachment));
}

export function isSessionInfo(value: unknown): value is SessionInfo {
  if (!isRecord(value)) {
    return false;
  }

  if (
    typeof value.id !== "string"
    || typeof value.cwd !== "string"
    || typeof value.model !== "string"
    || typeof value.pid !== "number"
    || typeof value.startedAt !== "number"
    || typeof value.lastActivity !== "number"
  ) {
    return false;
  }

  if (value.endpointEpoch !== undefined && typeof value.endpointEpoch !== "string") {
    return false;
  }

  if (value.name !== undefined && !isValidSessionName(value.name)) {
    return false;
  }
  if (value.description !== undefined && !isValidSessionDescription(value.description)) {
    return false;
  }

  if (value.runtimeFallbackAlias !== undefined && typeof value.runtimeFallbackAlias !== "boolean") {
    return false;
  }

  if (value.status !== undefined && typeof value.status !== "string") {
    return false;
  }

  if (value.peerUid !== undefined && typeof value.peerUid !== "number") {
    return false;
  }

  for (const key of ["contextPct", "contextTokens", "contextWindow"] as const) {
    if (value[key] !== undefined && typeof value[key] !== "number") {
      return false;
    }
  }

  if (value.tmuxPane !== undefined && typeof value.tmuxPane !== "string") {
    return false;
  }

  if (value.isSubagent !== undefined && typeof value.isSubagent !== "boolean") {
    return false;
  }
  if (value.supervisorSessionId !== undefined && typeof value.supervisorSessionId !== "string") {
    return false;
  }
  if (value.supervisorName !== undefined && typeof value.supervisorName !== "string") {
    return false;
  }
  if (value.advertised !== undefined && typeof value.advertised !== "boolean") {
    return false;
  }
  if (value.extensions !== undefined) {
    if (!Array.isArray(value.extensions) || value.extensions.length > 32) return false;
    for (const extension of value.extensions) {
      if (!isRecord(extension) || Object.keys(extension).some((key) => key !== "namespace" && key !== "ownerEligible")) return false;
      if (typeof extension.namespace !== "string"
        || extension.namespace.length === 0
        || extension.namespace.length > 64
        || !/^[a-z0-9][a-z0-9._/-]*$/.test(extension.namespace)
        || typeof extension.ownerEligible !== "boolean") return false;
    }
  }
  if (value.federation !== undefined) {
    if (!isRecord(value.federation)) return false;
    const keys = Object.keys(value.federation);
    if (!keys.every((key) => ["originId", "originLabel", "remoteScopeAlias", "remoteStableSessionId"].includes(key))) return false;
    if (!isCanonicalFederationOriginId(value.federation.originId)
      || !isCanonicalFederationScopeAlias(value.federation.remoteScopeAlias)
      || typeof value.federation.remoteStableSessionId !== "string"
      || value.federation.remoteStableSessionId.length < 1
      || value.federation.remoteStableSessionId.length > 512
      || /[\p{Cc}\p{Cf}]/u.test(value.federation.remoteStableSessionId)
      || (value.federation.originLabel !== undefined
        && (typeof value.federation.originLabel !== "string"
          || value.federation.originLabel.length < 1
          || value.federation.originLabel.length > 80
          || /[\p{Cc}\p{Cf}]/u.test(value.federation.originLabel)))) return false;
    if (value.trustedLocal !== false
      || value.peerUid !== undefined
      || value.isSubagent !== undefined
      || value.supervisorSessionId !== undefined
      || value.supervisorName !== undefined
      || value.advertised !== undefined
      || value.extensions !== undefined) return false;
    try {
      if (value.id !== encodeOriginQualifiedSessionIdentity({
        originId: value.federation.originId,
        remoteScopeAlias: value.federation.remoteScopeAlias,
        remoteStableSessionId: value.federation.remoteStableSessionId,
      })) return false;
    } catch {
      return false;
    }
  }

  return value.trustedLocal === undefined || typeof value.trustedLocal === "boolean";
}

export function isAuthoredMessage(value: unknown): value is Message {
  return isMessage(value)
    && value.peerCompaction === undefined
    && value.contactToken === undefined
    && value.contactBaseline === undefined
    && value.replyDeadline === undefined;
}

export function isSessionId(value: unknown): value is string {
  return typeof value === "string"
    && value.trim().length > 0
    && value.length <= 512
    && !/[\p{Cc}\p{Cf}]/u.test(value)
    && !value.startsWith("oqs1.");
}

export function isSessionRegistration(value: unknown): value is SessionRegistration {
  if (!isRecord(value)) {
    return false;
  }

  if (
    typeof value.cwd !== "string"
    || typeof value.model !== "string"
    || typeof value.pid !== "number"
    || typeof value.startedAt !== "number"
    || typeof value.lastActivity !== "number"
  ) {
    return false;
  }

  if (value.name !== undefined && !isValidSessionName(value.name)) {
    return false;
  }
  if (value.description !== undefined && !isValidSessionDescription(value.description)) {
    return false;
  }
  if (value.runtimeFallbackAlias !== undefined && typeof value.runtimeFallbackAlias !== "boolean") {
    return false;
  }
  if (value.extensions !== undefined && !Array.isArray(value.extensions)) {
    return false;
  }
  if (value.tmuxPane !== undefined && typeof value.tmuxPane !== "string") {
    return false;
  }
  if (value.isSubagent !== undefined && typeof value.isSubagent !== "boolean") {
    return false;
  }
  if (value.supervisorSessionId !== undefined && typeof value.supervisorSessionId !== "string") {
    return false;
  }
  if (value.supervisorName !== undefined && typeof value.supervisorName !== "string") {
    return false;
  }
  // advertised is intentionally NOT accepted on registration/presence -- it is
  // broker-authoritative, set only via the dedicated "advertise" request/response
  // exchange after uniqueness validation, never by a client asserting it directly.

  return value.status === undefined || typeof value.status === "string";
}
