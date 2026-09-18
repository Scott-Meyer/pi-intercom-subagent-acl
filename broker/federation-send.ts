/**
 * Single-hop routed direct text and negotiated conversation frames.
 *
 * A peer send carries one client message from an origin broker's locally
 * owned session to a destination broker's locally owned session over a
 * single negotiated peer-send-v1 link. Delivery is accepted only when the
 * destination broker's correlated result frame arrives; writing to the link
 * is never acceptance.
 *
 * Identity rules:
 * - The sender tuple is expressed in the SENDER broker's own exported
 *   namespace. The destination broker must resolve it against its imported
 *   roster from that exact link; it never trusts a peer-claimed projection.
 * - The target tuple is expressed in the DESTINATION broker's own namespace.
 *   The destination must resolve it to a locally owned session. A target
 *   that resolves only to an imported row is a single-hop violation.
 * - No remote mailbox queueing in federation v1: a disconnected target fails.
 * - peer-send-exact-v1 optionally pins the destination endpoint epoch. A pin
 *   is checked after authorization and before delivery; it is never ignored.
 * - peer-conversation-text-v1 requires author-qualified retained IDs and both
 *   endpoint/broker incarnation pins. Reply threading reverses a recorded edge,
 *   independent of the transport sendId and explicit ask-completion intent.
 */

import { decodeConversationMessageId } from "./federation-conversation.ts";
import type { SessionInfo } from "../types.ts";
import {
  FEDERATION_PROTOCOL_NAME,
  FEDERATION_PROTOCOL_VERSION,
  FEDERATION_SESSION_ID_MAX_LENGTH,
} from "./federation-types.ts";
import {
  isCanonicalFederationOriginId,
  isCanonicalFederationScopeAlias,
  isFederationCorrelationId,
} from "./federation-protocol.ts";

export const FEDERATION_SEND_TEXT_MAX_LENGTH = 32 * 1024;
export const FEDERATION_SEND_ERROR_MAX_LENGTH = 256;
export const FEDERATION_SEND_MAX_SEEN_IDS = 4096;
export const FEDERATION_SEND_MAX_PENDING = 4096;
/** Origin-side correlation deadline before a pending send becomes uncertain. Kept below
 * the ordinary client send timeout (with sweep-tick margin) so senders
 * observe structured remote uncertainty instead of a generic client-side
 * timeout. */
export const FEDERATION_SEND_TIMEOUT_MS = 8_500;

const CONTROL_OR_FORMAT_CHARACTERS = /[\p{Cc}\p{Cf}]/u;

export interface PeerSendMessagePayload {
  /** Original client message identity from the local sender. */
  id: string;
  timestamp: number;
  text: string;
  replyTo?: string;
  expectsReply?: boolean;
  completesAsk?: boolean;
  senderWaitMode?: "blocking" | "nonblocking";
}

export interface PeerSendRequest {
  type: "peer_send";
  protocol: typeof FEDERATION_PROTOCOL_NAME;
  version: typeof FEDERATION_PROTOCOL_VERSION;
  /** Sender broker's canonical origin id; must equal the link's remote origin. */
  originId: string;
  /** Broker-generated correlation id, unique per link. */
  sendId: string;
  /** Sender identity in the sender broker's own exported namespace. */
  senderScopeAlias: string;
  senderStableSessionId: string;
  /** Target identity in the destination broker's own namespace. */
  targetScopeAlias: string;
  targetStableSessionId: string;
  /** Only sent when peer-send-exact-v1 is negotiated; never silently ignored. */
  targetEndpointEpoch?: string;
  /** All three are required for the negotiated conversation envelope. */
  senderEndpointEpoch?: string;
  senderOriginEpoch?: string;
  targetOriginEpoch?: string;
  message: PeerSendMessagePayload;
}

export type PeerSendFailureCode =
  | "E_SEND_TARGET_NOT_FOUND"
  | "E_SEND_TARGET_DISCONNECTED"
  | "E_SEND_TARGET_REBOUND"
  | "E_SEND_UNAUTHORIZED"
  | "E_SEND_INVALID"
  | "E_SEND_DUPLICATE"
  | "E_SEND_UNSUPPORTED";

export type PeerSendResult =
  | {
      type: "peer_send_result";
      protocol: typeof FEDERATION_PROTOCOL_NAME;
      version: typeof FEDERATION_PROTOCOL_VERSION;
      /** Responder broker's canonical origin id. */
      originId: string;
      sendId: string;
      ok: true;
      deliveredAt: number;
    }
  | {
      type: "peer_send_result";
      protocol: typeof FEDERATION_PROTOCOL_NAME;
      version: typeof FEDERATION_PROTOCOL_VERSION;
      originId: string;
      sendId: string;
      ok: false;
      code: PeerSendFailureCode;
      error: string;
    };

export type FederationSendFrame = PeerSendRequest | PeerSendResult;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyKeys(value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []): boolean {
  for (const key of required) {
    if (!(key in value)) return false;
  }
  for (const key of Object.keys(value)) {
    if (!required.includes(key) && !optional.includes(key)) return false;
  }
  return true;
}

function isStableSessionId(value: unknown, conversation = false): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.length <= FEDERATION_SESSION_ID_MAX_LENGTH
    && !CONTROL_OR_FORMAT_CHARACTERS.test(value)
    && (conversation ? value.trim().length > 0 : value.trim() === value);
}

function isTimestamp(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

const PEER_SEND_FAILURE_CODES: readonly PeerSendFailureCode[] = [
  "E_SEND_TARGET_NOT_FOUND",
  "E_SEND_TARGET_DISCONNECTED",
  "E_SEND_TARGET_REBOUND",
  "E_SEND_UNAUTHORIZED",
  "E_SEND_INVALID",
  "E_SEND_DUPLICATE",
  "E_SEND_UNSUPPORTED",
];

export function isPeerSendRequest(value: unknown): value is PeerSendRequest {
  if (!isRecord(value) || !hasOnlyKeys(value, [
    "type",
    "protocol",
    "version",
    "originId",
    "sendId",
    "senderScopeAlias",
    "senderStableSessionId",
    "targetScopeAlias",
    "targetStableSessionId",
    "message",
  ], ["targetEndpointEpoch", "senderEndpointEpoch", "senderOriginEpoch", "targetOriginEpoch"])) return false;
  if (!isRecord(value.message)
    || !hasOnlyKeys(value.message, ["id", "timestamp", "text"], ["replyTo", "expectsReply", "completesAsk", "senderWaitMode"])) return false;
  const conversation = value.senderOriginEpoch !== undefined;
  if (conversation) {
    if (!isFederationCorrelationId(value.senderOriginEpoch) || !isFederationCorrelationId(value.senderEndpointEpoch)
      || !isFederationCorrelationId(value.targetOriginEpoch) || !isFederationCorrelationId(value.targetEndpointEpoch)
      || !decodeConversationMessageId(value.message.id)) return false;
  } else if (value.senderEndpointEpoch !== undefined || value.targetOriginEpoch !== undefined
    || Object.keys(value.message).some(key => !["id", "timestamp", "text"].includes(key))) return false;
  if (value.message.replyTo !== undefined && !decodeConversationMessageId(value.message.replyTo)) return false;
  if (value.message.expectsReply !== undefined && typeof value.message.expectsReply !== "boolean") return false;
  if (value.message.completesAsk !== undefined && (typeof value.message.completesAsk !== "boolean"
    || (value.message.completesAsk && !value.message.replyTo))) return false;
  if (value.message.senderWaitMode !== undefined && value.message.senderWaitMode !== "blocking"
    && value.message.senderWaitMode !== "nonblocking") return false;
  return value.type === "peer_send"
    && value.protocol === FEDERATION_PROTOCOL_NAME
    && value.version === FEDERATION_PROTOCOL_VERSION
    && isCanonicalFederationOriginId(value.originId)
    && isFederationCorrelationId(value.sendId)
    && isCanonicalFederationScopeAlias(value.senderScopeAlias)
    && isStableSessionId(value.senderStableSessionId, conversation)
    && isCanonicalFederationScopeAlias(value.targetScopeAlias)
    && isStableSessionId(value.targetStableSessionId, conversation)
    && (value.targetEndpointEpoch === undefined || isFederationCorrelationId(value.targetEndpointEpoch))
    && (conversation || isFederationCorrelationId(value.message.id))
    && isTimestamp(value.message.timestamp)
    && typeof value.message.text === "string"
    && value.message.text.length <= FEDERATION_SEND_TEXT_MAX_LENGTH;
}

export function isPeerSendResult(value: unknown): value is PeerSendResult {
  if (!isRecord(value) || !hasOnlyKeys(
    value,
    ["type", "protocol", "version", "originId", "sendId", "ok"],
    ["deliveredAt", "code", "error"],
  )) return false;
  if (value.type !== "peer_send_result"
    || value.protocol !== FEDERATION_PROTOCOL_NAME
    || value.version !== FEDERATION_PROTOCOL_VERSION
    || !isCanonicalFederationOriginId(value.originId)
    || !isFederationCorrelationId(value.sendId)) return false;
  if (value.ok === true) {
    return isTimestamp(value.deliveredAt);
  }
  if (value.ok !== false) return false;
  return PEER_SEND_FAILURE_CODES.includes(value.code as PeerSendFailureCode)
    && typeof value.error === "string"
    && value.error.length > 0
    && value.error.length <= FEDERATION_SEND_ERROR_MAX_LENGTH
    && !CONTROL_OR_FORMAT_CHARACTERS.test(value.error);
}

export function isFederationSendFrame(value: unknown): value is FederationSendFrame {
  if (!isRecord(value)) return false;
  return isPeerSendRequest(value) || isPeerSendResult(value);
}

/**
 * Destination-side bounded duplicate-send guard. sendIds are unique per link;
 * a replayed sendId is rejected rather than delivered twice. Eviction is
 * insertion-ordered and bounded so an abusive peer cannot grow state.
 */
export class PeerSendDedup {
  private readonly seen = new Set<string>();

  constructor(private readonly capacity = FEDERATION_SEND_MAX_SEEN_IDS) {}

  /** Returns true when sendId is new; false when it was already observed. */
  observe(sendId: string): boolean {
    if (this.seen.has(sendId)) return false;
    this.seen.add(sendId);
    if (this.seen.size > this.capacity) {
      const oldest = this.seen.values().next().value as string;
      this.seen.delete(oldest);
    }
    return true;
  }

  get size(): number {
    return this.seen.size;
  }
}

/** Origin-side bookkeeping linking a correlated peer send to its local sender. */
export interface PendingPeerSend {
  linkId: string;
  /** Original client message id used for delivery feedback. */
  messageId: string;
  /** Broker session key of the local sending session. */
  senderKey: string;
  /** Durable attempt barrier, independent of the per-link transport sendId. */
  dispatchId?: string;
  dispatchAlias?: string;
  recipient?: SessionInfo;
  /** Delivery fingerprint used for replay records once the result arrives. */
  fingerprint: string;
  createdAt: number;
  expiresAt: number;
}

/**
 * Bounded pending-send correlation table. Entries resolve exactly once, are
 * dropped with their link, and expire on a deadline so a silent peer cannot
 * pin broker state.
 */
export class PendingPeerSendTracker {
  private readonly pending = new Map<string, PendingPeerSend>();

  constructor(
    private readonly capacity = FEDERATION_SEND_MAX_PENDING,
    private readonly timeoutMs = FEDERATION_SEND_TIMEOUT_MS,
  ) {}

  add(
    sendId: string,
    entry: Omit<PendingPeerSend, "createdAt" | "expiresAt">,
    now = Date.now(),
  ): PendingPeerSend | undefined {
    if (this.pending.has(sendId) || this.pending.size >= this.capacity) return undefined;
    const pending: PendingPeerSend = {
      ...entry,
      createdAt: now,
      expiresAt: now + this.timeoutMs,
    };
    this.pending.set(sendId, pending);
    return pending;
  }

  peek(sendId: string): PendingPeerSend | undefined {
    return this.pending.get(sendId);
  }

  resolve(sendId: string): PendingPeerSend | undefined {
    const entry = this.pending.get(sendId);
    if (entry) this.pending.delete(sendId);
    return entry;
  }

  /** Drops and returns every pending send tied to a dead link. */
  dropLink(linkId: string): PendingPeerSend[] {
    const dropped: PendingPeerSend[] = [];
    for (const [sendId, entry] of this.pending) {
      if (entry.linkId !== linkId) continue;
      this.pending.delete(sendId);
      dropped.push(entry);
    }
    return dropped;
  }

  /** Drops and returns entries whose correlation deadline passed. */
  expire(now = Date.now()): PendingPeerSend[] {
    const expired: PendingPeerSend[] = [];
    for (const [sendId, entry] of this.pending) {
      if (entry.expiresAt > now) continue;
      this.pending.delete(sendId);
      expired.push(entry);
    }
    return expired;
  }

  get size(): number {
    return this.pending.size;
  }
}
