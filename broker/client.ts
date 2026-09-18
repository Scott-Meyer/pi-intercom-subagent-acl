import { EventEmitter } from "events";
import net from "net";
import { randomUUID } from "crypto";
import { writeMessage, createMessageReader } from "./framing.ts";
import { getBrokerConnectTarget, type BrokerConnectTarget } from "./paths.ts";
import { isMessage, isMessageControl, isMessageReceipt, isPeerCompactionNotice, isSessionInfo } from "./protocol.ts";
import { getParleyScopeId } from "../config.ts";
import { COMPACTION_AWARENESS_FEATURE, CONVERSATION_CONTRACT_FEATURE, FEDERATED_CONVERSATION_FEATURE, EXACT_SEND_FEATURE, EXTENSION_BUS_FEATURE, type DeliveryDetails } from "../types.ts";
import type {
  Attachment,
  BrokerMessage,
  ClientMessage,
  Message,
  MessageControl,
  MessageProvenance,
  MessageReceipt,
  SessionInfo,
  SessionRegistration,
  PreparedConversation,
} from "../types.ts";

export type { PreparedConversation } from "../types.ts";
export class ConversationPrepareError extends Error {
  constructor(readonly code: string, message: string, readonly outcomeKnown = true) {
    super(message); this.name = "ConversationPrepareError";
  }
}

export interface SendOptions {
  text: string;
  attachments?: Attachment[];
  replyTo?: string;
  completesAsk?: boolean;
  expectsReply?: boolean;
  senderWaitMode?: "blocking" | "nonblocking";
  /** Acceptance wait only; expiry is not proof of nondelivery or cancellation. */
  timeoutMs?: number;
  messageId?: string;
  supersedes?: string;
  retryOf?: string;
  provenance?: MessageProvenance;
  /** Stops retries that have not yet been written; it cannot retract an in-flight frame. */
  signal?: AbortSignal;
  /** Broadcast delivery never reads or advances direct-collaboration watermarks. */
  contactKind?: "direct" | "broadcast";
}

export interface SendResult extends DeliveryDetails {
  /** Full authored message ID, retained even on failure/uncertainty. For
   * cancellation this is the original message being withdrawn. */
  id: string;
  /** True means this operation was accepted, not that the model read the message. */
  delivered: boolean;
  reason?: string;
}

export interface CompactionRecordedResult {
  eventId: string;
  generation: number;
  compactedAt: number;
}

// ACL fork: result of a self-promotion "advertise" request/response exchange.
export interface AdvertiseResult {
  ok: boolean;
  name?: string;
  error?: string;
  code?: string;
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Liveness heartbeat interval. A half-open socket (peer killed with SIGKILL or
 * crashed without sending a FIN) stays "writable" indefinitely, so passive
 * close-event detection never fires and the client silently drops out of the
 * roster. The heartbeat actively round-trips a lightweight request and tears
 * down the socket if the broker does not respond within the timeout, letting
 * the existing onClose -> "disconnected" path drive reconnection.
 */
function getLivenessIntervalMs(): number {
  const raw = Number.parseInt(process.env.PI_PARLEY_LIVENESS_INTERVAL_MS ?? "", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : 30_000;
}

function getLivenessTimeoutMs(): number {
  const raw = Number.parseInt(process.env.PI_PARLEY_LIVENESS_TIMEOUT_MS ?? "", 10);
  return Number.isFinite(raw) && raw > 0 ? Math.min(raw, getLivenessIntervalMs()) : 5_000;
}

function connectToBrokerTarget(target: BrokerConnectTarget): net.Socket {
  return typeof target === "string"
    ? net.connect(target)
    : net.connect({ host: target.host, port: target.port });
}

export class ParleyClient extends EventEmitter {
  private socket: net.Socket | null = null;
  private _sessionId: string | null = null;
  private _selfSession: SessionInfo | null = null;
  private _features = new Set<string>();
  private pendingPreparations = new Map<string, { resolve: (r: PreparedConversation) => void; reject: (e: Error) => void }>();
  private pendingSends = new Map<string, { resolve: (r: SendResult) => void; reject: (e: Error) => void }>();
  private pendingCancellations = new Map<string, { messageId: string; resolve: (r: SendResult) => void; reject: (e: Error) => void }>();
  private pendingLists = new Map<string, { resolve: (sessions: SessionInfo[]) => void; reject: (e: Error) => void }>();
  private pendingAdvertise = new Map<string, { resolve: (result: AdvertiseResult) => void; reject: (e: Error) => void }>();
  private pendingCompactionReports = new Map<string, { resolve: (result: CompactionRecordedResult) => void; reject: (e: Error) => void }>();
  private nextSenderSequence = 1;
  private disconnecting = false;
  private disconnectError: Error | null = null;
  private livenessTimer: NodeJS.Timeout | null = null;
  private livenessInFlight = false;

  private failPending(error: Error): void {
    for (const pending of this.pendingPreparations.values()) pending.reject(error);
    this.pendingPreparations.clear();
    for (const pending of this.pendingSends.values()) {
      pending.reject(error);
    }
    this.pendingSends.clear();
    for (const pending of this.pendingCancellations.values()) pending.reject(error);
    this.pendingCancellations.clear();
    for (const pending of this.pendingAdvertise.values()) {
      pending.reject(error);
    }
    this.pendingAdvertise.clear();
    for (const pending of this.pendingLists.values()) {
      pending.reject(error);
    }
    this.pendingLists.clear();
    for (const pending of this.pendingCompactionReports.values()) {
      pending.reject(error);
    }
    this.pendingCompactionReports.clear();
  }

  get sessionId(): string | null {
    return this._sessionId;
  }

  supportsFeature(feature: string): boolean {
    return this._features.has(feature);
  }

  getSelfSession(): SessionInfo | undefined {
    return this._selfSession ? { ...this._selfSession } : undefined;
  }

  invalidateSelfSessionProjection(): void {
    this._selfSession = null;
  }

  isConnected(): boolean {
    const socket = this.socket;
    return Boolean(socket && this._sessionId && !this.disconnecting && !socket.destroyed && !socket.writableEnded && socket.writable);
  }

  /**
   * Start the liveness heartbeat. Must be called once the connection is
   * registered. The heartbeat periodically round-trips a lightweight list
   * request and tears down the socket if the broker does not respond within
   * the liveness timeout, so a half-open connection is detected within a
   * bounded window instead of silently lingering forever.
   */
  private startLivenessHeartbeat(): void {
    this.stopLivenessHeartbeat();
    this.livenessTimer = setInterval(() => {
      this.runLivenessProbe();
    }, getLivenessIntervalMs());
    this.livenessTimer.unref?.();
  }

  private stopLivenessHeartbeat(): void {
    if (this.livenessTimer) {
      clearInterval(this.livenessTimer);
      this.livenessTimer = null;
    }
    this.livenessInFlight = false;
  }

  private async runLivenessProbe(): Promise<void> {
    if (this.livenessInFlight || !this.isConnected()) {
      return;
    }
    this.livenessInFlight = true;
    try {
      await this.listSessions({ timeoutMs: getLivenessTimeoutMs() });
    } catch (error) {
      // A timeout or write error means the socket is half-open: the broker is
      // gone but the OS never delivered a close event. Destroy the socket so
      // the onClose handler emits "disconnected" and the extension reconnects.
      const socket = this.socket;
      if (socket && !socket.destroyed) {
        this.disconnectError = toError(error);
        socket.destroy();
      }
    } finally {
      this.livenessInFlight = false;
    }
  }

  private requireActiveSocket(): net.Socket {
    if (this.disconnecting) {
      throw new Error("Client disconnecting");
    }

    const socket = this.socket;
    if (!socket || !this._sessionId) {
      throw new Error("Not connected");
    }

    if (socket.destroyed || socket.writableEnded || !socket.writable) {
      throw new Error("Client disconnected");
    }

    return socket;
  }

  /** Establishes the current conversation and exact-target contract. Registration
   * rejects brokers that do not advertise both; other capabilities are optional. */
  connect(session: SessionRegistration, sessionId?: string): Promise<void> {
    if (this.socket) {
      return Promise.reject(new Error("Already connected"));
    }

    return new Promise((resolve, reject) => {
      let socket: net.Socket;
      let target: BrokerConnectTarget;
      try {
        target = getBrokerConnectTarget();
        socket = connectToBrokerTarget(target);
      } catch (error) {
        reject(toError(error));
        return;
      }
      this.socket = socket;
      this.disconnectError = null;
      let settled = false;
      const timeout = setTimeout(() => {
        if (!this._sessionId) {
          cleanupConnectionAttempt();
          cleanupSocketListeners();
          if (this.socket === socket) {
            this.socket = null;
          }
          socket.destroy();
          reject(new Error("Connection timeout"));
        }
      }, 10000);
      
      let connectionEstablished = false;
      
      const onRegistered = () => {
        settled = true;
        connectionEstablished = true;
        cleanupConnectionAttempt();
        this.startLivenessHeartbeat();
        resolve();
      };
      
      const onError = (err: Error) => {
        settled = true;
        cleanupConnectionAttempt();
        cleanupSocketListeners();
        if (this.socket === socket) {
          this.socket = null;
        }
        socket.destroy();
        reject(err);
      };
      
      const onClose = () => {
        const wasConnecting = !settled && !this._sessionId;
        const wasDisconnecting = this.disconnecting;
        const disconnectError = this.disconnectError ?? new Error("Client disconnected");
        this.disconnecting = false;
        this.stopLivenessHeartbeat();
        cleanupConnectionAttempt();
        cleanupSocketListeners();
        this.failPending(disconnectError);
        if (this.socket === socket) {
          this.socket = null;
        }
        this._sessionId = null;
        this._selfSession = null;
        this._features.clear();
        this.disconnectError = null;
        if (connectionEstablished && !wasDisconnecting) {
          this.emit("disconnected", disconnectError);
        }
        if (wasConnecting) {
          reject(new Error("Connection closed before registration"));
        }
      };

      const onSocketError = (err: Error) => {
        if (connectionEstablished) {
          this.disconnectError = err;
          // A socket error after registration means the connection is dead.
          // Destroy the socket so onClose fires and emits "disconnected",
          // driving the extension's reconnect path. Without this, a half-open
          // socket can linger with isConnected() returning true.
          if (!socket.destroyed) {
            socket.destroy();
          }
          if (this.listenerCount("error") > 0) this.emit("error", err);
        }
      };

      const onReaderError = (error: Error) => {
        const protocolError = new Error(`Parley protocol error: ${error.message}`, { cause: error });
        if (!connectionEstablished) {
          onError(protocolError);
          return;
        }
        this.disconnectError = protocolError;
        socket.destroy();
        if (this.listenerCount("error") > 0) this.emit("error", protocolError);
      };

      const reader = createMessageReader((msg) => {
        this.handleBrokerMessage(msg);
      }, onReaderError);
      
      const cleanupConnectionAttempt = () => {
        this.off("_registered", onRegistered);
        socket.off("error", onError);
        clearTimeout(timeout);
      };

      const cleanupSocketListeners = () => {
        socket.off("data", reader);
        socket.off("error", onSocketError);
        socket.off("close", onClose);
      };
      
      socket.on("data", reader);
      socket.on("error", onError);
      socket.on("close", onClose);
      
      socket.on("error", onSocketError);
      this.once("_registered", onRegistered);
      
      try {
        const scopeId = getParleyScopeId();
        writeMessage(socket, {
          type: "register",
          session,
          ...(sessionId ? { sessionId } : {}),
          ...(scopeId ? { scopeId } : {}),
          clientFeatures: [COMPACTION_AWARENESS_FEATURE, CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE, FEDERATED_CONVERSATION_FEATURE],
          ...(typeof target === "string" ? {} : { stateId: target.stateId }),
        });
      } catch (error) {
        cleanupConnectionAttempt();
        cleanupSocketListeners();
        if (this.socket === socket) {
          this.socket = null;
        }
        socket.destroy();
        reject(toError(error));
      }
    });
  }

  private takePendingDelivery(message: Record<string, unknown>) {
    if (message.requestId !== undefined) {
      if (typeof message.requestId !== "string" || message.requestId.length === 0) {
        throw new Error("Invalid delivery requestId");
      }
      if (
        (message.type === "delivered" && (message.cancellation === undefined || message.delivery !== "socket_delivered"))
        || (message.type === "delivery_failed" && message.cancellation !== undefined)
        || message.peerCompaction !== undefined
        || message.contactToken !== undefined
      ) {
        throw new Error("Invalid cancellation acknowledgement");
      }
      const pending = this.pendingCancellations.get(message.requestId);
      if (!pending) return undefined; // A late cancellation ACK cannot settle a send.
      if (pending.messageId !== message.messageId) {
        throw new Error("Cancellation acknowledgement messageId does not match requestId");
      }
      this.pendingCancellations.delete(message.requestId);
      return pending;
    }
    const pending = this.pendingSends.get(message.messageId as string);
    if (pending) this.pendingSends.delete(message.messageId as string);
    return pending;
  }

  private deliveryMetadata(message: Record<string, unknown>): Pick<DeliveryDetails, "recipient" | "cancellation"> {
    const { recipient, cancellation } = message;
    if (recipient !== undefined && !isSessionInfo(recipient)) throw new Error("Invalid delivery recipient");
    if (cancellation !== undefined && cancellation !== "removed_from_mailbox" && cancellation !== "withdrawal_requested" && cancellation !== "not_delivered") {
      throw new Error("Invalid cancellation state");
    }
    return {
      ...(recipient !== undefined ? { recipient: recipient as SessionInfo } : {}),
      ...(cancellation !== undefined ? { cancellation } : {}),
    };
  }

  private handleBrokerMessage(msg: unknown): void {
    if (typeof msg !== "object" || msg === null || !("type" in msg) || typeof msg.type !== "string") {
      throw new Error("Invalid broker message");
    }

    const brokerMessage = msg as { type: string } & Record<string, unknown>;

    if (this._sessionId === null && brokerMessage.type !== "registered" && brokerMessage.type !== "error") {
      throw new Error(`Received ${brokerMessage.type} before registered`);
    }

    switch (brokerMessage.type) {
      case "registered": {
        if (typeof brokerMessage.sessionId !== "string") {
          throw new Error("Invalid registered message");
        }

        if (this._sessionId !== null) {
          throw new Error("Received duplicate registered message");
        }

        if (
          !Array.isArray(brokerMessage.features)
          || !brokerMessage.features.every((feature) => typeof feature === "string")
        ) {
          throw new Error("Invalid registered features");
        }
        const features = new Set(brokerMessage.features as string[]);
        for (const required of [CONVERSATION_CONTRACT_FEATURE, EXACT_SEND_FEATURE]) {
          if (!features.has(required)) throw new Error(`Missing required broker feature: ${required}`);
        }
        if (
          brokerMessage.session !== undefined
          && (!isSessionInfo(brokerMessage.session) || brokerMessage.session.id !== brokerMessage.sessionId)
        ) {
          throw new Error("Invalid registered session");
        }

        this._sessionId = brokerMessage.sessionId;
        this._selfSession = brokerMessage.session as SessionInfo | undefined ?? null;
        this._features = features;
        const registered: BrokerMessage = {
          type: "registered",
          sessionId: brokerMessage.sessionId,
          features: [...this._features],
          ...(this._selfSession ? { session: { ...this._selfSession } } : {}),
        };
        this.emit("broker_message", registered);
        this.emit("_registered", registered);
        break;
      }

      case "direct_contact_recorded": {
        if (typeof brokerMessage.token !== "string") {
          throw new Error("Invalid direct_contact_recorded message");
        }
        this.emit("broker_message", { type: "direct_contact_recorded", token: brokerMessage.token } satisfies BrokerMessage);
        break;
      }

      case "direct_contact_unknown": {
        if (typeof brokerMessage.token !== "string") {
          throw new Error("Invalid direct_contact_unknown message");
        }
        this.emit("broker_message", { type: "direct_contact_unknown", token: brokerMessage.token } satisfies BrokerMessage);
        break;
      }

      case "compaction_recorded": {
        const { eventId, generation, compactedAt } = brokerMessage;
        if (
          typeof eventId !== "string"
          || !Number.isSafeInteger(generation)
          || (generation as number) < 1
          || !Number.isSafeInteger(compactedAt)
          || (compactedAt as number) < 0
        ) {
          throw new Error("Invalid compaction_recorded message");
        }
        const pending = this.pendingCompactionReports.get(eventId);
        if (!pending) return;
        this.pendingCompactionReports.delete(eventId);
        pending.resolve({
          eventId,
          generation: generation as number,
          compactedAt: compactedAt as number,
        });
        break;
      }

      case "compaction_record_failed": {
        const { eventId, error } = brokerMessage;
        if (typeof eventId !== "string" || typeof error !== "string") {
          throw new Error("Invalid compaction_record_failed message");
        }
        const pending = this.pendingCompactionReports.get(eventId);
        if (!pending) return;
        this.pendingCompactionReports.delete(eventId);
        pending.reject(new Error(error));
        break;
      }

      case "sessions": {
        const { requestId, sessions } = brokerMessage;
        if (typeof requestId !== "string" || !Array.isArray(sessions) || !sessions.every(isSessionInfo)) {
          throw new Error("Invalid sessions message");
        }

        const pending = this.pendingLists.get(requestId);
        if (!pending) {
          // Late list responses can still arrive after the caller has already timed out.
          return;
        }

        this.pendingLists.delete(requestId);
        this._selfSession = sessions.find((session) => session.id === this._sessionId) ?? this._selfSession;
        pending.resolve(sessions);
        break;
      }

      case "advertise_result": {
        const { requestId, ok, name, error, code } = brokerMessage;
        if (typeof requestId !== "string" || typeof ok !== "boolean") {
          throw new Error("Invalid advertise_result message");
        }
        const pending = this.pendingAdvertise.get(requestId);
        if (!pending) {
          return;
        }
        this.pendingAdvertise.delete(requestId);
        if (ok && typeof name === "string" && this._selfSession) {
          this._selfSession = { ...this._selfSession, name, advertised: true };
        }
        pending.resolve({
          ok,
          ...(typeof name === "string" ? { name } : {}),
          ...(typeof error === "string" ? { error } : {}),
          ...(typeof code === "string" ? { code } : {}),
        });
        break;
      }

      case "message": {
        const { from, message } = brokerMessage;
        if (!isSessionInfo(from) || !isMessage(message)) {
          throw new Error("Invalid message event");
        }

        this.emit("message", from, message);
        break;
      }

      case "delivered": {
        const { messageId, delivery, retryable, outcomeKnown, peerCompaction, contactToken } = brokerMessage;
        if (
          typeof messageId !== "string"
          || (delivery !== "socket_delivered" && delivery !== "queued")
          || retryable !== false
          || outcomeKnown !== true
          || (peerCompaction !== undefined && !isPeerCompactionNotice(peerCompaction))
          || (contactToken !== undefined && typeof contactToken !== "string")
        ) {
          throw new Error("Invalid delivered message");
        }

        if (brokerMessage.code !== undefined && typeof brokerMessage.code !== "string") throw new Error("Invalid delivery code");
        const metadata = this.deliveryMetadata(brokerMessage);
        const pending = this.takePendingDelivery(brokerMessage);
        if (!pending) {
          // Late responses are harmless once the caller has already timed out.
          return;
        }

        if (typeof contactToken === "string" && peerCompaction === undefined) {
          this.acknowledgeDirectContact(contactToken);
        }
        pending.resolve({
          ...metadata, id: messageId, delivered: true, delivery,
          retryable: false, outcomeKnown: true,
          ...(typeof brokerMessage.code === "string" ? { code: brokerMessage.code } : {}),
          ...(peerCompaction !== undefined ? { peerCompaction } : {}),
          ...(typeof contactToken === "string" ? { contactToken } : {}),
        });
        break;
      }

      case "delivery_failed": {
        const { messageId, reason, delivery, retryable, outcomeKnown } = brokerMessage;
        if (
          typeof messageId !== "string"
          || typeof reason !== "string"
          || (delivery !== "failed" && delivery !== "unknown")
          || typeof retryable !== "boolean"
          || outcomeKnown !== (delivery !== "unknown")
          || (delivery === "unknown" && retryable !== false)
        ) {
          throw new Error("Invalid delivery_failed message");
        }

        if (brokerMessage.code !== undefined && typeof brokerMessage.code !== "string") throw new Error("Invalid delivery code");
        const metadata = this.deliveryMetadata(brokerMessage);
        const pending = this.takePendingDelivery(brokerMessage);
        if (!pending) {
          // Late responses are harmless once the caller has already timed out.
          return;
        }

        pending.resolve({
          ...metadata, id: messageId, delivered: false, reason, delivery, retryable,
          outcomeKnown: outcomeKnown as boolean,
          ...(typeof brokerMessage.code === "string" ? { code: brokerMessage.code } : {}),
        });
        break;
      }

      case "conversation_prepared": {
        const prepared = brokerMessage.prepared as PreparedConversation | undefined;
        if (typeof brokerMessage.requestId !== "string" || !prepared || typeof prepared.messageId !== "string"
          || !isSessionInfo(prepared.author) || !isSessionInfo(prepared.recipient)) throw new Error("Invalid conversation preparation");
        const pending = this.pendingPreparations.get(brokerMessage.requestId);
        this.pendingPreparations.delete(brokerMessage.requestId);
        pending?.resolve(prepared);
        break;
      }
      case "conversation_prepare_failed": {
        if (typeof brokerMessage.requestId !== "string" || typeof brokerMessage.code !== "string"
          || typeof brokerMessage.error !== "string" || (brokerMessage.outcomeKnown !== undefined && typeof brokerMessage.outcomeKnown !== "boolean")) {
          throw new Error("Invalid conversation preparation failure");
        }
        const pending = this.pendingPreparations.get(brokerMessage.requestId);
        this.pendingPreparations.delete(brokerMessage.requestId);
        pending?.reject(new ConversationPrepareError(brokerMessage.code, brokerMessage.error, brokerMessage.outcomeKnown ?? true));
        break;
      }

      case "message_receipt": {
        if (!isSessionInfo(brokerMessage.from) || !isMessageReceipt(brokerMessage.receipt)) {
          throw new Error("Invalid message_receipt event");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("message_receipt", brokerMessage.from, brokerMessage.receipt);
        break;
      }

      case "message_control": {
        if (!isSessionInfo(brokerMessage.from) || !isMessageControl(brokerMessage.control)) {
          throw new Error("Invalid message_control event");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("message_control", brokerMessage.from, brokerMessage.control);
        break;
      }

      case "session_joined": {
        if (!isSessionInfo(brokerMessage.session)) {
          throw new Error("Invalid session_joined message");
        }

        if (brokerMessage.session.id === this._sessionId) this._selfSession = brokerMessage.session;
        const message: BrokerMessage = { type: "session_joined", session: brokerMessage.session };
        this.emit("broker_message", message);
        this.emit("session_joined", brokerMessage.session);
        break;
      }

      case "session_left": {
        if (typeof brokerMessage.sessionId !== "string") {
          throw new Error("Invalid session_left message");
        }

        const message: BrokerMessage = { type: "session_left", sessionId: brokerMessage.sessionId };
        this.emit("broker_message", message);
        this.emit("session_left", brokerMessage.sessionId);
        break;
      }

      case "presence_update": {
        if (!isSessionInfo(brokerMessage.session)) {
          throw new Error("Invalid presence_update message");
        }

        if (brokerMessage.session.id === this._sessionId) this._selfSession = brokerMessage.session;
        const message: BrokerMessage = { type: "presence_update", session: brokerMessage.session };
        this.emit("broker_message", message);
        this.emit("presence_update", brokerMessage.session);
        break;
      }

      case "error": {
        if (typeof brokerMessage.error !== "string") {
          throw new Error("Invalid error message");
        }

        if (this._sessionId === null) {
          throw new Error(brokerMessage.error);
        }
        this.emit("error", new Error(brokerMessage.error));
        break;
      }

      case "extension_owner": {
        const hasOwnerId = typeof brokerMessage.ownerId === "string";
        const hasOwnerEpoch = typeof brokerMessage.ownerEpoch === "string";
        if (
          typeof brokerMessage.namespace !== "string"
          || hasOwnerId !== hasOwnerEpoch
          || (brokerMessage.ownerId !== undefined && !hasOwnerId)
          || (brokerMessage.ownerEpoch !== undefined && !hasOwnerEpoch)
        ) {
          throw new Error("Invalid extension_owner message");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("extension_owner", brokerMessage);
        break;
      }

      case "extension_message": {
        const hasOwnerId = typeof brokerMessage.ownerId === "string";
        const hasOwnerEpoch = typeof brokerMessage.ownerEpoch === "string";
        if (
          typeof brokerMessage.namespace !== "string"
          || typeof brokerMessage.fromSessionId !== "string"
          || hasOwnerId !== hasOwnerEpoch
          || (brokerMessage.ownerId !== undefined && !hasOwnerId)
          || (brokerMessage.ownerEpoch !== undefined && !hasOwnerEpoch)
        ) {
          throw new Error("Invalid extension_message");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("extension_message", brokerMessage);
        break;
      }

      case "extension_state": {
        if (
          typeof brokerMessage.namespace !== "string"
          || !Number.isSafeInteger(brokerMessage.revision)
          || Number(brokerMessage.revision) < 0
        ) {
          throw new Error("Invalid extension_state");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("extension_state", brokerMessage);
        break;
      }

      case "extension_state_result": {
        if (
          typeof brokerMessage.namespace !== "string"
          || typeof brokerMessage.committed !== "boolean"
          || !Number.isSafeInteger(brokerMessage.revision)
          || Number(brokerMessage.revision) < 0
          || (brokerMessage.reason !== undefined && typeof brokerMessage.reason !== "string")
        ) {
          throw new Error("Invalid extension_state_result");
        }
        this.emit("broker_message", brokerMessage as BrokerMessage);
        this.emit("extension_state_result", brokerMessage);
        break;
      }

      default:
        throw new Error(`Unknown broker message type: ${brokerMessage.type}`);
    }
  }

  async disconnect(): Promise<void> {
    const socket = this.socket;
    if (!socket) {
      return;
    }

    this.disconnecting = true;
    this.disconnectError = null;
    this.stopLivenessHeartbeat();
    this.failPending(new Error("Client disconnected"));

    await new Promise<void>((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        socket.off("close", onClose);
        socket.off("error", onError);
        resolve();
      };
      const onClose = () => finish();
      const onError = () => {
        socket.destroy();
      };
      const timeout = setTimeout(() => {
        socket.destroy();
      }, 2000);

      socket.once("close", onClose);
      socket.once("error", onError);

      try {
        writeMessage(socket, { type: "unregister" });
        socket.end();
      } catch {
        // Disconnect should still finish even if the unregister write fails.
        socket.destroy();
      }
    });
  }

  updateExtensionCapabilities(extensions: SessionRegistration["extensions"]): void {
    if (!this.supportsFeature(EXTENSION_BUS_FEATURE)) return;
    const socket = this.requireActiveSocket();
    writeMessage(socket, { type: "extension_capabilities_update", extensions: extensions ?? [] });
  }

  // ACL fork: lets a subagent self-promote to full main-level visibility
  // under a chosen name. Broker-authoritative: the broker validates
  // eligibility (must currently be a restricted subagent) and name
  // uniqueness (including against live session IDs) before applying it, so
  // a client can never simply assert `advertised: true`.
  advertise(name: string, options: { timeoutMs?: number } = {}): Promise<AdvertiseResult> {
    let socket: net.Socket;
    try {
      socket = this.requireActiveSocket();
    } catch (error) {
      return Promise.reject(toError(error));
    }

    return new Promise((resolve, reject) => {
      const requestId = randomUUID();
      const wrappedResolve = (result: AdvertiseResult) => {
        clearTimeout(timeout);
        resolve(result);
      };
      const wrappedReject = (error: Error) => {
        clearTimeout(timeout);
        reject(error);
      };
      const timeout = setTimeout(() => {
        if (this.pendingAdvertise.has(requestId)) {
          this.pendingAdvertise.delete(requestId);
          wrappedReject(new Error("Advertise timeout"));
        }
      }, options.timeoutMs ?? 5000);
      this.pendingAdvertise.set(requestId, { resolve: wrappedResolve, reject: wrappedReject });
      try {
        writeMessage(socket, { type: "advertise", requestId, name });
      } catch (error) {
        clearTimeout(timeout);
        this.pendingAdvertise.delete(requestId);
        reject(toError(error));
      }
    });
  }

  listSessions(options: { timeoutMs?: number } = {}): Promise<SessionInfo[]> {
    let socket: net.Socket;
    try {
      socket = this.requireActiveSocket();
    } catch (error) {
      return Promise.reject(toError(error));
    }
    
    return new Promise((resolve, reject) => {
      const requestId = randomUUID();
      const wrappedResolve = (sessions: SessionInfo[]) => {
        clearTimeout(timeout);
        resolve(sessions);
      };
      const wrappedReject = (error: Error) => {
        clearTimeout(timeout);
        reject(error);
      };
      const timeout = setTimeout(() => {
        if (this.pendingLists.has(requestId)) {
          this.pendingLists.delete(requestId);
          wrappedReject(new Error("List sessions timeout"));
        }
      }, options.timeoutMs ?? 5000);
      this.pendingLists.set(requestId, { resolve: wrappedResolve, reject: wrappedReject });
      try {
        writeMessage(socket, { type: "list", requestId });
      } catch (error) {
        clearTimeout(timeout);
        this.pendingLists.delete(requestId);
        reject(toError(error));
      }
    });
  }

  /** Allocate/recheck the canonical retained identity BEFORE installing reply waiters.
   * No message is dispatched. Send the returned recipient snapshot and messageId;
   * the broker binds both author and recipient incarnations again at delivery.
   * A supplied qualified ID stays unchanged (including on failed rechecks).
   * A retained scalar ID is checked before conversion; earlier uncertainty is
   * reported through ConversationPrepareError.outcomeKnown, never rearmed. */
  async prepareConversation(recipient: SessionInfo | string, options: { messageId?: string; timeoutMs?: number } = {}): Promise<PreparedConversation> {
    const recheck = options.messageId !== undefined;
    const unavailable = (error: unknown): ConversationPrepareError => error instanceof ConversationPrepareError ? error
      : new ConversationPrepareError("E_PREPARE_TRANSPORT", `Conversation preflight unavailable; no new delivery was attempted: ${toError(error).message}`, !recheck);
    let socket: net.Socket;
    try { socket = this.requireActiveSocket(); }
    catch (error) { throw unavailable(error); }
    const requestId = randomUUID();
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingPreparations.delete(requestId);
        reject(new ConversationPrepareError("E_PREPARE_TIMEOUT", "Conversation preflight timed out; no new delivery was attempted", !recheck));
      }, options.timeoutMs ?? 5000);
      this.pendingPreparations.set(requestId, {
        resolve: result => { clearTimeout(timeout); resolve(result); },
        reject: error => { clearTimeout(timeout); reject(unavailable(error)); },
      });
      try {
        writeMessage(socket, { type: "prepare_conversation", requestId,
          to: typeof recipient === "string" ? recipient : recipient.id,
          ...(typeof recipient !== "string" && recipient.endpointEpoch ? { targetEpoch: recipient.endpointEpoch } : {}),
          ...(options.messageId !== undefined ? { messageId: options.messageId } : {}),
        });
      } catch (error) {
        this.pendingPreparations.delete(requestId);
        clearTimeout(timeout);
        reject(unavailable(error));
      }
    });
  }

  async send(to: string, options: SendOptions): Promise<SendResult> {
    return this.sendInternal(to, options);
  }

  /**
   * Send to a session from a caller-owned roster snapshot without listing the
   * roster again. Multi-recipient callers can therefore expand visibility once,
   * while every recipient still gets an ordinary message ID, delivery record,
   * receipt route, rate-limit charge, and exact-endpoint rebound check. An
   * endpoint epoch supplied by the caller is never silently re-resolved.
   * Snapshots without an epoch, or whose remote peer cannot enforce endpoint
   * pins, fail closed. Ordinary send() discovery can use legacy federation.
   */
  async sendToSession(session: SessionInfo, options: SendOptions): Promise<SendResult> {
    const retainedIdentity = options.messageId !== undefined;
    if (!session.endpointEpoch) {
      const outcomeKnown = options.messageId === undefined;
      return {
        id: options.messageId ?? randomUUID(), delivered: false, delivery: outcomeKnown ? "failed" : "unknown",
        outcomeKnown, retryable: false, code: "E_INVALID_TARGET",
        reason: "Session snapshot has no endpoint epoch; exact delivery cannot be guaranteed",
      };
    }
    if (session.federation?.conversation && !options.messageId?.startsWith("oqm1.")) {
      try {
        const prepared = await this.prepareConversation(session, { messageId: options.messageId });
        options = { ...options, messageId: prepared.messageId };
      } catch (error) {
        const outcomeKnown = error instanceof ConversationPrepareError ? error.outcomeKnown : options.messageId === undefined;
        return { id: options.messageId ?? randomUUID(), delivered: false, delivery: outcomeKnown ? "failed" : "unknown", outcomeKnown,
          retryable: false, code: error instanceof ConversationPrepareError ? error.code : "E_CONVERSATION_PREPARE", reason: toError(error).message };
      }
    }
    return this.sendInternal(session.id, options, { id: session.id, epoch: session.endpointEpoch }, retainedIdentity);
  }

  private async sendInternal(
    to: string,
    options: SendOptions,
    rosterTarget?: { id: string; epoch: string },
    retainedIdentity = options.messageId !== undefined,
  ): Promise<SendResult> {
    let messageId = options.messageId ?? randomUUID();
    const failedBeforeSend = (error: unknown, code = "E_NOT_CONNECTED"): SendResult => {
      const preparation = error instanceof ConversationPrepareError;
      const outcomeKnown = preparation ? error.outcomeKnown : !retainedIdentity;
      return { id: messageId, delivered: false, delivery: outcomeKnown ? "failed" : "unknown", outcomeKnown,
        retryable: !preparation && outcomeKnown, code, reason: toError(error).message };
    };
    let socket: net.Socket;
    try {
      socket = this.requireActiveSocket();
    } catch (error) {
      return failedBeforeSend(error);
    }
    const message: Message = {
      id: messageId,
      timestamp: Date.now(),
      senderSequence: this.nextSenderSequence++,
      supersedes: options.supersedes,
      retryOf: options.retryOf,
      replyTo: options.replyTo,
      completesAsk: options.completesAsk,
      expectsReply: options.expectsReply,
      senderWaitMode: options.senderWaitMode,
      provenance: options.provenance,
      content: { text: options.text, attachments: options.attachments },
    };
    const cancelledResult = (): SendResult => failedBeforeSend("Send cancelled before the next delivery attempt", "E_CANCELLED");
    const unknownResult = (error: unknown, code = "E_DELIVERY_UNKNOWN"): SendResult => ({
      id: messageId, delivered: false, delivery: "unknown", outcomeKnown: false,
      retryable: false, code,
      reason: `${toError(error).message}; delivery may have occurred. The message has not been withdrawn.`,
    });
    const sendOnce = (targetId?: string, targetEpoch?: string): Promise<SendResult> => {
      if (options.signal?.aborted) return Promise.resolve(cancelledResult());
      if (this.pendingSends.has(messageId)) return Promise.resolve(unknownResult("This message ID already has an in-flight send", "E_MESSAGE_IN_FLIGHT"));
      return new Promise((resolve) => {
        const wrappedResolve = (result: SendResult) => {
          clearTimeout(timeout);
          resolve(result);
        };
        const wrappedReject = (error: Error) => wrappedResolve(unknownResult(error));
        const timeout = setTimeout(() => {
          this.pendingSends.delete(messageId);
          wrappedResolve(unknownResult("Send acknowledgement timed out", "E_SEND_TIMEOUT"));
        }, options.timeoutMs ?? 10_000);
        this.pendingSends.set(messageId, { resolve: wrappedResolve, reject: wrappedReject });
        try {
          writeMessage(socket, {
            type: "send", to, message,
            ...(targetId && targetEpoch ? { targetId, targetEpoch, targetMode: rosterTarget === undefined && !messageId.startsWith("oqm1.") ? "resolved" : "snapshot" } : {}),
            ...(options.contactKind ? { contactKind: options.contactKind } : {}),
          });
        } catch (error) {
          this.pendingSends.delete(messageId);
          wrappedReject(toError(error));
        }
      });
    };
    if (options.replyTo && rosterTarget === undefined && !options.replyTo.startsWith("oqm1.") && !to.startsWith("oqs1.")) return sendOnce();
    const resolveTarget = async (): Promise<{ id: string; epoch: string; session: SessionInfo } | null> => {
      const sessions = await this.listSessions();
      const byId = sessions.find((session) => session.id === to);
      const byName = byId ? [] : sessions.filter((session) => session.name?.toLowerCase() === to.toLowerCase());
      const byPrefix = byId || byName.length > 0 ? [] : sessions.filter((session) => session.id.startsWith(to));
      const matches = byId ? [byId] : byName.length > 0 ? byName : byPrefix;
      const target = matches.length === 1 ? matches[0]! : null;
      return target?.endpointEpoch ? { id: target.id, epoch: target.endpointEpoch, session: target } : null;
    };
    // Discovery failures happen before any send, and retain the caller's handle.
    try {
      const target: { id: string; epoch: string; session?: SessionInfo } | null = rosterTarget === undefined ? await resolveTarget() : rosterTarget;
      if (options.signal?.aborted) return cancelledResult();
      if (!target) return sendOnce();
      if (target.session?.federation?.conversation && !messageId.startsWith("oqm1.")) {
        try {
          const prepared = await this.prepareConversation(target.session, { messageId: options.messageId });
          messageId = prepared.messageId;
          message.id = messageId;
          // Negotiated conversations are pinned, including ordinary notifications.
          // Rebound never silently mints a new author-qualified instruction.
          return sendOnce(prepared.recipient.id, prepared.recipient.endpointEpoch);
        } catch (error) {
          return failedBeforeSend(error, error instanceof ConversationPrepareError ? error.code : "E_CONVERSATION_PREPARE");
        }
      }
      const result = await sendOnce(target.id, target.epoch);
      // Never turn an uncertain, already-written attempt into known cancellation.
      if (rosterTarget !== undefined || messageId.startsWith("oqm1.") || result.code !== "E_TARGET_REBOUND" || !result.outcomeKnown || options.signal?.aborted) return result;
      const reboundTarget = await resolveTarget();
      if (options.signal?.aborted) return cancelledResult();
      return reboundTarget ? sendOnce(reboundTarget.id, reboundTarget.epoch) : result;
    } catch (error) {
      return failedBeforeSend(error, "E_TARGET_RESOLUTION");
    }
  }

  /** Withdraw a message. A live endpoint can only receive a withdrawal notice;
   * already-executed work cannot be retracted. An unknown result retains the ID. */
  cancelMessage(messageId: string, options: { timeoutMs?: number } = {}): Promise<SendResult> {
    let socket: net.Socket;
    try {
      socket = this.requireActiveSocket();
    } catch (error) {
      return Promise.resolve({ id: messageId, delivered: false, delivery: "failed", outcomeKnown: true,
        retryable: true, code: "E_NOT_CONNECTED", reason: toError(error).message });
    }
    const requestId = randomUUID();
    return new Promise((resolve) => {
      const wrappedResolve = (result: SendResult) => {
        clearTimeout(timeout);
        resolve(result);
      };
      const wrappedReject = (error: Error) => wrappedResolve({
        id: messageId, delivered: false, delivery: "unknown", outcomeKnown: false,
        retryable: false, code: "E_CANCELLATION_UNKNOWN",
        reason: `${error.message}; withdrawal may or may not have reached the recipient.`,
      });
      const timeout = setTimeout(() => {
        this.pendingCancellations.delete(requestId);
        wrappedReject(new Error("Cancel acknowledgement timed out"));
      }, options.timeoutMs ?? 10_000);
      this.pendingCancellations.set(requestId, { messageId, resolve: wrappedResolve, reject: wrappedReject });
      try {
        writeMessage(socket, { type: "cancel_message", messageId, requestId });
      } catch (error) {
        this.pendingCancellations.delete(requestId);
        wrappedReject(toError(error));
      }
    });
  }

  sendMessageReceipt(receipt: MessageReceipt): void {
    if (this.disconnecting) {
      return;
    }

    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) {
      return;
    }

    writeMessage(socket, { type: "message_receipt", receipt });
  }

  cancelAsk(messageId: string): void {
    if (this.disconnecting) {
      return;
    }

    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) {
      return;
    }

    try {
      writeMessage(socket, { type: "cancel_ask", messageId });
    } catch {
      // Cancellation is best-effort; local waiter cleanup must still proceed.
    }
  }

  acknowledgeSendContact(result: Pick<SendResult, "contactToken">): void {
    if (result.contactToken) this.acknowledgeDirectContact(result.contactToken);
  }

  acknowledgeMessageContact(message: Pick<Message, "contactToken">): void {
    if (message.contactToken) this.acknowledgeDirectContact(message.contactToken);
  }

  acknowledgeContactToken(token: string): void {
    if (token) this.acknowledgeDirectContact(token);
  }

  private acknowledgeDirectContact(token: string): void {
    if (!this.supportsFeature(COMPACTION_AWARENESS_FEATURE) || this.disconnecting) return;
    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) return;
    try {
      writeMessage(socket, { type: "direct_contact_seen", token });
    } catch {
      // The peer notice remains useful. Missing acknowledgment leaves the
      // broker watermark pending, preferring a repeated notice over a lost one.
    }
  }

  async reportCompactionCompleted(eventId: string = randomUUID()): Promise<CompactionRecordedResult> {
    if (!eventId) throw new Error("Compaction event ID is required");
    if (!this.supportsFeature(COMPACTION_AWARENESS_FEATURE) || this.disconnecting) {
      throw new Error("Compaction awareness is unavailable");
    }
    const socket = this.requireActiveSocket();
    if (this.pendingCompactionReports.has(eventId)) {
      throw new Error(`Compaction event ${eventId} is already pending`);
    }

    return new Promise<CompactionRecordedResult>((resolve, reject) => {
      const timeout = setTimeout(() => {
        const pending = this.pendingCompactionReports.get(eventId);
        if (!pending) return;
        this.pendingCompactionReports.delete(eventId);
        pending.reject(new Error("Compaction persistence acknowledgement timed out"));
      }, 10_000);
      timeout.unref?.();

      this.pendingCompactionReports.set(eventId, {
        resolve: (result) => {
          clearTimeout(timeout);
          resolve(result);
        },
        reject: (error) => {
          clearTimeout(timeout);
          reject(error);
        },
      });

      try {
        writeMessage(socket, { type: "compaction_completed", eventId });
      } catch (error) {
        clearTimeout(timeout);
        this.pendingCompactionReports.delete(eventId);
        reject(toError(error));
      }
    });
  }

  updatePresence(updates: { name?: string; description?: string | null; runtimeFallbackAlias?: boolean; status?: string; model?: string; contextPct?: number | null; contextTokens?: number | null; contextWindow?: number | null }): void {
    if (this.disconnecting) {
      return;
    }

    const socket = this.socket;
    if (!socket || !this._sessionId || socket.destroyed || socket.writableEnded || !socket.writable) {
      return;
    }

    if (this._selfSession && updates.description !== undefined) {
      if (updates.description === null) {
        const { description: _description, ...session } = this._selfSession;
        this._selfSession = session;
      } else {
        this._selfSession = { ...this._selfSession, description: updates.description };
      }
    }
    writeMessage(socket, { type: "presence", ...updates });
  }

  sendExtensionMessage(message: Extract<ClientMessage, { type: "extension_publish" | "extension_state_commit" }>): void {
    if (!this.supportsFeature(EXTENSION_BUS_FEATURE)) {
      throw new Error(`Connected broker does not support ${EXTENSION_BUS_FEATURE}`);
    }
    const socket = this.requireActiveSocket();
    writeMessage(socket, message);
  }

  onBrokerMessage(handler: (message: BrokerMessage) => void): () => void {
    this.on("broker_message", handler);
    return () => this.off("broker_message", handler);
  }

  onMessageReceipt(handler: (from: SessionInfo, receipt: MessageReceipt) => void): () => void {
    this.on("message_receipt", handler);
    return () => this.off("message_receipt", handler);
  }

  onMessageControl(handler: (from: SessionInfo, control: MessageControl) => void): () => void {
    this.on("message_control", handler);
    return () => this.off("message_control", handler);
  }
}
