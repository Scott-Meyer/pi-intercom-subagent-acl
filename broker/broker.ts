import net from "net";
import { chmodSync, mkdirSync, readdirSync, readFileSync, writeFileSync, unlinkSync } from "fs";
import { join } from "path";
import { createHash, randomUUID } from "crypto";
import { writeMessage, createMessageReader } from "./framing.ts";
import { isAuthoredMessage, isMessageReceipt, isSessionId, isSessionRegistration } from "./protocol.ts";
import {
  ensureIntercomRuntimeDir,
  getBrokerListenTarget,
  getBrokerPortFilePath,
  getIntercomDirPath,
  INTERCOM_DIR_MODE,
  INTERCOM_PROTOCOL_NAME,
  INTERCOM_PROTOCOL_VERSION,
  INTERCOM_RUNTIME_FILE_MODE,
  restrictIntercomRuntimeFile,
  type BrokerConnectTarget,
} from "./paths.ts";
import { getAskTimeoutMs } from "../config.ts";
import { sameCwd } from "../cwd.ts";
import { COMPACTION_AWARENESS_FEATURE, EXACT_SEND_FEATURE, EXTENSION_BUS_FEATURE } from "../types.ts";
import type { DeliveryState, SessionInfo, Message, BrokerMessage, ExtensionCapability, MessageControl, PeerCompactionNotice } from "../types.ts";
import { ExtensionStateManager } from "./extension-state.ts";
import { assertNoLiveBroker } from "./runtime-claim.ts";
import { CollaborationStateStore } from "./collaboration-state.ts";

const INTERCOM_DIR = getIntercomDirPath();
const LISTEN_TARGET = getBrokerListenTarget();
const PID_PATH = join(INTERCOM_DIR, "broker.pid");
const PORT_PATH = getBrokerPortFilePath(INTERCOM_DIR);
const PENDING_ASKS_DIR = join(INTERCOM_DIR, "pending-asks");
const BROKER_STATE_ID = randomUUID();
const MAX_SESSIONS = 128;
const MAX_UNREGISTERED_CONNECTIONS = 32;
const REGISTRATION_TIMEOUT_MS = 1000;
const RATE_LIMIT_CAPACITY = 240;
const RATE_LIMIT_REFILL_PER_SECOND = 120;
const ACK_RATE_LIMIT_CAPACITY = 1_024;
const ACK_RATE_LIMIT_REFILL_PER_SECOND = 512;
const PRESENCE_HEARTBEAT_MS = 1000;
const MAX_EXTENSIONS_PER_SESSION = 32;
const MAX_CLIENT_FEATURES_PER_SESSION = 32;
const MAX_EXTENSION_MESSAGE_BYTES = 16 * 1024;
const MAX_EXTENSION_STATE_BYTES = 64 * 1024;
const MESSAGE_RECEIPT_ROUTE_RETENTION_MS = 60 * 60 * 1000;
const DISCONNECTED_SESSION_RETENTION_MS = 24 * 60 * 60 * 1000;
const MAILBOX_MESSAGE_RETENTION_MS = 24 * 60 * 60 * 1000;
const MAX_MAILBOX_MESSAGES = 256;
// Periodic sweep so mailbox expiries (and their undelivered-receipt
// notifications to senders) fire on schedule instead of lazily on the
// next unrelated mailbox queue operation.
const MAILBOX_SWEEP_INTERVAL_MS = 60 * 1000;
const DELIVERY_RECORD_RETENTION_MS = 60 * 60 * 1000;
const MAX_DELIVERY_RECORDS = 4096;
const DIRECT_CONTACT_TOKEN_RETENTION_MS = 60 * 60 * 1000;
const MAX_PENDING_DIRECT_CONTACTS = 4096;

function serializedPayloadSize(payload: unknown): number | null {
  try {
    const json = JSON.stringify(payload);
    return json === undefined ? null : Buffer.byteLength(json, "utf8");
  } catch {
    return null;
  }
}

interface ConnectedSession {
  socket: net.Socket;
  info: SessionInfo;
  key: string;
  scopeId?: string;
  lastPresenceBroadcastAt: number;
  ownerOrder: number;
  extensions?: ExtensionCapability[];
  clientFeatures: Set<string>;
}

interface DeliveryRecord {
  fingerprint: string;
  state: DeliveryState;
  reason?: string;
  code?: string;
  retryable: boolean;
  outcomeKnown: boolean;
  peerCompaction?: PeerCompactionNotice;
  contactToken?: string;
  senderContact?: DirectContactPlan;
  createdAt: number;
}

interface NamespaceOwner {
  namespace: string;
  sessionKey: string;
  sessionId: string;
  socket: net.Socket;
  epoch: string;
  scopeId?: string;
}

interface ConnectionState {
  socket: net.Socket;
  tokens: number;
  lastRefillAt: number;
  ackTokens: number;
  lastAckRefillAt: number;
}

interface AskEdge {
  from: string;
  to: string;
  scopeId?: string;
  createdAt: number;
}

interface PendingAskRecord {
  askId: string;
  messageId: string;
  asker: { sessionId: string; name: string | null };
  target: { sessionId: string; name: string | null };
  question: string;
  createdAt: number;
  expiresAt: number;
}

interface MessageReceiptRoute {
  from: string;
  to: string;
  createdAt: number;
}

interface DisconnectedSession {
  info: SessionInfo;
  key: string;
  scopeId?: string;
  disconnectedAt: number;
}

interface PendingDirectContact {
  observerKey: string;
  plan: DirectContactPlan;
  createdAt: number;
}

interface DirectContactPlan {
  scopeId?: string;
  observerSessionId: string;
  peerSessionId: string;
  observedPeerGeneration: number;
  durableBaseline: boolean;
  notice?: PeerCompactionNotice;
}

interface MailboxMessage {
  from: SessionInfo;
  fromKey: string;
  fromScopeId?: string;
  target: SessionInfo;
  targetKey: string;
  targetScopeId?: string;
  message: Message;
  contactKind: "direct" | "broadcast";
  queuedAt: number;
}

function normalizeScopeId(value: unknown): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error("Invalid register scopeId");
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function sameScope(a: string | undefined, b: string | undefined): boolean {
  return a === b;
}

function scopedSessionKey(scopeId: string | undefined, sessionId: string): string {
  return JSON.stringify([scopeId ?? null, sessionId]);
}

function scopedExtensionKey(scopeId: string | undefined, namespace: string): string {
  return JSON.stringify([scopeId ?? null, namespace]);
}

function scopedExtensionStateNamespace(scopeId: string | undefined, namespace: string): string {
  if (!scopeId) {
    return namespace;
  }
  return JSON.stringify(["scope", createHash("sha256").update(scopeId).digest("hex"), namespace]);
}

function scopedPendingAskRecordPath(scopeId: string | undefined, messageId: string): string {
  if (!scopeId) {
    return pendingAskRecordPath(messageId);
  }
  const scopeHash = createHash("sha256").update(scopeId).digest("hex");
  return join(PENDING_ASKS_DIR, `${scopeHash}-${encodeURIComponent(messageId)}.json`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// ACL fork: subagent visibility scoping.
//
// A session tagged isSubagent may only see/reach the supervisor it was
// delegated by. A main (non-subagent) session sees every other main session
// plus only the subagent children it personally supervises — never another
// main's children, and never a sibling child of its own children.
//
// Matching is (supervisorSessionId matches) OR (supervisorName matches),
// never strict ID-then-name precedence. pi-subagents passes the parent's raw
// pi session ID as PI_SUBAGENT_ORCHESTRATOR_SESSION_ID, but the parent may be
// registered with pi-intercom under a different id (PI_INTERCOM_STABLE_ID /
// config.stableId). In that case the ID never matches even though this is
// genuinely the child's supervisor, and only the name fallback saves it —
// mirroring resolveSupervisorTarget's own id-then-name resolution intent, but
// evaluated as an OR so a stale/mismatched id can't shadow a correct name.
function matchesSupervisor(child: SessionInfo, candidateSupervisor: SessionInfo): boolean {
  if (child.supervisorSessionId && child.supervisorSessionId === candidateSupervisor.id) {
    return true;
  }
  if (child.supervisorName && candidateSupervisor.name && candidateSupervisor.name.toLowerCase() === child.supervisorName.toLowerCase()) {
    return true;
  }
  return false;
}

// A subagent that has explicitly self-promoted via "advertise" is treated as
// an ordinary main for visibility in both directions, while isSubagent /
// supervisorSessionId / supervisorName remain in place as provenance --
// advertising does not erase where it came from, it only lifts the ACL.
function isRestrictedSubagent(info: SessionInfo): boolean {
  return info.isSubagent === true && info.advertised !== true;
}

function canSeeSession(observer: SessionInfo, subject: SessionInfo): boolean {
  if (observer.id === subject.id) {
    return true;
  }
  if (isRestrictedSubagent(observer)) {
    // Subagents see only their own supervisor, never siblings or other mains.
    return matchesSupervisor(observer, subject);
  }
  if (!isRestrictedSubagent(subject)) {
    // Mains (and advertised subagents) see every other main / advertised subagent.
    return true;
  }
  // Mains see only the subagent children they personally supervise.
  return matchesSupervisor(subject, observer);
}

const MAX_ADVERTISE_NAME_LENGTH = 128;

function isPendingAskRecord(value: unknown): value is PendingAskRecord {
  if (!isRecord(value) || !isRecord(value.asker) || !isRecord(value.target)) {
    return false;
  }
  return typeof value.askId === "string"
    && typeof value.messageId === "string"
    && typeof value.asker.sessionId === "string"
    && (typeof value.asker.name === "string" || value.asker.name === null)
    && typeof value.target.sessionId === "string"
    && (typeof value.target.name === "string" || value.target.name === null)
    && typeof value.question === "string"
    && Number.isSafeInteger(value.createdAt)
    && Number.isSafeInteger(value.expiresAt)
    && (value.expiresAt as number) >= (value.createdAt as number);
}

function pendingAskRecordPath(messageId: string): string {
  return join(PENDING_ASKS_DIR, `${encodeURIComponent(messageId)}.json`);
}

function ensurePendingAskRecordDir(): void {
  mkdirSync(PENDING_ASKS_DIR, { recursive: true, mode: INTERCOM_DIR_MODE });
  if (process.platform !== "win32") {
    chmodSync(PENDING_ASKS_DIR, INTERCOM_DIR_MODE);
  }
}

class IntercomBroker {
  private sessions = new Map<string, ConnectedSession>();
  private askEdges = new Map<string, AskEdge>();
  private messageReceiptRoutes = new Map<string, MessageReceiptRoute>();
  private disconnectedSessions = new Map<string, DisconnectedSession>();
  private mailboxMessages: MailboxMessage[] = [];
  private deliveryRecords = new Map<string, DeliveryRecord>();
  private pendingDirectContacts = new Map<string, PendingDirectContact>();
  private connections = new Set<net.Socket>();
  private unregisteredConnections = new Set<net.Socket>();
  private server: net.Server;
  private shutdownTimer: NodeJS.Timeout | null = null;
  private maintenanceTimer: NodeJS.Timeout | null = null;
  private readonly askTimeoutMs = getAskTimeoutMs();
  private namespaceOwners = new Map<string, NamespaceOwner>();
  private nextOwnerOrder = 1;
  private extensionStateManager: ExtensionStateManager;
  private collaborationState: CollaborationStateStore;

  constructor() {
    ensureIntercomRuntimeDir(INTERCOM_DIR);
    assertNoLiveBroker(PID_PATH);
    ensurePendingAskRecordDir();
    this.prunePendingAskRecords();
    this.extensionStateManager = new ExtensionStateManager(INTERCOM_DIR);
    this.collaborationState = new CollaborationStateStore(INTERCOM_DIR);
    if (typeof LISTEN_TARGET === "string" && process.platform !== "win32") {
      try {
        unlinkSync(LISTEN_TARGET);
      } catch {
        // A clean startup has no stale socket to remove.
      }
    }
    this.server = net.createServer(this.handleConnection.bind(this));
  }

  private supportsCompactionAwareness(session: ConnectedSession): boolean {
    return session.clientFeatures.has(COMPACTION_AWARENESS_FEATURE);
  }

  private directContactPlan(
    scopeId: string | undefined,
    observer: SessionInfo,
    peer: SessionInfo,
    includePeerContext = true,
    requestedPeerSessionId?: string,
  ): DirectContactPlan {
    const snapshot = this.collaborationState.readContact(scopeId, observer.id, peer.id);
    const notice = snapshot.compactedSinceLastContact && snapshot.lastContactGeneration !== undefined
      ? {
          peerSessionId: peer.id,
          ...(peer.name ? { peerName: peer.name } : {}),
          ...(requestedPeerSessionId && requestedPeerSessionId !== peer.id ? { requestedPeerSessionId } : {}),
          generation: snapshot.peerGeneration,
          previousGeneration: snapshot.lastContactGeneration,
          compactedAt: snapshot.peerCompactedAt!,
          ...(!includePeerContext || peer.contextPct === undefined ? {} : { contextPct: peer.contextPct }),
        }
      : undefined;
    return {
      ...(scopeId ? { scopeId } : {}),
      observerSessionId: observer.id,
      peerSessionId: peer.id,
      observedPeerGeneration: snapshot.peerGeneration,
      durableBaseline: snapshot.lastContactGeneration === undefined,
      ...(notice ? { notice } : {}),
    };
  }

  private trackDirectContact(observerKey: string, plan: DirectContactPlan): string {
    this.prunePendingDirectContacts();
    while (this.pendingDirectContacts.size >= MAX_PENDING_DIRECT_CONTACTS) {
      const oldest = this.pendingDirectContacts.keys().next().value;
      if (oldest === undefined) break;
      this.pendingDirectContacts.delete(oldest);
    }
    const token = randomUUID();
    if (plan.durableBaseline) {
      this.collaborationState.stageFirstContactBaseline(
        plan.scopeId,
        plan.observerSessionId,
        plan.peerSessionId,
        plan.observedPeerGeneration,
        token,
      );
    }
    this.pendingDirectContacts.set(token, { observerKey, plan, createdAt: Date.now() });
    return token;
  }

  private acknowledgeDirectContact(
    observerKey: string,
    token: string,
    scopeId: string | undefined,
    observerSessionId: string,
  ): "accepted" | "unknown" | "retry" {
    const pending = this.pendingDirectContacts.get(token);
    if (pending && pending.observerKey !== observerKey) return "unknown";
    try {
      const accepted = pending?.plan.durableBaseline
        ? this.collaborationState.acceptStagedFirstContactBaseline(scopeId, observerSessionId, token)
        : pending
          ? this.commitDirectContact(pending.plan)
          : this.collaborationState.acceptStagedFirstContactBaseline(scopeId, observerSessionId, token);
      if (accepted) {
        this.pendingDirectContacts.delete(token);
        return "accepted";
      }
      // A known volatile plan failed to commit and should be retried. A token
      // absent from both memory and durable staged/accepted state is terminal.
      return pending ? "retry" : "unknown";
    } catch (error) {
      console.error("Failed to acknowledge staged collaboration baseline:", error);
      return "retry";
    }
  }

  private prunePendingDirectContacts(now = Date.now()): void {
    for (const [token, pending] of this.pendingDirectContacts) {
      if (now - pending.createdAt > DIRECT_CONTACT_TOKEN_RETENTION_MS) {
        this.pendingDirectContacts.delete(token);
      }
    }
  }

  private commitDirectContact(plan: DirectContactPlan): boolean {
    try {
      this.collaborationState.recordAcceptedContact(
        plan.scopeId,
        plan.observerSessionId,
        plan.peerSessionId,
        plan.observedPeerGeneration,
        plan.durableBaseline,
      );
      return true;
    } catch (error) {
      // For an already delivered receiver notice, retaining the token repeats on
      // retry. Sender baselines call this before delivery and fail closed.
      console.error("Failed to record direct collaboration contact:", error);
      return false;
    }
  }

  private prepareSenderContact(observerKey: string, plan: DirectContactPlan | undefined): {
    clientToken?: string;
    stagedBaselineToken?: string;
  } {
    if (!plan) return {};
    if (!plan.durableBaseline) {
      return { clientToken: this.trackDirectContact(observerKey, plan) };
    }
    const stagedBaselineToken = randomUUID();
    this.collaborationState.stageFirstContactBaseline(
      plan.scopeId,
      plan.observerSessionId,
      plan.peerSessionId,
      plan.observedPeerGeneration,
      stagedBaselineToken,
    );
    return { stagedBaselineToken };
  }

  private finalizeSenderContact(plan: DirectContactPlan | undefined, stagedBaselineToken?: string): void {
    if (!plan?.durableBaseline || !stagedBaselineToken) return;
    if (!this.collaborationState.acceptStagedFirstContactBaseline(
      plan.scopeId,
      plan.observerSessionId,
      stagedBaselineToken,
    )) {
      throw new Error("Failed to commit first-contact collaboration baseline");
    }
  }

  start(): void {
    const onListening = () => {
      if (typeof LISTEN_TARGET === "string") {
        restrictIntercomRuntimeFile(LISTEN_TARGET);
      } else {
        const address = this.server.address();
        if (!address || typeof address === "string") {
          throw new Error("Intercom TCP broker started without a TCP address");
        }
        const endpoint: BrokerConnectTarget = {
          transport: "tcp",
          host: LISTEN_TARGET.host,
          port: address.port,
          stateId: BROKER_STATE_ID,
        };
        writeFileSync(PORT_PATH, `${JSON.stringify(endpoint)}\n`, { mode: INTERCOM_RUNTIME_FILE_MODE });
        restrictIntercomRuntimeFile(PORT_PATH);
      }
      writeFileSync(PID_PATH, String(process.pid), { mode: INTERCOM_RUNTIME_FILE_MODE });
      restrictIntercomRuntimeFile(PID_PATH);
      console.log(`Intercom broker started (pid: ${process.pid})`);
    };

    if (typeof LISTEN_TARGET === "string") {
      this.server.listen(LISTEN_TARGET, onListening);
    } else {
      this.server.listen({ host: LISTEN_TARGET.host, port: LISTEN_TARGET.port }, onListening);
    }
    process.on("SIGTERM", () => this.shutdown());
    process.on("SIGINT", () => this.shutdown());
    this.maintenanceTimer = setInterval(() => {
      this.pruneMailboxMessages();
      this.pruneDisconnectedSessions();
      this.prunePendingDirectContacts();
    }, MAILBOX_SWEEP_INTERVAL_MS);
    this.maintenanceTimer.unref?.();
  }

  private handleConnection(socket: net.Socket): void {
    this.connections.add(socket);
    let sessionKey: string | null = null;
    let registrationTimeout: NodeJS.Timeout | null = null;
    const armRegistrationTimeout = () => {
      if (registrationTimeout) {
        clearTimeout(registrationTimeout);
      }
      this.unregisteredConnections.delete(socket);
      this.unregisteredConnections.add(socket);
      this.evictOldestUnregisteredConnections(socket);
      registrationTimeout = setTimeout(() => {
        if (!sessionKey) {
          socket.destroy();
        }
      }, REGISTRATION_TIMEOUT_MS);
      registrationTimeout.unref?.();
    };
    const clearRegistrationTimeout = () => {
      if (registrationTimeout) {
        clearTimeout(registrationTimeout);
        registrationTimeout = null;
      }
      this.unregisteredConnections.delete(socket);
    };
    armRegistrationTimeout();
    const connection: ConnectionState = {
      socket,
      tokens: RATE_LIMIT_CAPACITY,
      lastRefillAt: Date.now(),
      ackTokens: ACK_RATE_LIMIT_CAPACITY,
      lastAckRefillAt: Date.now(),
    };

    const reader = createMessageReader((msg) => {
      const isDirectContactAck = typeof msg === "object"
        && msg !== null
        && "type" in msg
        && msg.type === "direct_contact_seen";
      if (!(isDirectContactAck ? this.consumeAckToken(connection) : this.consumeToken(connection))) {
        writeMessage(socket, { type: "error", error: "Intercom broker rate limit exceeded" });
        socket.destroy(new Error("Intercom broker rate limit exceeded"));
        return;
      }
      this.handleMessage(socket, msg, sessionKey, (id) => {
        sessionKey = id;
        if (id) {
          clearRegistrationTimeout();
        } else {
          armRegistrationTimeout();
        }
      });
    }, (error) => {
      socket.destroy(error);
    });

    socket.on("data", reader);

    socket.on("close", () => {
      clearRegistrationTimeout();
      this.connections.delete(socket);
      if (sessionKey) {
        const existing = this.sessions.get(sessionKey);
        if (existing?.socket === socket) {
          this.rememberDisconnectedSession(existing);
          this.sessions.delete(sessionKey);
          this.clearMessageReceiptRoutesForSession(sessionKey);
          this.broadcastScoped({ type: "session_left", sessionId: existing.info.id }, existing.info, sessionKey, existing.scopeId);
          this.recomputeNamespaceOwners();
          this.scheduleShutdownCheck();
        }
      }
    });

    socket.on("error", (error) => {
      console.error("Socket error:", error);
    });
  }

  private evictOldestUnregisteredConnections(currentSocket: net.Socket): void {
    while (this.unregisteredConnections.size > MAX_UNREGISTERED_CONNECTIONS) {
      const [oldest] = this.unregisteredConnections;
      if (!oldest) {
        return;
      }
      if (oldest === currentSocket && this.unregisteredConnections.size === 1) {
        return;
      }
      this.unregisteredConnections.delete(oldest);
      oldest.destroy();
    }
  }

  private consumeToken(connection: ConnectionState, now = Date.now()): boolean {
    const elapsedMs = now - connection.lastRefillAt;
    if (elapsedMs > 0) {
      connection.tokens = Math.min(
        RATE_LIMIT_CAPACITY,
        connection.tokens + elapsedMs * RATE_LIMIT_REFILL_PER_SECOND / 1000,
      );
      connection.lastRefillAt = now;
    }
    if (connection.tokens < 1) {
      return false;
    }
    connection.tokens -= 1;
    return true;
  }

  private consumeAckToken(connection: ConnectionState, now = Date.now()): boolean {
    const elapsedMs = now - connection.lastAckRefillAt;
    if (elapsedMs > 0) {
      connection.ackTokens = Math.min(
        ACK_RATE_LIMIT_CAPACITY,
        connection.ackTokens + elapsedMs * ACK_RATE_LIMIT_REFILL_PER_SECOND / 1000,
      );
      connection.lastAckRefillAt = now;
    }
    if (connection.ackTokens < 1) return false;
    connection.ackTokens -= 1;
    return true;
  }

  private scheduleShutdownCheck(): void {
    if (this.shutdownTimer) return;

    this.shutdownTimer = setTimeout(() => {
      this.shutdownTimer = null;
      if (this.sessions.size === 0) {
        console.log("No sessions connected, shutting down");
        this.shutdown();
      }
    }, 5000);
  }

  private handleMessage(
    socket: net.Socket,
    msg: unknown,
    currentKey: string | null,
    setKey: (key: string | null) => void,
  ): void {
    if (typeof msg !== "object" || msg === null || !("type" in msg) || typeof msg.type !== "string") {
      throw new Error("Invalid client message");
    }

    const clientMessage = msg as { type: string } & Record<string, unknown>;
    const requiresEndpointAuth = typeof LISTEN_TARGET !== "string";
    const hasEndpointAuth = clientMessage.stateId === BROKER_STATE_ID;

    if (clientMessage.type === "health") {
      if (typeof clientMessage.requestId !== "string") {
        throw new Error("Invalid health message");
      }
      if (requiresEndpointAuth && !hasEndpointAuth) {
        throw new Error("Invalid intercom TCP endpoint credentials");
      }
      writeMessage(socket, {
        type: "health_ok",
        requestId: clientMessage.requestId,
        protocol: INTERCOM_PROTOCOL_NAME,
        version: INTERCOM_PROTOCOL_VERSION,
      });
      return;
    }

    if (requiresEndpointAuth && clientMessage.type === "register" && !hasEndpointAuth) {
      throw new Error("Invalid intercom TCP endpoint credentials");
    }

    if (currentKey === null && clientMessage.type !== "register") {
      throw new Error(`Received ${clientMessage.type} before register`);
    }

    switch (clientMessage.type) {
      case "register": {
        if (!isSessionRegistration(clientMessage.session)) {
          throw new Error("Invalid register message");
        }

        if (currentKey) {
          throw new Error("Received duplicate register message");
        }
        
        let id: string = randomUUID();
        if (clientMessage.sessionId !== undefined) {
          if (!isSessionId(clientMessage.sessionId)) {
            throw new Error("Invalid register sessionId");
          }
          id = clientMessage.sessionId;
        }
        const scopeId = normalizeScopeId(clientMessage.scopeId);
        const key = scopedSessionKey(scopeId, id);
        const session = clientMessage.session;
        const extensions = session.extensions;
        const rawClientFeatures = clientMessage.clientFeatures;
        if (
          rawClientFeatures !== undefined
          && (
            !Array.isArray(rawClientFeatures)
            || rawClientFeatures.length > MAX_CLIENT_FEATURES_PER_SESSION
            || !rawClientFeatures.every((feature) => typeof feature === "string" && feature.length > 0 && feature.length <= 128)
          )
        ) {
          throw new Error("Invalid register clientFeatures");
        }
        const clientFeatures = new Set(rawClientFeatures as string[] | undefined ?? []);
        if (extensions !== undefined) {
          if (!Array.isArray(extensions) || extensions.length > MAX_EXTENSIONS_PER_SESSION) {
            throw new Error(`Invalid extensions field (maximum ${MAX_EXTENSIONS_PER_SESSION})`);
          }
          for (const extension of extensions) {
            if (!this.validateExtensionCapability(extension)) {
              throw new Error(`Invalid extension capability: ${JSON.stringify(extension)}`);
            }
          }
        }

        this.pruneDisconnectedSessions();
        this.pruneMailboxMessages();
        const previous = this.sessions.get(key);
        if (!previous && this.sessions.size >= MAX_SESSIONS) {
          writeMessage(socket, { type: "error", error: "Too many registered intercom sessions" });
          socket.destroy();
          break;
        }
        if (previous) {
          this.clearMessageReceiptRoutesForSession(key);
          previous.socket.end();
        }
        setKey(key);
        const effectiveName = this.dedupeSessionName(session.name, scopeId, key);
        const info: SessionInfo = {
          id,
          endpointEpoch: randomUUID(),
          ...(effectiveName !== undefined ? { name: effectiveName } : {}),
          ...(session.runtimeFallbackAlias !== undefined ? { runtimeFallbackAlias: session.runtimeFallbackAlias } : {}),
          cwd: session.cwd,
          model: session.model,
          pid: session.pid,
          startedAt: session.startedAt,
          lastActivity: session.lastActivity,
          ...(session.status !== undefined ? { status: session.status } : {}),
          ...(session.tmuxPane !== undefined ? { tmuxPane: session.tmuxPane } : {}),
          ...(session.isSubagent !== undefined ? { isSubagent: session.isSubagent } : {}),
          ...(session.supervisorSessionId !== undefined ? { supervisorSessionId: session.supervisorSessionId } : {}),
          ...(session.supervisorName !== undefined ? { supervisorName: session.supervisorName } : {}),
          trustedLocal: typeof LISTEN_TARGET === "string" && process.platform !== "win32",
        };

        const connectedSession: ConnectedSession = {
          socket,
          info,
          key,
          ...(scopeId ? { scopeId } : {}),
          lastPresenceBroadcastAt: Date.now(),
          ownerOrder: previous?.ownerOrder ?? this.nextOwnerOrder++,
          extensions,
          clientFeatures,
        };
        this.sessions.set(key, connectedSession);
        this.disconnectedSessions.delete(key);
        
        if (this.shutdownTimer) {
          clearTimeout(this.shutdownTimer);
          this.shutdownTimer = null;
        }

        // This must be the first broker message. Older clients ignore the
        // additive features field; newer clients use it to avoid sending
        // extension operations to an older broker.
        writeMessage(socket, {
          type: "registered",
          sessionId: id,
          features: [EXTENSION_BUS_FEATURE, EXACT_SEND_FEATURE, COMPACTION_AWARENESS_FEATURE],
        });
        this.broadcastScoped({ type: "session_joined", session: info }, info, key, scopeId);

        this.recomputeNamespaceOwners();
        this.flushMailboxForSession(connectedSession);

        if (extensions) {
          for (const ext of extensions) {
            const owner = this.namespaceOwners.get(scopedExtensionKey(scopeId, ext.namespace));
            writeMessage(socket, {
              type: "extension_owner",
              namespace: ext.namespace,
              ...(owner ? { ownerId: owner.sessionId, ownerEpoch: owner.epoch } : {}),
            });
            const state = this.extensionStateManager.loadState(scopedExtensionStateNamespace(scopeId, ext.namespace));
            if (state) {
              writeMessage(socket, {
                type: "extension_state",
                namespace: ext.namespace,
                revision: state.revision,
                payload: state.payload,
              });
            }
          }
        }
        break;
      }

      case "unregister": {
        if (!currentKey) {
          throw new Error("Received unregister before register");
        }
        const existing = this.sessions.get(currentKey);
        if (existing?.socket === socket) {
          this.rememberDisconnectedSession(existing);
          this.sessions.delete(currentKey);
          this.clearMessageReceiptRoutesForSession(currentKey);
          this.broadcastScoped({ type: "session_left", sessionId: existing.info.id }, existing.info, currentKey, existing.scopeId);
          this.recomputeNamespaceOwners();
          this.scheduleShutdownCheck();
        }
        setKey(null);
        break;
      }

      case "extension_capabilities_update": {
        if (!currentKey) {
          throw new Error("Received extension_capabilities_update before register");
        }
        const session = this.sessions.get(currentKey);
        if (!session || session.socket !== socket) {
          throw new Error("Extension capability session not found");
        }
        const extensions = clientMessage.extensions;
        if (!Array.isArray(extensions) || extensions.length > MAX_EXTENSIONS_PER_SESSION) {
          throw new Error(`Invalid extensions field (maximum ${MAX_EXTENSIONS_PER_SESSION})`);
        }
        for (const extension of extensions) {
          if (!this.validateExtensionCapability(extension)) {
            throw new Error(`Invalid extension capability: ${JSON.stringify(extension)}`);
          }
        }
        session.extensions = extensions;
        this.recomputeNamespaceOwners();
        for (const extension of extensions) {
          const owner = this.namespaceOwners.get(scopedExtensionKey(session.scopeId, extension.namespace));
          writeMessage(socket, {
            type: "extension_owner",
            namespace: extension.namespace,
            ...(owner ? { ownerId: owner.sessionId, ownerEpoch: owner.epoch } : {}),
          });
          const state = this.extensionStateManager.loadState(scopedExtensionStateNamespace(session.scopeId, extension.namespace));
          if (state) {
            writeMessage(socket, {
              type: "extension_state",
              namespace: extension.namespace,
              revision: state.revision,
              payload: state.payload,
            });
          }
        }
        break;
      }

      case "list": {
        if (typeof clientMessage.requestId !== "string") {
          throw new Error("Invalid list message");
        }
        const requester = currentKey ? this.sessions.get(currentKey) : undefined;
        if (!requester || requester.socket !== socket) {
          throw new Error("List session not found");
        }
        const sessions = Array.from(this.sessions.values())
          .filter(session => sameScope(session.scopeId, requester.scopeId) && canSeeSession(requester.info, session.info))
          .map(s => s.info);
        writeMessage(socket, { type: "sessions", requestId: clientMessage.requestId, sessions });
        break;
      }

      case "advertise": {
        if (!currentKey) {
          throw new Error("Received advertise before register");
        }
        const requestId = clientMessage.requestId;
        if (typeof requestId !== "string") {
          throw new Error("Invalid advertise message");
        }
        const respond = (ok: boolean, extra: { name?: string; error?: string; code?: string } = {}) => {
          writeMessage(socket, { type: "advertise_result", requestId, ok, ...extra });
        };

        const self = this.sessions.get(currentKey);
        if (!self || self.socket !== socket) {
          respond(false, { error: "Sender session not found", code: "E_SENDER_NOT_FOUND" });
          break;
        }

        // Only a tagged subagent has anything to gain from advertising; a main
        // is already fully visible both ways. Reject rather than silently no-op
        // so the caller's model gets a clear, actionable result.
        if (!isRestrictedSubagent(self.info)) {
          respond(false, {
            error: self.info.isSubagent
              ? "This session has already advertised itself."
              : "Only subagent sessions can advertise themselves; this session is already fully visible.",
            code: "E_NOT_ELIGIBLE",
          });
          break;
        }

        const rawName = clientMessage.name;
        if (typeof rawName !== "string") {
          respond(false, { error: "advertise requires a string name", code: "E_INVALID_NAME" });
          break;
        }
        const name = rawName.trim();
        if (name.length === 0 || name.length > MAX_ADVERTISE_NAME_LENGTH) {
          respond(false, { error: `name must be 1-${MAX_ADVERTISE_NAME_LENGTH} characters after trimming`, code: "E_INVALID_NAME" });
          break;
        }
        // Single-line, printable only. This name is interpolated verbatim into
        // roster rows, target strings, and error text on every client that can
        // see it; control characters (newlines especially) let a chosen name
        // forge extra fake roster lines or corrupt terminal rendering.
        if (/[\p{Cc}\p{Cf}]/u.test(name)) {
          respond(false, { error: "name must not contain control or formatting characters", code: "E_INVALID_NAME" });
          break;
        }

        // Case-insensitive uniqueness against every other currently connected
        // session in the same scope -- advertising is a deliberate
        // public-identity claim, stricter than the ordinary same-name tolerance
        // regular sessions have (which is only resolved lazily at send time
        // via E_AMBIGUOUS_TARGET).
        const lowerName = name.toLowerCase();
        const nameCollision = Array.from(this.sessions.values()).some(
          (session) => session.key !== currentKey && sameScope(session.scopeId, self.scopeId) && session.info.name?.toLowerCase() === lowerName,
        );
        if (nameCollision) {
          respond(false, { error: `Name "${name}" is already in use by another connected session`, code: "E_NAME_TAKEN" });
          break;
        }
        // findSessions() resolves an exact session ID before it ever checks
        // names. If the requested name equalled another live session's real
        // ID, that other session would silently win every lookup by this
        // name and the advertised session would be unreachable by it.
        const idCollision = Array.from(this.sessions.values()).some(
          (session) => session.key !== currentKey && sameScope(session.scopeId, self.scopeId) && session.info.id === name,
        );
        if (idCollision) {
          respond(false, { error: `Name "${name}" collides with another connected session's ID`, code: "E_NAME_TAKEN" });
          break;
        }

        // Promote in place. isSubagent/supervisorSessionId/supervisorName are
        // preserved as provenance -- advertising lifts the ACL, it does not
        // erase where this session came from.
        self.info.name = name;
        self.info.runtimeFallbackAlias = false;
        self.info.advertised = true;
        respond(true, { name });

        // The promoted info now reads as an ordinary main under canSeeSession,
        // so this reaches every previously-blind session as well as everyone
        // who could already see it -- exactly the newly-widened audience.
        this.broadcastScoped({ type: "presence_update", session: self.info }, self.info, currentKey, self.scopeId);
        break;
      }

      case "send": {
        if (!currentKey) {
          throw new Error("Received send before register");
        }
        const message = clientMessage.message;
        const messageId = isAuthoredMessage(message) ? message.id : "unknown";

        if (typeof clientMessage.to !== "string" || !isAuthoredMessage(message)) {
          this.writeDeliveryFailure(socket, messageId, "Invalid message format", "E_INVALID_MESSAGE");
          break;
        }
        const contactKind = clientMessage.contactKind ?? "direct";
        if (contactKind !== "direct" && contactKind !== "broadcast") {
          this.writeDeliveryFailure(socket, message.id, "Invalid contact kind", "E_INVALID_MESSAGE");
          break;
        }
        const fromSession = this.sessions.get(currentKey);
        if (!fromSession || fromSession.socket !== socket) {
          this.writeDeliveryFailure(socket, message.id, "Sender session not found", "E_SENDER_NOT_FOUND");
          break;
        }

        const brokerReceivedAt = Date.now();
        this.pruneAskEdges();
        this.pruneMessageReceiptRoutes(brokerReceivedAt);
        const replyEdge = message.replyTo ? this.askEdges.get(message.replyTo) : undefined;

        const hasTargetId = clientMessage.targetId !== undefined;
        const hasTargetEpoch = clientMessage.targetEpoch !== undefined;
        if (
          hasTargetId !== hasTargetEpoch
          || (hasTargetId && (typeof clientMessage.targetId !== "string" || clientMessage.targetId.length === 0))
          || (hasTargetEpoch && (typeof clientMessage.targetEpoch !== "string" || clientMessage.targetEpoch.length === 0))
        ) {
          this.writeDeliveryFailure(socket, message.id, "Exact target requires an id and endpoint epoch", "E_INVALID_TARGET");
          break;
        }
        if (hasTargetId && hasTargetEpoch) {
          const targetId = clientMessage.targetId as string;
          const targetEpoch = clientMessage.targetEpoch as string;
          const fingerprint = this.deliveryFingerprint(message, targetId, contactKind);
          if (this.replayOrReject(socket, currentKey, message.id, fingerprint)) {
            break;
          }
          const exactTarget = this.sessions.get(scopedSessionKey(fromSession.scopeId, targetId));
          const exactTargetVisible = exactTarget ? this.isVisibleTo(currentKey, exactTarget.info) : false;
          if (!exactTarget || !exactTargetVisible) {
            this.recordDelivery(currentKey, message.id, fingerprint, "failed", "Session not found", "E_TARGET_NOT_FOUND");
            this.writeDeliveryFailure(socket, message.id, "Session not found", "E_TARGET_NOT_FOUND");
            break;
          }
          if (exactTarget.info.endpointEpoch !== targetEpoch) {
            this.recordDelivery(currentKey, message.id, fingerprint, "failed", "Target endpoint changed before delivery", "E_TARGET_REBOUND", true);
            this.writeDeliveryFailure(socket, message.id, "Target endpoint changed before delivery", "E_TARGET_REBOUND", true);
            break;
          }
          clientMessage.to = targetId;
        }

        const targets = this.findSessions(clientMessage.to as string, fromSession.scopeId, currentKey);
        if (targets.length === 1) {
          if (message.replyTo && !replyEdge) {
            this.writeDeliveryFailure(socket, message.id, "Reply target does not match a pending ask", "E_REPLY_TARGET");
            break;
          }
          const target = targets[0];
          const fingerprint = this.deliveryFingerprint(message, target.info.id, contactKind);
          if (this.replayOrReject(socket, currentKey, message.id, fingerprint)) {
            break;
          }
          if (message.supersedes) {
            const supersededRoute = this.messageReceiptRoutes.get(message.supersedes);
            if (!supersededRoute || supersededRoute.from !== currentKey || supersededRoute.to !== target.key) {
              this.writeDeliveryFailure(socket, message.id, "Supersede target does not match a previous message from this sender to this receiver", "E_SUPERSEDE_TARGET");
              break;
            }
          }
          if (replyEdge && (replyEdge.to !== currentKey || replyEdge.from !== target.key)) {
            this.writeDeliveryFailure(socket, message.id, "Reply target does not match the pending ask", "E_REPLY_TARGET");
            break;
          }
          if (message.expectsReply) {
            const reverseEdge = Array.from(this.askEdges.entries()).find(([edgeMessageId, edge]) => edgeMessageId !== message.replyTo && edge.from === target.key && edge.to === currentKey);
            if (reverseEdge) {
              this.writeDeliveryFailure(socket, message.id, "Mutual ask refused: target session is already waiting for a reply from this session.", "E_MUTUAL_ASK");
              break;
            }
            this.writePendingAskRecord(message, fromSession, target.info, brokerReceivedAt);
            this.askEdges.set(message.id, {
              from: currentKey,
              to: target.key,
              ...(fromSession.scopeId ? { scopeId: fromSession.scopeId } : {}),
              createdAt: brokerReceivedAt,
            });
          }
          const senderContact = contactKind === "direct" && this.supportsCompactionAwareness(fromSession)
            ? this.directContactPlan(fromSession.scopeId, fromSession.info, target.info)
            : undefined;
          const receiverContact = contactKind === "direct" && this.supportsCompactionAwareness(target)
            ? this.directContactPlan(fromSession.scopeId, target.info, fromSession.info)
            : undefined;
          const {
            clientToken: senderContactToken,
            stagedBaselineToken: senderBaselineToken,
          } = this.prepareSenderContact(currentKey, senderContact);
          const receiverContactToken = receiverContact
            ? this.trackDirectContact(target.key, receiverContact)
            : undefined;
          const deliveredMessage: Message = {
            ...message,
            brokerReceivedAt,
            brokerDeliveredAt: Date.now(),
            ...(receiverContact?.notice ? { peerCompaction: receiverContact.notice } : {}),
            ...(receiverContactToken ? { contactToken: receiverContactToken } : {}),
            ...(receiverContact?.durableBaseline ? { contactBaseline: true } : {}),
          };
          if (message.supersedes) {
            const control: MessageControl = {
              action: "supersede",
              messageId: message.supersedes,
              supersededBy: message.id,
              timestamp: Date.now(),
            };
            writeMessage(target.socket, {
              type: "message_control",
              from: fromSession.info,
              control,
            });
            this.updateDeliveryRecord(currentKey, message.supersedes, "failed", `Superseded by ${message.id}`, "E_DELIVERY_SUPERSEDED");
          }
          writeMessage(target.socket, {
            type: "message",
            from: fromSession.info,
            message: deliveredMessage,
          });
          this.finalizeSenderContact(senderContact, senderBaselineToken);
          if (message.replyTo) {
            this.askEdges.delete(message.replyTo);
            this.removePendingAskRecord(message.replyTo, fromSession.scopeId);
          }
          this.messageReceiptRoutes.set(message.id, {
            from: currentKey,
            to: target.key,
            createdAt: brokerReceivedAt,
          });
          this.recordDelivery(
            currentKey,
            message.id,
            fingerprint,
            "socket_delivered",
            undefined,
            undefined,
            false,
            senderContact?.notice,
            senderContactToken,
            senderContact,
          );
          this.writeDeliverySuccess(
            socket,
            message.id,
            "socket_delivered",
            senderContact?.notice,
            senderContactToken,
          );
          break;
        }

        if (targets.length > 1) {
          this.writeDeliveryFailure(socket, message.id, `Multiple sessions named \"${clientMessage.to}\" are connected. Use the session ID instead.`, "E_AMBIGUOUS_TARGET");
          break;
        }

        const disconnectedTargets = this.findDisconnectedSessions(clientMessage.to as string, fromSession.scopeId, currentKey);
        if (disconnectedTargets.length === 1) {
          if (contactKind === "broadcast") {
            this.writeDeliveryFailure(socket, message.id, "Broadcast recipients must still be connected", "E_TARGET_NOT_FOUND");
            break;
          }
          if (message.replyTo && !replyEdge) {
            this.writeDeliveryFailure(socket, message.id, "Reply target does not match a pending ask", "E_REPLY_TARGET");
            break;
          }
          const disconnectedTarget = disconnectedTargets[0]!;
          const target = disconnectedTarget.info;
          const fingerprint = this.deliveryFingerprint(message, target.id, contactKind);
          if (this.replayOrReject(socket, currentKey, message.id, fingerprint)) {
            break;
          }
          if (message.supersedes) {
            this.writeDeliveryFailure(socket, message.id, "Supersede target is not connected", "E_SUPERSEDE_TARGET");
            break;
          }
          if (replyEdge && (replyEdge.to !== currentKey || replyEdge.from !== disconnectedTarget.key)) {
            this.writeDeliveryFailure(socket, message.id, "Reply target does not match the pending ask", "E_REPLY_TARGET");
            break;
          }
          if (message.expectsReply) {
            this.writeDeliveryFailure(socket, message.id, "Target session is not currently connected; blocking asks are not queued", "E_TARGET_DISCONNECTED");
            break;
          }
          const liveMailboxTarget = this.findUniqueLiveSessionForDisconnectedSession(disconnectedTarget, currentKey);
          const acceptedTarget = liveMailboxTarget?.info ?? target;
          const senderContact = this.supportsCompactionAwareness(fromSession)
            ? this.directContactPlan(
                fromSession.scopeId,
                fromSession.info,
                acceptedTarget,
                liveMailboxTarget !== null,
                target.id,
              )
            : undefined;
          const {
            clientToken: senderContactToken,
            stagedBaselineToken: senderBaselineToken,
          } = this.prepareSenderContact(currentKey, senderContact);
          if (liveMailboxTarget) {
            const receiverContact = this.supportsCompactionAwareness(liveMailboxTarget)
              ? this.directContactPlan(fromSession.scopeId, liveMailboxTarget.info, fromSession.info)
              : undefined;
            const receiverContactToken = receiverContact
              ? this.trackDirectContact(liveMailboxTarget.key, receiverContact)
              : undefined;
            const deliveredMessage: Message = {
              ...message,
              brokerReceivedAt,
              brokerDeliveredAt: Date.now(),
              ...(receiverContact?.notice ? { peerCompaction: receiverContact.notice } : {}),
              ...(receiverContactToken ? { contactToken: receiverContactToken } : {}),
              ...(receiverContact?.durableBaseline ? { contactBaseline: true } : {}),
            };
            writeMessage(liveMailboxTarget.socket, {
              type: "message",
              from: fromSession.info,
              message: deliveredMessage,
            });
            this.messageReceiptRoutes.set(message.id, { from: currentKey, to: liveMailboxTarget.key, createdAt: brokerReceivedAt });
          } else {
            this.queueMailboxMessage(fromSession, disconnectedTarget, message, contactKind, brokerReceivedAt);
          }
          this.finalizeSenderContact(senderContact, senderBaselineToken);
          if (message.replyTo) {
            this.askEdges.delete(message.replyTo);
            this.removePendingAskRecord(message.replyTo, fromSession.scopeId);
          }
          const acceptedDelivery = liveMailboxTarget ? "socket_delivered" : "queued";
          this.recordDelivery(
            currentKey,
            message.id,
            fingerprint,
            acceptedDelivery,
            undefined,
            undefined,
            false,
            senderContact?.notice,
            senderContactToken,
            senderContact,
          );
          this.writeDeliverySuccess(
            socket,
            message.id,
            acceptedDelivery,
            senderContact?.notice,
            senderContactToken,
          );
          break;
        }

        if (disconnectedTargets.length > 1) {
          this.writeDeliveryFailure(socket, message.id, `Multiple disconnected sessions named \"${clientMessage.to}\" can receive queued mail. Use the session ID instead.`, "E_AMBIGUOUS_TARGET");
          break;
        }

        this.writeDeliveryFailure(socket, message.id, "Session not found", "E_TARGET_NOT_FOUND");
        break;
      }

      case "compaction_completed": {
        if (!currentKey) {
          throw new Error("Received compaction_completed before register");
        }
        if (
          typeof clientMessage.eventId !== "string"
          || clientMessage.eventId.length === 0
          || clientMessage.eventId.length > 128
        ) {
          throw new Error("Invalid compaction event ID");
        }
        const session = this.sessions.get(currentKey);
        if (!session || session.socket !== socket) {
          throw new Error("Compaction session not found");
        }
        try {
          const compactedAt = Date.now();
          const state = this.collaborationState.recordSuccessfulCompaction(
            session.scopeId,
            session.info.id,
            clientMessage.eventId,
            compactedAt,
          );
          session.info.lastActivity = compactedAt;
          session.lastPresenceBroadcastAt = compactedAt;
          this.broadcastScoped(
            { type: "presence_update", session: session.info },
            session.info,
            currentKey,
            session.scopeId,
          );
          writeMessage(socket, {
            type: "compaction_recorded",
            eventId: clientMessage.eventId,
            generation: state.generation,
            compactedAt: state.compactedAt,
          });
        } catch (error) {
          console.error("Failed to record successful compaction:", error);
          writeMessage(socket, {
            type: "compaction_record_failed",
            eventId: clientMessage.eventId,
            error: "Failed to persist compaction awareness",
          });
        }
        break;
      }

      case "direct_contact_seen": {
        if (!currentKey) {
          throw new Error("Received direct_contact_seen before register");
        }
        if (typeof clientMessage.token !== "string" || clientMessage.token.length === 0) {
          throw new Error("Invalid direct_contact_seen token");
        }
        const observer = this.sessions.get(currentKey);
        if (observer?.socket === socket) {
          const outcome = this.acknowledgeDirectContact(
            currentKey,
            clientMessage.token,
            observer.scopeId,
            observer.info.id,
          );
          if (outcome === "accepted") {
            writeMessage(socket, { type: "direct_contact_recorded", token: clientMessage.token });
          } else if (outcome === "unknown") {
            writeMessage(socket, { type: "direct_contact_unknown", token: clientMessage.token });
          }
        }
        break;
      }

      case "message_receipt": {
        if (!currentKey) {
          throw new Error("Received message_receipt before register");
        }
        if (!isMessageReceipt(clientMessage.receipt)) {
          throw new Error("Invalid message_receipt message");
        }
        this.pruneMessageReceiptRoutes();
        const route = this.messageReceiptRoutes.get(clientMessage.receipt.messageId);
        const receiver = this.sessions.get(currentKey);
        const sender = route ? this.sessions.get(route.from) : undefined;
        if (route?.to === currentKey && receiver?.socket === socket && sender) {
          writeMessage(sender.socket, {
            type: "message_receipt",
            from: receiver.info,
            receipt: clientMessage.receipt,
          });
        }
        break;
      }

      case "cancel_message": {
        if (!currentKey) {
          throw new Error("Received cancel_message before register");
        }
        if (typeof clientMessage.messageId !== "string") {
          throw new Error("Invalid cancel_message message");
        }
        this.pruneMessageReceiptRoutes();
        this.pruneMailboxMessages();
        const sender = this.sessions.get(currentKey);
        const queuedIndex = this.mailboxMessages.findIndex(entry => entry.message.id === clientMessage.messageId && entry.fromKey === currentKey);
        if (queuedIndex >= 0 && sender?.socket === socket) {
          this.mailboxMessages.splice(queuedIndex, 1);
          this.updateDeliveryRecord(currentKey, clientMessage.messageId, "failed", "Sender cancelled the queued delivery", "E_DELIVERY_CANCELLED");
          const edge = this.askEdges.get(clientMessage.messageId);
          if (edge?.from === currentKey) {
            this.askEdges.delete(clientMessage.messageId);
            this.removePendingAskRecord(clientMessage.messageId, sender.scopeId);
          }
          writeMessage(socket, { type: "delivered", messageId: clientMessage.messageId });
          break;
        }
        const route = this.messageReceiptRoutes.get(clientMessage.messageId);
        const receiver = route ? this.sessions.get(route.to) : undefined;
        if (route?.from !== currentKey || sender?.socket !== socket || !receiver) {
          writeMessage(socket, {
            type: "delivery_failed",
            messageId: clientMessage.messageId,
            reason: "Message cannot be cancelled by this session",
          });
          break;
        }
        writeMessage(receiver.socket, {
          type: "message_control",
          from: sender.info,
          control: {
            action: "cancel",
            messageId: clientMessage.messageId,
            timestamp: Date.now(),
          },
        });
        const edge = this.askEdges.get(clientMessage.messageId);
        if (edge?.from === currentKey) {
          this.askEdges.delete(clientMessage.messageId);
          this.removePendingAskRecord(clientMessage.messageId, sender.scopeId);
        }
        this.updateDeliveryRecord(currentKey, clientMessage.messageId, "failed", "Sender cancelled the delivery", "E_DELIVERY_CANCELLED");
        writeMessage(socket, { type: "delivered", messageId: clientMessage.messageId });
        break;
      }

      case "cancel_ask": {
        if (!currentKey) {
          throw new Error("Received cancel_ask before register");
        }
        if (typeof clientMessage.messageId !== "string") {
          throw new Error("Invalid cancel_ask message");
        }
        const session = this.sessions.get(currentKey);
        const edge = this.askEdges.get(clientMessage.messageId);
        if (session?.socket === socket && edge?.from === currentKey) {
          this.askEdges.delete(clientMessage.messageId);
          this.removePendingAskRecord(clientMessage.messageId, session.scopeId);
        }
        break;
      }

      case "presence": {
        if (!currentKey) {
          throw new Error("Received presence before register");
        }
        const session = this.sessions.get(currentKey);
        if (session?.socket === socket) {
          let changed = false;
          // ACL fork: once advertised, the public name/alias-ness is a
          // deliberate, uniqueness-checked identity claim made through the
          // dedicated "advertise" exchange. Routine presence syncs (which fire
          // on every intercom tool call and normally just re-report the live
          // runtime identity) must not silently revert it back to the
          // pre-advertise fallback name behind the caller's back.
          const identityLocked = session.info.advertised === true;
          if (clientMessage.name !== undefined) {
            if (typeof clientMessage.name !== "string") {
              throw new Error("Invalid presence name");
            }
            if (!identityLocked) {
              const effectiveName = this.dedupeSessionName(clientMessage.name, session.scopeId, currentKey);
              if (effectiveName !== undefined && session.info.name !== effectiveName) {
                session.info.name = effectiveName;
                changed = true;
              }
            }
          }
          if (clientMessage.runtimeFallbackAlias !== undefined) {
            if (typeof clientMessage.runtimeFallbackAlias !== "boolean") {
              throw new Error("Invalid presence runtimeFallbackAlias");
            }
            if (!identityLocked && session.info.runtimeFallbackAlias !== clientMessage.runtimeFallbackAlias) {
              session.info.runtimeFallbackAlias = clientMessage.runtimeFallbackAlias;
              changed = true;
            }
          }
          if (clientMessage.status !== undefined) {
            if (typeof clientMessage.status !== "string") {
              throw new Error("Invalid presence status");
            }
            if (session.info.status !== clientMessage.status) {
              session.info.status = clientMessage.status;
              changed = true;
            }
          }
          if (clientMessage.model !== undefined) {
            if (typeof clientMessage.model !== "string") {
              throw new Error("Invalid presence model");
            }
            if (session.info.model !== clientMessage.model) {
              session.info.model = clientMessage.model;
              changed = true;
            }
          }
          // Context-usage fields: a number updates, an explicit null CLEARS (the
          // value is unknown right after a compaction — delete rather than carry
          // the stale-high value forward), undefined leaves the field untouched.
          if (clientMessage.contextPct !== undefined) {
            if (clientMessage.contextPct === null) {
              if (session.info.contextPct !== undefined) { delete session.info.contextPct; changed = true; }
            } else if (typeof clientMessage.contextPct !== "number") {
              throw new Error("Invalid presence contextPct");
            } else if (session.info.contextPct !== clientMessage.contextPct) {
              session.info.contextPct = clientMessage.contextPct;
              changed = true;
            }
          }
          if (clientMessage.contextTokens !== undefined) {
            if (clientMessage.contextTokens === null) {
              if (session.info.contextTokens !== undefined) { delete session.info.contextTokens; changed = true; }
            } else if (typeof clientMessage.contextTokens !== "number") {
              throw new Error("Invalid presence contextTokens");
            } else if (session.info.contextTokens !== clientMessage.contextTokens) {
              session.info.contextTokens = clientMessage.contextTokens;
              changed = true;
            }
          }
          if (clientMessage.contextWindow !== undefined) {
            if (clientMessage.contextWindow === null) {
              if (session.info.contextWindow !== undefined) { delete session.info.contextWindow; changed = true; }
            } else if (typeof clientMessage.contextWindow !== "number") {
              throw new Error("Invalid presence contextWindow");
            } else if (session.info.contextWindow !== clientMessage.contextWindow) {
              session.info.contextWindow = clientMessage.contextWindow;
              changed = true;
            }
          }
          const now = Date.now();
          session.info.lastActivity = now;
          if (changed || now - session.lastPresenceBroadcastAt >= PRESENCE_HEARTBEAT_MS) {
            session.lastPresenceBroadcastAt = now;
            this.broadcastScoped({ type: "presence_update", session: session.info }, session.info, currentKey, session.scopeId);
          }
        }
        break;
      }

      case "extension_publish": {
        this.handleExtensionPublish(socket, currentKey, clientMessage);
        break;
      }

      case "extension_state_commit": {
        this.handleExtensionStateCommit(socket, currentKey, clientMessage);
        break;
      }

      default:
        throw new Error(`Unknown client message type: ${clientMessage.type}`);
    }
  }

  private rememberDisconnectedSession(session: ConnectedSession, now = Date.now()): void {
    // ACL fork: "advertised" is a live-connection promotion, not a durable
    // identity. Carrying it into the disconnected-mailbox snapshot would let
    // any unrelated main keep finding and queueing mail to a former child's
    // public name/ID indefinitely (up to the 24h mailbox retention window)
    // after the session that earned it is long gone. Strip it so a
    // disconnected former child reverts to supervisor-only mailbox
    // visibility, matching "advertised only while live".
    const { advertised, ...info } = session.info;
    this.disconnectedSessions.set(session.key, {
      info,
      key: session.key,
      ...(session.scopeId ? { scopeId: session.scopeId } : {}),
      disconnectedAt: now,
    });
    this.pruneDisconnectedSessions(now);
  }

  private pruneDisconnectedSessions(now = Date.now()): void {
    for (const [sessionId, session] of this.disconnectedSessions) {
      if (now - session.disconnectedAt > DISCONNECTED_SESSION_RETENTION_MS) {
        this.disconnectedSessions.delete(sessionId);
      }
    }
  }

  // Fork: when a queued mailbox message can no longer be delivered, the
  // sender gets an explicit receipt instead of silence. Without this a send
  // to a dead session reports "delivered (queued)" and then quietly expires
  // up to a day later with no feedback at all.
  private notifyMailboxUndelivered(entry: MailboxMessage, detail: string): void {
    const sender = this.sessions.get(entry.fromKey);
    if (!sender) {
      return;
    }
    writeMessage(sender.socket, {
      type: "message_receipt",
      from: entry.target,
      receipt: {
        messageId: entry.message.id,
        status: "expired",
        timestamp: Date.now(),
        detail,
      },
    });
  }

  private pruneMailboxMessages(now = Date.now()): void {
    for (let index = this.mailboxMessages.length - 1; index >= 0; index -= 1) {
      const entry = this.mailboxMessages[index]!;
      if (now - entry.queuedAt > MAILBOX_MESSAGE_RETENTION_MS) {
        if (entry.message.expectsReply) {
          this.askEdges.delete(entry.message.id);
          this.removePendingAskRecord(entry.message.id, entry.fromScopeId);
        }
        this.notifyMailboxUndelivered(entry, "Mailbox delivery expired: the target session never reconnected");
        this.messageReceiptRoutes.delete(entry.message.id);
        this.updateDeliveryRecord(entry.fromKey, entry.message.id, "failed", "Mailbox delivery expired", "E_DELIVERY_EXPIRED");
        this.mailboxMessages.splice(index, 1);
      }
    }
  }

  private queueMailboxMessage(
    from: ConnectedSession,
    target: DisconnectedSession,
    message: Message,
    contactKind: "direct" | "broadcast",
    brokerReceivedAt: number,
  ): void {
    this.pruneMailboxMessages(brokerReceivedAt);
    while (this.mailboxMessages.length >= MAX_MAILBOX_MESSAGES) {
      const evicted = this.mailboxMessages.shift();
      if (!evicted) break;
      if (evicted.message.expectsReply) {
        this.askEdges.delete(evicted.message.id);
        this.removePendingAskRecord(evicted.message.id, evicted.fromScopeId);
      }
      this.notifyMailboxUndelivered(evicted, "Mailbox capacity evicted the delivery before the target session reconnected");
      this.messageReceiptRoutes.delete(evicted.message.id);
      this.updateDeliveryRecord(evicted.fromKey, evicted.message.id, "failed", "Mailbox capacity evicted the delivery", "E_DELIVERY_EVICTED");
    }
    this.mailboxMessages.push({
      from: { ...from.info },
      fromKey: from.key,
      ...(from.scopeId ? { fromScopeId: from.scopeId } : {}),
      target: { ...target.info },
      targetKey: target.key,
      ...(target.scopeId ? { targetScopeId: target.scopeId } : {}),
      message: { ...message, brokerReceivedAt },
      contactKind,
      queuedAt: brokerReceivedAt,
    });
  }

  private writeDeliverySuccess(
    socket: net.Socket,
    messageId: string,
    delivery: "socket_delivered" | "queued",
    peerCompaction?: PeerCompactionNotice,
    contactToken?: string,
  ): void {
    writeMessage(socket, {
      type: "delivered",
      messageId,
      delivery,
      retryable: false,
      outcomeKnown: true,
      ...(peerCompaction ? { peerCompaction } : {}),
      ...(contactToken ? { contactToken } : {}),
    });
  }

  private writeDeliveryFailure(socket: net.Socket, messageId: string, reason: string, code: string, retryable = false): void {
    writeMessage(socket, { type: "delivery_failed", messageId, reason, delivery: "failed", code, retryable, outcomeKnown: true });
  }

  private deliveryFingerprint(message: Message, targetId: string, contactKind: "direct" | "broadcast"): string {
    return JSON.stringify({
      targetId,
      contactKind,
      text: message.content.text,
      attachments: message.content.attachments,
      replyTo: message.replyTo,
      expectsReply: message.expectsReply,
      supersedes: message.supersedes,
      retryOf: message.retryOf,
      provenance: message.provenance,
    });
  }

  private deliveryRecordKey(fromSessionId: string, messageId: string): string {
    return JSON.stringify([fromSessionId, messageId]);
  }

  private replayOrReject(socket: net.Socket, fromSessionId: string, messageId: string, fingerprint: string): boolean {
    this.pruneDeliveryRecords();
    const record = this.deliveryRecords.get(this.deliveryRecordKey(fromSessionId, messageId));
    if (!record) return false;
    if (record.fingerprint !== fingerprint) {
      this.writeDeliveryFailure(socket, messageId, "Message id was reused with different authored content", "E_MESSAGE_ID_REUSE");
      return true;
    }
    if (record.code === "E_TARGET_REBOUND" && record.retryable) {
      return false;
    }
    if (record.state === "socket_delivered" || record.state === "queued") {
      if (record.senderContact && !record.senderContact.durableBaseline && (!record.contactToken || !this.pendingDirectContacts.has(record.contactToken))) {
        record.contactToken = this.trackDirectContact(fromSessionId, record.senderContact);
      }
      this.writeDeliverySuccess(socket, messageId, record.state, record.peerCompaction, record.contactToken);
    } else {
      this.writeDeliveryFailure(socket, messageId, record.reason ?? "Previous delivery failed", record.code ?? "E_DELIVERY_FAILED", record.retryable);
    }
    return true;
  }

  private recordDelivery(
    fromSessionId: string,
    messageId: string,
    fingerprint: string,
    state: DeliveryState,
    reason?: string,
    code?: string,
    retryable = false,
    peerCompaction?: PeerCompactionNotice,
    contactToken?: string,
    senderContact?: DirectContactPlan,
  ): void {
    this.pruneDeliveryRecords();
    while (this.deliveryRecords.size >= MAX_DELIVERY_RECORDS) {
      const oldest = this.deliveryRecords.keys().next().value;
      if (oldest === undefined) break;
      this.deliveryRecords.delete(oldest);
    }
    this.deliveryRecords.set(this.deliveryRecordKey(fromSessionId, messageId), {
      fingerprint,
      state,
      ...(reason ? { reason } : {}),
      ...(code ? { code } : {}),
      retryable,
      outcomeKnown: true,
      ...(peerCompaction ? { peerCompaction } : {}),
      ...(contactToken ? { contactToken } : {}),
      ...(senderContact ? { senderContact } : {}),
      createdAt: Date.now(),
    });
  }

  private pruneDeliveryRecords(now = Date.now()): void {
    for (const [key, record] of this.deliveryRecords) {
      if (now - record.createdAt > DELIVERY_RECORD_RETENTION_MS) this.deliveryRecords.delete(key);
    }
  }

  private updateDeliveryRecord(fromSessionId: string, messageId: string, state: DeliveryState, reason?: string, code?: string): void {
    const record = this.deliveryRecords.get(this.deliveryRecordKey(fromSessionId, messageId));
    if (!record) return;
    record.state = state;
    record.reason = reason;
    record.code = code;
    record.retryable = false;
    record.outcomeKnown = true;
  }

  private flushMailboxForSession(session: ConnectedSession, now = Date.now()): void {
    this.pruneMailboxMessages(now);
    const sessionName = session.info.name?.toLowerCase();
    const uniqueMailboxIdentity = this.findLiveSessionsSharingMailboxIdentity(session).length === 1;

    for (let index = 0; index < this.mailboxMessages.length;) {
      const entry = this.mailboxMessages[index]!;
      if (!sameScope(entry.targetScopeId, session.scopeId)) {
        index += 1;
        continue;
      }
      const matchesId = entry.targetKey === session.key;
      const matchesSenderIdentity = Boolean(
        sessionName
        && sameScope(entry.fromScopeId, session.scopeId)
        && entry.from.name?.toLowerCase() === sessionName
        && sameCwd(entry.from.cwd, session.info.cwd),
      );
      const matchesUniqueName = Boolean(
        uniqueMailboxIdentity
        && sessionName
        && !matchesSenderIdentity
        && entry.target.name?.toLowerCase() === sessionName
        && sameCwd(entry.target.cwd, session.info.cwd),
      );
      if (!matchesId && !matchesUniqueName) {
        index += 1;
        continue;
      }

      const liveSender = this.sessions.get(entry.fromKey);
      const receiverContact = entry.contactKind === "direct" && this.supportsCompactionAwareness(session)
        ? this.directContactPlan(
            session.scopeId,
            session.info,
            liveSender?.info ?? entry.from,
            liveSender !== undefined,
          )
        : undefined;
      const receiverContactToken = receiverContact
        ? this.trackDirectContact(session.key, receiverContact)
        : undefined;
      const deliveredMessage: Message = {
        ...entry.message,
        brokerDeliveredAt: Date.now(),
        ...(receiverContact?.notice ? { peerCompaction: receiverContact.notice } : {}),
        ...(receiverContactToken ? { contactToken: receiverContactToken } : {}),
        ...(receiverContact?.durableBaseline ? { contactBaseline: true } : {}),
      };
      writeMessage(session.socket, {
        type: "message",
        from: entry.from,
        message: deliveredMessage,
      });
      this.mailboxMessages.splice(index, 1);
      const edge = this.askEdges.get(entry.message.id);
      if (edge?.to === entry.targetKey) {
        edge.to = session.key;
      }
      this.messageReceiptRoutes.set(entry.message.id, {
        from: entry.fromKey,
        to: session.key,
        createdAt: entry.message.brokerReceivedAt ?? entry.queuedAt,
      });
      this.updateDeliveryRecord(entry.fromKey, entry.message.id, "socket_delivered");
    }
  }

  private pruneAskEdges(now = Date.now()): void {
    this.prunePendingAskRecords(now);
    for (const [messageId, edge] of this.askEdges) {
      if (now - edge.createdAt > this.askTimeoutMs) {
        this.askEdges.delete(messageId);
        this.removePendingAskRecord(messageId, edge.scopeId);
      }
    }
  }

  private clearAskEdgesForSession(sessionKey: string): void {
    for (const [messageId, edge] of this.askEdges) {
      if (edge.from === sessionKey || edge.to === sessionKey) {
        this.askEdges.delete(messageId);
        this.removePendingAskRecord(messageId, edge.scopeId);
      }
    }
  }

  private writePendingAskRecord(message: Message, from: ConnectedSession, target: SessionInfo, createdAt: number): void {
    ensurePendingAskRecordDir();
    const record: PendingAskRecord = {
      askId: message.id,
      messageId: message.id,
      asker: { sessionId: from.info.id, name: from.info.name ?? null },
      target: { sessionId: target.id, name: target.name ?? null },
      question: message.content.text,
      createdAt,
      expiresAt: createdAt + this.askTimeoutMs,
    };
    const filePath = scopedPendingAskRecordPath(from.scopeId, message.id);
    writeFileSync(filePath, `${JSON.stringify(record, null, 2)}\n`, { mode: INTERCOM_RUNTIME_FILE_MODE });
    restrictIntercomRuntimeFile(filePath);
  }

  private removePendingAskRecord(messageId: string, scopeId?: string): void {
    try {
      unlinkSync(scopedPendingAskRecordPath(scopeId, messageId));
    } catch (error) {
      if (!isRecord(error) || error.code !== "ENOENT") {
        throw error;
      }
    }
  }

  private prunePendingAskRecords(now = Date.now()): void {
    ensurePendingAskRecordDir();
    for (const entry of readdirSync(PENDING_ASKS_DIR, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith(".json")) {
        continue;
      }
      const filePath = join(PENDING_ASKS_DIR, entry.name);
      let parsed: unknown;
      try {
        parsed = JSON.parse(readFileSync(filePath, "utf-8"));
      } catch {
        unlinkSync(filePath);
        continue;
      }
      if (!isPendingAskRecord(parsed) || now > parsed.expiresAt) {
        unlinkSync(filePath);
      }
    }
  }

  private pruneMessageReceiptRoutes(now = Date.now()): void {
    for (const [messageId, route] of this.messageReceiptRoutes) {
      if (now - route.createdAt > MESSAGE_RECEIPT_ROUTE_RETENTION_MS) {
        this.messageReceiptRoutes.delete(messageId);
      }
    }
  }

  private clearMessageReceiptRoutesForSession(sessionKey: string): void {
    for (const [messageId, route] of this.messageReceiptRoutes) {
      if (route.from === sessionKey || route.to === sessionKey) {
        this.messageReceiptRoutes.delete(messageId);
      }
    }
  }

  // ACL fork: every lookup path is scoped through the requester's own
  // visibility. A hidden session behaves exactly like a nonexistent one —
  // callers get "Session not found", never a distinguishable ACL error, so
  // existence of an out-of-scope session is never leaked.
  // Fork: registration/presence names are de-duplicated against every other
  // live session in the same scope (independent of ACL visibility, so no
  // observer can ever face a by-name ambiguity) so roster names are always
  // uniquely addressable. A colliding name is auto-suffixed ("name-2",
  // "name-3", ...) instead of failing at send time with E_AMBIGUOUS_TARGET.
  // Deliberate advertise claims keep the stricter reject (E_NAME_TAKEN).
  private dedupeSessionName(name: string | undefined, scopeId: string | undefined, selfKey: string): string | undefined {
    const trimmed = name?.trim();
    if (!trimmed) {
      return name;
    }
    const taken = (candidate: string): boolean => {
      const lower = candidate.toLowerCase();
      return Array.from(this.sessions.values()).some(
        (session) =>
          session.key !== selfKey
          && sameScope(session.scopeId, scopeId)
          && (session.info.name?.toLowerCase() === lower || session.info.id === candidate),
      );
    };
    if (!taken(trimmed)) {
      return name;
    }
    for (let attempt = 2; attempt < 1000; attempt += 1) {
      const candidate = `${trimmed}-${attempt}`;
      if (!taken(candidate)) {
        return candidate;
      }
    }
    return `${trimmed}-${randomUUID().slice(0, 8)}`;
  }

  private isVisibleTo(observerKey: string, subject: SessionInfo): boolean {
    const observer = this.sessions.get(observerKey);
    if (!observer) {
      return false;
    }
    return canSeeSession(observer.info, subject);
  }

  private findSessions(nameOrId: string, scopeId: string | undefined, requesterKey: string): ConnectedSession[] {
    const visible = (session: ConnectedSession) => this.isVisibleTo(requesterKey, session.info);

    const byId = this.sessions.get(scopedSessionKey(scopeId, nameOrId));
    if (byId) {
      return visible(byId) ? [byId] : [];
    }

    const lowerName = nameOrId.toLowerCase();
    const byName = Array.from(this.sessions.values()).filter(session => sameScope(session.scopeId, scopeId) && session.info.name?.toLowerCase() === lowerName && visible(session));
    if (byName.length > 0) {
      return byName;
    }

    return Array.from(this.sessions.entries())
      .filter(([, session]) => sameScope(session.scopeId, scopeId) && session.info.id.startsWith(nameOrId) && visible(session))
      .map(([, session]) => session);
  }

  private findDisconnectedSessions(nameOrId: string, scopeId: string | undefined, requesterKey: string): DisconnectedSession[] {
    this.pruneDisconnectedSessions();
    const observer = this.sessions.get(requesterKey);
    const visible = (session: DisconnectedSession) => Boolean(observer) && canSeeSession(observer!.info, session.info);

    const byId = this.disconnectedSessions.get(scopedSessionKey(scopeId, nameOrId));
    if (byId) {
      return visible(byId) ? [byId] : [];
    }

    const lowerName = nameOrId.toLowerCase();
    const byName = Array.from(this.disconnectedSessions.values()).filter(session => sameScope(session.scopeId, scopeId) && session.info.name?.toLowerCase() === lowerName && visible(session));
    if (byName.length > 0) {
      return byName;
    }

    return Array.from(this.disconnectedSessions.entries())
      .filter(([, session]) => sameScope(session.scopeId, scopeId) && session.info.id.startsWith(nameOrId) && visible(session))
      .map(([, session]) => session);
  }

  private findUniqueLiveSessionForDisconnectedSession(disconnected: DisconnectedSession, senderKey?: string): ConnectedSession | null {
    const matches = this.findLiveSessionsSharingMailboxIdentity(disconnected)
      .filter((session) => session.key !== senderKey);
    return matches.length === 1 ? matches[0]! : null;
  }

  /**
   * Mailbox identity is an explicit name plus directory, never name alone. A
   * runtime fallback alias is derived from the session id rather than chosen as
   * a durable identity, so it must not transfer mail to another process. This
   * also prevents two unnamed UUIDv7 sessions started close together from
   * inheriting each other's mailbox through a shared short alias.
   *
   * Directories compare through sameCwd so a relaunch that reports the same
   * directory differently (trailing slash, "."/"..", or a symlink such as macOS
   * /tmp vs /private/tmp) still matches.
   */
  private findLiveSessionsSharingMailboxIdentity(sessionInfo: ConnectedSession | DisconnectedSession): ConnectedSession[] {
    const lowerName = sessionInfo.info.name?.toLowerCase();
    if (!lowerName || sessionInfo.info.runtimeFallbackAlias) {
      return [];
    }
    return Array.from(this.sessions.values()).filter(session =>
      sameScope(session.scopeId, sessionInfo.scopeId)
      && !session.info.runtimeFallbackAlias
      && session.info.name?.toLowerCase() === lowerName
      && sameCwd(session.info.cwd, sessionInfo.info.cwd)
    );
  }

  private broadcast(msg: BrokerMessage, exclude?: string, scopeId?: string): void {
    for (const [id, session] of this.sessions) {
      if (id !== exclude && sameScope(session.scopeId, scopeId)) {
        writeMessage(session.socket, msg);
      }
    }
  }

  // ACL fork: like broadcast(), but only reaches sessions that are allowed to
  // see `subject` (the session that joined/left/changed presence) within the
  // same scope. Used for session_joined, session_left, and presence_update so
  // hidden peers never leak into a client's live session cache via these push
  // events, even though the pull-based "list" response is separately filtered
  // too.
  private broadcastScoped(msg: BrokerMessage, subject: SessionInfo, exclude?: string, scopeId?: string): void {
    for (const [id, session] of this.sessions) {
      if (id === exclude) {
        continue;
      }
      if (!sameScope(session.scopeId, scopeId)) {
        continue;
      }
      if (!canSeeSession(session.info, subject)) {
        continue;
      }
      writeMessage(session.socket, msg);
    }
  }

  private validateExtensionCapability(cap: unknown): cap is ExtensionCapability {
    if (typeof cap !== "object" || cap === null) {
      return false;
    }
    const c = cap as Record<string, unknown>;
    if (typeof c.namespace !== "string" || typeof c.ownerEligible !== "boolean") {
      return false;
    }
    return this.validateNamespace(c.namespace);
  }

  private validateNamespace(ns: string): boolean {
    // ^[a-z0-9][a-z0-9._/-]{0,63}$
    if (ns.length === 0 || ns.length > 64) {
      return false;
    }
    if (!/^[a-z0-9]/.test(ns)) {
      return false;
    }
    if (!/^[a-z0-9][a-z0-9._/-]*$/.test(ns)) {
      return false;
    }
    return true;
  }

  private recomputeNamespaceOwners(): void {
    const namespaces = new Map<string, { namespace: string; scopeId?: string }>();
    for (const [key, owner] of this.namespaceOwners) {
      namespaces.set(key, {
        namespace: owner.namespace,
        ...(owner.scopeId ? { scopeId: owner.scopeId } : {}),
      });
    }
    for (const session of this.sessions.values()) {
      for (const extension of session.extensions ?? []) {
        namespaces.set(scopedExtensionKey(session.scopeId, extension.namespace), {
          namespace: extension.namespace,
          ...(session.scopeId ? { scopeId: session.scopeId } : {}),
        });
      }
    }

    // For each namespace, elect owner by (startedAt, sessionId).
    for (const [namespaceKey, scopedNamespace] of namespaces) {
      const { namespace, scopeId } = scopedNamespace;
      const candidates: Array<{ sessionKey: string; session: ConnectedSession }> = [];
      for (const [sessionKey, session] of this.sessions) {
        if (session.extensions) {
          const hasNamespace = session.extensions.some(
            (ext) => sameScope(session.scopeId, scopeId) && ext.namespace === namespace && ext.ownerEligible
          );
          if (hasNamespace) {
            candidates.push({ sessionKey, session });
          }
        }
      }

      if (candidates.length === 0) {
        if (this.namespaceOwners.delete(namespaceKey)) {
          for (const session of this.sessions.values()) {
            const isCapable = sameScope(session.scopeId, scopeId)
              && session.extensions?.some((extension) => extension.namespace === namespace);
            if (isCapable) {
              writeMessage(session.socket, { type: "extension_owner", namespace });
            }
          }
        }
        continue;
      }

      // Use broker-owned registration order so clients cannot seize authority
      // by backdating their advertised session start time. Stable-ID socket
      // replacements preserve the original order.
      candidates.sort((a, b) => {
        if (a.session.ownerOrder !== b.session.ownerOrder) {
          return a.session.ownerOrder - b.session.ownerOrder;
        }
        return a.session.info.id.localeCompare(b.session.info.id);
      });

      const winner = candidates[0];
      const existing = this.namespaceOwners.get(namespaceKey);

      const ownerChanged = !existing || existing.sessionKey !== winner.sessionKey;
      const socketChanged = existing && existing.socket !== winner.session.socket;

      if (ownerChanged || socketChanged) {
        const epoch = randomUUID();
        this.namespaceOwners.set(namespaceKey, {
          namespace,
          sessionKey: winner.sessionKey,
          sessionId: winner.session.info.id,
          socket: winner.session.socket,
          epoch,
          ...(scopeId ? { scopeId } : {}),
        });

        for (const session of this.sessions.values()) {
          if (session.extensions?.length) {
            const isCapable = sameScope(session.scopeId, scopeId)
              && session.extensions.some((ext) => ext.namespace === namespace);
            if (isCapable) {
              writeMessage(session.socket, {
                type: "extension_owner",
                namespace,
                ownerId: winner.session.info.id,
                ownerEpoch: epoch,
              });
            }
          }
        }
      }
    }
  }

  private handleExtensionPublish(
    socket: net.Socket,
    currentKey: string | null,
    msg: Record<string, unknown>
  ): void {
    if (!currentKey) {
      throw new Error("Received extension_publish before register");
    }

    const session = this.sessions.get(currentKey);
    if (!session || session.socket !== socket) {
      writeMessage(socket, { type: "error", error: "Session not found" });
      return;
    }

    if (!session.extensions?.length) {
      writeMessage(socket, { type: "error", error: "Session has not advertised extension capability" });
      return;
    }

    const namespace = msg.namespace;
    const audience = msg.audience;
    const ownerOnly = msg.ownerOnly === true;
    const ownerEpoch = msg.ownerEpoch;
    const payload = msg.payload;

    if (typeof namespace !== "string" || !this.validateNamespace(namespace)) {
      writeMessage(socket, { type: "error", error: "Invalid namespace" });
      return;
    }

    if (audience !== "owner" && audience !== "capable") {
      writeMessage(socket, { type: "error", error: "Invalid audience" });
      return;
    }

    const payloadSize = serializedPayloadSize(payload);
    if (payloadSize === null || payloadSize > MAX_EXTENSION_MESSAGE_BYTES) {
      writeMessage(socket, { type: "error", error: "Invalid extension payload or payload exceeds 16 KiB limit" });
      return;
    }

    // Verify sender has capability for this namespace
    const hasCapability = session.extensions?.some((ext) => ext.namespace === namespace);
    if (!hasCapability) {
      writeMessage(socket, { type: "error", error: "Sender does not have capability for this namespace" });
      return;
    }

    const owner = this.namespaceOwners.get(scopedExtensionKey(session.scopeId, namespace));
    if ((audience === "owner" || ownerOnly) && !owner) {
      writeMessage(socket, { type: "error", error: "No owner for this namespace" });
      return;
    }

    // For owner-only messages, validate exact socket and epoch
    if (ownerOnly && owner) {
      if (typeof ownerEpoch !== "string") {
        writeMessage(socket, { type: "error", error: "ownerEpoch required for owner-only messages" });
        return;
      }
      if (currentKey !== owner.sessionKey || socket !== owner.socket || ownerEpoch !== owner.epoch) {
        writeMessage(socket, { type: "error", error: "Owner validation failed" });
        return;
      }
    }

    // Route message to appropriate audience
    for (const [recipientId, recipientSession] of this.sessions) {
      if (!sameScope(recipientSession.scopeId, session.scopeId)) {
        continue;
      }
      if (!recipientSession.extensions?.length) {
        continue;
      }

      const isCapable = recipientSession.extensions.some((ext) => ext.namespace === namespace);
      if (!isCapable) {
        continue;
      }

      const shouldReceive =
        audience === "capable" ||
        (audience === "owner" && owner !== undefined &&
          recipientId === owner.sessionKey &&
          recipientSession.socket === owner.socket);

      if (shouldReceive) {
        writeMessage(recipientSession.socket, {
          type: "extension_message",
          namespace,
          fromSessionId: session.info.id,
          ...(owner ? { ownerId: owner.sessionId, ownerEpoch: owner.epoch } : {}),
          payload,
        });
      }
    }
  }

  private handleExtensionStateCommit(
    socket: net.Socket,
    currentKey: string | null,
    msg: Record<string, unknown>
  ): void {
    if (!currentKey) {
      throw new Error("Received extension_state_commit before register");
    }

    const session = this.sessions.get(currentKey);
    if (!session || session.socket !== socket) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace: String(msg.namespace || ""),
        committed: false,
        revision: 0,
        reason: "Session not found",
      });
      return;
    }

    if (!session.extensions?.length) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace: String(msg.namespace || ""),
        committed: false,
        revision: 0,
        reason: "Session has not advertised extension capability",
      });
      return;
    }

    const namespace = msg.namespace;
    const ownerEpoch = msg.ownerEpoch;
    const expectedRevision = msg.expectedRevision;
    const payload = msg.payload;

    if (typeof namespace !== "string" || !this.validateNamespace(namespace)) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace: String(namespace),
        committed: false,
        revision: 0,
        reason: "Invalid namespace",
      });
      return;
    }
    const stateNamespace = scopedExtensionStateNamespace(session.scopeId, namespace);

    if (typeof ownerEpoch !== "string") {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Invalid ownerEpoch",
      });
      return;
    }

    if (typeof expectedRevision !== "number" || !Number.isSafeInteger(expectedRevision) || expectedRevision < 0) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Invalid expectedRevision",
      });
      return;
    }

    const payloadSize = serializedPayloadSize(payload);
    if (payloadSize === null || payloadSize > MAX_EXTENSION_STATE_BYTES) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Invalid extension state or payload exceeds 64 KiB limit",
      });
      return;
    }

    // Verify sender has capability for this namespace
    const hasCapability = session.extensions?.some((ext) => ext.namespace === namespace);
    if (!hasCapability) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Sender does not have capability for this namespace",
      });
      return;
    }

    const owner = this.namespaceOwners.get(scopedExtensionKey(session.scopeId, namespace));
    if (!owner) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "No owner for this namespace",
      });
      return;
    }

    // Validate owner, socket, and epoch
    if (currentKey !== owner.sessionKey || socket !== owner.socket || ownerEpoch !== owner.epoch) {
      writeMessage(socket, {
        type: "extension_state_result",
        namespace,
        committed: false,
        revision: this.extensionStateManager.getCurrentRevision(stateNamespace),
        reason: "Owner validation failed",
      });
      return;
    }

    const result = this.extensionStateManager.commitState(stateNamespace, expectedRevision, payload);

    // Send result to committer
    writeMessage(socket, {
      type: "extension_state_result",
      namespace,
      committed: result.committed,
      revision: result.revision,
      reason: result.reason,
    });

    // If committed, broadcast new state to all capable sessions
    if (result.committed) {
      for (const recipientSession of this.sessions.values()) {
        if (!sameScope(recipientSession.scopeId, session.scopeId)) {
          continue;
        }
        if (!recipientSession.extensions?.length) {
          continue;
        }

        const isCapable = recipientSession.extensions.some((ext) => ext.namespace === namespace);
        if (isCapable) {
          writeMessage(recipientSession.socket, {
            type: "extension_state",
            namespace,
            revision: result.revision,
            payload,
          });
        }
      }
    }
  }

  private shutdown(): void {
    console.log("Broker shutting down");

    if (this.maintenanceTimer) {
      clearInterval(this.maintenanceTimer);
      this.maintenanceTimer = null;
    }
    for (const session of this.sessions.values()) {
      session.socket.end();
    }
    this.sessions.clear();
    this.askEdges.clear();
    this.messageReceiptRoutes.clear();
    this.pendingDirectContacts.clear();
    this.disconnectedSessions.clear();
    this.mailboxMessages.length = 0;
    try {
      this.collaborationState.close();
    } catch (error) {
      console.error("Failed to flush collaboration state during shutdown:", error);
    }
    if (typeof LISTEN_TARGET === "string" && process.platform !== "win32") {
      try {
        unlinkSync(LISTEN_TARGET);
      } catch {
        // The socket may already be gone if shutdown started after a disconnect.
      }
    }
    try {
      unlinkSync(PORT_PATH);
    } catch {
      // The TCP endpoint file only exists when opt-in TCP transport is active.
    }
    try {
      unlinkSync(PID_PATH);
    } catch {
      // The PID file may already be gone if startup never completed.
    }
    this.server.close();
    process.exit(0);
  }
}

new IntercomBroker().start();
