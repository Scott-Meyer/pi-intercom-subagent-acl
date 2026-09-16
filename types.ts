export const EXTENSION_BUS_FEATURE = "extension-bus-v1";
export const EXACT_SEND_FEATURE = "exact-send-v1";
export const COMPACTION_AWARENESS_FEATURE = "compaction-awareness-v1";
export const SESSION_PROFILE_FEATURE = "session-profile-v1";

export type DeliveryState = "socket_delivered" | "queued" | "failed" | "unknown";

export interface PeerCompactionNotice {
  /** Stable broker session ID of the peer whose context was compacted. */
  peerSessionId: string;
  /** Current broker display name, used to disambiguate mailbox identity rebound. */
  peerName?: string;
  /** Originally selected stable ID when mailbox delivery rebound to another live identity. */
  requestedPeerSessionId?: string;
  /** Current successful-compaction generation for that logical peer. */
  generation: number;
  /** Generation this session had observed at its previous direct contact. */
  previousGeneration: number;
  /** Informational timestamp only; generation comparison determines the notice. */
  compactedAt: number;
  /** Current live context usage when known. */
  contextPct?: number;
}

export interface DeliveryDetails {
  delivery: DeliveryState;
  code?: string;
  retryable: boolean;
  outcomeKnown: boolean;
  /** Present once when the recipient compacted since the prior direct contact. */
  peerCompaction?: PeerCompactionNotice;
  /** Opaque broker correlation acknowledged by capable clients; not user-authored. */
  contactToken?: string;
}

export interface SessionInfo {
  id: string;
  /** Broker-owned lifetime of this live endpoint. */
  endpointEpoch?: string;
  name?: string;
  /** Concise self-authored current focus for peer discovery (5-9 words).
   *  Display metadata only: never used for routing, identity, ACLs, or mailbox ownership. */
  description?: string;
  /** True only when the extension synthesized name for an unnamed runtime. */
  runtimeFallbackAlias?: boolean;
  cwd: string;
  model: string;
  pid: number;
  startedAt: number;
  lastActivity: number;
  status?: string;
  peerUid?: number;
  trustedLocal?: boolean;
  /** Live context-window usage, pushed via presence from the source session's
   *  getContextUsage(). contextPct is 0..100 (rounded); contextTokens /
   *  contextWindow are raw token counts. All optional: unknown right after a
   *  compaction (before the next assistant response), when no model is selected,
   *  or on older clients that never report it. */
  contextPct?: number;
  contextTokens?: number;
  contextWindow?: number;
  /** tmux pane id (e.g. "%212") of the session's terminal, read from
   *  $TMUX_PANE at registration. Present only when the session runs inside a
   *  tmux pane; absent for cloud, headless, IDE-embedded, or terminal-manager sessions.
   *  The pane id is immutable for the process lifetime — unlike the window
   *  name, which is mutable — so a peer can live-resolve the current window
   *  from it via tmux when it needs to introspect or drive that pane. */
  tmuxPane?: string;
  /** ACL fork: true when this session is a pi-subagents delegated child.
   *  Set from local PI_SUBAGENT_* env vars at registration; the broker never
   *  infers this from name/shape heuristics. */
  isSubagent?: boolean;
  /** ACL fork: the supervisor session's broker session ID, when the child
   *  process knows it (PI_SUBAGENT_ORCHESTRATOR_SESSION_ID). Preferred over
   *  supervisorName for matching because IDs are stable and unambiguous. */
  supervisorSessionId?: string;
  /** ACL fork: the supervisor session's intercom name/target
   *  (PI_SUBAGENT_ORCHESTRATOR_TARGET), used to match a supervisor when the
   *  child does not have the supervisor's session ID yet. */
  supervisorName?: string;
  /** ACL fork: true once a subagent has explicitly self-promoted via the
   *  `advertise` action. An advertised child is treated as an ordinary main
   *  for visibility in both directions (everyone sees it, it sees everyone),
   *  while isSubagent/supervisorSessionId/supervisorName are preserved as
   *  provenance rather than erased. Broker-authoritative; never set directly
   *  by a presence update. */
  advertised?: boolean;
  /** Generic capabilities this session offers to other sessions through the
   *  roster (e.g. the pi-intercom/project-launch-v1 provider namespace).
   *  Broker-authoritative from registration and extension_capabilities_update. */
  extensions?: ExtensionCapability[];
  /** Broker-authored provenance for a live session imported from one peer link.
   *  The tuple is the canonical remote routing identity; labels remain display-only. */
  federation?: {
    originId: string;
    originLabel?: string;
    remoteScopeAlias: string;
    remoteStableSessionId: string;
  };
}

export interface Message {
  id: string;
  timestamp: number;
  senderSequence?: number;
  brokerReceivedAt?: number;
  brokerDeliveredAt?: number;
  receiverReceivedAt?: number;
  injectedAt?: number;
  supersedes?: string;
  retryOf?: string;
  replyTo?: string;
  expectsReply?: boolean;
  provenance?: MessageProvenance;
  /** Broker-authored notice about the sender, attached only to direct contact. */
  peerCompaction?: PeerCompactionNotice;
  /** Opaque broker correlation proving this direct message reached a capable client. */
  contactToken?: string;
  /** True when the token stages a first-contact baseline pending receiver ACK. */
  contactBaseline?: boolean;
  content: {
    text: string;
    attachments?: Attachment[];
  };
}

export interface MessageProvenance {
  type: "extension_outbox";
  extensionId: string;
  extensionName: string;
  requestId: string;
}

export interface Attachment {
  type: "file" | "snippet" | "context";
  name: string;
  content: string;
  language?: string;
}

export type MessageReceiptStatus = "receiver_received" | "queued" | "injected" | "acknowledged" | "expired" | "cancelled" | "superseded" | "cancellation_requested";

export interface MessageReceipt {
  messageId: string;
  status: MessageReceiptStatus;
  timestamp: number;
  detail?: string;
}

export type MessageControlAction = "cancel" | "supersede";

export interface MessageControl {
  messageId: string;
  action: MessageControlAction;
  timestamp: number;
  supersededBy?: string;
  detail?: string;
}

export interface ExtensionCapability {
  namespace: string;
  ownerEligible: boolean;
}

export type SessionRegistration = Omit<SessionInfo, "id" | "endpointEpoch" | "peerUid" | "trustedLocal">;

export type ClientMessage =
  | { type: "register"; session: SessionRegistration; sessionId?: string; stateId?: string; scopeId?: string; clientFeatures?: string[] }
  | { type: "unregister" }
  | { type: "extension_capabilities_update"; extensions: ExtensionCapability[] }
  | { type: "list"; requestId: string }
  | { type: "advertise"; requestId: string; name: string }
  | { type: "send"; to: string; message: Message; targetId?: string; targetEpoch?: string; contactKind?: "direct" | "broadcast" }
  | { type: "compaction_completed"; eventId: string }
  | { type: "direct_contact_seen"; token: string }
  | { type: "message_receipt"; receipt: MessageReceipt }
  | { type: "cancel_message"; messageId: string }
  | { type: "cancel_ask"; messageId: string }
  | { type: "presence"; name?: string; description?: string | null; runtimeFallbackAlias?: boolean; status?: string; model?: string; contextPct?: number | null; contextTokens?: number | null; contextWindow?: number | null }
  | {
      type: "extension_publish";
      namespace: string;
      audience: "owner" | "capable";
      ownerEpoch?: string;
      ownerOnly?: boolean;
      payload: unknown;
    }
  | {
      type: "extension_state_commit";
      namespace: string;
      ownerEpoch: string;
      expectedRevision: number;
      payload: unknown;
    };

export type BrokerMessage =
  | { type: "registered"; sessionId: string; features?: string[]; session?: SessionInfo }
  | { type: "direct_contact_recorded"; token: string }
  | { type: "direct_contact_unknown"; token: string }
  | { type: "compaction_recorded"; eventId: string; generation: number; compactedAt: number }
  | { type: "compaction_record_failed"; eventId: string; error: string }
  | { type: "sessions"; requestId: string; sessions: SessionInfo[] }
  | { type: "advertise_result"; requestId: string; ok: boolean; name?: string; error?: string; code?: string }
  | { type: "message"; from: SessionInfo; message: Message }
  | { type: "presence_update"; session: SessionInfo }
  | { type: "session_joined"; session: SessionInfo }
  | { type: "session_left"; sessionId: string }
  | { type: "error"; error: string }
  | ({ type: "delivered"; messageId: string } & DeliveryDetails)
  | ({ type: "delivery_failed"; messageId: string; reason: string } & DeliveryDetails)
  | { type: "message_receipt"; from: SessionInfo; receipt: MessageReceipt }
  | { type: "message_control"; from: SessionInfo; control: MessageControl }
  | { type: "extension_owner"; namespace: string; ownerId?: string; ownerEpoch?: string }
  | {
      type: "extension_message";
      namespace: string;
      fromSessionId: string;
      ownerId?: string;
      ownerEpoch?: string;
      payload: unknown;
    }
  | {
      type: "extension_state";
      namespace: string;
      revision: number;
      payload: unknown;
    }
  | {
      type: "extension_state_result";
      namespace: string;
      committed: boolean;
      revision: number;
      reason?: string;
    };
