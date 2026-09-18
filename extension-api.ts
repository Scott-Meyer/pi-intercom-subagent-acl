import type { SessionInfo } from "./types.ts";

export const PARLEY_EXTENSION_REGISTER_EVENT = "parley:extension-register";
export const PARLEY_EXTENSION_REGISTRY_READY_EVENT = "parley:extension-registry-ready";
export const PARLEY_OUTBOX_REQUEST_EVENT = "parley:outbox-request";
export const PARLEY_OUTBOX_RESULT_EVENT = "parley:outbox-result";

export type ParleyOutboxResultStatus = "sent" | "rejected" | "blocked" | "failed";

export type ParleyOutboxResultCode =
  | "user_cancelled"
  | "confirmation_unavailable"
  | "session_unavailable"
  | "session_ended"
  | "invalid_request"
  | "duplicate_request"
  | "target_not_found"
  | "target_ambiguous"
  | "self_target"
  | "delivery_failed";

export interface ParleyOutboxRequestV1 {
  version: 1;
  requestId: string;
  extensionId: string;
  extensionName: string;
  to: string;
  message: string;
}

export type ParleyOutboxRequest = ParleyOutboxRequestV1;

export interface ParleyOutboxResultV1 {
  version: 1;
  requestId: string;
  status: ParleyOutboxResultStatus;
  code?: ParleyOutboxResultCode;
  extensionId?: string;
  extensionName?: string;
  messageId?: string;
  detail?: string;
}

export type ParleyOutboxResult = ParleyOutboxResultV1;

export interface ParleyExtensionOwner {
  sessionId: string;
  epoch: string;
}

export interface ParleyExtensionState {
  revision: number;
  payload: unknown;
}

export type ParleyExtensionEvent =
  | { type: "connection"; connected: boolean; supported: boolean }
  | { type: "owner"; owner?: ParleyExtensionOwner }
  | { type: "message"; fromSessionId: string; owner?: ParleyExtensionOwner; payload: unknown }
  | { type: "state"; state: ParleyExtensionState }
  | { type: "state_result"; committed: boolean; revision: number; reason?: string }
  | { type: "session_joined"; session: SessionInfo }
  | { type: "session_left"; sessionId: string }
  | { type: "presence_update"; session: SessionInfo };

export interface ParleyExtensionChannel {
  readonly namespace: string;
  snapshot(): {
    connected: boolean;
    supported: boolean;
    owner?: ParleyExtensionOwner;
    state?: ParleyExtensionState;
  };
  publish(payload: unknown, options?: { audience?: "owner" | "capable"; ownerOnly?: boolean }): void;
  commitState(payload: unknown, expectedRevision?: number): void;
  listSessions(): Promise<SessionInfo[]>;
}

export interface ParleyExtensionRegistration {
  namespace: string;
  ownerEligible: boolean;
  onEvent(event: ParleyExtensionEvent): void;
  onReady(channel: ParleyExtensionChannel): void;
}
