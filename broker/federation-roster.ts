import type { SessionInfo } from "../types.ts";
import { isSessionInfo } from "./protocol.ts";
import {
  FEDERATION_PROTOCOL_NAME,
  FEDERATION_PROTOCOL_VERSION,
  FEDERATION_ROSTER_FEATURE,
  FEDERATION_SESSION_ID_MAX_LENGTH,
  type FederationScopeBinding,
} from "./federation-types.ts";
import {
  encodeOriginQualifiedSessionIdentity,
  isCanonicalFederationOriginId,
  isCanonicalFederationScopeAlias,
  isFederationCorrelationId,
} from "./federation-protocol.ts";
import type { FederationPeerLink } from "./peer-link.ts";

const MAX_SNAPSHOT_SESSIONS = 128;
const MAX_DELTA_CHANGES = 256;
const MAX_IMPORT_EPOCHS_PER_LINK = 16;
const MAX_ROSTER_FRAME_BYTES = 900 * 1024;
const MAX_VISIBLE_ROSTER_BYTES_PER_LOCAL_SCOPE = 800 * 1024;
// 128 maximum entries remain below the 900 KiB frame budget even when every
// bounded stable ID requires worst-case JSON escaping.
const MAX_SESSION_PROJECTION_BYTES = 1400;
const CONTROL_OR_FORMAT_CHARACTERS = /[\p{Cc}\p{Cf}]/u;

export interface FederationRosterSessionProjection {
  endpointEpoch?: string;
  name?: string;
  description?: string;
  runtimeFallbackAlias?: boolean;
  cwd: string;
  model: string;
  pid: number;
  startedAt: number;
  lastActivity: number;
  status?: string;
  contextPct?: number;
  contextTokens?: number;
  contextWindow?: number;
  tmuxPane?: string;
}

export interface FederationRosterEntry {
  /** Alias in the exporting broker's namespace. */
  scopeAlias: string;
  stableSessionId: string;
  session: FederationRosterSessionProjection;
}

export interface FederationRosterRemoval {
  scopeAlias: string;
  stableSessionId: string;
}

export interface PeerRosterSnapshot {
  type: "peer_roster_snapshot";
  protocol: typeof FEDERATION_PROTOCOL_NAME;
  version: typeof FEDERATION_PROTOCOL_VERSION;
  originId: string;
  originEpoch: string;
  sequence: number;
  sessions: FederationRosterEntry[];
}

export interface PeerRosterDelta {
  type: "peer_roster_delta";
  protocol: typeof FEDERATION_PROTOCOL_NAME;
  version: typeof FEDERATION_PROTOCOL_VERSION;
  originId: string;
  originEpoch: string;
  sequence: number;
  upserts: FederationRosterEntry[];
  removals: FederationRosterRemoval[];
}

export interface PeerRosterResyncRequest {
  type: "peer_roster_resync_request";
  protocol: typeof FEDERATION_PROTOCOL_NAME;
  version: typeof FEDERATION_PROTOCOL_VERSION;
  originId: string;
  expectedOriginEpoch?: string;
  expectedSequence: number;
}

export type FederationRosterFrame = PeerRosterSnapshot | PeerRosterDelta | PeerRosterResyncRequest;

/** Deliberate single-hop input contract: imported records do not satisfy it. */
export interface LocallyOwnedFederationSession {
  ownership: "local";
  exportEligible: boolean;
  localScopeId: string | null;
  info: SessionInfo;
}

export interface FederationSessionProvenance {
  originId: string;
  originLabel?: string;
  remoteScopeAlias: string;
  remoteStableSessionId: string;
}

export type ImportedFederatedSessionInfo = SessionInfo & {
  trustedLocal: false;
  federation: FederationSessionProvenance;
};

export interface ImportedFederatedSession {
  linkId: string;
  originEpoch: string;
  localScopeId: string | null;
  info: ImportedFederatedSessionInfo;
}

export interface ImportedRosterChange {
  joined: ImportedFederatedSession[];
  updated: ImportedFederatedSession[];
  left: ImportedFederatedSession[];
}

export interface FederationRosterOptions {
  originEpoch: string;
  listLocalSessions: () => readonly LocallyOwnedFederationSession[];
  send: (link: FederationPeerLink, frame: FederationRosterFrame) => void;
  onImportedChange?: (change: ImportedRosterChange) => void;
  onLinkError?: (link: FederationPeerLink, error: FederationRosterError) => void;
}

interface LinkRosterState {
  link: FederationPeerLink;
  exportSequence: number;
  exported: Map<string, FederationRosterEntry>;
  importOriginEpoch?: string;
  importSequence: number;
  awaitingSnapshot: boolean;
  resyncRequested: boolean;
  seenImportEpochs: Set<string>;
  lastResyncResponseSequence?: number;
  imported: Map<string, ImportedFederatedSession>;
}

export class FederationRosterError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FederationRosterError";
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyKeys(value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []): boolean {
  const allowed = new Set([...required, ...optional]);
  return required.every((key) => Object.prototype.hasOwnProperty.call(value, key))
    && Object.keys(value).every((key) => allowed.has(key));
}

function serializedSize(value: unknown): number {
  try {
    return Buffer.byteLength(JSON.stringify(value), "utf8");
  } catch {
    return Number.POSITIVE_INFINITY;
  }
}

function isSafeBoundedText(value: unknown, maxLength: number, allowEmpty = false): value is string {
  return typeof value === "string"
    && value.length <= maxLength
    && (allowEmpty || value.length > 0)
    && !CONTROL_OR_FORMAT_CHARACTERS.test(value);
}

function isStableSessionId(value: unknown): value is string {
  return isSafeBoundedText(value, FEDERATION_SESSION_ID_MAX_LENGTH)
    && value.trim().length > 0;
}

function isSequence(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 1;
}

function isProjection(value: unknown): value is FederationRosterSessionProjection {
  if (!isRecord(value) || !hasOnlyKeys(value, ["cwd", "model", "pid", "startedAt", "lastActivity"], [
    "endpointEpoch", "name", "description", "runtimeFallbackAlias", "status",
    "contextPct", "contextTokens", "contextWindow", "tmuxPane",
  ])) return false;
  if (!isSafeBoundedText(value.cwd, 4096, true) || !isSafeBoundedText(value.model, 256, true)) return false;
  if (!Number.isSafeInteger(value.pid) || (value.pid as number) < 0) return false;
  if (!Number.isSafeInteger(value.startedAt) || (value.startedAt as number) < 0) return false;
  if (!Number.isSafeInteger(value.lastActivity) || (value.lastActivity as number) < 0) return false;
  if (value.endpointEpoch !== undefined && !isFederationCorrelationId(value.endpointEpoch)) return false;
  if (value.status !== undefined && !isSafeBoundedText(value.status, 256, true)) return false;
  if (value.tmuxPane !== undefined && !isSafeBoundedText(value.tmuxPane, 128)) return false;
  if (value.runtimeFallbackAlias !== undefined && typeof value.runtimeFallbackAlias !== "boolean") return false;
  if (value.contextPct !== undefined && (typeof value.contextPct !== "number" || !Number.isFinite(value.contextPct) || value.contextPct < 0 || value.contextPct > 100)) return false;
  for (const field of ["contextTokens", "contextWindow"] as const) {
    if (value[field] !== undefined && (!Number.isSafeInteger(value[field]) || (value[field] as number) < 0)) return false;
  }
  // Reuse canonical name/description validation without accepting trust fields.
  const candidate: SessionInfo = {
    id: "projection",
    cwd: value.cwd,
    model: value.model,
    pid: value.pid as number,
    startedAt: value.startedAt as number,
    lastActivity: value.lastActivity as number,
    ...(value.endpointEpoch !== undefined ? { endpointEpoch: value.endpointEpoch } : {}),
    ...(value.name !== undefined ? { name: value.name as string } : {}),
    ...(value.description !== undefined ? { description: value.description as string } : {}),
    ...(value.runtimeFallbackAlias !== undefined ? { runtimeFallbackAlias: value.runtimeFallbackAlias } : {}),
    ...(value.status !== undefined ? { status: value.status } : {}),
    ...(value.contextPct !== undefined ? { contextPct: value.contextPct } : {}),
    ...(value.contextTokens !== undefined ? { contextTokens: value.contextTokens as number } : {}),
    ...(value.contextWindow !== undefined ? { contextWindow: value.contextWindow as number } : {}),
    ...(value.tmuxPane !== undefined ? { tmuxPane: value.tmuxPane } : {}),
  };
  return isSessionInfo(candidate) && serializedSize(value) <= MAX_SESSION_PROJECTION_BYTES;
}

function isRosterEntry(value: unknown): value is FederationRosterEntry {
  return isRecord(value)
    && hasOnlyKeys(value, ["scopeAlias", "stableSessionId", "session"])
    && isCanonicalFederationScopeAlias(value.scopeAlias)
    && isStableSessionId(value.stableSessionId)
    && isProjection(value.session);
}

function isRosterRemoval(value: unknown): value is FederationRosterRemoval {
  return isRecord(value)
    && hasOnlyKeys(value, ["scopeAlias", "stableSessionId"])
    && isCanonicalFederationScopeAlias(value.scopeAlias)
    && isStableSessionId(value.stableSessionId);
}

function rosterKey(scopeAlias: string, stableSessionId: string): string {
  return JSON.stringify([scopeAlias, stableSessionId]);
}

function noDuplicateEntries(entries: readonly FederationRosterEntry[]): boolean {
  return new Set(entries.map((entry) => rosterKey(entry.scopeAlias, entry.stableSessionId))).size === entries.length;
}

export function isPeerRosterSnapshot(value: unknown): value is PeerRosterSnapshot {
  return isRecord(value)
    && hasOnlyKeys(value, ["type", "protocol", "version", "originId", "originEpoch", "sequence", "sessions"])
    && value.type === "peer_roster_snapshot"
    && value.protocol === FEDERATION_PROTOCOL_NAME
    && value.version === FEDERATION_PROTOCOL_VERSION
    && isCanonicalFederationOriginId(value.originId)
    && isFederationCorrelationId(value.originEpoch)
    && isSequence(value.sequence)
    && Array.isArray(value.sessions)
    && value.sessions.length <= MAX_SNAPSHOT_SESSIONS
    && value.sessions.every(isRosterEntry)
    && noDuplicateEntries(value.sessions)
    && serializedSize(value) <= MAX_ROSTER_FRAME_BYTES;
}

export function isPeerRosterDelta(value: unknown): value is PeerRosterDelta {
  if (!isRecord(value)
    || !hasOnlyKeys(value, ["type", "protocol", "version", "originId", "originEpoch", "sequence", "upserts", "removals"])
    || value.type !== "peer_roster_delta"
    || value.protocol !== FEDERATION_PROTOCOL_NAME
    || value.version !== FEDERATION_PROTOCOL_VERSION
    || !isCanonicalFederationOriginId(value.originId)
    || !isFederationCorrelationId(value.originEpoch)
    || !isSequence(value.sequence)
    || !Array.isArray(value.upserts)
    || !Array.isArray(value.removals)
    || value.upserts.length + value.removals.length < 1
    || value.upserts.length + value.removals.length > MAX_DELTA_CHANGES
    || !value.upserts.every(isRosterEntry)
    || !value.removals.every(isRosterRemoval)
    || !noDuplicateEntries(value.upserts)
    || serializedSize(value) > MAX_ROSTER_FRAME_BYTES) return false;
  const changedKeys = [
    ...value.upserts.map((entry) => rosterKey(entry.scopeAlias, entry.stableSessionId)),
    ...value.removals.map((entry) => rosterKey(entry.scopeAlias, entry.stableSessionId)),
  ];
  return new Set(changedKeys).size === changedKeys.length;
}

export function isPeerRosterResyncRequest(value: unknown): value is PeerRosterResyncRequest {
  return isRecord(value)
    && hasOnlyKeys(value, ["type", "protocol", "version", "originId", "expectedSequence"], ["expectedOriginEpoch"])
    && value.type === "peer_roster_resync_request"
    && value.protocol === FEDERATION_PROTOCOL_NAME
    && value.version === FEDERATION_PROTOCOL_VERSION
    && isCanonicalFederationOriginId(value.originId)
    && isSequence(value.expectedSequence)
    && (value.expectedOriginEpoch === undefined || isFederationCorrelationId(value.expectedOriginEpoch))
    && serializedSize(value) <= MAX_ROSTER_FRAME_BYTES;
}

export function isFederationRosterFrame(value: unknown): value is FederationRosterFrame {
  return isPeerRosterSnapshot(value) || isPeerRosterDelta(value) || isPeerRosterResyncRequest(value);
}

function projectionFromSession(info: SessionInfo): FederationRosterSessionProjection {
  const safeInteger = (value: number): number => Number.isSafeInteger(value) && value >= 0 ? value : 0;
  const cwd = isSafeBoundedText(info.cwd, 4096, true) ? info.cwd : "";
  const model = isSafeBoundedText(info.model, 256, true) ? info.model : "unknown";
  const projection: FederationRosterSessionProjection = {
    ...(info.endpointEpoch !== undefined && isFederationCorrelationId(info.endpointEpoch) ? { endpointEpoch: info.endpointEpoch } : {}),
    ...(info.name !== undefined ? { name: info.name } : {}),
    ...(info.description !== undefined ? { description: info.description } : {}),
    ...(info.runtimeFallbackAlias !== undefined ? { runtimeFallbackAlias: info.runtimeFallbackAlias } : {}),
    cwd,
    model,
    pid: safeInteger(info.pid),
    startedAt: safeInteger(info.startedAt),
    lastActivity: safeInteger(info.lastActivity),
    ...(info.status !== undefined && isSafeBoundedText(info.status, 256, true) ? { status: info.status } : {}),
    ...(info.contextPct !== undefined && Number.isFinite(info.contextPct) && info.contextPct >= 0 && info.contextPct <= 100 ? { contextPct: info.contextPct } : {}),
    ...(info.contextTokens !== undefined && Number.isSafeInteger(info.contextTokens) && info.contextTokens >= 0 ? { contextTokens: info.contextTokens } : {}),
    ...(info.contextWindow !== undefined && Number.isSafeInteger(info.contextWindow) && info.contextWindow >= 0 ? { contextWindow: info.contextWindow } : {}),
    ...(info.tmuxPane !== undefined && isSafeBoundedText(info.tmuxPane, 128) ? { tmuxPane: info.tmuxPane } : {}),
  };
  if (isProjection(projection)) return projection;
  // Preserve identity/presentation fields when possible, but shrink legacy
  // unbounded paths before giving up on an otherwise eligible local session.
  const compact: FederationRosterSessionProjection = {
    cwd: cwd.slice(0, 256),
    model: model.slice(0, 128),
    pid: safeInteger(info.pid),
    startedAt: safeInteger(info.startedAt),
    lastActivity: safeInteger(info.lastActivity),
    ...(info.name !== undefined && info.name.length <= 128 ? { name: info.name } : {}),
    ...(info.description !== undefined ? { description: info.description } : {}),
  };
  if (!isProjection(compact)) throw new FederationRosterError("Local session metadata cannot be represented safely in federation");
  return compact;
}

function sameEntry(left: FederationRosterEntry, right: FederationRosterEntry): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function sameImported(left: ImportedFederatedSession, right: ImportedFederatedSession): boolean {
  return left.localScopeId === right.localScopeId && JSON.stringify(left.info) === JSON.stringify(right.info);
}

export class FederationRosterState {
  private readonly links = new Map<string, LinkRosterState>();

  constructor(private readonly options: FederationRosterOptions) {
    if (!isFederationCorrelationId(options.originEpoch)) {
      throw new FederationRosterError("Invalid local federation origin epoch");
    }
  }

  linkUp(link: FederationPeerLink): void {
    this.linkDown(link.linkId);
    if (!link.features.includes(FEDERATION_ROSTER_FEATURE)) return;
    const desired = this.buildExportedRoster(link);
    const state: LinkRosterState = {
      link,
      exportSequence: 1,
      exported: desired,
      importSequence: 0,
      awaitingSnapshot: true,
      resyncRequested: false,
      seenImportEpochs: new Set(),
      imported: new Map(),
    };
    this.links.set(link.linkId, state);
    this.sendSnapshot(state);
  }

  linkDown(linkId: string): void {
    const state = this.links.get(linkId);
    if (!state) return;
    this.links.delete(linkId);
    if (state.imported.size > 0) {
      this.notify({ joined: [], updated: [], left: [...state.imported.values()] });
    }
  }

  reconcileLocalRoster(): void {
    this.pruneImportsExceedingVisibleBudget();
    for (const state of this.links.values()) {
      try {
        const desired = this.buildExportedRoster(state.link);
        const upserts: FederationRosterEntry[] = [];
        const removals: FederationRosterRemoval[] = [];
        for (const [key, entry] of desired) {
          const previous = state.exported.get(key);
          if (!previous || !sameEntry(previous, entry)) upserts.push(entry);
        }
        for (const [key, previous] of state.exported) {
          if (!desired.has(key)) removals.push({ scopeAlias: previous.scopeAlias, stableSessionId: previous.stableSessionId });
        }
        state.exported = desired;
        this.sendDeltaChunks(state, upserts, removals);
      } catch (error) {
        const failure = error instanceof FederationRosterError
          ? error
          : new FederationRosterError("Failed to reconcile the local federation roster");
        if (this.options.onLinkError) this.options.onLinkError(state.link, failure);
        else throw failure;
      }
    }
  }

  handlePeerFrame(linkId: string, value: unknown): boolean {
    const state = this.links.get(linkId);
    if (!state) throw new FederationRosterError("Roster frame arrived for an unknown peer link");
    if (!isFederationRosterFrame(value)) return false;
    if (value.originId !== state.link.remoteOrigin.id) {
      throw new FederationRosterError("Roster frame origin does not match the peer link");
    }
    if (isPeerRosterResyncRequest(value)) {
      // One authoritative response is sufficient on this reliable stream. A
      // new response is allowed only after local roster state advances.
      if (state.lastResyncResponseSequence !== state.exportSequence) {
        this.sendSnapshot(state, true);
        state.lastResyncResponseSequence = state.exportSequence;
      }
      return true;
    }
    if (isPeerRosterSnapshot(value)) {
      this.applySnapshot(state, value);
      return true;
    }
    this.applyDelta(state, value);
    return true;
  }

  listImported(): ImportedFederatedSession[] {
    return [...this.links.values()].flatMap((state) => [...state.imported.values()]);
  }

  private sendDeltaChunks(
    state: LinkRosterState,
    upserts: FederationRosterEntry[],
    removals: FederationRosterRemoval[],
  ): void {
    const pending: Array<{ kind: "upsert"; value: FederationRosterEntry } | { kind: "removal"; value: FederationRosterRemoval }> = [
      ...upserts.map((value) => ({ kind: "upsert" as const, value })),
      ...removals.map((value) => ({ kind: "removal" as const, value })),
    ];
    let batchUpserts: FederationRosterEntry[] = [];
    let batchRemovals: FederationRosterRemoval[] = [];
    let batchBytes = 0;
    const frame = (): PeerRosterDelta => ({
      type: "peer_roster_delta",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: state.link.localOrigin.id,
      originEpoch: this.options.originEpoch,
      sequence: state.exportSequence + 1,
      upserts: batchUpserts,
      removals: batchRemovals,
    });
    const flush = (): void => {
      if (batchUpserts.length === 0 && batchRemovals.length === 0) return;
      const next = frame();
      if (!isPeerRosterDelta(next)) throw new FederationRosterError("Local roster delta exceeds federation bounds");
      state.exportSequence = next.sequence;
      this.options.send(state.link, next);
      batchUpserts = [];
      batchRemovals = [];
      batchBytes = 0;
    };
    for (const change of pending) {
      const changeBytes = serializedSize(change.value) + 32;
      const count = batchUpserts.length + batchRemovals.length;
      if (count > 0 && (count >= MAX_DELTA_CHANGES || batchBytes + changeBytes > MAX_ROSTER_FRAME_BYTES - 4096)) {
        flush();
      }
      if (change.kind === "upsert") batchUpserts.push(change.value);
      else batchRemovals.push(change.value);
      batchBytes += changeBytes;
    }
    flush();
  }

  private visibleBytesByScope(
    replacingLinkId?: string,
    replacement?: Map<string, ImportedFederatedSession>,
  ): Map<string, number> {
    const bytesByScope = new Map<string, number>();
    for (const local of this.options.listLocalSessions()) {
      if (local.ownership !== "local") continue;
      const scopeKey = JSON.stringify(local.localScopeId);
      bytesByScope.set(scopeKey, (bytesByScope.get(scopeKey) ?? 0) + serializedSize(local.info) + 32);
    }
    for (const state of this.links.values()) {
      const imported = state.link.linkId === replacingLinkId && replacement ? replacement : state.imported;
      for (const session of imported.values()) {
        const scopeKey = JSON.stringify(session.localScopeId);
        bytesByScope.set(scopeKey, (bytesByScope.get(scopeKey) ?? 0) + serializedSize(session.info) + 32);
      }
    }
    return bytesByScope;
  }

  private assertImportedBudget(
    replacingLinkId: string,
    replacement: Map<string, ImportedFederatedSession>,
  ): void {
    if ([...this.visibleBytesByScope(replacingLinkId, replacement).values()]
      .some((bytes) => bytes > MAX_VISIBLE_ROSTER_BYTES_PER_LOCAL_SCOPE)) {
      throw new FederationRosterError("Federated sessions would exceed the local client roster byte budget");
    }
  }

  private pruneImportsExceedingVisibleBudget(): void {
    while (true) {
      const offendingScopes = new Set(
        [...this.visibleBytesByScope().entries()]
          .filter(([, bytes]) => bytes > MAX_VISIBLE_ROSTER_BYTES_PER_LOCAL_SCOPE)
          .map(([scope]) => scope),
      );
      if (offendingScopes.size === 0) return;
      const victim = [...this.links.values()].reverse().find((state) =>
        [...state.imported.values()].some((session) => offendingScopes.has(JSON.stringify(session.localScopeId))));
      if (!victim) return; // Local-only overflow predates federation and cannot be repaired here.
      const link = victim.link;
      this.linkDown(link.linkId);
      this.options.onLinkError?.(link, new FederationRosterError(
        "Local roster growth exceeded the client roster byte budget; pruned the newest contributing peer link",
      ));
    }
  }

  private buildExportedRoster(link: FederationPeerLink): Map<string, FederationRosterEntry> {
    const result = new Map<string, FederationRosterEntry>();
    for (const local of this.options.listLocalSessions()) {
      if (local.ownership !== "local" || local.exportEligible !== true) continue;
      for (const binding of link.scopeBindings) {
        if (binding.localScopeId !== local.localScopeId) continue;
        if (!isStableSessionId(local.info.id)) {
          throw new FederationRosterError("Local stable session identity cannot be represented safely in federation");
        }
        const entry: FederationRosterEntry = {
          scopeAlias: binding.localScopeAlias,
          stableSessionId: local.info.id,
          session: projectionFromSession(local.info),
        };
        if (!isRosterEntry(entry)) throw new FederationRosterError("Local session cannot be represented in federation");
        result.set(rosterKey(entry.scopeAlias, entry.stableSessionId), entry);
      }
    }
    if (result.size > MAX_SNAPSHOT_SESSIONS) {
      throw new FederationRosterError(`Federation roster exceeds ${MAX_SNAPSHOT_SESSIONS} sessions for one peer`);
    }
    return result;
  }

  private sendSnapshot(state: LinkRosterState, advance = false): void {
    if (advance) state.exportSequence += 1;
    const frame: PeerRosterSnapshot = {
      type: "peer_roster_snapshot",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: state.link.localOrigin.id,
      originEpoch: this.options.originEpoch,
      sequence: state.exportSequence,
      sessions: [...state.exported.values()],
    };
    if (!isPeerRosterSnapshot(frame)) throw new FederationRosterError("Local snapshot exceeds federation bounds");
    this.options.send(state.link, frame);
  }

  private applySnapshot(state: LinkRosterState, frame: PeerRosterSnapshot): void {
    if (state.importOriginEpoch === frame.originEpoch) {
      if (frame.sequence <= state.importSequence) return;
    } else if (state.seenImportEpochs.has(frame.originEpoch)) {
      return;
    } else if (state.seenImportEpochs.size >= MAX_IMPORT_EPOCHS_PER_LINK) {
      throw new FederationRosterError("Peer changed roster origin epoch too many times on one link");
    }
    const imported = this.materializeEntries(state.link, frame.originEpoch, frame.sessions);
    this.assertImportedBudget(state.link.linkId, imported);
    const previous = state.imported;
    state.imported = imported;
    state.importOriginEpoch = frame.originEpoch;
    state.importSequence = frame.sequence;
    state.awaitingSnapshot = false;
    state.resyncRequested = false;
    state.seenImportEpochs.add(frame.originEpoch);
    this.notifyDiff(previous, imported);
  }

  private applyDelta(state: LinkRosterState, frame: PeerRosterDelta): void {
    if (state.importOriginEpoch !== frame.originEpoch && state.seenImportEpochs.has(frame.originEpoch)) return;
    if (state.awaitingSnapshot || state.importOriginEpoch !== frame.originEpoch) {
      if (!state.resyncRequested) this.requestResync(state);
      return;
    }
    if (frame.sequence <= state.importSequence) return;
    if (frame.sequence !== state.importSequence + 1) {
      state.awaitingSnapshot = true;
      this.requestResync(state);
      return;
    }
    const next = new Map(state.imported);
    const upserts = this.materializeEntries(state.link, frame.originEpoch, frame.upserts);
    for (const removal of frame.removals) {
      const qualifiedId = this.qualifiedIdForRemote(state.link, removal.scopeAlias, removal.stableSessionId);
      next.delete(qualifiedId);
    }
    for (const [qualifiedId, imported] of upserts) next.set(qualifiedId, imported);
    this.assertImportedBudget(state.link.linkId, next);
    const previous = state.imported;
    state.imported = next;
    state.importSequence = frame.sequence;
    this.notifyDiff(previous, next);
  }

  private requestResync(state: LinkRosterState): void {
    if (!state.awaitingSnapshot) state.awaitingSnapshot = true;
    state.resyncRequested = true;
    this.options.send(state.link, {
      type: "peer_roster_resync_request",
      protocol: FEDERATION_PROTOCOL_NAME,
      version: FEDERATION_PROTOCOL_VERSION,
      originId: state.link.localOrigin.id,
      ...(state.importOriginEpoch ? { expectedOriginEpoch: state.importOriginEpoch } : {}),
      expectedSequence: Math.max(1, state.importSequence + 1),
    });
  }

  private materializeEntries(
    link: FederationPeerLink,
    originEpoch: string,
    entries: readonly FederationRosterEntry[],
  ): Map<string, ImportedFederatedSession> {
    const result = new Map<string, ImportedFederatedSession>();
    for (const entry of entries) {
      const binding = this.bindingForRemoteAlias(link.scopeBindings, entry.scopeAlias);
      if (!binding) throw new FederationRosterError(`Remote scope alias ${entry.scopeAlias} is not authorized on this link`);
      const qualifiedId = this.qualifiedIdForRemote(link, entry.scopeAlias, entry.stableSessionId);
      if (result.has(qualifiedId)) throw new FederationRosterError("Duplicate qualified session identity in roster frame");
      const info: ImportedFederatedSessionInfo = {
        id: qualifiedId,
        ...entry.session,
        trustedLocal: false,
        federation: {
          originId: link.remoteOrigin.id,
          ...(link.remoteOrigin.label ? { originLabel: link.remoteOrigin.label } : {}),
          remoteScopeAlias: entry.scopeAlias,
          remoteStableSessionId: entry.stableSessionId,
        },
      };
      result.set(qualifiedId, { linkId: link.linkId, originEpoch, localScopeId: binding.localScopeId, info });
    }
    return result;
  }

  private bindingForRemoteAlias(bindings: readonly FederationScopeBinding[], alias: string): FederationScopeBinding | undefined {
    return bindings.find((binding) => binding.remoteScopeAlias === alias);
  }

  private qualifiedIdForRemote(link: FederationPeerLink, scopeAlias: string, stableSessionId: string): string {
    if (!this.bindingForRemoteAlias(link.scopeBindings, scopeAlias)) {
      throw new FederationRosterError(`Remote scope alias ${scopeAlias} is not authorized on this link`);
    }
    return encodeOriginQualifiedSessionIdentity({
      originId: link.remoteOrigin.id,
      remoteScopeAlias: scopeAlias,
      remoteStableSessionId: stableSessionId,
    });
  }

  private notifyDiff(
    previous: Map<string, ImportedFederatedSession>,
    next: Map<string, ImportedFederatedSession>,
  ): void {
    const change: ImportedRosterChange = { joined: [], updated: [], left: [] };
    for (const [id, imported] of next) {
      const before = previous.get(id);
      if (!before) change.joined.push(imported);
      else if (!sameImported(before, imported)) change.updated.push(imported);
    }
    for (const [id, imported] of previous) {
      if (!next.has(id)) change.left.push(imported);
    }
    this.notify(change);
  }

  private notify(change: ImportedRosterChange): void {
    if (change.joined.length === 0 && change.updated.length === 0 && change.left.length === 0) return;
    this.options.onImportedChange?.(change);
  }
}
