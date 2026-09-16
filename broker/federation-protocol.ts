import {
  FEDERATION_CAPABILITY_MAX_LENGTH,
  FEDERATION_CAPABILITY_MIN_LENGTH,
  FEDERATION_CORRELATION_ID_MAX_LENGTH,
  FEDERATION_DISPLAY_LABEL_MAX_LENGTH,
  FEDERATION_IDENTITY_FEATURE,
  FEDERATION_LOCAL_SCOPE_ID_MAX_LENGTH,
  FEDERATION_MAX_FEATURES,
  FEDERATION_MAX_SCOPE_MAPPINGS,
  FEDERATION_ORIGIN_ID_MAX_LENGTH,
  FEDERATION_PROTOCOL_NAME,
  FEDERATION_PROTOCOL_VERSION,
  FEDERATION_SCOPE_ALIAS_MAX_LENGTH,
  FEDERATION_SESSION_ID_MAX_LENGTH,
  FEDERATION_SINGLE_HOP_FEATURE,
  type BrokerAcceptPeerRequest,
  type BrokerAcceptPeerResult,
  type BrokerDialPeerRequest,
  type BrokerDialPeerResult,
  type FederationBridgeAttach,
  type FederationFailureCode,
  type FederationLoopbackEndpoint,
  type FederationOrigin,
  type FederationScopeBinding,
  type FederationScopeMapping,
  type OriginQualifiedSessionIdentity,
  type PeerHello,
  type PeerHelloAck,
} from "./federation-types.ts";

const CONTROL_OR_FORMAT_CHARACTERS = /[\p{Cc}\p{Cf}]/u;
const ORIGIN_ID = /^[a-z][a-z0-9]*(?::[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/;
const SCOPE_ALIAS = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;
const CORRELATION_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{7,127}$/;
const CAPABILITY = /^[A-Za-z0-9_-]+$/;
const FEATURE = /^[a-z0-9][a-z0-9.-]*$/;
const QUALIFIED_ID_PREFIX = "oqs1.";
// Covers the worst-case JSON escaping of every accepted 512-code-unit stable
// ID (including lone surrogate code units), plus the bounded origin and scope.
const MAX_QUALIFIED_ID_LENGTH = 8192;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyKeys(value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []): boolean {
  const keys = Object.keys(value);
  const allowed = new Set([...required, ...optional]);
  return required.every((key) => Object.prototype.hasOwnProperty.call(value, key))
    && keys.every((key) => allowed.has(key));
}

function isSafeText(value: unknown, maxLength: number, requireTrimmed = true): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.length <= maxLength
    && (!requireTrimmed || value.trim() === value)
    && value.trim().length > 0
    && !CONTROL_OR_FORMAT_CHARACTERS.test(value);
}

export function isCanonicalFederationOriginId(value: unknown): value is string {
  return typeof value === "string"
    && value.length <= FEDERATION_ORIGIN_ID_MAX_LENGTH
    && ORIGIN_ID.test(value);
}

export function isCanonicalFederationScopeAlias(value: unknown): value is string {
  return typeof value === "string"
    && value.length <= FEDERATION_SCOPE_ALIAS_MAX_LENGTH
    && SCOPE_ALIAS.test(value);
}

export function isFederationCorrelationId(value: unknown): value is string {
  return typeof value === "string"
    && value.length <= FEDERATION_CORRELATION_ID_MAX_LENGTH
    && CORRELATION_ID.test(value);
}

function isFederationCapability(value: unknown): value is string {
  return typeof value === "string"
    && value.length >= FEDERATION_CAPABILITY_MIN_LENGTH
    && value.length <= FEDERATION_CAPABILITY_MAX_LENGTH
    && CAPABILITY.test(value);
}

export function isFederationOrigin(value: unknown): value is FederationOrigin {
  if (!isRecord(value) || !hasOnlyKeys(value, ["id"], ["label"])) return false;
  return isCanonicalFederationOriginId(value.id)
    && (value.label === undefined || isSafeText(value.label, FEDERATION_DISPLAY_LABEL_MAX_LENGTH));
}

export function isFederationLoopbackEndpoint(value: unknown): value is FederationLoopbackEndpoint {
  return isRecord(value)
    && hasOnlyKeys(value, ["transport", "host", "port"])
    && value.transport === "tcp"
    && (value.host === "127.0.0.1" || value.host === "::1")
    && Number.isInteger(value.port)
    && (value.port as number) >= 1
    && (value.port as number) <= 65_535;
}

export function isFederationScopeMapping(value: unknown): value is FederationScopeMapping {
  return isRecord(value)
    && hasOnlyKeys(value, ["localScopeAlias", "remoteScopeAlias"])
    && isCanonicalFederationScopeAlias(value.localScopeAlias)
    && isCanonicalFederationScopeAlias(value.remoteScopeAlias);
}

export function isFederationScopeBinding(value: unknown): value is FederationScopeBinding {
  return isRecord(value)
    && hasOnlyKeys(value, ["localScopeId", "localScopeAlias", "remoteScopeAlias"])
    && (value.localScopeId === null || isSafeText(value.localScopeId, FEDERATION_LOCAL_SCOPE_ID_MAX_LENGTH))
    && isCanonicalFederationScopeAlias(value.localScopeAlias)
    && isCanonicalFederationScopeAlias(value.remoteScopeAlias);
}

function hasUniqueMappingAliases(mappings: FederationScopeMapping[]): boolean {
  return new Set(mappings.map((mapping) => mapping.localScopeAlias)).size === mappings.length
    && new Set(mappings.map((mapping) => mapping.remoteScopeAlias)).size === mappings.length;
}

function isScopeMappings(value: unknown): value is FederationScopeMapping[] {
  return Array.isArray(value)
    && value.length >= 1
    && value.length <= FEDERATION_MAX_SCOPE_MAPPINGS
    && value.every(isFederationScopeMapping)
    && hasUniqueMappingAliases(value);
}

function isScopeBindings(value: unknown): value is FederationScopeBinding[] {
  if (!Array.isArray(value)
    || value.length < 1
    || value.length > FEDERATION_MAX_SCOPE_MAPPINGS
    || !value.every(isFederationScopeBinding)
    || !hasUniqueMappingAliases(value)) return false;
  return new Set(value.map((binding) => binding.localScopeId)).size === value.length;
}

function isFeatures(value: unknown): value is string[] {
  if (!Array.isArray(value) || value.length < 1 || value.length > FEDERATION_MAX_FEATURES) return false;
  if (!value.every((feature) => typeof feature === "string" && feature.length <= 64 && FEATURE.test(feature))) return false;
  if (new Set(value).size !== value.length) return false;
  return value.includes(FEDERATION_IDENTITY_FEATURE) && value.includes(FEDERATION_SINGLE_HOP_FEATURE);
}

export function scopeMappingsFromBindings(bindings: FederationScopeBinding[]): FederationScopeMapping[] {
  return bindings.map(({ localScopeAlias, remoteScopeAlias }) => ({ localScopeAlias, remoteScopeAlias }));
}

export function invertScopeMappings(mappings: FederationScopeMapping[]): FederationScopeMapping[] {
  return mappings.map(({ localScopeAlias, remoteScopeAlias }) => ({
    localScopeAlias: remoteScopeAlias,
    remoteScopeAlias: localScopeAlias,
  }));
}

export function bindingsMatchPeerMappings(
  bindings: FederationScopeBinding[],
  peerMappings: FederationScopeMapping[],
): boolean {
  const expected = scopeMappingsFromBindings(bindings);
  return expected.length === peerMappings.length && expected.every((mapping, index) => {
    const peer = peerMappings[index];
    return peer?.localScopeAlias === mapping.remoteScopeAlias
      && peer.remoteScopeAlias === mapping.localScopeAlias;
  });
}

export function isBrokerDialPeerRequest(value: unknown): value is BrokerDialPeerRequest {
  if (!isRecord(value) || !hasOnlyKeys(
    value,
    ["type", "requestId", "endpoint", "capability", "localOrigin", "remoteOrigin", "scopeBindings"],
    ["stateId"],
  )) return false;
  return value.type === "broker_dial_peer"
    && isFederationCorrelationId(value.requestId)
    && isFederationLoopbackEndpoint(value.endpoint)
    && isFederationCapability(value.capability)
    && isFederationOrigin(value.localOrigin)
    && isFederationOrigin(value.remoteOrigin)
    && value.localOrigin.id !== value.remoteOrigin.id
    && isScopeBindings(value.scopeBindings)
    && (value.stateId === undefined || isFederationCorrelationId(value.stateId));
}

export function isBrokerAcceptPeerRequest(value: unknown): value is BrokerAcceptPeerRequest {
  if (!isRecord(value) || !hasOnlyKeys(
    value,
    ["type", "requestId", "linkId", "localOrigin", "remoteOrigin", "scopeBindings"],
    ["stateId"],
  )) return false;
  return value.type === "broker_accept_peer"
    && isFederationCorrelationId(value.requestId)
    && isFederationCorrelationId(value.linkId)
    && isFederationOrigin(value.localOrigin)
    && isFederationOrigin(value.remoteOrigin)
    && value.localOrigin.id !== value.remoteOrigin.id
    && isScopeBindings(value.scopeBindings)
    && (value.stateId === undefined || isFederationCorrelationId(value.stateId));
}

export function isFederationBridgeAttach(value: unknown): value is FederationBridgeAttach {
  return isRecord(value)
    && hasOnlyKeys(value, ["type", "protocol", "version", "linkId", "capability"])
    && value.type === "bridge_attach"
    && value.protocol === FEDERATION_PROTOCOL_NAME
    && value.version === FEDERATION_PROTOCOL_VERSION
    && isFederationCorrelationId(value.linkId)
    && isFederationCapability(value.capability);
}

export function isPeerHello(value: unknown): value is PeerHello {
  if (!isRecord(value) || !hasOnlyKeys(
    value,
    ["type", "protocol", "version", "linkId", "origin", "expectedPeerOrigin", "scopeMappings", "features"],
  )) return false;
  return value.type === "peer_hello"
    && value.protocol === FEDERATION_PROTOCOL_NAME
    && value.version === FEDERATION_PROTOCOL_VERSION
    && isFederationCorrelationId(value.linkId)
    && isFederationOrigin(value.origin)
    && isFederationOrigin(value.expectedPeerOrigin)
    && value.origin.id !== value.expectedPeerOrigin.id
    && isScopeMappings(value.scopeMappings)
    && isFeatures(value.features);
}

const FAILURE_CODES = new Set<FederationFailureCode>([
  "E_INVALID_REQUEST",
  "E_DIAL_FAILED",
  "E_HANDSHAKE_FAILED",
  "E_VERSION_UNSUPPORTED",
  "E_FEATURE_UNSUPPORTED",
  "E_ORIGIN_MISMATCH",
  "E_SCOPE_MISMATCH",
  "E_ALREADY_CONNECTED",
  "E_NOT_PREPARED",
]);

function isFailureCode(value: unknown): value is FederationFailureCode {
  return typeof value === "string" && FAILURE_CODES.has(value as FederationFailureCode);
}

export function isPeerHelloAck(value: unknown): value is PeerHelloAck {
  if (!isRecord(value) || value.type !== "peer_hello_ack" || value.protocol !== FEDERATION_PROTOCOL_NAME
    || value.version !== FEDERATION_PROTOCOL_VERSION || !isFederationCorrelationId(value.linkId)
    || typeof value.accepted !== "boolean") return false;
  if (value.accepted) {
    return hasOnlyKeys(value, ["type", "protocol", "version", "linkId", "accepted", "origin", "acceptedPeerOriginId", "scopeMappings", "features"])
      && isFederationOrigin(value.origin)
      && isCanonicalFederationOriginId(value.acceptedPeerOriginId)
      && value.origin.id !== value.acceptedPeerOriginId
      && isScopeMappings(value.scopeMappings)
      && isFeatures(value.features);
  }
  return hasOnlyKeys(value, ["type", "protocol", "version", "linkId", "accepted", "code", "error"])
    && isFailureCode(value.code)
    && isSafeText(value.error, 256);
}

function isControlResult(
  value: unknown,
  type: "broker_dial_peer_result" | "broker_accept_peer_result",
): value is BrokerDialPeerResult | BrokerAcceptPeerResult {
  if (!isRecord(value) || value.type !== type || !isFederationCorrelationId(value.requestId) || typeof value.ok !== "boolean") return false;
  if (value.ok) {
    return hasOnlyKeys(value, ["type", "requestId", "ok", "linkId"])
      && isFederationCorrelationId(value.linkId);
  }
  return hasOnlyKeys(value, ["type", "requestId", "ok", "code", "error"])
    && isFailureCode(value.code)
    && isSafeText(value.error, 256);
}

export function isBrokerDialPeerResult(value: unknown): value is BrokerDialPeerResult {
  return isControlResult(value, "broker_dial_peer_result");
}

export function isBrokerAcceptPeerResult(value: unknown): value is BrokerAcceptPeerResult {
  return isControlResult(value, "broker_accept_peer_result");
}

function isStableSessionId(value: unknown): value is string {
  return isSafeText(value, FEDERATION_SESSION_ID_MAX_LENGTH, false);
}

function isOriginQualifiedSessionIdentity(value: unknown): value is OriginQualifiedSessionIdentity {
  if (!isRecord(value) || !hasOnlyKeys(value, ["originId", "remoteScopeAlias", "remoteStableSessionId"])) return false;
  return isCanonicalFederationOriginId(value.originId)
    && isCanonicalFederationScopeAlias(value.remoteScopeAlias)
    && isStableSessionId(value.remoteStableSessionId);
}

export function encodeOriginQualifiedSessionIdentity(identity: OriginQualifiedSessionIdentity): string {
  if (!isOriginQualifiedSessionIdentity(identity)) throw new Error("Invalid origin-qualified session identity");
  const tuple = [identity.originId, identity.remoteScopeAlias, identity.remoteStableSessionId];
  return `${QUALIFIED_ID_PREFIX}${Buffer.from(JSON.stringify(tuple), "utf8").toString("base64url")}`;
}

export function decodeOriginQualifiedSessionIdentity(value: string): OriginQualifiedSessionIdentity | undefined {
  if (!value.startsWith(QUALIFIED_ID_PREFIX) || value.length > MAX_QUALIFIED_ID_LENGTH) return undefined;
  const encoded = value.slice(QUALIFIED_ID_PREFIX.length);
  if (!encoded || !/^[A-Za-z0-9_-]+$/.test(encoded)) return undefined;
  try {
    const decoded = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
    if (!Array.isArray(decoded) || decoded.length !== 3) return undefined;
    const identity = {
      originId: decoded[0],
      remoteScopeAlias: decoded[1],
      remoteStableSessionId: decoded[2],
    };
    if (!isOriginQualifiedSessionIdentity(identity)) return undefined;
    return encodeOriginQualifiedSessionIdentity(identity) === value ? identity : undefined;
  } catch {
    return undefined;
  }
}
