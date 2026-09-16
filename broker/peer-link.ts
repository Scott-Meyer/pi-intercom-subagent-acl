import net from "node:net";
import { randomUUID } from "node:crypto";
import { createMessageReader, writeMessage } from "./framing.ts";
import {
  FEDERATION_PROTOCOL_NAME,
  FEDERATION_PROTOCOL_VERSION,
  FEDERATION_REQUIRED_FEATURES,
  type BrokerAcceptPeerRequest,
  type BrokerDialPeerRequest,
  type FederationFailureCode,
  type FederationOrigin,
  type FederationScopeBinding,
  type FederationScopeMapping,
  type PeerHello,
  type PeerHelloAck,
} from "./federation-types.ts";
import {
  bindingsMatchPeerMappings,
  isBrokerAcceptPeerRequest,
  isBrokerDialPeerRequest,
  isPeerHello,
  isPeerHelloAck,
  scopeMappingsFromBindings,
} from "./federation-protocol.ts";

const PEER_HANDSHAKE_TIMEOUT_MS = 5_000;
const MAX_PEER_LINKS = 16;

export interface FederationPeerLink {
  linkId: string;
  direction: "inbound" | "outbound";
  socket: net.Socket;
  localOrigin: FederationOrigin;
  remoteOrigin: FederationOrigin;
  /** Local authority binding; raw scope IDs are never sent to the peer. */
  scopeBindings: FederationScopeBinding[];
  features: string[];
  connectedAt: number;
}

export interface PreparedInboundPeer {
  linkId: string;
  localOrigin: FederationOrigin;
  remoteOrigin: FederationOrigin;
  scopeBindings: FederationScopeBinding[];
}

export class FederationPeerError extends Error {
  constructor(readonly code: FederationFailureCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "FederationPeerError";
  }
}

export interface PeerLinkManagerOptions {
  onSocketOpened?: (socket: net.Socket) => void;
  onSocketClosed?: (socket: net.Socket) => void;
  onLinkUp?: (link: FederationPeerLink) => void;
  onLinkDown?: (link: FederationPeerLink) => void;
}

function sameFeatures(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((feature) => right.includes(feature));
}

function sameMappings(left: FederationScopeMapping[], right: FederationScopeMapping[]): boolean {
  return left.length === right.length && left.every((mapping, index) => {
    const other = right[index];
    return other?.localScopeAlias === mapping.localScopeAlias
      && other.remoteScopeAlias === mapping.remoteScopeAlias;
  });
}

function rejection(hello: PeerHello, code: FederationFailureCode, error: string): PeerHelloAck {
  return {
    type: "peer_hello_ack",
    protocol: FEDERATION_PROTOCOL_NAME,
    version: FEDERATION_PROTOCOL_VERSION,
    linkId: hello.linkId,
    accepted: false,
    code,
    error,
  };
}

/** Owns peer authority preparation, handshake, deterministic link choice, and lifecycle. */
export class PeerLinkManager {
  private readonly linksById = new Map<string, FederationPeerLink>();
  private readonly linkIdByRemoteOrigin = new Map<string, string>();
  private localOrigin: FederationOrigin | undefined;

  constructor(private readonly options: PeerLinkManagerOptions = {}) {}

  get size(): number {
    return this.linksById.size;
  }

  getLink(linkId: string): FederationPeerLink | undefined {
    return this.linksById.get(linkId);
  }

  listLinks(): FederationPeerLink[] {
    return [...this.linksById.values()];
  }

  prepareInbound(value: unknown): PreparedInboundPeer {
    if (!isBrokerAcceptPeerRequest(value)) {
      throw new FederationPeerError("E_INVALID_REQUEST", "Invalid broker accept peer request");
    }
    if (this.localOrigin && this.localOrigin.id !== value.localOrigin.id) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Prepared destination origin does not match this broker");
    }
    if (this.linksById.has(value.linkId)) {
      throw new FederationPeerError("E_ALREADY_CONNECTED", "Peer link ID is already connected");
    }
    return {
      linkId: value.linkId,
      localOrigin: value.localOrigin,
      remoteOrigin: value.remoteOrigin,
      scopeBindings: value.scopeBindings,
    };
  }

  acceptInbound(
    socket: net.Socket,
    value: unknown,
    prepared: PreparedInboundPeer,
  ): { ack: PeerHelloAck; link?: FederationPeerLink } {
    if (!isPeerHello(value)) throw new FederationPeerError("E_INVALID_REQUEST", "Invalid peer hello");
    const hello = value;
    if (hello.linkId !== prepared.linkId) return { ack: rejection(hello, "E_NOT_PREPARED", "Peer link was not prepared") };
    if (hello.origin.id !== prepared.remoteOrigin.id || hello.expectedPeerOrigin.id !== prepared.localOrigin.id) {
      return { ack: rejection(hello, "E_ORIGIN_MISMATCH", "Peer origins do not match destination authority") };
    }
    if (!bindingsMatchPeerMappings(prepared.scopeBindings, hello.scopeMappings)) {
      return { ack: rejection(hello, "E_SCOPE_MISMATCH", "Peer scope aliases do not match destination authority") };
    }
    if (this.linksById.has(hello.linkId)) {
      return { ack: rejection(hello, "E_ALREADY_CONNECTED", "Peer link ID is already connected") };
    }
    const existing = this.getLinkForRemoteOrigin(hello.origin.id);
    if (existing && (
      existing.direction === "inbound"
      || !this.isPreferredDirection("inbound", prepared.localOrigin.id, prepared.remoteOrigin.id)
    )) {
      return { ack: rejection(hello, "E_ALREADY_CONNECTED", "The existing peer link has the deterministic preferred direction") };
    }
    if (!existing && this.linksById.size >= MAX_PEER_LINKS) {
      return { ack: rejection(hello, "E_ALREADY_CONNECTED", "Peer link limit reached") };
    }

    const link: FederationPeerLink = {
      linkId: hello.linkId,
      direction: "inbound",
      socket,
      localOrigin: prepared.localOrigin,
      remoteOrigin: prepared.remoteOrigin,
      scopeBindings: prepared.scopeBindings,
      features: [...FEDERATION_REQUIRED_FEATURES],
      connectedAt: Date.now(),
    };
    try {
      this.registerLink(link);
    } catch (error) {
      const failure = error instanceof FederationPeerError
        ? error
        : new FederationPeerError("E_HANDSHAKE_FAILED", "Failed to register peer link", { cause: error });
      return { ack: rejection(hello, failure.code, failure.message) };
    }
    return {
      link,
      ack: {
        type: "peer_hello_ack",
        protocol: FEDERATION_PROTOCOL_NAME,
        version: FEDERATION_PROTOCOL_VERSION,
        linkId: hello.linkId,
        accepted: true,
        origin: prepared.localOrigin,
        acceptedPeerOriginId: prepared.remoteOrigin.id,
        scopeMappings: hello.scopeMappings,
        features: [...FEDERATION_REQUIRED_FEATURES],
      },
    };
  }

  async dial(request: BrokerDialPeerRequest, signal?: AbortSignal): Promise<FederationPeerLink> {
    if (!isBrokerDialPeerRequest(request)) {
      throw new FederationPeerError("E_INVALID_REQUEST", "Invalid broker dial peer request");
    }
    if (signal?.aborted) throw new FederationPeerError("E_DIAL_FAILED", "Peer dial control was abandoned");
    const endpoint = request.endpoint;
    const requestedLocalOrigin = request.localOrigin;
    const remoteOrigin = request.remoteOrigin;
    const scopeBindings = request.scopeBindings;
    const scopeMappings = scopeMappingsFromBindings(scopeBindings);
    let capability = request.capability;
    // Ensure the long-lived socket reader's function environment cannot retain
    // the authority-bearing request object after the attachment write.
    request = { ...request, capability: "" };

    if (this.localOrigin && this.localOrigin.id !== requestedLocalOrigin.id) {
      capability = "";
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Local federation origin is already fixed for this broker");
    }
    const localOrigin = this.localOrigin ?? requestedLocalOrigin;
    const existing = this.getLinkForRemoteOrigin(remoteOrigin.id);
    if (existing && (
      existing.direction === "outbound"
      || !this.isPreferredDirection("outbound", localOrigin.id, remoteOrigin.id)
    )) {
      capability = "";
      throw new FederationPeerError("E_ALREADY_CONNECTED", "The existing peer link has the deterministic preferred direction");
    }
    if (!existing && this.linksById.size >= MAX_PEER_LINKS) {
      capability = "";
      throw new FederationPeerError("E_ALREADY_CONNECTED", "Peer link limit reached");
    }

    const linkId = randomUUID();
    const socket = net.connect({ host: endpoint.host, port: endpoint.port });
    this.options.onSocketOpened?.(socket);

    return await new Promise<FederationPeerLink>((resolve, reject) => {
      let settled = false;
      let active = false;
      const finishFailure = (error: FederationPeerError): void => {
        capability = "";
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        signal?.removeEventListener("abort", onAbort);
        socket.off("data", reader);
        socket.destroy();
        reject(error);
      };
      const timeout = setTimeout(() => {
        finishFailure(new FederationPeerError("E_HANDSHAKE_FAILED", "Peer handshake timed out"));
      }, PEER_HANDSHAKE_TIMEOUT_MS);
      timeout.unref?.();

      const reader = createMessageReader((value) => {
        if (active) {
          socket.destroy(new Error("Federation Slice 1 does not accept post-handshake frames"));
          return;
        }
        if (!isPeerHelloAck(value)) {
          finishFailure(new FederationPeerError("E_HANDSHAKE_FAILED", "Invalid peer hello acknowledgement"));
          return;
        }
        if (!value.accepted) {
          finishFailure(new FederationPeerError(value.code, value.error));
          return;
        }
        if (value.linkId !== linkId || value.origin.id !== remoteOrigin.id || value.acceptedPeerOriginId !== localOrigin.id) {
          finishFailure(new FederationPeerError("E_ORIGIN_MISMATCH", "Peer acknowledgement did not match the requested origins"));
          return;
        }
        if (!sameMappings(value.scopeMappings, scopeMappings)) {
          finishFailure(new FederationPeerError("E_SCOPE_MISMATCH", "Peer acknowledgement did not match the requested scopes"));
          return;
        }
        if (!sameFeatures(value.features, FEDERATION_REQUIRED_FEATURES)) {
          finishFailure(new FederationPeerError("E_FEATURE_UNSUPPORTED", "Peer acknowledged features that were not offered"));
          return;
        }
        const link: FederationPeerLink = {
          linkId,
          direction: "outbound",
          socket,
          localOrigin,
          remoteOrigin: value.origin,
          scopeBindings,
          features: value.features,
          connectedAt: Date.now(),
        };
        try {
          this.registerLink(link);
        } catch (error) {
          finishFailure(error instanceof FederationPeerError
            ? error
            : new FederationPeerError("E_HANDSHAKE_FAILED", "Failed to register peer link", { cause: error }));
          return;
        }
        active = true;
        settled = true;
        clearTimeout(timeout);
        signal?.removeEventListener("abort", onAbort);
        resolve(link);
      }, (error) => {
        finishFailure(new FederationPeerError("E_HANDSHAKE_FAILED", "Failed to read peer handshake", { cause: error }));
      });

      const onAbort = () => finishFailure(new FederationPeerError("E_DIAL_FAILED", "Peer dial control was abandoned"));
      signal?.addEventListener("abort", onAbort, { once: true });
      socket.on("data", reader);
      socket.once("connect", () => {
        try {
          writeMessage(socket, {
            type: "bridge_attach",
            protocol: FEDERATION_PROTOCOL_NAME,
            version: FEDERATION_PROTOCOL_VERSION,
            linkId,
            capability,
          });
          capability = "";
          writeMessage(socket, {
            type: "peer_hello",
            protocol: FEDERATION_PROTOCOL_NAME,
            version: FEDERATION_PROTOCOL_VERSION,
            linkId,
            origin: localOrigin,
            expectedPeerOrigin: remoteOrigin,
            scopeMappings,
            features: [...FEDERATION_REQUIRED_FEATURES],
          });
        } catch (error) {
          capability = "";
          finishFailure(new FederationPeerError("E_DIAL_FAILED", "Failed to write peer handshake", { cause: error }));
        }
      });
      socket.once("error", (error) => {
        if (!active) finishFailure(new FederationPeerError("E_DIAL_FAILED", "Failed to connect to FlightDeck peer endpoint", { cause: error }));
      });
      socket.once("close", () => {
        clearTimeout(timeout);
        capability = "";
        signal?.removeEventListener("abort", onAbort);
        this.options.onSocketClosed?.(socket);
        const link = this.removeLinkBySocket(socket);
        if (link) this.options.onLinkDown?.(link);
        if (!settled) {
          settled = true;
          reject(new FederationPeerError("E_DIAL_FAILED", "Peer connection closed before handshake completed"));
        }
      });
    });
  }

  assertNoPostHandshakeMessage(linkId: string, _value: unknown): void {
    if (!this.linksById.has(linkId)) throw new FederationPeerError("E_INVALID_REQUEST", "Unknown peer link");
    throw new FederationPeerError("E_FEATURE_UNSUPPORTED", "Federation Slice 1 does not accept roster or routing frames");
  }

  closeLink(linkId: string): boolean {
    const link = this.linksById.get(linkId);
    if (!link) return false;
    this.unregisterLink(link);
    link.socket.destroy();
    this.options.onLinkDown?.(link);
    return true;
  }

  removeInbound(linkId: string, socket: net.Socket): FederationPeerLink | undefined {
    const link = this.linksById.get(linkId);
    if (!link || link.socket !== socket) return undefined;
    this.unregisterLink(link);
    this.options.onLinkDown?.(link);
    return link;
  }

  close(): void {
    const links = [...this.linksById.values()];
    for (const link of links) {
      this.unregisterLink(link);
      link.socket.end();
      link.socket.destroy();
      this.options.onLinkDown?.(link);
    }
  }

  private getLinkForRemoteOrigin(remoteOriginId: string): FederationPeerLink | undefined {
    const linkId = this.linkIdByRemoteOrigin.get(remoteOriginId);
    return linkId ? this.linksById.get(linkId) : undefined;
  }

  private isPreferredDirection(
    direction: FederationPeerLink["direction"],
    localOriginId: string,
    remoteOriginId: string,
  ): boolean {
    return direction === (localOriginId < remoteOriginId ? "outbound" : "inbound");
  }

  private registerLink(link: FederationPeerLink): void {
    if (this.linksById.has(link.linkId)) {
      throw new FederationPeerError("E_ALREADY_CONNECTED", "Peer link ID is already connected");
    }
    if (this.localOrigin && this.localOrigin.id !== link.localOrigin.id) {
      throw new FederationPeerError("E_ORIGIN_MISMATCH", "Local federation origin is already fixed for this broker");
    }
    const existing = this.getLinkForRemoteOrigin(link.remoteOrigin.id);
    if (existing) {
      if (
        this.isPreferredDirection(existing.direction, existing.localOrigin.id, existing.remoteOrigin.id)
        || !this.isPreferredDirection(link.direction, link.localOrigin.id, link.remoteOrigin.id)
      ) {
        throw new FederationPeerError("E_ALREADY_CONNECTED", "The existing peer link has the deterministic preferred direction");
      }
      this.unregisterLink(existing);
      existing.socket.destroy();
      this.options.onLinkDown?.(existing);
    }
    this.localOrigin ??= link.localOrigin;
    this.linksById.set(link.linkId, link);
    this.linkIdByRemoteOrigin.set(link.remoteOrigin.id, link.linkId);
    this.options.onLinkUp?.(link);
  }

  private removeLinkBySocket(socket: net.Socket): FederationPeerLink | undefined {
    for (const link of this.linksById.values()) {
      if (link.socket !== socket) continue;
      this.unregisterLink(link);
      return link;
    }
    return undefined;
  }

  private unregisterLink(link: FederationPeerLink): void {
    this.linksById.delete(link.linkId);
    if (this.linkIdByRemoteOrigin.get(link.remoteOrigin.id) === link.linkId) {
      this.linkIdByRemoteOrigin.delete(link.remoteOrigin.id);
    }
  }
}
