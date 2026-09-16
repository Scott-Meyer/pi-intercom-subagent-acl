import type { PeerCompactionNotice } from "./types.ts";

/** Human-facing explanation attached to direct contact after a peer compacts. */
export function formatPeerCompactionNotice(
  peerDisplay: string,
  notice: PeerCompactionNotice,
  expectedPeerSessionId?: string,
): string {
  const compactionCount = notice.generation - notice.previousGeneration;
  const countText = compactionCount === 1 ? "" : ` (${compactionCount} compactions)`;
  const contextText = notice.contextPct === undefined ? "" : ` Current context usage is ${notice.contextPct}%.`;
  const shortPeerId = notice.peerSessionId.slice(0, 8);
  const requested = peerDisplay.trim();
  const peerName = notice.peerName?.trim();
  // Broker-authored rebound metadata is authoritative. A caller may pass a
  // routing name when no live roster ID was available, so a value matching the
  // actual peer name must not be mistaken for a different stable identity.
  const expectedPeerId = notice.requestedPeerSessionId ?? expectedPeerSessionId;
  const expectedMatchesName = notice.requestedPeerSessionId === undefined
    && peerName !== undefined
    && expectedPeerId?.toLocaleLowerCase() === peerName.toLocaleLowerCase();
  const rebound = expectedPeerId !== undefined
    && expectedPeerId !== notice.peerSessionId
    && !expectedMatchesName;
  const requestedMatchesActual = requested === notice.peerSessionId
    || requested.includes(shortPeerId)
    || (peerName !== undefined && requested.toLocaleLowerCase() === peerName.toLocaleLowerCase());
  const identifiedPeer = rebound
    ? `${peerName || "peer"} [session ${shortPeerId}] (message was requested for ${requested} [session ${expectedPeerId!.slice(0, 8)}])`
    : requestedMatchesActual
      ? requested
      : peerName
        ? `${peerName} (requested as ${requested})`
        : `${requested} [peer session ${shortPeerId}]`;
  return `Notice: ${identifiedPeer} compacted context since your last direct contact${countText}.${contextText} Their active turn memory is summary-based; include explicit file paths or ticket IDs when referring to earlier details.`;
}
