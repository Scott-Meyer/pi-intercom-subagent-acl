/**
 * Persisted canonical federation origin.
 *
 * The origin identity belongs to the broker installation, not to any
 * controller. First federation use adopts a controller-supplied canonical id
 * when one is offered and nothing is persisted yet, or mints a fresh
 * `install:<uuid>` otherwise. After that the id is durable across broker
 * restarts and every controller must present exactly it.
 */

import { existsSync, readFileSync, renameSync, writeFileSync } from "fs";
import { join } from "path";
import { randomUUID } from "crypto";
import { isCanonicalFederationOriginId } from "./federation-protocol.ts";
import { restrictParleyRuntimeFile } from "./paths.ts";

export const FEDERATION_ORIGIN_FILE = "federation-origin.json";

export interface PersistedFederationOrigin {
  originId: string;
  mintedAt: number;
}

export function loadPersistedFederationOrigin(parleyDir: string): PersistedFederationOrigin | undefined {
  const path = join(parleyDir, FEDERATION_ORIGIN_FILE);
  if (!existsSync(path)) return undefined;
  try {
    const parsed: unknown = JSON.parse(readFileSync(path, "utf8"));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      console.warn(`pi-parley: ${FEDERATION_ORIGIN_FILE} is corrupt; treating the federation origin as absent and adopting or minting a fresh identity.`);
      return undefined;
    }
    const record = parsed as Record<string, unknown>;
    if (!isCanonicalFederationOriginId(record.originId)
      || !Number.isSafeInteger(record.mintedAt)
      || (record.mintedAt as number) <= 0) {
      console.warn(`pi-parley: ${FEDERATION_ORIGIN_FILE} has invalid contents; treating the federation origin as absent and adopting or minting a fresh identity.`);
      return undefined;
    }
    return { originId: record.originId, mintedAt: record.mintedAt as number };
  } catch {
    // A corrupt or hostile file never blocks the broker: treat it as absent
    // and let the next first use adopt or mint a fresh durable identity.
    console.warn(`pi-parley: ${FEDERATION_ORIGIN_FILE} could not be read; treating the federation origin as absent and adopting or minting a fresh identity.`);
    return undefined;
  }
}

export function persistFederationOrigin(
  parleyDir: string,
  origin: PersistedFederationOrigin,
  platform: NodeJS.Platform = process.platform,
): void {
  const path = join(parleyDir, FEDERATION_ORIGIN_FILE);
  const temp = `${path}.${process.pid}.tmp`;
  writeFileSync(temp, JSON.stringify(origin), { mode: 0o600 });
  renameSync(temp, path);
  restrictParleyRuntimeFile(path, platform);
}

export function mintFederationOriginId(): string {
  return `install:${randomUUID()}`;
}
