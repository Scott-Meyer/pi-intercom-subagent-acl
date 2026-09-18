import { lstatSync } from "node:fs";
import net from "node:net";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { ensureParleyRuntimeDir, readBrokerTcpEndpoint, type BrokerConnectTarget, type BrokerTcpEndpoint } from "./paths.ts";
import { createMessageReader, writeMessage } from "./framing.ts";
import { isBrokerHealthOkMessage } from "./protocol.ts";
import { tryAcquireProcessLock, type ProcessLockLease, type ProcessLockOwner } from "./process-lock.ts";

/** A competing lifetime owner is a temporary startup outcome, not broker failure. */
export const BROKER_RUNTIME_OCCUPIED_EXIT_CODE = 75;
export class BrokerRuntimeOccupiedError extends Error {
  readonly code = "E_RUNTIME_OWNED";
  constructor(runtimeDir: string, readonly reportedOwner: ProcessLockOwner | null) {
    const owner = reportedOwner ? ` (reported pid ${reportedOwner.pid})` : "";
    super(`Parley runtime is already owned${owner}: ${runtimeDir}`);
    this.name = "BrokerRuntimeOccupiedError";
  }
}

/** Own one broker runtime before initializing stores or altering IPC files.
 * Retain the returned lease through listener shutdown and file cleanup. PID
 * files are discovery metadata, never authority to acquire or remove ownership.
 * An already-serving IPC endpoint is a collision, not a stale file to unlink. */
export async function claimBrokerRuntime(
  runtimeDir: string,
  listenTarget: BrokerConnectTarget,
): Promise<ProcessLockLease> {
  ensureParleyRuntimeDir(runtimeDir);
  const claim = tryAcquireProcessLock(join(runtimeDir, "broker.ownership"));
  if (claim.status === "occupied") {
    throw new BrokerRuntimeOccupiedError(runtimeDir, claim.owner);
  }
  try {
    if (typeof listenTarget === "string") {
      await assertEndpointInactive(listenTarget);
    }
    const publishedTcp = readPublishedTcpEndpoint(runtimeDir);
    if (publishedTcp && await servesParley(publishedTcp)) {
      throw new Error(`Parley TCP endpoint is already serving: ${publishedTcp.host}:${publishedTcp.port}`);
    }
    return claim.lease;
  } catch (error) {
    claim.lease.release();
    throw error;
  }
}

function assertEndpointInactive(target: string): Promise<void> {
  if (process.platform !== "win32") {
    try {
      if (!lstatSync(target).isSocket()) {
        return Promise.reject(new Error(`Refusing to replace a non-socket Parley IPC path: ${target}`));
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return Promise.resolve();
      return Promise.reject(error);
    }
  }
  return new Promise((resolve, reject) => {
    const socket = net.connect(target);
    let settled = false;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.destroy();
      if (error) reject(error);
      else resolve();
    };
    socket.once("connect", () => finish(new Error(`Parley IPC endpoint is already serving: ${target}`)));
    socket.once("error", (error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT" || error.code === "ECONNREFUSED") finish();
      else finish(error);
    });
    const timeout = setTimeout(() => finish(new Error(`Could not establish whether Parley IPC endpoint is inactive: ${target}`)), 1000);
  });
}

/** Discovery records are hints, not ownership. A damaged/refused record is
 * replaceable; a live service answering its authenticated health request is
 * a collision even if the selected transport is different. */
function readPublishedTcpEndpoint(runtimeDir: string): BrokerTcpEndpoint | undefined {
  try {
    return readBrokerTcpEndpoint(runtimeDir);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT" || error instanceof SyntaxError
      || (error instanceof Error && error.message.startsWith("Invalid parley TCP endpoint at "))) return undefined;
    throw error;
  }
}

function servesParley(target: BrokerTcpEndpoint): Promise<boolean> {
  return new Promise((resolve) => {
    const requestId = randomUUID();
    const socket = net.connect({ host: target.host, port: target.port });
    let settled = false;
    const finish = (serving: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.destroy();
      resolve(serving);
    };
    socket.once("connect", () => {
      try { writeMessage(socket, { type: "health", requestId, stateId: target.stateId }); }
      catch { finish(false); }
    });
    socket.once("error", () => finish(false));
    socket.once("close", () => finish(false));
    socket.on("data", createMessageReader(
      (message) => finish(isBrokerHealthOkMessage(message, requestId)),
      () => finish(false),
    ));
    const timeout = setTimeout(() => finish(false), 1000);
  });
}
