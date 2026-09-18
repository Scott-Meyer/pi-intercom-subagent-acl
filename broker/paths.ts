import { chmodSync, existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, unlinkSync, writeFileSync } from "fs";
import { isAbsolute, join, resolve } from "path";
import { parleyEnv } from "../env-compat.ts";
import { homedir } from "os";

export const PARLEY_DIR_MODE = 0o700;
export const PARLEY_RUNTIME_FILE_MODE = 0o600;
export const PARLEY_TCP_HOST = "127.0.0.1";
export const PARLEY_PROTOCOL_NAME = "pi-parley";
export const PARLEY_PROTOCOL_VERSION = 1;

export interface BrokerTcpEndpoint {
  transport: "tcp";
  host: string;
  port: number;
  stateId?: string;
}

export type BrokerConnectTarget = string | BrokerTcpEndpoint;

function sanitizePipeSegment(value: string): string {
  return value
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase() || "default";
}

export function getAgentDirPath(
  env: NodeJS.ProcessEnv = process.env,
  homeDir: string = homedir(),
  cwd: string = process.cwd(),
): string {
  const configured = env.PI_CODING_AGENT_DIR?.trim();
  if (!configured) {
    return join(homeDir, ".pi/agent");
  }

  return isAbsolute(configured) ? configured : resolve(cwd, configured);
}

export function getParleyDirPath(agentDir: string = getAgentDirPath()): string {
  return join(agentDir, "parley");
}

export function shouldUseWindowsTcpTransport(
  platform: NodeJS.Platform = process.platform,
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  if (platform !== "win32") {
    return false;
  }

  const transport = parleyEnv("PI_PARLEY_TRANSPORT", env)?.trim().toLowerCase();
  if (transport === "tcp") {
    return true;
  }

  const legacyOptIn = parleyEnv("PI_PARLEY_TCP", env)?.trim().toLowerCase();
  return legacyOptIn === "1" || legacyOptIn === "true";
}

export function getBrokerPortFilePath(parleyDir: string = getParleyDirPath()): string {
  return join(parleyDir, "broker.port.json");
}

export function getBrokerSocketPath(
  platform: NodeJS.Platform = process.platform,
  agentDir: string = getAgentDirPath(),
): string {
  if (platform === "win32") {
    return `\\\\.\\pipe\\pi-parley-${sanitizePipeSegment(agentDir)}`;
  }

  return join(getParleyDirPath(agentDir), "broker.sock");
}

export function getBrokerConnectTarget(
  platform: NodeJS.Platform = process.platform,
  env: NodeJS.ProcessEnv = process.env,
  parleyDir: string = getParleyDirPath(getAgentDirPath(env)),
): BrokerConnectTarget {
  if (shouldUseWindowsTcpTransport(platform, env)) {
    const endpointFile = getBrokerPortFilePath(parleyDir);
    const raw = readFileSync(endpointFile, "utf-8");
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      throw new Error(`Invalid parley TCP endpoint at ${endpointFile}: expected a JSON object`);
    }
    const endpoint = parsed as Record<string, unknown>;
    if (
      endpoint.transport !== "tcp"
      || endpoint.host !== PARLEY_TCP_HOST
      || typeof endpoint.port !== "number"
      || !Number.isSafeInteger(endpoint.port)
      || endpoint.port <= 0
      || endpoint.port > 65535
      || typeof endpoint.stateId !== "string"
      || endpoint.stateId.length === 0
    ) {
      throw new Error(`Invalid parley TCP endpoint at ${endpointFile}`);
    }
    return { transport: "tcp", host: endpoint.host, port: endpoint.port, stateId: endpoint.stateId };
  }

  return getBrokerSocketPath(platform, getAgentDirPath(env));
}

export function getBrokerListenTarget(
  platform: NodeJS.Platform = process.platform,
  env: NodeJS.ProcessEnv = process.env,
): BrokerConnectTarget {
  if (shouldUseWindowsTcpTransport(platform, env)) {
    return { transport: "tcp", host: PARLEY_TCP_HOST, port: 0 };
  }

  return getBrokerSocketPath(platform, getAgentDirPath(env));
}

export function ensureParleyRuntimeDir(
  parleyDir: string = getParleyDirPath(),
  platform: NodeJS.Platform = process.platform,
): void {
  mkdirSync(parleyDir, { recursive: true, mode: PARLEY_DIR_MODE });
  if (platform !== "win32") {
    chmodSync(parleyDir, PARLEY_DIR_MODE);
  }
}

/** Broker-process files are never migrated: they belong to the broker that
 * wrote them, not to the state that survives it. */
const BROKER_RUNTIME_FILES = new Set([
  "broker.pid",
  "broker.sock",
  "broker.port.json",
  "broker.spawn.lock",
  "broker-launch.vbs",
]);
export type LegacyRuntimeMigration =
  | { status: "none" }
  | { status: "migrated" }
  /** A live broker still runs from the legacy directory. Starting a second
   * broker would split the roster, so startup refuses until the legacy
   * broker idles out (its sessions restart) and a later start migrates. */
  | { status: "blocked"; liveLegacyBroker: true }
  /** Both directories hold state; nothing is moved silently. Resolve
   * manually by removing or merging one of the two. */
  | { status: "conflict"; legacyDir: string; targetDir: string };

function isPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function legacyBrokerIsRunning(legacyDir: string): boolean {
  const pidPath = join(legacyDir, "broker.pid");
  if (!existsSync(pidPath)) return false;
  const raw = readFileSync(pidPath, "utf-8").trim();
  const pid = Number.parseInt(raw, 10);
  return Number.isSafeInteger(pid) && pid > 0 && isPidAlive(pid);
}

/** Mutual exclusion for the cutover: acquire the legacy spawn lock (same
 * protocol legacy-version clients use — pid + timestamp, wx, stale after a
 * dead owner or 10s) before touching legacy state. Holding it excludes a
 * concurrent legacy startup across the pid publish / lock release handoff,
 * which sampling broker.pid and the lock separately cannot. */
function acquireLegacyRuntimeLock(legacyDir: string): boolean {
  const lockPath = join(legacyDir, "broker.spawn.lock");
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      writeFileSync(lockPath, `${process.pid}\n${Date.now()}\n`, { flag: "wx", mode: PARLEY_RUNTIME_FILE_MODE });
      restrictParleyRuntimeFile(lockPath);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      try {
        const [pidLine = "", createdAtLine = ""] = readFileSync(lockPath, "utf-8").trim().split("\n");
        const pid = Number.parseInt(pidLine, 10);
        const createdAt = Number.parseInt(createdAtLine, 10);
        const lockAgeMs = Date.now() - statSync(lockPath).mtimeMs;
        if (Number.isFinite(pid) && Number.isFinite(createdAt)) {
          const ownerGone = !isPidAlive(pid);
          const staleAge = Date.now() - createdAt > 10_000;
          if (ownerGone || staleAge) {
            try { unlinkSync(lockPath); } catch { /* retry below */ }
            continue;
          }
        } else if (lockAgeMs > 10_000) {
          // Empty or partial content that has also sat untouched: abandoned.
          try { unlinkSync(lockPath); } catch { /* retry below */ }
          continue;
        } else {
          // Freshly created with no readable content yet: a holder is
          // between its exclusive create and its write. Treat as live.
          return false;
        }
      } catch {
        // The holder may have removed the lock between our check and read;
        // retry the acquisition rather than blocking on a vanished file.
        if (!existsSync(lockPath)) continue;
        return false;
      }
      return false;
    }
  }
  return false;
}

/** The legacy lock we acquired moves with a whole-rename; every other path
 * leaves it in the legacy directory (or removes it with the directory). Only
 * the tracked path is released — the target directory's own spawn lock is
 * owned by the spawning client and must never be deleted here. */
function releaseLegacyRuntimeLock(lockPath: string): void {
  try { unlinkSync(lockPath); } catch { /* already absent */ }
}

function hasStateEntries(dir: string): boolean {
  if (!existsSync(dir)) return false;
  return readdirSync(dir).some((entry) => !BROKER_RUNTIME_FILES.has(entry));
}

/** Parley 1.1.0 relocated the runtime dir from `intercom/` to `parley/`.
 * Migration is a coordinated cutover, never a live move:
 * - no legacy dir: nothing to do.
 * - legacy broker live: startup is blocked; retry after it idles out.
 * - legacy state, no target: the directory moves whole.
 * - legacy state, target fresh (only broker process files): legacy state is
 *   adopted and the husk is removed.
 * - legacy state, target state: an explicit conflict; nothing is moved.
 * - legacy holds only broker process files: the husk is removed. */
export function migrateLegacyRuntimeDir(
  agentDir: string = getAgentDirPath(),
): LegacyRuntimeMigration {
  const legacyDir = join(agentDir, "intercom");
  const targetDir = getParleyDirPath(agentDir);
  try {
    return migrateLegacyRuntimeDirInner(legacyDir, targetDir);
  } catch (error) {
    // A concurrent first start may have moved the legacy dir between our
    // check and our operation; that competitor completing the cutover is
    // success, not failure.
    if (!existsSync(legacyDir) && existsSync(targetDir)) {
      return { status: "migrated" };
    }
    throw error;
  }
}

function migrateLegacyRuntimeDirInner(legacyDir: string, targetDir: string): LegacyRuntimeMigration {
  if (!existsSync(legacyDir)) return { status: "none" };
  if (!acquireLegacyRuntimeLock(legacyDir)) {
    return { status: "blocked", liveLegacyBroker: true };
  }
  // Where our acquired lock currently lives; a whole-rename moves it.
  let lockPath = join(legacyDir, "broker.spawn.lock");
  try {
    if (legacyBrokerIsRunning(legacyDir)) {
      return { status: "blocked", liveLegacyBroker: true };
    }
    const legacyHasState = hasStateEntries(legacyDir);
    if (!legacyHasState) {
      rmSync(legacyDir, { recursive: true, force: true });
      return { status: "migrated" };
    }
    if (!existsSync(targetDir)) {
      renameSync(legacyDir, targetDir);
      lockPath = join(targetDir, "broker.spawn.lock");
      return { status: "migrated" };
    }
    if (hasStateEntries(targetDir)) {
      return { status: "conflict", legacyDir, targetDir };
    }
    // Target is a fresh husk: adopt legacy state, drop process files. The
    // target's own broker.spawn.lock (owned by the spawning client) is a
    // runtime file and is deliberately left alone.
    for (const entry of readdirSync(legacyDir)) {
      if (BROKER_RUNTIME_FILES.has(entry)) continue;
      renameSync(join(legacyDir, entry), join(targetDir, entry));
    }
    rmSync(legacyDir, { recursive: true, force: true });
    return { status: "migrated" };
  } finally {
    releaseLegacyRuntimeLock(lockPath);
  }
}

export function restrictParleyRuntimeFile(
  filePath: string,
  platform: NodeJS.Platform = process.platform,
): void {
  if (platform !== "win32") {
    chmodSync(filePath, PARLEY_RUNTIME_FILE_MODE);
  }
}
