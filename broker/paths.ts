import { chmodSync, mkdirSync, readFileSync } from "fs";
import { isAbsolute, join, resolve } from "path";
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

export function shouldUseTcpTransport(
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  const transport = env.PI_PARLEY_TRANSPORT?.trim().toLowerCase();
  if (transport === "tcp") {
    return true;
  }

  const tcpOptIn = env.PI_PARLEY_TCP?.trim().toLowerCase();
  return tcpOptIn === "1" || tcpOptIn === "true";
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

/** Read one published authenticated loopback endpoint independently of transport
 * selection. Discovery is a snapshot of where to probe, not proof of ownership.
 * Missing, interrupted or invalid records throw. */
export function readBrokerTcpEndpoint(parleyDir: string = getParleyDirPath()): BrokerTcpEndpoint {
  const endpointFile = getBrokerPortFilePath(parleyDir);
  const parsed: unknown = JSON.parse(readFileSync(endpointFile, "utf-8"));
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
  return Object.freeze({ transport: "tcp", host: endpoint.host, port: endpoint.port, stateId: endpoint.stateId });
}

export function getBrokerConnectTarget(
  platform: NodeJS.Platform = process.platform,
  env: NodeJS.ProcessEnv = process.env,
  parleyDir: string = getParleyDirPath(getAgentDirPath(env)),
): BrokerConnectTarget {
  return shouldUseTcpTransport(env)
    ? readBrokerTcpEndpoint(parleyDir)
    : getBrokerSocketPath(platform, getAgentDirPath(env));
}

export function getBrokerListenTarget(
  platform: NodeJS.Platform = process.platform,
  env: NodeJS.ProcessEnv = process.env,
): BrokerConnectTarget {
  if (shouldUseTcpTransport(env)) {
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

export function restrictParleyRuntimeFile(
  filePath: string,
  platform: NodeJS.Platform = process.platform,
): void {
  if (platform !== "win32") {
    chmodSync(filePath, PARLEY_RUNTIME_FILE_MODE);
  }
}
