import { readFileSync } from "fs";
import { join } from "path";
import { getParleyDirPath } from "./broker/paths.ts";

const DEFAULT_ASK_TIMEOUT_MS = 10 * 60 * 1000;
const PARLEY_SCOPE_ID_ENV = "PI_PARLEY_SCOPE_ID";

export function getAskTimeoutMs(): number {
  const raw = process.env.PI_PARLEY_ASK_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") {
    return DEFAULT_ASK_TIMEOUT_MS;
  }

  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error("PI_PARLEY_ASK_TIMEOUT_MS must be a positive integer number of milliseconds");
  }
  return value;
}

export function getParleyScopeId(env: NodeJS.ProcessEnv = process.env): string | undefined {
  const scopeId = env[PARLEY_SCOPE_ID_ENV]?.trim();
  return scopeId ? scopeId : undefined;
}

export type InboundTriggerPolicy = "always" | "replies" | "never";

export interface ParleyConfig {
  /** Broker command used to spawn the broker process (e.g. "npx" or "bun") */
  brokerCommand: string;

  /** Arguments passed to the broker command before the broker script path */
  brokerArgs: string[];

  /** Require confirmation before non-reply sends from interactive sessions */
  confirmSend: boolean;

  /** Controls whether inbound broker messages may automatically trigger a model turn */
  inboundTrigger: InboundTriggerPolicy;

  /** Optional custom status suffix shown after automatic lifecycle status */
  status?: string;

  /** Optional stable parley session ID for restart-stable addressing */
  stableId?: string;

  /** Optional default project launch command for openProjectPaneIfMissing
   * (e.g. `tmux new-window -c "{root}" pi`). No built-in default; a live
   * mesh provider advertising pi-parley/project-launch-v1 is preferred. */
  projectLauncher?: string;
  
  /** Enable/disable parley (default: true) */
  enabled: boolean;
  
  /** Show reply hint in incoming messages (default: true) */
  replyHint: boolean;
}

export function getConfigPath(parleyDir: string = getParleyDirPath()): string {
  return join(parleyDir, "config.json");
}

function readConfigRaw(): string | undefined {
  try {
    return readFileSync(getConfigPath(), "utf-8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}

const defaults: ParleyConfig = {
  brokerCommand: "npx",
  brokerArgs: ["--no-install", "tsx"],
  confirmSend: false,
  inboundTrigger: "always",
  enabled: true,
  replyHint: true,
};

export function loadConfig(): ParleyConfig {
  const raw = readConfigRaw();
  if (raw === undefined) {
    return { ...defaults };
  }

  try {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      throw new Error("Config must be a JSON object");
    }

    const parsedConfig = parsed as Record<string, unknown>;
    const config: ParleyConfig = { ...defaults };

    if (Object.hasOwn(parsedConfig, "brokerCommand")) {
      if (typeof parsedConfig.brokerCommand !== "string") {
        throw new Error(`"brokerCommand" must be a string`);
      }
      const brokerCommand = parsedConfig.brokerCommand.trim();
      if (!brokerCommand) {
        throw new Error(`"brokerCommand" must not be empty`);
      }
      config.brokerCommand = brokerCommand;
    }

    if (Object.hasOwn(parsedConfig, "brokerArgs")) {
      if (!Array.isArray(parsedConfig.brokerArgs)) {
        throw new Error(`"brokerArgs" must be an array`);
      }
      const brokerArgs: string[] = [];
      for (const arg of parsedConfig.brokerArgs) {
        if (typeof arg !== "string") {
          throw new Error(`"brokerArgs" items must be strings`);
        }
        brokerArgs.push(arg);
      }
      config.brokerArgs = brokerArgs;
    }

    if (Object.hasOwn(parsedConfig, "confirmSend")) {
      if (typeof parsedConfig.confirmSend !== "boolean") {
        throw new Error(`"confirmSend" must be a boolean`);
      }
      config.confirmSend = parsedConfig.confirmSend;
    }

    if (Object.hasOwn(parsedConfig, "enabled")) {
      if (typeof parsedConfig.enabled !== "boolean") {
        throw new Error(`"enabled" must be a boolean`);
      }
      config.enabled = parsedConfig.enabled;
    }

    if (Object.hasOwn(parsedConfig, "inboundTrigger")) {
      if (
        parsedConfig.inboundTrigger !== "always"
        && parsedConfig.inboundTrigger !== "replies"
        && parsedConfig.inboundTrigger !== "never"
      ) {
        throw new Error(`"inboundTrigger" must be "always", "replies", or "never"`);
      }
      config.inboundTrigger = parsedConfig.inboundTrigger;
    }

    if (Object.hasOwn(parsedConfig, "replyHint")) {
      if (typeof parsedConfig.replyHint !== "boolean") {
        throw new Error(`"replyHint" must be a boolean`);
      }
      config.replyHint = parsedConfig.replyHint;
    }

    if (Object.hasOwn(parsedConfig, "status")) {
      if (typeof parsedConfig.status !== "string") {
        throw new Error(`"status" must be a string`);
      }
      config.status = parsedConfig.status;
    }

    if (Object.hasOwn(parsedConfig, "stableId")) {
      if (typeof parsedConfig.stableId !== "string") {
        throw new Error(`"stableId" must be a string`);
      }
      const stableId = parsedConfig.stableId.trim();
      if (!stableId) {
        throw new Error(`"stableId" must not be empty`);
      }
      config.stableId = stableId;
    }

    if (Object.hasOwn(parsedConfig, "projectLauncher")) {
      if (typeof parsedConfig.projectLauncher !== "string") {
        throw new Error(`"projectLauncher" must be a string`);
      }
      const projectLauncher = parsedConfig.projectLauncher.trim();
      if (!projectLauncher) {
        throw new Error(`"projectLauncher" must not be empty`);
      }
      if (projectLauncher.length > 1024) {
        throw new Error(`"projectLauncher" must be at most 1024 characters`);
      }
      config.projectLauncher = projectLauncher;
    }

    return config;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to load parley config: ${message}`, { cause: error });
  }
}
