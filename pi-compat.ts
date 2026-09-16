import type { ToolDefinition } from "@mariozechner/pi-coding-agent";
import { readFileSync } from "node:fs";
import { Type, type TSchema, type TUnsafe } from "typebox";

/**
 * Host-neutral identity helper for extension tool definitions.
 *
 * Pi's defineTool helper exists only to provide contextual typing. Keeping the
 * runtime identity function here avoids pulling either Pi distribution into an
 * installed extension merely for that no-op.
 */
export function defineTool<TParams extends TSchema, TDetails = unknown, TState = unknown>(
  tool: ToolDefinition<TParams, TDetails, TState>,
): ToolDefinition<TParams, TDetails, TState> & ToolDefinition<any, any, any> {
  return tool as ToolDefinition<TParams, TDetails, TState> & ToolDefinition<any, any, any>;
}

/**
 * String-enum schema accepted by Pi tool hosts and providers that reject
 * `anyOf`/`const` enum encodings.
 */
export function StringEnum<T extends readonly string[]>(
  values: T,
  options?: { description?: string; default?: T[number] },
): TUnsafe<T[number]> {
  return Type.Unsafe({
    type: "string",
    enum: values,
    ...(options?.description ? { description: options.description } : {}),
    ...(options?.default ? { default: options.default } : {}),
  });
}

/**
 * Whether the running host forwards `session_info_changed` to extensions.
 * Upstream 0.73.1 exposes the core/RPC event but not the extension event; the
 * fork does both. Unknown launchers conservatively use the compatibility poll.
 */
export function sessionInfoChangesReachExtensions(hostEntry = process.argv[1] ?? ""): boolean {
  const normalized = hostEntry.replaceAll("\\", "/");
  return normalized.includes("/@earendil-works/pi-coding-agent/");
}

function versionAtLeast(version: string, minimum: readonly [number, number, number]): boolean {
  const match = /^(\d+)\.(\d+)\.(\d+)/.exec(version);
  if (!match) return false;
  const actual = [Number(match[1]), Number(match[2]), Number(match[3])] as const;
  for (let index = 0; index < minimum.length; index += 1) {
    if (actual[index]! !== minimum[index]!) return actual[index]! > minimum[index]!;
  }
  return true;
}

function readEarendilHostVersion(hostEntry: string): string | undefined {
  const normalized = hostEntry.replaceAll("\\", "/");
  const marker = "/@earendil-works/pi-coding-agent/";
  const markerIndex = normalized.lastIndexOf(marker);
  if (markerIndex < 0) return undefined;
  const packageRoot = normalized.slice(0, markerIndex + marker.length - 1);
  try {
    const manifest = JSON.parse(readFileSync(`${packageRoot}/package.json`, "utf8")) as { name?: unknown; version?: unknown };
    return manifest.name === "@earendil-works/pi-coding-agent" && typeof manifest.version === "string"
      ? manifest.version
      : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Whether the host reports unsuccessful compactions back to extensions.
 * Earendil added `session_compact_failed` in 0.85.0. Older hosts only expose
 * before/success, so publishing a temporary status there could remain stale
 * after an ordinary provider failure.
 */
export function sessionCompactFailuresReachExtensions(
  hostEntry = process.argv[1] ?? "",
  hostVersion = readEarendilHostVersion(hostEntry),
): boolean {
  const normalized = hostEntry.replaceAll("\\", "/");
  return normalized.includes("/@earendil-works/pi-coding-agent/")
    && typeof hostVersion === "string"
    && versionAtLeast(hostVersion, [0, 85, 0]);
}
