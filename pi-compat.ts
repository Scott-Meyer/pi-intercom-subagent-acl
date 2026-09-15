import type { ToolDefinition } from "@mariozechner/pi-coding-agent";
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
