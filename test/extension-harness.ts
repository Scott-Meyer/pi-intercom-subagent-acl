import { EventEmitter } from "node:events";

const repoDir = process.cwd();

export interface CapturedToolResult {
  content: Array<{ type: string; text: string }>;
  details?: Record<string, unknown>;
}

export interface RenderToolResult {
  content: Array<{ type: string; text: string }>;
  details?: Record<string, unknown>;
}

export interface RenderedComponent {
  render(width: number): string[];
}

export interface RenderTheme {
  fg(name: string, text: string): string;
  bold(text: string): string;
}

export interface CapturedTool {
  name: string;
  parameters?: unknown;
  execute: (toolCallId: string, params: Record<string, unknown>, signal: AbortSignal, onUpdate: unknown, ctx: unknown) => Promise<CapturedToolResult>;
  renderCall?: (args: Record<string, unknown>, theme: RenderTheme, context: Record<string, unknown>) => RenderedComponent;
  renderResult?: (result: RenderToolResult, options: { expanded?: boolean; isPartial?: boolean }, theme: RenderTheme, context: Record<string, unknown>) => RenderedComponent;
}

export function createExtensionHarness(sessionName: string | (() => string) = "child-worker", options: {
  abort?: () => void;
  hasUI?: boolean;
  isIdle?: () => boolean;
  mode?: "tui" | "rpc" | "json" | "print";
  ui?: unknown;
  sessionId?: string | (() => string);
  activeTools?: string[];
  appendEntryError?: () => Error | undefined;
  /** False models a queued or asynchronously rejected host send, not yet persisted. */
  persistMessages?: boolean;
} = {}) {
  const harnessOptions = options;
  const events = new EventEmitter();
  const lifecycleHandlers = new Map<string, Array<(event: unknown, ctx: unknown) => unknown>>();
  const commands = new Map<string, (args: string, ctx: unknown) => unknown>();
  const shortcuts = new Map<string, (ctx: unknown) => unknown>();
  const tools: CapturedTool[] = [];
  let currentSessionName = typeof sessionName === "function" ? sessionName() : sessionName;
  let activeToolNames = [...(options.activeTools ?? [])];
  const entries: Array<{ type: string; data: unknown }> = [];
  const persistedMessages: Array<{ customType?: string; content?: string; details?: unknown }> = [];
  const toolResults: Array<{ role: "toolResult"; toolCallId: string; toolName: string } & CapturedToolResult> = [];
  const sentMessages: Array<{ message: { customType?: string; content?: string; details?: unknown }; options?: { triggerTurn?: boolean; deliverAs?: string }; activeTools: string[] }> = [];
  const pi = {
    getSessionName: () => typeof sessionName === "function" ? sessionName() : currentSessionName,
    setSessionName: (name: string) => { currentSessionName = name; },
    events: {
      on: (channel: string, handler: (payload: unknown) => void) => {
        events.on(channel, handler);
        return () => events.off(channel, handler);
      },
      emit: (channel: string, payload: unknown) => events.emit(channel, payload),
    },
    on: (event: string, handler: (payload: unknown, ctx: unknown) => unknown) => {
      const handlers = lifecycleHandlers.get(event) ?? [];
      handlers.push(handler);
      lifecycleHandlers.set(event, handlers);
    },
    registerMessageRenderer: () => undefined,
    registerTool: (tool: CapturedTool) => {
      tools.push({
        ...tool,
        execute: async (...args: Parameters<CapturedTool["execute"]>) => {
          const result = await tool.execute(...args);
          toolResults.push({ role: "toolResult", toolCallId: args[0], toolName: tool.name, ...result });
          return result;
        },
      });
      if (!activeToolNames.includes(tool.name)) activeToolNames.push(tool.name);
    },
    getActiveTools: () => [...activeToolNames],
    setActiveTools: (names: string[]) => { activeToolNames = [...names]; },
    registerCommand: (name: string, command: { handler: (args: string, ctx: unknown) => unknown }) => {
      commands.set(name, command.handler);
    },
    registerShortcut: (key: string, shortcut: { handler: (ctx: unknown) => unknown }) => {
      shortcuts.set(key, shortcut.handler);
    },
    sendMessage: (message: { customType?: string; content?: string; details?: unknown }, options?: { triggerTurn?: boolean; deliverAs?: string }) => {
      sentMessages.push({ message, options, activeTools: [...activeToolNames] });
      if (harnessOptions.persistMessages !== false) persistedMessages.push(message);
    },
    appendEntry: (type: string, data: unknown) => {
      const error = options.appendEntryError?.();
      if (error) throw error;
      entries.push({ type, data });
    },
  };
  const ctx = {
    cwd: repoDir,
    ...(options.mode === undefined ? {} : { mode: options.mode }),
    model: { id: "child-model" },
    sessionManager: {
      getSessionId: () => typeof options.sessionId === "function" ? options.sessionId() : options.sessionId ?? "session-child-test",
      getEntries: () => [...entries.map((entry, index) => ({
        type: "custom",
        customType: entry.type,
        data: entry.data,
        id: `entry-${index}`,
        parentId: index > 0 ? `entry-${index - 1}` : null,
        timestamp: new Date().toISOString(),
      })), ...persistedMessages.map((message, index) => ({
        type: "custom_message", ...message, id: `message-${index}`, parentId: null, timestamp: new Date().toISOString(),
      })), ...toolResults.map((message, index) => ({
        type: "message", message, id: `tool-result-${index}`, parentId: null, timestamp: new Date().toISOString(),
      }))],
    },
    isIdle: options.isIdle ?? (() => true),
    hasUI: options.hasUI ?? false,
    abort: options.abort ?? (() => undefined),
    ui: options.ui,
  };
  return {
    pi,
    ctx,
    tools,
    commands,
    shortcuts,
    entries,
    sentMessages,
    persistedMessages,
    toolResults,
    getActiveTools: () => pi.getActiveTools(),
    async emitLifecycle(event: string, payload: unknown = {}, eventContext: unknown = ctx) {
      for (const handler of lifecycleHandlers.get(event) ?? []) {
        await handler(payload, eventContext);
      }
    },
    async emitLifecycleResults(event: string, payload: unknown = {}, eventContext: unknown = ctx) {
      const results: unknown[] = [];
      for (const handler of lifecycleHandlers.get(event) ?? []) {
        results.push(await handler(payload, eventContext));
      }
      return results;
    },
  };
}
