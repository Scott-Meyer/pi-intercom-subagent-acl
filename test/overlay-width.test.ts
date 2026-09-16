import test from "node:test";
import assert from "node:assert/strict";

import { visibleWidth } from "@mariozechner/pi-tui";
import { ComposeOverlay } from "../ui/compose.ts";
import { SessionListOverlay } from "../ui/session-list.ts";
import type { SessionInfo } from "../types.ts";

const theme = {
  fg(_name: string, text: string): string {
    return text;
  },
  bold(text: string): string {
    return text;
  },
};

const keybindings = {
  matches(): boolean {
    return false;
  },
  getKeys(id: string): string[] {
    return id.includes("confirm") ? ["enter"] : ["escape", "ctrl+c"];
  },
};

const session: SessionInfo = {
  id: "session-12345678",
  name: "session-019ecaf6",
  description: "Reviewing peer discovery profiles now",
  cwd: "/Users/envvar/.config/ghostty",
  model: "bsy-deepseek-v4-pro",
  pid: 1,
  startedAt: 0,
  lastActivity: 0,
};

function assertLineWidths(label: string, lines: string[], expectedWidth: number): void {
  assert.ok(lines.length > 0, `${label} should render lines`);
  for (const [index, line] of lines.entries()) {
    assert.equal(visibleWidth(line), expectedWidth, `${label} line ${index} should match overlay width`);
  }
}

test("compose overlay renders lines at the declared overlay width", () => {
  const overlay = new ComposeOverlay(
    { requestRender() {} } as any,
    theme as any,
    keybindings as any,
    session,
    "session-019ecaf6",
    { send: async () => ({ delivered: true, id: "message-1" }) } as any,
    () => {},
  );

  for (const width of [1, 2, 20, 40, 72]) {
    assertLineWidths("compose overlay", overlay.render(width), width);
  }
});

test("compose overlay returns the contact token without acknowledging before the caller surfaces it", async () => {
  let acknowledged = 0;
  let completed: unknown;
  const overlay = new ComposeOverlay(
    { requestRender() {} } as any,
    theme as any,
    keybindings as any,
    session,
    "session-019ecaf6",
    {
      send: async () => ({
        delivered: true,
        id: "message-1",
        delivery: "socket_delivered",
        contactToken: "contact-token",
        peerCompaction: {
          peerSessionId: session.id,
          peerName: session.name,
          generation: 2,
          previousGeneration: 1,
          compactedAt: 123,
        },
      }),
      acknowledgeSendContact: () => { acknowledged += 1; },
    } as any,
    (result) => { completed = result; },
  );
  (overlay as any).inputBuffer = "hello";
  await (overlay as any).sendMessage();

  assert.equal(acknowledged, 0);
  assert.equal((completed as { contactToken?: string }).contactToken, "contact-token");
});

test("session list overlay renders lines at the declared overlay width", () => {
  const overlay = new SessionListOverlay(theme as any, keybindings as any, session, [session], () => {});

  for (const width of [1, 2, 20, 50, 88]) {
    assertLineWidths("session list overlay", overlay.render(width), width);
  }
  assert.match(overlay.render(88).join("\n"), /Reviewing peer discovery profiles now/);
});

test("session list overlay identifies federated sessions by remote origin", () => {
  const remote: SessionInfo = {
    ...session,
    id: "oqs1.remote-qualified-id",
    name: "Remote Specialist",
    federation: {
      originId: "host:penguin",
      originLabel: "Penguin",
      remoteScopeAlias: "mistfall-remote",
      remoteStableSessionId: "remote-session-123",
    },
    trustedLocal: false,
  };
  let selected: SessionInfo | undefined;
  const selectingKeys = {
    ...keybindings,
    matches(data: string, id: string): boolean {
      return data === "enter" && id === "tui.select.confirm";
    },
  };
  const overlay = new SessionListOverlay(theme as any, selectingKeys as any, session, [remote], (value) => { selected = value; });
  const rendered = overlay.render(88).join("\n");
  assert.match(rendered, /remote:Penguin/);
  assert.match(rendered, /roster only/);
  assert.match(rendered, /remote-s/);
  overlay.handleInput("enter");
  assert.equal(selected, undefined);
});
