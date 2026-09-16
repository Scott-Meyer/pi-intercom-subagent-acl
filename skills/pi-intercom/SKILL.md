---
name: pi-intercom
description: |
  Coordinate with other Pi sessions on this machine. Send messages, ask
  blocking questions, share context, and collaborate across codebases.
---

# Pi Intercom

Pi-intercom connects Pi sessions on the same machine so they can work together like colleagues in a shared space. Instead of operating in isolation, sessions can share findings, ask questions, review decisions, or divide up work across codebases.

## Core Concepts

- **Share context (`send`)**: Fire-and-forget notification. Use when informing a peer of an update, handoff, or discovery without needing an answer. Send to one peer with `to` or a deliberate group with `targets: ["a", "b"]`. (Machine-wide `broadcast` exists for rare global notices, but avoid interrupting everyone when specific targets are known).
- **Ask and answer (`ask` / `reply`)**: Blocking question and reply. Use `ask` when you genuinely need information or approval before proceeding. The recipient answers with `reply`, returning the answer directly as your tool result. The default timeout is 10 minutes (configurable via `PI_INTERCOM_ASK_TIMEOUT_MS`).
- **Discovery (`list` / `list-cwd`)**: See connected peers, their directories, models, and current activity. Any call can publish or update your own 5–9 word current focus via `profile: { description: "..." }` (`description: null` clears it).
- **Review pending (`pending`)**: List unresolved inbound questions if answering outside the turn triggered by an ask.

## Cross-Project Coordination

You can message peers in other directories by specifying `to` or `cwd`.

When work in another codebase needs a durable, visible collaborator rather than a bounded subagent, pass `cwd` and `openProjectPaneIfMissing: true` to launch Pi there via the registered project launcher.

**Rule:** If no project launcher is available (no mesh provider and no configured launcher command), do not invent a terminal workaround inside the workflow. Ask the user before opening another visible surface manually.

## Subagent Escalation (`contact_supervisor`)

When delegated as a child agent by `pi-subagents` with bridge metadata, you have a dedicated `contact_supervisor` tool for reaching your task orchestrator:

- `need_decision`: Block and wait for an owner decision or scope clarification.
- `interview_request`: Block and wait for structured answers to multiple questions. The supervisor replies with JSON matching this shape:
  ```json
  { "responses": [{ "id": "question_id", "value": "answer" }] }
  ```
- `progress_update`: Non-blocking update for meaningful, plan-changing discoveries. Do not use for routine completions; return final task results normally through `pi-subagents`.

Use `contact_supervisor` for your task owner, and regular `intercom` to collaborate with other peers.

## Compaction Awareness

When a message or delivery receipt notes that a peer recently compacted, their active conversational memory now relies on a summary. They haven't lost files or stopped working, but older conversational nuance may be hazy. Restate fragile references explicitly—include exact file paths, commit hashes, or ticket IDs—and continue normally.
