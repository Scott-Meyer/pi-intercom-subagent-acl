# Changelog

All notable changes to the `pi-parley` extension will be documented in this file.

## [1.1.0] - 2026-09-17

### Changed

The intercom legacy is fully retired from runtime surfaces. Coordinated cutover, not a live move:

- Runtime dir moves from `~/.pi/agent/intercom/` to `~/.pi/agent/parley/`. A stopped legacy runtime migrates automatically on first start (state adopted wholesale); a live legacy broker — or an in-flight legacy startup holding the old spawn lock — blocks a second broker from starting until the legacy one drains. If a federation peer link (e.g. a FlightDeck bridge) holds the legacy broker open, disconnect the link or stop the drained broker. Dual populated runtimes are an explicit conflict, never silently merged.
- Wire protocol renamed (`pi-intercom` v1 → `pi-parley`; `pi-intercom-peer` → `pi-parley-peer`). All brokers and federation peers must run 1.1.0 together.
- Extension-API events renamed: `intercom:*` → `parley:*`. Subagent relay events renamed: `subagent:*intercom*` → `subagent:*parley*`. The project-launch capability namespace is now `pi-parley/project-launch-v1`. Consumers (coord-observer, pi-subagents, FlightDeck) must update in step.
- Env vars renamed `PI_INTERCOM_*` → `PI_PARLEY_*`.
- Windows named pipe renamed `pi-intercom-*` → `pi-parley-*`.

### Fixed

- Pre-1.1 journals stay recoverable: session history replays legacy `intercom_*` entry types while writing `parley_*`.
- A legacy `intercom/config.json` still governs until the cutover completes; config reads tolerate a concurrent migration.
- Reconnect retries keep firing while a legacy broker blocks startup, including after interactive `/parley` attempts and a first failed retry.

## [1.0.1] - 2026-09-17

### Fixed

- Package metadata: repository link for GitHub — npm provenance verification and the npm page now point at the parley repo.

## [1.0.0] - 2026-09-17

### Renamed

The ACL fork of `pi-intercom` is now its own extension: **pi-parley** (repo `Scott-Meyer/pi-parley`, install `pi install git:github.com/Scott-Meyer/pi-parley@v1.0.0`).

- Agent tool `intercom` → `parley`; commands `/intercom` → `/parley` and `/intercom-id` → `/parley-id`; skill `pi-intercom` → `pi-parley`. `/alias` and Alt+M are unchanged.
- Deliberately unchanged for compatibility: the broker wire protocol (`pi-intercom` v1, `pi-intercom-peer`), runtime dir `~/.pi/agent/intercom/` (config, sockets, queued mail, federation state), persisted session-history entry types (`intercom_*`), extension-API event names (`intercom:*`), the `pi-intercom/project-launch-v1` capability namespace, and `PI_INTERCOM_*` env vars. Existing deployments upgrade in place; remove any earlier intercom extension first so Pi does not load both copies.

### Added
- Non-blocking asks (`blocking: false`) return an initial delivery outcome and receive the answer later in the conversation, including while a headless caller works. Outstanding questions and received-but-not-yet-persisted conversation content have journaled reload/resume recovery.
- Communication results include bounded pending/outstanding context. `pending` exposes unanswered inbound requests, `status` shows local outstanding-question tracking, and `read` retrieves a retained incoming message's full text and attachment snapshots by exact ID.
- `rename` changes the current session's canonical Pi name. Send/ask call displays and delivery results identify the sender used for the contact; results include exact message IDs and resolved recipient identities when known.

### Changed
- Notifications remain notifications: `send` no longer infers an answer or settles a pending ask. `reply` explicitly answers questions and also supports ordinary conversation threads. Active questions no longer block clarification asks, reverse asks, or contact with other colleagues.
- Cancellation and supersession surface recipient-visible withdrawal/update context without claiming to undo work. Results distinguish offline mailbox removal from live withdrawal and unknown cancellation outcomes.
- Delivery results distinguish accepted, queued, known nondelivery, and unknown outcomes. Lost acknowledgements do not imply a retry is safe. Offline queue notices state the broker-memory lifetime rather than promising durable delivery.
- Project-launch results preserve request/command startup, observed registration, and message-delivery stages. Failures disclose prior side effects and uncertainty; a v1 roster match does not prove which launch created a peer.

### Fixed
- Implicit replies keep their active conversation across tool calls, not across unrelated runs. Ambiguous arrivals remain explicit, and naming the sender no longer redirects a reply from the current note to an older question.
- Asynchronous reply tracking remains until host acceptance rather than disappearing before headless delivery; an unrelated failed async ask no longer tears down another blocking waiter. Elapsed reply windows are not treated as completion or withdrawal.
- Broadcast excludes remote rows. Federation rows identify remote origin and text-only constraints; remote asks, replies, and attachments remain unsupported.
- Tool contracts, README, and the conceptual skill replace workflow recipes with communication meaning and observable outcomes, correcting blocking defaults, native-supervisor precedence, visibility scope, attachment snapshots, and queue/timeout boundaries.

## [0.13.0-acl.11] - 2026-09-16

### Added
- Federation Slice 3 (`peer-send-v1`): routed direct sends from locally owned sessions to imported `oqs1.*` targets over a negotiated peer link, with end-to-end result correlation — delivery is accepted only when the destination broker's correlated `peer_send_result` arrives, never because a frame was written. Remote sessions deliver through the ordinary message pipeline, so steering, waking, and visibility behave exactly like local delivery.
- `broker_list_scopes` trusted-local control: canonical federation origin plus live local scope enumeration (raw scope ids with live session counts) in one call, for controller pickers.
- Persisted canonical federation origin: first federation use adopts a controller-supplied canonical id or mints `install:<uuid>`, durable across broker restarts in the runtime dir; every later dial/accept must present exactly it (`E_ORIGIN_MISMATCH` otherwise).
- Generic project-launch integration replaces the previous Herdr-specific pane launching: any local intercom session registers as a provider by advertising the `pi-intercom/project-launch-v1` extension capability and answering its JSON launch requests; `PI_INTERCOM_PROJECT_LAUNCHER` or config `projectLauncher` supplies an opt-in default command (`{root}` is substituted with a safely shell-quoted path — no injection surface, no built-in default). Session `extensions` capabilities are now visible to local peers through the roster and update live; federation v1 deliberately does not carry capabilities across links, and remote rows are never picked as providers.

### Fixed
- Adapters that duplicate the recipient into both `to` and `targets` are treated as a singular send when the single target is identical (multicast already delivers same-session aliases once); genuinely different or multi-element lists still conflict.
- Short roster IDs never truncate the final id segment: ids like `mistfall-remote:game:t226` whose unique tail follows the last separator previously displayed as `mistfall-remote:game:t2`; uniqueness prefixes now extend to the next `-`/`:` boundary or keep the full id.
- Corrupt persisted federation-origin files warn before adopting or minting a fresh identity.

### Security
- Remote v1 send contract: direct text-only sends (≤32 KiB), no remote asks/replies/supersession, attachments, broadcast, or mailbox queueing; senders must be exported on the link (restricted subagents cannot send remotely), targets must be locally owned, scope-matched, and ACL-visible to the broker-authoritative imported sender; per-link duplicate-send dedupe and in-flight duplicate message id refusal; pending correlations fail deterministically on link drop or a bounded deadline (below the client timeout).
- Routed sends to unknown qualified ids and sends on roster-only links fail with explicit codes instead of falling through to local name or mailbox resolution.

### Notes
- Asks/replies/receipts across federation remain Slice 4; broadcast, queued mailboxes, extension channels, and compaction awareness remain host-local in federation v1.
- Routed-send correlation is at-least-once under timeout races: if a destination result races the bounded deadline, the sender observes a retryable failure even though delivery may have completed, and a client retry delivers again.
- Destination duplicate-send dedup is a bounded FIFO window (4096 sendIds per link): a peer churning sendIds can evict old ids and replay them. Trusted-peer federation makes this a low-priority concern; the origin never reuses sendIds.
- ACL.9/ACL.10 peers remain compatible: `peer-send-v1` is intersection-negotiated and absent from older links.

## [0.13.0-acl.10] - 2026-09-16

### Added
- Federation Slice 2 negotiates optional `peer-roster-v1` support and replicates only locally owned mains and explicitly advertised subagents through bounded authoritative snapshots plus sequenced deltas. Origin epochs, exact-next sequencing, stale-epoch rejection, deduplicated resync requests, and atomic per-link disconnect pruning prevent partial or ghost rosters.
- Imported sessions use canonical origin-qualified IDs, bind remote aliases to independently authorized local scopes, carry broker-authored federation provenance, remain explicitly untrusted, and appear in list/overlay output as remote roster-only rows. Local scope and subagent visibility rules still apply.

### Security
- The `oqs1.` imported-ID namespace is reserved from local registration, and clients reject federation metadata unless `trustedLocal` is false and the visible ID exactly encodes its origin/scope/stable-ID tuple.
- Snapshot, delta, aggregate visible-roster, origin-epoch, and peer-frame rate bounds prevent one peer from overflowing framing or unbounded state. Malformed outbound-link frames, roster overflow, and reconciliation errors tear down only the peer link instead of disconnecting ordinary local sessions.

### Notes
- Remote rows are intentionally non-selectable and direct routing, broadcast, queued mailboxes, extension channels, and compaction awareness remain host-local. ACL.9 peers can retain the base federation link without falsely negotiating roster support.

## [0.13.0-acl.9] - 2026-09-16

### Added
- Federation Slice 1 adds strict broker-peer v1 wire contracts, broker-generated link IDs, capability-bearing FlightDeck attachment prefaces, trusted destination preparation, explicit peer connection roles, origin-qualified session identity encoding, and correlated dial/accept/hello results.
- Trusted local scope bindings keep exact broker scope IDs out of the peer stream while binding public origin/scope aliases to independently authorized local namespaces. Peer links keep an otherwise idle broker alive and are atomically removed when their transport disconnects.

### Security
- Federation dials accept only literal loopback endpoints, bounded one-time capabilities, canonical origins, and strict no-extra-field payloads. The destination broker accepts a hello only after FlightDeck independently prepares the expected link, origins, and local scope bindings; peer assertions cannot choose destination identity or export authorization.
- Reciprocal simultaneous dials converge on one direction through canonical origin ordering, abandoned control requests tear down pending dials, failed dials cannot poison broker origin, and the active link does not retain the bridge capability.

### Notes
- This slice establishes authenticated transport attachment, identity negotiation, and lifecycle only. Remote roster import and message routing remain disabled until later SCO-116 phases; broadcast, mailboxes, extension channels, and compaction awareness remain host-local in federation v1.

## [0.13.0-acl.8] - 2026-09-16

### Added
- Every `intercom` action can optionally publish or clear a 5–9 word self-description and safely fill an unnamed/generated session name through `profile: { name?, description? }` (`description: null` clears stale focus). Descriptions appear in rosters and overlays, remain presentation-only, persist with the Pi session, and never participate in routing, ACLs, mailboxes, or continuity.
- Tool results now distinguish the caller's canonical Pi name, broker-confirmed effective intercom name, description, and broker publication status as lightweight metadata, while static tool and skill guidance encourages early peer discovery for substantial potentially overlapping work.

### Fixed
- Profile-driven naming cannot replace an explicit user-, Pi-, or host-assigned session name; only unnamed/generated identities and previously profile-managed names can be changed through the profile addon.
- Blank placeholder values materialized by tool-schema adapters (for example `targets: [""]` or an empty optional profile) are treated as omitted, while real `to`/`targets` conflicts and mixed invalid target arrays still fail.
- Broker registration and presence reject control/format characters in raw session names, and profile-owned name provenance uses staged recovery plus durable revocation tombstones so journal failures, reconnects, explicit renames, and restarts cannot grant accidental rename authority.

## [0.13.0-acl.7] - 2026-09-16

### Added
- Successful compactions now advance a private durable broker-owned generation through idempotent, acknowledged event reports. The next accepted direct unicast, ask, reply, compose-overlay send, or explicit multicast contact reports when that peer compacted since the previous direct contact; incoming notices ride the message already being delivered and never wake a peer on their own.
- Capability-gated contact tokens keep awareness pending until a compatible client parses and surfaces it, preferring a repeated notice over a lost one across crashes and mixed-version rollout. Receiver first-contact tokens are durably staged and journaled for retry across broker restarts.
- Directional contact watermarks persist under hashed scope/session identities with per-scope bounded, corruption-tolerant primary/backup storage and an explicit backup-stage/primary-commit protocol. First contact establishes a synchronously durable baseline, delivery rejected before acceptance does not advance it, disconnected snapshots never claim current context usage, and broadcast neither reads nor updates the collaboration graph.

## [0.13.0-acl.6] - 2026-09-16

### Added
- `send` accepts up to 32 explicit `targets`, delivering an independent message and outcome to each while deduplicating aliases for the same live session and preserving partial successes.
- `broadcast` sends to every currently visible live session on the machine. Tool guidance deliberately recommends explicit targets instead; broadcast remains bounded by the caller's scope and subagent ACL and never queues disconnected sessions.
- On Pi hosts that report unsuccessful compactions to extensions (Earendil Pi 0.85+), session presence publishes `compacting` from pre-compaction until success, failure, or abort, then restores the underlying thinking/tool/idle state without waking peers. Older hosts leave compaction presence disabled to avoid stale status.

### Fixed
- Multi-target confirmation resolves and displays the exact recipient snapshot before approval, then refuses to remap a departed approved endpoint to a replacement alias.
- Fanout cancellation is rechecked before endpoint-rebound and offline-mailbox retries, and case-sensitive disconnected IDs remain distinct.

## [0.13.0-acl.5] - 2026-09-15

### Changed
- Session-name presence follows Pi's `session_info_changed` extension event immediately where available. Upstream Pi 0.73.1 exposes the core event without forwarding it to extensions, so it uses a one-second compatibility fallback.
- Pi, TUI, and TypeBox host modules are optional peers, so packaging installs neither coding-agent distribution nor duplicate host libraries. The extension supports upstream Pi 0.73.1 and fork 0.80.3+ with a compatible sibling-package set; tests cover a fully pinned 0.80.3 family and the default 0.85.1 resolution.

### Fixed
- A name change that lands while the broker registration acknowledgment is pending is replayed after connection instead of remaining stale.
- Name events received outside a live session no longer poison compatibility-fallback deduplication.

## [0.13.0-acl.4] - Upstream 0.13.0 rebase

### Changed
- Rebased the ACL fork onto upstream 0.13.0, including `/alias`, safer active-reply routing, stable intercom tool visibility, and hardened Windows broker startup.
- Preserved the fork's subagent visibility ACL, explicit self-promotion, delivery feedback, live-name deduplication, and neutral unnamed-session aliases.

## [0.13.0-acl.3] - Neutral unnamed session aliases

### Fixed
- Unnamed ordinary sessions now use neutral `session-<id>` runtime aliases instead of looking like delegated subagents.

## [0.13.0-acl.2] - Delivery feedback

### Fork highlights (on top of 0.13.0-acl.1)
- **Honest send results:** a send that lands in a disconnected session's mailbox now says so explicitly ("Queued for offline session ... delivered only if it reconnects within 24h") instead of the misleading "Message sent". Applies to the send tool, reply tool, and compose overlay.
- **Undelivered-message receipts:** when a queued mailbox message expires (24h retention) or is evicted (mailbox capacity), the broker now pushes an `expired` receipt to the sender, and the sender session surfaces it as a visible, agent-visible delivery-failure notice instead of silently dying. Expiry also runs on a periodic 60s sweep instead of lazily on the next unrelated mailbox queue operation.
- **Name dedup at registration:** registering or renaming onto a name already held by another live session in the same scope auto-suffixes (`name-2`, `name-3`, ...) instead of accepting an ambiguous name that only fails at send time with `E_AMBIGUOUS_TARGET`. Deliberate `advertise` claims keep the stricter reject (`E_NAME_TAKEN`). Suffixes self-heal: when the colliding session leaves, the next presence sync reclaims the original name.

## [0.13.0-acl.1] - ACL fork, rebased onto upstream 0.12.0

### Fork highlights (on top of upstream 0.12.0)
- Subagent visibility ACL: a main sees every other main plus only the subagent children it personally supervises; a subagent sees only its own supervisor. All broker lookup paths (list, send, exact-target, disconnected-mailbox) are scoped through the requester's visibility, and a hidden session behaves exactly like a nonexistent one.
- `advertise`: opt-in self-promotion for a tagged subagent to full main-level visibility both ways, with name/ID collision and control-character guards. Advertised status is stripped from disconnected-session snapshots (live-connection promotion only).
- Upstream 0.12.0 features included: scoped intercom routing (scope-aware session keys and broadcast scoping compose with the ACL), lazy intercom tool visibility, and all earlier upstream fixes.

## [0.13.0] - 2026-09-02

### Highlights
- You can now give the current session a friendly alias from pi-intercom.
- Aliases show up right away in the session list, messages, replies, overlays, and incoming-message displays.
- Windows broker startup is more reliable with non-ASCII profile paths and stricter Windows Script Host setups.

### Added
- Added `/alias <name>` plus the interactive `/alias` and `/alias menu` forms for naming the current session. Thanks to [@yceachan](https://github.com/yceachan) for issue #122.

### Fixed
- Fixed hidden Windows broker startup when the user profile path contains non-ASCII characters or Windows Script Host cannot infer the VBScript engine. Thanks to [@maelo1028](https://github.com/maelo1028) for issue #121 and [@Agustin-Prieto](https://github.com/Agustin-Prieto) for issue #123.

## [0.12.1] - 2026-08-29

### Highlights
- Replies to inbound asks are now harder to send to the wrong local session by mistake.
- The `intercom` tool now stays in the active tool set, which avoids a late-session prompt-cache reset when intercom first becomes useful.
- Existing configs that still mention `toolVisibility` keep loading; the old setting is simply ignored.

### Fixed
- Refuse non-reply `send` calls to a different target during a turn triggered by an inbound ask, preventing CWD hierarchy or roster guesses from misdirecting replies. Thanks to [@yceachan](https://github.com/yceachan) for issue #117.

### Removed
- Removed `toolVisibility` and the `after-first-use` reveal path. The generic `intercom` schema and prompt snippet now stay stable for provider prompt caches, and existing `toolVisibility` config keys are ignored. Thanks to [@XWIlluDelu](https://github.com/XWIlluDelu) for issue #118.

## [0.12.0] - 2026-08-22

### Highlights
- Extensions can now ask pi-intercom to send a message through the current session without losing user consent, sender attribution, or delivery feedback.
- Teams can isolate intercom traffic with `PI_INTERCOM_SCOPE_ID`, so unrelated sessions do not see or receive each other's scoped messages.
- The generic `intercom` tool can stay hidden until it is first useful, keeping quiet sessions less cluttered.

### Added
- Added a consent-aware extension outbox API with `intercom:outbox-request` and `intercom:outbox-result` events for notify-only same-process extension sends. Outbox sends honor `confirmSend`, always return a terminal result for valid request IDs, use scoped target resolution, and leave attributed sender-side transcript traces. Thanks to [@elecnix](https://github.com/elecnix) for #110.
- Added opt-in broker-enforced routing scopes through `PI_INTERCOM_SCOPE_ID`. Scoped sessions only see, route, recover mailbox messages, receive presence events, and use extension-bus owner, publish, and state traffic with sessions in the exact same opaque scope. Unscoped sessions keep existing behavior. Thanks to [@YeungKC](https://github.com/YeungKC) for issue #112.
- Added opt-in `after-first-use` visibility for the generic `intercom` tool, keeping its model schema and prompt out of unused sessions until an inbound message, overlay send, or bundled skill load reveals it. Broker reception and the child-only `contact_supervisor` tool remain available while it is hidden. Thanks to [@XWIlluDelu](https://github.com/XWIlluDelu) for PR #111.

## [0.11.0] - 2026-08-19

### Highlights
- Messages can no longer land on a stale peer: if the target session restarted or was replaced, the send fails clearly or retries against the live session instead of silently reaching the wrong endpoint.
- Retrying a send with the same message ID is now safe. Identical retries never deliver twice, and reusing an ID with different content is rejected.
- Send results now include structured delivery details (state, error code, whether a retry is safe), so failures are actionable instead of guesswork.
- The session list now shows each peer's tmux pane ID, making it easier to find and drive the right terminal.
- Delivered blocking asks leave a local pending-ask record, so you can see what a peer is still waiting on.

### Added
- Endpoint-bound direct delivery: sends target the exact live session and safely retry once when the target reconnects mid-send, with bounded replay protection for repeated message IDs. Thanks to [@xiangbianpangde](https://github.com/xiangbianpangde) for #106.
- Local pending-ask records for delivered blocking asks. Thanks to [@bcanvural](https://github.com/bcanvural) for issue #104.
- tmux pane IDs in the session roster. Thanks to [@odfalik](https://github.com/odfalik) for issue #102 and PR #101.

### Changed
- Reusing a message ID with different content now fails with a clear error instead of relying on receiver-side duplicate suppression.
- Clarified that agents should re-list stale intercom session IDs and skip self-targets.

## [0.10.1] - 2026-08-12

### Fixed
- Resolve the default broker `tsx` launcher from flat plugin-store installs when package resolution fails, and include broker stderr when startup exits early. Thanks to Eduardo Marquez (`DocksDocks`) for issue #97.
- Preserve attachments when replying through `intercom({ action: "reply" })`. Thanks to Ruoshan Huang (`ruoshan`) for issue #99.

## [0.10.0] - 2026-08-09

### Added
- Added cwd-scoped `send` and `ask` targeting plus `openProjectPaneIfMissing` for visible cross-codebase peer conversations through Herdr project panes.

### Changed
- Cleaned intercom tool copy, visible-peer skill guidance, and broker protocol validation structure.

### Fixed
- Surface malformed intercom config errors with path context instead of silently falling back to defaults.
- Fail blocking `ask` and supervisor-decision requests immediately when the target is not connected instead of accepting a mailbox delivery that can wait until timeout.
- Prevent disconnected mailbox routing from delivering a message back to its sender or transferring mail through runtime-only unnamed-session aliases. Thanks to ELA718 for PR #93.
- Extend unnamed-session fallback aliases with enough session-ID characters to distinguish UUIDv7 sessions started close together.

## [0.9.3] - 2026-08-08

### Fixed
- Allow replies to target pending asks by a unique sender session-ID prefix. Thanks to Benjamin Jesuiter (`bjesuiter`) for PR #85.
- Detect half-open broker sockets and reconnect clients. Thanks to Nicolas Marchildon (`elecnix`) for issue #89 and PR #88.
- Hand busy interactive inbound messages directly to Pi's safe steering queue instead of waiting for aggregate idle, preventing stale coordination from appearing hours after it was received. Thanks to Xiangzhe (`xz-dev`) for issue #86 and PR #87.
- Treat a public send to the sole pending asker as its reply. Thanks to Grant Hutchins (`nertzy`) for PR #90.
- Display session-ID prefixes that distinguish listed sessions. Thanks to Chris Goddard (`chrisgoddard`) for issue #83.

## [0.9.2] - 2026-08-03

### Fixed
- Avoid relaunching standalone Pi executables as the Node runtime when starting the default broker process. Thanks to ZacharyQin for PR #82 and to jeffutter and awaae001 for confirming the impact.

## [0.9.1] - 2026-07-30

### Fixed
- Scoped name-based queued-mail redelivery to sessions that also match the target's directory. A disconnected session's queued messages, including replies addressed to its exact session ID, could previously be delivered to an unrelated same-named session in a different project folder. Directories compare through the same normalization used by `list-cwd`, so a relaunch reporting the same directory via a trailing slash or symlink still receives its mail.

### Changed
- Rewrote the broker frame reader as a bounded state machine and made frame writes a single allocation, removing quadratic `Buffer.concat` accumulation on fragmented socket reads (up to ~28x faster on heavily fragmented frames).
- Cached the collapsed preview and width-keyed wrapped body lines in the inline message renderer, cutting repeated rerender cost of long messages by ~2-3x while keeping live theme changes applied per render.

## [0.9.0] - 2026-07-29

### Added
- Added a bounded in-memory broker mailbox so replies to recently disconnected named CLI senders are queued and delivered when a process reconnects with the same name. Thanks to Luke (`valkyriweb`) for issue #63.
- Added protocol-visible delivery metadata, receiver lifecycle receipts, receiver-side inbound message dedupe, explicit cancel/supersede controls, and clearer ask-timeout receipts for ordered delivery diagnostics. Thanks to Donnie Thomas (`donnielrt`) for issue #65.

## [0.8.0] - 2026-07-29

### Added
- Added opt-in restart-stable intercom session IDs via `PI_INTERCOM_STABLE_ID` or `stableId` in `config.json`. Thanks to iRonin for issue #39.
- Added `/intercom-id` to insert a stable handoff snippet for the current session into the editor. Thanks to dataforxyz for PR #60.
- Added `intercom({ action: "list-cwd" })` to list peers scoped to the same working directory. Thanks to iRonin for PR #58.
- Added live context-window usage to session presence and list output. Thanks to iRonin for PR #59.
- Added a silent namespaced extension bus for non-conversational extension coordination. Thanks to Kieran Bond for PR #69.

## [0.7.0] - 2026-07-29

### Changed
- Documented `PI_INTERCOM_ASK_TIMEOUT_MS` for configurable ask/supervisor timeouts. Thanks to wiansapu for issue #14.
- Clarified session addressing copy so the short IDs shown by `list` are documented as usable prefixes. Thanks to Grant Hutchins for PR #66.
- Updated Pi runtime peer metadata and tool schemas for the `@earendil-works` package scope and Pi-bundled `typebox`/`pi-ai` packages.
- Centralized pi-intercom runtime and config paths under `PI_CODING_AGENT_DIR` when set, defaulting to `~/.pi/agent`.
- Hardened default broker auto-spawn to launch the resolved bundled `tsx` CLI through the current Node executable instead of resolving `npx` through `PATH`; custom `brokerCommand`/`brokerArgs` remain available as advanced trusted config.
- Added an `inboundTrigger` policy (`always`, `replies`, or `never`) so users can reduce inbound auto-trigger risk while preserving existing behavior by default.
- Made inline intercom messages collapse and expand with Pi's `Ctrl+O` custom-message toggle while keeping sender, preview, reply, and attachment cues visible. Thanks to RyanKim17920 for PR #32.
- Improved inline message theme hierarchy with separate semantic styling for borders, headers, body text, and metadata. Thanks to Sreenath for PR #68.

### Fixed
- Added broker-owned local trust metadata, clearer stable-ID trust boundaries for duplicate names, per-connection rate limiting, and no-op presence coalescing for local IPC abuse hardening.
- Added an inbound broker frame size cap to reject oversized local IPC messages before buffering their payloads.
- Restricted Unix intercom runtime directory, socket, PID, and spawn-lock permissions.
- Rechecked single-flight ask state after session target resolution so concurrent regular asks fail safely instead of crashing on an unhandled rejected reply waiter.
- Refused broker-level mutual asks that would deadlock two sessions, and cleared outstanding ask edges when asks are replied to, cancelled, or disconnected.
- Stabilized intercom session addressing across reconnects, idle `/name` changes, replaced Pi sessions, supervisor routing, pending replies, and short-ID targeting.
- Aligned intercom overlay widths with their rendered modal boxes. Thanks to Cat for PR #43.
- Marked failed `intercom` and `contact_supervisor` tool results through Pi's `tool_result` error flag path while preserving structured renderer details.
- Limited the intercom overlay to TUI mode and unsubscribed subagent relay event handlers during session shutdown.
- Added an opt-in Windows localhost TCP transport using a dynamic port, broker protocol health checks, and a local endpoint secret instead of a fixed-port default.
- Stabilized reply/supervisor routing by respecting explicit reply targets, suppressing legacy supervisor tools when native supervisor channels are present, and clearing replied idle-queued asks. Thanks to ThanhNT29Jacky for PR #64.

## [0.6.0] - 2026-05-03

### Added
- Added `brokerCommand` and `brokerArgs` config options for choosing the broker runtime command. Thanks to William Fligor for PR #12.

## [0.5.0] - 2026-05-03

### Changed
- Busy interactive sessions now queue inbound intercom messages until the receiver is idle instead of aborting the active turn.
- Sessions now publish automatic lifecycle status (`idle`, `thinking`, or `tool:<name>`) through intercom presence updates.
- Deferred startup connection, delayed inbound flushes, overlay work, reconnect attempts, and relay callbacks now guard against stale session contexts after shutdown or reload.
- `intercom` and `contact_supervisor` tool calls/results now use compact custom transcript renderers.

## [0.4.1] - 2026-05-02

### Added
- Added `contact_supervisor` `reason: "interview_request"` for child subagents to send structured supervisor interviews, wait for a reply, and receive parsed JSON replies in tool result details when available.

### Fixed
- Busy non-interactive sessions now auto-reply to top-level intercom messages instead of aborting and losing the message.

## [0.4.0] - 2026-05-02

### Added
- Added a `contact_supervisor` tool for `pi-subagents` child sessions so delegated agents can request supervisor decisions or send meaningful progress updates with run metadata.
- Documented subagent-to-supervisor escalation in the README and bundled `pi-intercom` skill.

### Fixed
- Made inline intercom message cards use the available terminal width instead of a narrow fixed width.
- Cleared supervisor ask waiters correctly after cancellation or delivery failure so subagents can ask again.

### Changed
- Stopped tracking `package-lock.json` and ignored local `progress.md` memory files.

## [0.3.0] - 2026-04-27

### Added
- Added `pi-subagents` grouped result relay support over `pi-intercom`, including delivery acknowledgments so parent runs can return compact receipts only after the orchestrator receives the result message.

## [0.2.1] - 2026-04-26

### Fixed
- Delivered `pi-subagents` needs-attention control events to the orchestrator over intercom.

## [0.2.0] - 2026-04-22

### Added
- Added receiver-side `reply` ergonomics for inbound asks. Agents can now use `intercom({ action: "reply", message })` in the triggered turn or later against a single pending ask, plus `intercom({ action: "pending" })` to inspect unresolved inbound asks.

### Fixed
- Migrated extension tool schemas from `@sinclair/typebox` to `typebox` 1.x so packaged installs follow Pi's current extension runtime contract.
- Included `reply-tracker.ts` in the published package so installed extensions can load the new reply-tracking helper at runtime.
- Updated the integration test harness to set `USERPROFILE` alongside `HOME`, keeping temp-home isolation reliable on Windows.

### Changed
- Moved TypeBox from `peerDependencies` to a real `dependencies` entry so `pi install` production installs keep the schema package available at runtime.
- Incoming ask reply hints now prefer `intercom({ action: "reply", ... })` instead of exposing raw `to` and `replyTo` identifiers.
- Updated the bundled `pi-intercom` skill and README examples to prefer `reply`/`pending` over manual reply threading.

## [0.1.11] - 2026-04-20

### Added
- Bundled `pi-intercom` skill with coordination patterns, error handling, constraints, and optional cmux/tmux peer-session spawning for visible multi-session workflows.
- `pi.skills` manifest in `package.json` so `pi install` loads the skill automatically.
- AGENTS.md snippet in README recommending a project-level coordination hint for agents.
- Attachments example to Quick Start section in README.

### Changed
- Incoming message reply hints now say "To reply, use the intercom tool:" instead of "— reply:" so agents are more likely to use the intercom tool instead of replying inline.
- `ask` action now documents the one-at-a-time constraint in the Tool Reference.
- `status` action now clarifies that the session count includes the current session.
- Broker startup no longer uses a non-null assertion for sender session lookup in the `send` handler — missing sessions now produce a `delivery_failed` response instead of a crash.
- Broker spawn lock error handling tightened to check `instanceof Error` before accessing `.code`.
- Broker PID parsing now guards against `NaN` from corrupt PID files.
- `isConnected()` readability cleanup in `IntercomClient`.
- README file structure updated to include `broker/paths.ts`, test files, and `skills/` directory.
- README runtime files section now clarifies that `broker.sock` is macOS/Linux only; Windows uses a named pipe.
- README mermaid diagram changed "Unix Socket" to "Local Socket/Pipe" for cross-platform accuracy.
- README broker limitation rephrased from "must be running" to "auto-spawns on first use and exits when idle."
- README Install section now mentions that the bundled skill is registered on startup.

## [0.1.10] - 2026-04-17

### Fixed
- Broker startup now works on Windows by launching the local `tsx` CLI through a hidden `wscript.exe` helper without treating the helper's expected early exit as a broker failure.

### Changed
- The broker now uses a Windows named pipe instead of a Unix socket on Windows, while keeping the existing Unix socket transport on macOS and Linux.

## [0.1.9] - 2026-04-17

### Fixed
- Declared the extension entry in `package.json` via `pi.extensions` so `pi install npm:pi-intercom` can discover and load the extension from the npm package.

### Changed
- Added `pi-package` package metadata plus peer dependency declarations for every Pi runtime package the extension imports, including `@mariozechner/pi-tui`.

## [0.1.8] - 2026-04-14

### Changed
- Intercom sessions now reconnect automatically after broker disconnects or sleep/wake interruptions instead of staying offline until reload or restart.
- Replaced raw runtime `console.error` intercom disconnect logging with silent recovery so transient broker churn no longer splashes stray text into the Pi TUI.

## [0.1.7] - 2026-04-13

### Changed
- Unnamed sessions now register a runtime-only `subagent-chat-<id>` intercom alias instead of persisting a generic session title into Pi session history, so `pi --resume` can keep showing transcript snippets while unnamed sessions remain reachable over intercom.
- Intercom presence updates now refresh the advertised session name during later turn/intercom activity, so renaming a session does not leave subagents and peers targeting a stale startup alias.

## [0.1.6] - 2026-04-13

### Changed
- Busy incoming intercom messages now try a graceful detach handshake with `pi-subagents` before falling back to interrupting the active turn.
- Reply follow-ups are deferred and re-delivered as follow-up wakeups so final confirmation messages stop causing unnecessary `Operation aborted` interruptions.
- Unnamed sessions now auto-register a stable `session-<id>` display name so orchestrators and delegated children can target each other reliably without a manual `/name`.

## [0.1.5] - 2026-04-13

### Changed
- Switched intercom send confirmation to opt-in. `send` now delivers immediately by default, and interactive confirmation only appears when `confirmSend: true` is set in `~/.pi/agent/intercom/config.json`.
- Replaced the old inverted `autoSend` config with `confirmSend` to make the behavior easier to understand.

## [0.1.4] - 2026-04-13

### Added
- Added an MIT `LICENSE` file and set `package.json` `license` to `MIT`.

### Changed
- Updated `README.md` to mention the `pi-subagents` integration and link to https://github.com/nicobailon/pi-subagents.

## [0.1.3] - 2026-04-10

### Changed
- **Clearer self vs peer identity** — `intercom({ action: "list" })` now shows `Current session` and `Other sessions`, includes short session IDs, and marks same-folder peers with `[same cwd]` so agents are less likely to mistake another session in the same repo for themselves.
- **Picker self anchor** — The session picker now shows the current session as a disabled `[self]` row at the top while keeping only peer sessions selectable.

## [0.1.2] - 2026-04-04

### Changed
- **Reply flows skip approval** — `send` calls that include `replyTo` now bypass the confirmation dialog so reply-hint conversations can continue without an extra approval step.
- **Overlay readability** — The session picker now shows session name/model on the first line and the cwd on a second line with middle truncation, making long paths much easier to distinguish.
- **Documentation clarity** — The README now explains which sessions appear in the picker, how sessions become intercom-connected, and the difference between user-facing `/intercom` usage and agent tool calls.

### Fixed
- **Compose overlay crash** — Replaced the invalid `tui.scheduleRender()` calls with `tui.requestRender()`, fixing the compose overlay crash while typing or sending.
- **Overlay panel chrome** — Restored bordered modal rendering for the session picker and compose overlay so they display as proper overlays instead of floating unboxed content.

## [0.1.1] - 2026-04-04

### Changed
- Added a `promptSnippet` for the `intercom` tool so Pi 0.59+ includes it in the default tool prompt section and improves session-to-session coordination discoverability.

### Changed
- **Pi compatibility refresh** — Updated the extension to match current Pi lifecycle and custom UI APIs, including `session_start` / `session_shutdown` and injected `ctx.ui.custom()` keybindings.
- **Overlay keybindings** — The session picker and compose overlay now use injected, namespaced Pi keybindings instead of reading editor-global bindings directly.
- **Session list correlation** — `list` / `sessions` now carry a `requestId`, so a delayed broker reply cannot be mistaken for a newer session-list request.
- **Reply sends skip approval** — `send` calls that include `replyTo` now bypass the confirmation dialog so reply-hint flows work without an extra approval step.
- **Documentation accuracy** — The README now matches the current implementation, including request correlation, persistence behavior, broker disconnect behavior, and the file layout.

### Fixed
- **Protocol state handling** — Broker and client now reject malformed, unknown, duplicate, and out-of-order protocol messages instead of silently accepting them.
- **Duplicate-name routing** — Sends to a duplicated session name now fail with an explicit error instead of routing to the first match.
- **Delivery failure visibility** — `delivery_failed.reason` now flows through the client, tool results, and compose overlay error UI.
- **Disconnect and startup errors** — Broker spawn failures, early broker exits, protocol failures, and disconnects now preserve the real error instead of collapsing to generic messages.
- **Disconnect-time writes** — Client operations now fail cleanly during disconnect instead of writing to a closing socket and triggering `write after end` errors.
- **Late-response handling** — Timed-out send/list requests no longer disconnect the client, and delayed list responses can no longer contaminate a later request with stale data.
- **Config validation** — Invalid intercom config values are now reported and ignored instead of silently producing a broken runtime config.

## [0.1.0] - 2026-03-12

### Added
- **`ask` action** — `intercom({ action: "ask", to, message })` now sends a message and blocks until the recipient replies, returning the reply as the tool result. Includes a 10-minute timeout, abort handling, disconnect handling, and shutdown cleanup.
- **Exact reply hints** — Incoming messages can now include a ready-to-run reply command that uses the sender's exact session ID as `to` and the original message ID as `replyTo`, making synchronous `ask`/reply flows reliable.
- **Attachment body rendering for incoming messages** — Incoming attachment contents are now appended to the agent-visible message body so recipients can read attached file/snippet/context content directly.
- **Planner/worker workflow documentation** — README now documents the intended planner-worker loop, including `send` vs `ask`, clarification patterns, and reply-hint behavior.

### Changed
- **Session target resolution** — `send` and `ask` now resolve a unique case-insensitive session name to its exact session ID before sending. Ambiguous names are rejected instead of guessed.
- **Duplicate-name presentation** — Session labels are now disambiguated consistently across `list`, the session picker, the compose overlay, and send notifications by appending a short session ID when names collide.
- **Send confirmation dialog** — Confirmation text now includes attachment content previews and `replyTo` metadata so outgoing messages are reviewed accurately before sending.
- **Inline message rendering** — The custom inline renderer now shows the fully rendered message body, optional reply command, attachment summaries, and reply metadata consistently with what the agent receives.

### Fixed
- **False `ask` completions from unrelated messages** — Reply matching now requires an exact `replyTo` match and the expected sender, preventing unrelated incoming messages from unblocking a waiting `ask`.
- **Self-targeted messages** — `send` and `ask` now reject attempts to message the current session instead of allowing loops or self-waits.
- **Undelivered `ask` cleanup** — If an `ask` message is not delivered, the waiting state is torn down cleanly instead of lingering.
- **Inline renderer/body mismatch** — The custom message renderer now matches the actual delivered message body for messages with attachments instead of showing a reduced view.
- **Duplicate-name ambiguity when self shares a name** — Duplicate-name detection now considers all connected sessions, so another session is still disambiguated when it shares a name with the current session.
- **`broker/client.ts` `sessions` switch scoping** — Braced the `sessions` case to avoid block-scoping hazards in the message handler.
