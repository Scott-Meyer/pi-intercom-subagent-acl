# Changelog

## 1.2.0

### Added

- Targeted messaging and explicit asks/replies between Pi sessions, with independent outcomes for group sends.
- Delivery receipts that distinguish acceptance, offline queueing, known failure, and uncertain outcomes.
- Conversation journals for pending requests, received messages, controls, and interrupted delivery.
- Canonical session naming, concise focus labels, and live presence.
- Scoped child-session visibility and an optional supervisor channel.
- Durable compaction awareness for direct collaborators.
- Extension channels with capability discovery, ownership, and revisioned state.
- Experimental federation for remote discovery and direct text messaging.
- A session picker and message composer through `/parley` or Alt+M.
- OS-managed broker lifetime ownership, with safe concurrent startup and automatic crash recovery.
- Correlated cancellation acknowledgements and required current conversation capabilities.
- Broker health identity with a startup package/source snapshot.
- Unknown SDK context usage clears old presence estimates; notices identify the last reported sample.
- Retained model-context snapshots are historical, preserve arrival timing/disposition, and reconcile late host persistence without reviving answered requests.
- Optional authenticated loopback TCP on all supported platforms.
- Packed fresh-install checks and cross-platform ownership validation before automated publishing.
