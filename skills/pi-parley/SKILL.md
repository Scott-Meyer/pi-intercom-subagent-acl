---
name: pi-parley
description: |
  Conversation with other Pi sessions: exchange findings, ask questions,
  and discuss work across projects.
---

# Pi Parley

Parley connects Pi sessions so they can work together like colleagues. Each has its own conversation and working context; messages carry the information they share, not everything the sender knows.

`send` shares information. `ask` requests an answer and can wait for it or receive it later while work continues. `reply` communicates back to the colleague and answers a question when applicable. An update can remain an update even while a question is outstanding.

Messages and receipts carry identities, exact message IDs, related questions, and delivery state. Acceptance by a session, an answer from a colleague, and completion of work are different events. Missing acknowledgements and withdrawn requests appear as context where they matter.

Discovery reflects this session's visibility, not every Pi process. Some remote peers support only text messages. Optional project launches report the request, the session observed, and message delivery separately.

`pending`, `status`, and `read` expose more conversation context when needed. Retained text can outlive broker routing. Attachments are inline snapshots. A compaction notice means older conversational details may now be summarized; files and ongoing work are unchanged.
