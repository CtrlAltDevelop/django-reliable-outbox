# Architecture decision records

Short records of the decisions that shape the guarantees, and what each one costs.

| ADR | Decision |
|---|---|
| [0001](0001-skip-locked-over-advisory-locks.md) | Claim work with `FOR UPDATE SKIP LOCKED`, not advisory locks |
| [0002](0002-lease-model.md) | Leases with a fencing token instead of long-held locks |
| [0003](0003-per-key-ordering.md) | Per-key ordering by claiming only the head of each key |
| [0004](0004-at-least-once-and-inbox.md) | At-least-once delivery, and an inbox for effectively-once effects |
| [0005](0005-notify-trigger-with-polling-fallback.md) | Wake workers with a NOTIFY trigger; keep polling underneath |
