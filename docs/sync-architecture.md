# Sync prototype: CF Worker + Durable Object

Goal: let multiple clients of the same kanban board converge in real time, without
replacing the existing event-sourced core. Ship a thin layer, not a rewrite.

## Why this shape

The existing app has four properties that make it 80% ready for multi-client sync:

1. **UUID-identified events** (`lib/events.js` → `createEvent`) so anything can be
   deduplicated by id on the receiving end.
2. **`appendEvents()` is already idempotent** — it checks the `byId` index and
   skips existing events. Self-echoes and reconnect replays cost nothing.
3. **A `synced: false` flag + `bySynced` index** already in the IDB schema
   (`lib/db/idb-adapter.js`). Intent declared, implementation missing.
4. **Fractional-indexed positions** (`lib/position.js`) so concurrent card/column
   moves produce commuting ops — no reindexing, no merge needed.

Given these, the question "what sync layer fits" has a specific answer: anything
that provides **a single writer per board to stamp total order**, then broadcasts.
The rest of the machinery is already in place.

## Architecture

```
Browser A                              Cloudflare                           Browser B
┌───────────────────────┐              ┌──────────────────────┐              ┌───────────────────────┐
│  Service Worker       │              │  Worker (index.js)   │              │  Service Worker       │
│                       │              │  - route /boards/:id │              │                       │
│  Hono app             │              │  - DO stub forward   │              │  Hono app             │
│    ↕ events           │              └──────────┬───────────┘              │    ↕ events           │
│  appendEvents()       │                         │                          │  appendEvents()       │
│    ↕                  │                         ▼                          │    ↕                  │
│  IDB  ───── unsynced ─┼─── wss ── →  ┌──────────────────────┐  ── wss ────┼→ IDB                   │
│                       │              │  BoardRoom DO        │              │                       │
│  sync-client.js  ←────┼──── wss ────── append log (storage)  ────── wss ───┼──→ sync-client.js     │
│                       │              │  broadcast to sockets│              │                       │
└───────────────────────┘              └──────────────────────┘              └───────────────────────┘
```

One DO per board (via `env.BOARD_ROOMS.idFromName(boardId)`). Hibernatable
WebSockets mean idle rooms cost ~nothing; the DO wakes on the next packet.

## Wire protocol

All messages are JSON over a single WebSocket.

| Direction | Type    | Fields                        | Purpose                          |
| --------- | ------- | ----------------------------- | -------------------------------- |
| C → S     | `hello` | `actorId`, `boardId`, `lastKnownSeq` | Resume cursor                    |
| S → C     | `events`| `events[]` (each with `serverSeq`)   | Replay or live broadcast         |
| C → S     | `events`| `events[]` (no `serverSeq`)          | Push unsynced from client        |
| S → C     | `error` | `error`                              | Malformed input, debugging       |

The DO assigns `serverSeq` on append. That single fact gives the system a total
order per board. Clients persist `serverSeq` alongside each event and use it as
their resume cursor.

## Conflict handling

Not needed for most of the op set:

- `card.created`, `column.created`, `board.created`: fresh UUIDs, always commute.
- `card.moved`, `column.moved`: fractional indexing makes concurrent inserts
  between the same neighbours produce distinct keys. No manual tie-break.
- `card.deleted`, `column.deleted`: `applyEvent` already tolerates missing
  entities (see the `if (!card) break` guards).

Genuine ties only happen on field-update events targeting the same entity
(e.g. two clients retitling the same card). For those, the DO-assigned
`serverSeq` is the tie-breaker — the later `serverSeq` wins. Good enough for a
kanban; not good enough for text collaboration, but that's not on the roadmap.

## What changes in the existing code

Nothing, structurally. The sync module reads/writes the same IDB and pipes
through `appendEvents()`. Two small additions worth calling out:

1. Each event grows an optional `serverSeq: number` field after it's round-
   tripped through the DO. IDB tolerates schema drift, no migration needed.
2. `synced` flips from `false` to `true` once the DO has acked (by broadcasting
   the event back with a `serverSeq`).

The service worker doesn't auto-start the sync client. To enable, call
`startSyncClient({ boardId, workerUrl })` from whichever lifecycle hook makes
sense (e.g. the board-page SSE stream opening, or a global startup in the SW
`activate` handler).

## Known gotchas (flagged but not solved)

- **SW lifetime**: browsers kill idle SWs after ~30s. The sync WS dies with it.
  On next `fetch` event the SW reboots and `startSyncClient` needs to be re-run.
  Live pages keep the SW warm via their SSE stream; offline-accumulated events
  don't drain until the next page load.

- **Per-board event isolation**: the client pushes *all* unsynced events on
  every connect, regardless of which board they belong to. For a single-board
  prototype that's fine; for production, partition the push by board.

- **Auth**: none. Any client can connect to any board ID and read/write the
  log. Add an auth token in the query string or upgrade request header, verify
  in the Worker before forwarding to the DO.

- **Storage growth**: the DO log is append-only. No snapshot/compaction logic.
  For production, snapshot projection state to R2 on a schedule and truncate
  the log below the snapshot seq.

- **Presence / cursors**: not modelled. Would fit naturally as non-persisted
  ephemeral messages routed through the same WS (distinct message `type`).

- **Boolean index edge case**: `events.bySynced` is a boolean-keyed IDB index.
  Older browsers may not index `false` values uniformly. The prototype uses a
  `getAll + filter` scan instead of an index query to sidestep this.

## Deploy

```bash
cd worker
pnpm install
pnpm wrangler login
pnpm deploy
```

Output will print a `*.workers.dev` URL. Pass that URL as `workerUrl` when
calling `startSyncClient()` from the SW.

## Comparison with plat-trunk's Automerge-over-PartyKit approach

For plat-trunk's CAD document itself, Automerge-over-DO is strictly stronger:
concurrent structural edits on nested assemblies genuinely need merge semantics
that LWW can't provide. An event log with serverSeq can't correctly merge
"A adds a fillet to face F" + "B mirrors the part containing F" — Automerge can.

For surrounding metadata (comments, project membership, audit trail, presence)
the event-log-over-DO pattern is lighter and keeps an auditable trail that
Automerge compactions would eventually lose. The split to keep in mind:

- **Automerge**: the document (the thing being collaboratively edited)
- **Event log over DO**: everything around the document

This prototype is a minimal worked example of the second half.
