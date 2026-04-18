# datastar-sw-sync

Cloudflare Worker + Durable Object that provides multi-client sync for the
kanban app. See `../docs/sync-architecture.md` for the design rationale and
wire protocol.

## Layout

- `wrangler.toml` — Worker config, DO binding, migration.
- `src/index.js` — entry. Validates upgrade, routes to per-board DO.
- `src/board-room.js` — `BoardRoom` DO. Hibernatable WS, append-only log,
  broadcast.

## Develop

```bash
pnpm install
pnpm dev
```

`wrangler dev` runs the Worker locally at `http://localhost:8787` with a real
DO. WebSocket endpoint: `ws://localhost:8787/boards/<boardId>/ws`.

## Deploy

```bash
pnpm wrangler login
pnpm deploy
```

Wrangler will print the `*.workers.dev` hostname. That's what you pass as
`workerUrl` (with `wss://` prefix) when calling `startSyncClient()` on the
client side.

## Minimal smoke test

```bash
# Terminal 1
pnpm dev

# Terminal 2 — open two wscat connections to the same board, watch them share events
npx wscat -c ws://localhost:8787/boards/test/ws
# > {"type":"hello","actorId":"a1","lastKnownSeq":0}
# > {"type":"events","events":[{"id":"<uuid>","type":"card.created","data":{...}}]}
```

## Known limitations

Prototype only. In particular: no auth, no rate limiting, no per-board push
isolation on the client side, no log compaction. See the "Known gotchas"
section of `../docs/sync-architecture.md`.
