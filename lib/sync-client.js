// Client-side sync for the BoardRoom Durable Object.
//
// Drops in on top of the existing event-sourced architecture without modifying
// its core. Two responsibilities:
//
//   1. Push: drain events where synced=false up to the DO.
//   2. Pull: receive events broadcast by the DO and play them through the
//      existing appendEvents() — which is already idempotent-by-id, so self-
//      echoes and reconnect-replays are free.
//
// Runs inside the service worker (where this app's server lives) OR in a
// page context. The only requirement is access to the same IDB via getDb()
// and the exported appendEvents/bus hooks from lib/events.js + lib/db.js.
//
// Not auto-started. Call startSyncClient({ boardId, workerUrl }) to enable.

import { getDb, bus, actorId } from './db.js'
import { appendEvents } from './events.js'

const INITIAL_BACKOFF_MS = 500
const MAX_BACKOFF_MS = 15000

export function startSyncClient({ boardId, workerUrl, log = () => {} }) {
  let ws = null
  let backoff = INITIAL_BACKOFF_MS
  let running = true
  let reconnectTimer = null

  // --- Local queries against the projection DB ----------------------------

  async function getLastServerSeq() {
    // Scan events for the highest serverSeq we've seen. Cheap enough for the
    // prototype; for large logs, persist a per-board meta record instead.
    const db = await getDb()
    const all = await db.getAll('events')
    let max = 0
    for (const e of all) {
      if (typeof e.serverSeq === 'number' && e.serverSeq > max) max = e.serverSeq
    }
    return max
  }

  async function getUnsyncedEvents() {
    const db = await getDb()
    const all = await db.getAll('events')
    // Only send events that belong to this board (or are board-level for this board).
    // The existing model doesn't tag every event with boardId directly — we rely on
    // correlationId/causationId resolution on the server side if we need to enforce
    // per-room isolation. For the prototype, we send everything unsynced.
    return all.filter((e) => e.synced === false)
  }

  async function markSynced(incoming) {
    // Update the local event row: flip synced=true and stamp serverSeq.
    const db = await getDb()
    const tx = db.transaction('events', 'readwrite')
    const store = tx.objectStore('events')
    const existing = await store.index('byId').get(incoming.id)
    if (existing) {
      existing.synced = true
      if (typeof incoming.serverSeq === 'number') {
        existing.serverSeq = incoming.serverSeq
      }
      await store.put(existing)
    }
    await tx.done
  }

  // --- Transport ----------------------------------------------------------

  async function handleIncoming(events) {
    // Tag as synced before applying so any not-yet-locally-known events land
    // in IDB with synced=true. appendEvents dedupes by id, so events we already
    // have are skipped; we still run markSynced to upgrade the flag on those.
    const toApply = events.map((e) => ({ ...e, synced: true }))
    await appendEvents(toApply)
    for (const e of events) {
      await markSynced(e)
    }
  }

  async function pushUnsynced() {
    if (!ws || ws.readyState !== WebSocket.OPEN) return
    const unsynced = await getUnsyncedEvents()
    if (!unsynced.length) return
    ws.send(JSON.stringify({ type: 'events', events: unsynced }))
  }

  async function connect() {
    if (!running) return
    const lastKnownSeq = await getLastServerSeq()

    try {
      ws = new WebSocket(`${workerUrl}/boards/${boardId}/ws`)
    } catch (err) {
      log('ws-construct-failed', err)
      scheduleReconnect()
      return
    }

    ws.addEventListener('open', async () => {
      backoff = INITIAL_BACKOFF_MS
      log('ws-open', { boardId, lastKnownSeq })
      ws.send(JSON.stringify({
        type: 'hello',
        actorId: actorId.value,
        boardId,
        lastKnownSeq,
      }))
      await pushUnsynced()
    })

    ws.addEventListener('message', async (ev) => {
      let msg
      try { msg = JSON.parse(ev.data) } catch { return }
      if (msg.type === 'events' && Array.isArray(msg.events)) {
        await handleIncoming(msg.events)
      } else if (msg.type === 'error') {
        log('ws-error-msg', msg.error)
      }
    })

    ws.addEventListener('close', () => {
      log('ws-close')
      ws = null
      scheduleReconnect()
    })

    ws.addEventListener('error', () => {
      log('ws-error')
      try { ws?.close() } catch { /* ignore */ }
    })
  }

  function scheduleReconnect() {
    if (!running) return
    if (reconnectTimer) clearTimeout(reconnectTimer)
    reconnectTimer = setTimeout(connect, backoff)
    backoff = Math.min(backoff * 2, MAX_BACKOFF_MS)
  }

  // When any new events land in the local log (including those created by the
  // user in this tab), push immediately if we're connected. The existing
  // appendEvents dispatches 'events:changed' on the bus after commit.
  const onLocalChange = () => { pushUnsynced().catch(() => {}) }
  bus.addEventListener('events:changed', onLocalChange)

  connect()

  return {
    stop() {
      running = false
      bus.removeEventListener('events:changed', onLocalChange)
      if (reconnectTimer) clearTimeout(reconnectTimer)
      try { ws?.close() } catch { /* ignore */ }
    },
    async flush() { await pushUnsynced() },
    get readyState() { return ws?.readyState ?? WebSocket.CLOSED },
  }
}
