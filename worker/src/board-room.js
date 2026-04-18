// BoardRoom: one Durable Object per board. Holds the authoritative event log
// and broadcasts to all connected clients.
//
// Protocol (JSON, all over a single WebSocket):
//
//   → HELLO  { type: 'hello',  actorId, lastKnownSeq }
//   ← REPLAY { type: 'events', events: [{...event, serverSeq}] }   // catch-up
//   → PUSH   { type: 'events', events: [{...event}] }              // unsynced from client
//   ← ECHO   { type: 'events', events: [{...event, serverSeq}] }   // broadcast to all (incl. sender)
//
// Storage layout in DO state:
//   meta:lastSeq              → integer, latest assigned server sequence
//   event-by-id:<uuid>        → integer, the server seq assigned to that event id (dedup)
//   events:<zpad-seq>         → full event record, lexicographically ordered by seq
//
// Idempotency: events are deduplicated by their UUID `id`. Re-sending the same
// event (e.g. after a flaky reconnect) is a no-op.
//
// Conflict resolution: the DO imposes a total order by assigning `serverSeq`.
// For the kanban's op set (fractional-indexed moves, per-id title updates,
// delete-before-update tolerated by applyEvent) LWW on serverSeq is adequate.

import { DurableObject } from 'cloudflare:workers'

const SEQ_PAD = 16 // zero-pad for lex-sortable key ordering

function pad(n) {
  return String(n).padStart(SEQ_PAD, '0')
}

export class BoardRoom extends DurableObject {
  async fetch(request) {
    const pair = new WebSocketPair()
    const [client, server] = Object.values(pair)
    this.ctx.acceptWebSocket(server)
    return new Response(null, { status: 101, webSocket: client })
  }

  async webSocketMessage(ws, message) {
    let msg
    try {
      msg = JSON.parse(typeof message === 'string' ? message : new TextDecoder().decode(message))
    } catch {
      ws.send(JSON.stringify({ type: 'error', error: 'invalid-json' }))
      return
    }

    if (msg.type === 'hello') {
      const lastKnown = Number.isFinite(msg.lastKnownSeq) ? msg.lastKnownSeq : 0
      const catchUp = await this.replayAfter(lastKnown)
      if (catchUp.length) {
        ws.send(JSON.stringify({ type: 'events', events: catchUp }))
      }
      return
    }

    if (msg.type === 'events' && Array.isArray(msg.events) && msg.events.length) {
      const stored = await this.appendEvents(msg.events)
      if (stored.length) this.broadcast(stored)
      return
    }

    ws.send(JSON.stringify({ type: 'error', error: 'unknown-message-type' }))
  }

  async webSocketClose(ws /*, code, reason, wasClean */) {
    // Hibernation handles the rest; nothing to clean up here.
    try { ws.close() } catch { /* already closed */ }
  }

  async webSocketError(ws /*, err */) {
    try { ws.close(1011, 'error') } catch { /* ignore */ }
  }

  // --- Log operations ------------------------------------------------------

  async appendEvents(events) {
    let lastSeq = (await this.ctx.storage.get('meta:lastSeq')) || 0
    const toWrite = {}
    const stored = []

    for (const e of events) {
      if (!e || typeof e.id !== 'string') continue
      const existingSeq = await this.ctx.storage.get(`event-by-id:${e.id}`)
      if (existingSeq !== undefined) continue // idempotent dedup by id
      lastSeq += 1
      const withSeq = { ...e, serverSeq: lastSeq }
      toWrite[`events:${pad(lastSeq)}`] = withSeq
      toWrite[`event-by-id:${e.id}`] = lastSeq
      stored.push(withSeq)
    }

    if (stored.length) {
      toWrite['meta:lastSeq'] = lastSeq
      await this.ctx.storage.put(toWrite)
    }
    return stored
  }

  async replayAfter(lastKnownSeq) {
    const start = `events:${pad(lastKnownSeq + 1)}`
    const end = 'events;' // ';' is one past ':' in ASCII, caps the prefix
    const list = await this.ctx.storage.list({ start, end })
    return [...list.values()]
  }

  broadcast(events) {
    const payload = JSON.stringify({ type: 'events', events })
    for (const ws of this.ctx.getWebSockets()) {
      try { ws.send(payload) } catch { /* closing */ }
    }
  }
}
