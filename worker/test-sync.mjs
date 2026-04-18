// End-to-end test of the sync Worker using Miniflare + real WebSocket clients.
//
// Covers:
//   1. HELLO replay of the log to a reconnecting client.
//   2. Client push → DO append (serverSeq assigned) → broadcast to all sockets.
//   3. Idempotent dedup by event UUID (sending the same event twice is a no-op).
//   4. Second client joining late gets the full backlog via HELLO.
//   5. Monotonicity of serverSeq across clients.

import { Miniflare } from 'miniflare'
import WebSocket from 'ws'

const mf = new Miniflare({
  modules: [
    { type: 'ESModule', path: 'src/index.js' },
    { type: 'ESModule', path: 'src/board-room.js' },
  ],
  modulesRoot: '.',
  compatibilityDate: '2025-04-01',
  durableObjects: { BOARD_ROOMS: 'BoardRoom' },
})

const url = await mf.ready
const wsUrl = url.toString().replace(/^http/, 'ws')
console.log(`[miniflare] ${url}`)

function open(boardId = 'test-board') {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`${wsUrl}boards/${boardId}/ws`)
    const received = []
    socket.on('message', (buf) => received.push(JSON.parse(buf.toString())))
    socket.on('error', reject)
    socket.on('open', () => resolve({ socket, received }))
  })
}

function send(socket, obj) {
  socket.send(JSON.stringify(obj))
}

async function waitFor(predicate, { timeout = 3000, interval = 20 } = {}) {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    if (predicate()) return true
    await new Promise((r) => setTimeout(r, interval))
  }
  throw new Error(`waitFor timed out after ${timeout}ms`)
}

function assert(cond, msg) {
  if (!cond) { console.error(`  ✗ ${msg}`); process.exitCode = 1; throw new Error(msg) }
  console.log(`  ✓ ${msg}`)
}

function makeEvent(type, data = {}) {
  return {
    id: crypto.randomUUID(),
    type,
    v: 1,
    data,
    ts: Date.now(),
    synced: false,
    correlationId: crypto.randomUUID(),
    causationId: null,
    actorId: 'test-actor',
  }
}

let fail = 0
try {
  // --- Scenario 1: single-client push round-trip ------------------------------
  console.log('\n[1] single client push round-trip')
  {
    const a = await open()
    send(a.socket, { type: 'hello', actorId: 'a', lastKnownSeq: 0 })
    await new Promise((r) => setTimeout(r, 100)) // HELLO with empty log → no reply

    const e1 = makeEvent('board.created', { id: 'b1', title: 'Test' })
    send(a.socket, { type: 'events', events: [e1] })

    await waitFor(() => a.received.length >= 1)
    const msg = a.received[0]
    assert(msg.type === 'events', 'broadcast is a type=events message')
    assert(msg.events.length === 1, 'one event broadcast back')
    assert(msg.events[0].id === e1.id, 'same event id echoed')
    assert(msg.events[0].serverSeq === 1, 'first event gets serverSeq=1')

    a.socket.close()
    await new Promise((r) => setTimeout(r, 50))
  }

  // --- Scenario 2: two clients, live broadcast --------------------------------
  console.log('\n[2] two clients see each other live')
  {
    const a = await open()
    const b = await open()
    send(a.socket, { type: 'hello', actorId: 'a', lastKnownSeq: 1 })
    send(b.socket, { type: 'hello', actorId: 'b', lastKnownSeq: 1 })
    await new Promise((r) => setTimeout(r, 100))

    const e = makeEvent('card.created', { id: 'c1', title: 'Hello' })
    send(a.socket, { type: 'events', events: [e] })

    await waitFor(() => a.received.length >= 1 && b.received.length >= 1)
    assert(a.received[0].events[0].id === e.id, 'sender A receives echo')
    assert(b.received[0].events[0].id === e.id, 'other client B receives broadcast')
    assert(a.received[0].events[0].serverSeq === 2, 'serverSeq continues from 2')
    assert(b.received[0].events[0].serverSeq === 2, 'both clients see the same serverSeq')

    a.socket.close()
    b.socket.close()
    await new Promise((r) => setTimeout(r, 50))
  }

  // --- Scenario 3: idempotent dedup -------------------------------------------
  console.log('\n[3] idempotent dedup by event id')
  {
    const a = await open()
    send(a.socket, { type: 'hello', actorId: 'a', lastKnownSeq: 2 })
    await new Promise((r) => setTimeout(r, 100))

    const e = makeEvent('column.created', { id: 'col1', title: 'Todo' })
    send(a.socket, { type: 'events', events: [e] })
    await waitFor(() => a.received.length >= 1)
    const firstSeq = a.received[0].events[0].serverSeq
    assert(firstSeq === 3, 'first append gets serverSeq=3')

    // Send the SAME event id again — should be deduped (no new broadcast).
    send(a.socket, { type: 'events', events: [e] })
    await new Promise((r) => setTimeout(r, 200))
    assert(a.received.length === 1, 'duplicate send produces no additional broadcast')

    // A different event id goes through as serverSeq=4, not 5 (proves no seq burn).
    const e2 = makeEvent('column.created', { id: 'col2', title: 'Doing' })
    send(a.socket, { type: 'events', events: [e2] })
    await waitFor(() => a.received.length >= 2)
    assert(a.received[1].events[0].serverSeq === 4, 'next distinct event gets serverSeq=4 (no gap)')

    a.socket.close()
    await new Promise((r) => setTimeout(r, 50))
  }

  // --- Scenario 4: late joiner gets full backlog via HELLO --------------------
  console.log('\n[4] late joiner gets backlog via HELLO replay')
  {
    const c = await open()
    send(c.socket, { type: 'hello', actorId: 'c', lastKnownSeq: 0 })
    await waitFor(() => c.received.length >= 1)
    const replay = c.received[0]
    assert(replay.type === 'events', 'replay comes as events message')
    assert(replay.events.length === 4, 'replay contains all 4 events from prior scenarios')
    const seqs = replay.events.map((e) => e.serverSeq)
    assert(JSON.stringify(seqs) === '[1,2,3,4]', `serverSeqs are [1,2,3,4], got ${JSON.stringify(seqs)}`)

    // Partial resume: lastKnownSeq=2 should skip first two.
    const d = await open()
    send(d.socket, { type: 'hello', actorId: 'd', lastKnownSeq: 2 })
    await waitFor(() => d.received.length >= 1)
    assert(d.received[0].events.length === 2, 'partial resume sends only events after cursor')
    assert(d.received[0].events[0].serverSeq === 3, 'first replayed seq is cursor+1')

    c.socket.close()
    d.socket.close()
    await new Promise((r) => setTimeout(r, 50))
  }

  // --- Scenario 5: board isolation --------------------------------------------
  console.log('\n[5] events are scoped to a board (different DO per id)')
  {
    const onA = await open('alpha')
    const onB = await open('beta')
    send(onA.socket, { type: 'hello', actorId: 'x', lastKnownSeq: 0 })
    send(onB.socket, { type: 'hello', actorId: 'y', lastKnownSeq: 0 })
    await new Promise((r) => setTimeout(r, 100))

    const e = makeEvent('card.created', { id: 'alpha-c1', title: 'Only in alpha' })
    send(onA.socket, { type: 'events', events: [e] })
    await waitFor(() => onA.received.length >= 1)
    await new Promise((r) => setTimeout(r, 200)) // give B time to (not) get anything

    assert(onA.received.length === 1, 'alpha client sees its own event')
    assert(onB.received.length === 0, 'beta client sees no cross-board leakage')
    assert(onA.received[0].events[0].serverSeq === 1, 'alpha DO has its own seq counter starting at 1')

    onA.socket.close()
    onB.socket.close()
  }

  console.log('\nall scenarios passed')
} catch (err) {
  console.error('\nfailed:', err.message)
  fail = 1
} finally {
  await mf.dispose()
  process.exit(fail)
}
