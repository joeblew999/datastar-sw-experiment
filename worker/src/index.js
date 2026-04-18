// Cloudflare Worker entry for the sync prototype.
//
// Responsibilities:
//   1. Extract the board ID from the path.
//   2. Validate that this is a WebSocket upgrade request.
//   3. Forward to the BoardRoom Durable Object named after the board ID.
//
// One DO per board. Clients connect to wss://<worker>/boards/<id>/ws and the
// DO handles all further protocol (see src/board-room.js).

export { BoardRoom } from './board-room.js'

const ORIGIN_ALLOW = '*' // Tighten before production (e.g. the GH Pages origin)

export default {
  async fetch(request, env) {
    const url = new URL(request.url)

    // CORS preflight — kept permissive for the prototype.
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'access-control-allow-origin': ORIGIN_ALLOW,
          'access-control-allow-methods': 'GET, POST, OPTIONS',
          'access-control-allow-headers': 'content-type, upgrade',
        },
      })
    }

    // Health check for deploy verification.
    if (url.pathname === '/health') {
      return new Response('ok', { headers: { 'content-type': 'text/plain' } })
    }

    // /boards/<boardId>/ws
    const match = url.pathname.match(/^\/boards\/([^/]+)\/ws$/)
    if (!match) {
      return new Response('not found', { status: 404 })
    }
    const boardId = match[1]

    // Require WebSocket upgrade — validated here so we don't bill the DO for bogus hits.
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('expected websocket upgrade', { status: 426 })
    }

    const id = env.BOARD_ROOMS.idFromName(boardId)
    const stub = env.BOARD_ROOMS.get(id)
    return stub.fetch(request)
  },
}
