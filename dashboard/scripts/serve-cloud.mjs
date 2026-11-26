// Local preview of the built dashboard (`out/`) against a REMOTE Actae cloud
// instance — no deploy needed.
//
// Why a proxy: the cloud API does not send `Access-Control-Allow-Origin`, so a
// browser at localhost cannot call it cross-origin. This serves the static
// export and reverse-proxies `/api`, `/ws` (and health/metrics) to the cloud,
// making everything same-origin — exactly the production topology.
//
// Usage:
//   NEXT_PUBLIC_API_BASE= npm run build            # build `out/` with same-origin API calls
//   ACTAE_CLOUD_ORIGIN=https://<instance>.cloud.actae.dev npm run serve:cloud
//   # open http://127.0.0.1:4180  and log in with your normal account
//
// Node builtins only — no dependencies.
import { createServer } from 'node:http'
import { request as httpsRequest } from 'node:https'
import { request as httpRequest } from 'node:http'
import { connect as tlsConnect } from 'node:tls'
import { connect as netConnect } from 'node:net'
import { readFile, stat } from 'node:fs/promises'
import { join, extname, normalize, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const RAW_ORIGIN = process.env.ACTAE_CLOUD_ORIGIN
if (!RAW_ORIGIN) {
  console.error('ACTAE_CLOUD_ORIGIN is required, e.g. ACTAE_CLOUD_ORIGIN=https://<instance>.cloud.actae.dev npm run serve:cloud')
  process.exit(1)
}
const upstream = new URL(RAW_ORIGIN)
const IS_TLS = upstream.protocol !== 'http:'
const PORT = Number(process.env.PORT || 4180)
const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..', 'out')

const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.ico': 'image/x-icon',
  '.woff': 'font/woff', '.woff2': 'font/woff2', '.txt': 'text/plain; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
}

async function resolveFile(pathname) {
  let p = decodeURIComponent(pathname.split('?')[0])
  if (p.endsWith('/')) p += 'index.html'
  const full = join(ROOT, normalize(p).replace(/^(\.\.[/\\])+/, ''))
  try { if ((await stat(full)).isDirectory()) return join(full, 'index.html') } catch { /* fall through */ }
  return full
}

function proxyHttp(req, res) {
  const send = IS_TLS ? httpsRequest : httpRequest
  const up = send(
    { host: upstream.hostname, port: upstream.port || undefined, method: req.method, path: req.url,
      headers: { ...req.headers, host: upstream.host } },
    (ur) => { res.writeHead(ur.statusCode || 502, ur.headers); ur.pipe(res) },
  )
  up.on('error', (e) => { res.statusCode = 502; res.end('proxy error: ' + e.message) })
  req.pipe(up)
}

const server = createServer(async (req, res) => {
  const url = req.url || '/'
  if (url.startsWith('/api/') || url.startsWith('/healthz') || url.startsWith('/readyz') || url.startsWith('/metrics')) {
    return proxyHttp(req, res)
  }
  try {
    const full = await resolveFile(url)
    const body = await readFile(full)
    res.setHeader('content-type', TYPES[extname(full)] || 'application/octet-stream')
    res.end(body)
  } catch {
    try {
      const body = await readFile(join(ROOT, '404.html'))
      res.statusCode = 404; res.setHeader('content-type', 'text/html; charset=utf-8'); res.end(body)
    } catch { res.statusCode = 404; res.end('Not found') }
  }
})

server.on('upgrade', (req, socket) => {
  if (!(req.url || '').startsWith('/ws')) { socket.destroy(); return }
  const onConnect = () => {
    const lines = Object.entries({ ...req.headers, host: upstream.host }).map(([k, v]) => `${k}: ${v}`)
    up.write(`GET ${req.url} HTTP/1.1\r\n${lines.join('\r\n')}\r\n\r\n`)
    up.pipe(socket); socket.pipe(up)
  }
  const up = IS_TLS
    ? tlsConnect({ host: upstream.hostname, port: Number(upstream.port || 443) }, onConnect)
    : netConnect({ host: upstream.hostname, port: Number(upstream.port || 80) }, onConnect)
  up.on('error', () => socket.destroy())
  socket.on('error', () => up.destroy())
})

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Dashboard (out/) on http://127.0.0.1:${PORT}  →  ${upstream.origin}`)
})
