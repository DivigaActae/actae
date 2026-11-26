// Minimal static file server for the dashboard's `next build` export (`out/`).
// Used by the Playwright smoke suite's webServer; no extra dependency.
import { createServer } from 'node:http'
import { readFile, stat } from 'node:fs/promises'
import { join, extname, normalize, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..', 'out')
const port = Number(process.env.PORT || 4173)

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.txt': 'text/plain; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
}

async function resolveFile(pathname) {
  let p = decodeURIComponent(pathname.split('?')[0])
  if (p.endsWith('/')) p += 'index.html'
  // Prevent path traversal above the export root.
  const full = join(root, normalize(p).replace(/^(\.\.[/\\])+/, ''))
  try {
    const s = await stat(full)
    if (s.isDirectory()) return join(full, 'index.html')
  } catch {
    /* fall through */
  }
  return full
}

createServer(async (req, res) => {
  try {
    const full = await resolveFile(req.url || '/')
    const body = await readFile(full)
    res.setHeader('content-type', TYPES[extname(full)] || 'application/octet-stream')
    res.end(body)
  } catch {
    try {
      const body = await readFile(join(root, '404.html'))
      res.statusCode = 404
      res.setHeader('content-type', 'text/html; charset=utf-8')
      res.end(body)
    } catch {
      res.statusCode = 404
      res.end('Not found')
    }
  }
}).listen(port, '127.0.0.1', () => {
  console.log(`dashboard static server on http://127.0.0.1:${port}`)
})
