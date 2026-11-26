/** @type {import('next').NextConfig} */
// `DASHBOARD_BASE_PATH` lets the same static export be hosted under a subpath
// (e.g. the marketing site embeds a read-only demo at /demo/dashboard-app).
// Unset (the default) serves the dashboard at the origin root.
const basePath = process.env.DASHBOARD_BASE_PATH || ''

const nextConfig = {
  output: 'export',
  // Ensure trailing slashes for static export compatibility
  trailingSlash: true,
  // Disable image optimization — no server for static export
  images: { unoptimized: true },
  // Keep distDir as 'out' (default)
  basePath: basePath || undefined,
  // The vendored demo is committed: a fixed build id keeps the per-build
  // `_next/static/<id>/` directory stable across re-vendors, so only the chunk
  // files that actually changed are rewritten. (Next's chunk contents are not
  // byte-reproducible, so some churn on rebuild is unavoidable.) Scoped to the
  // base-path (demo) build so the production build keeps Next's default.
  ...(basePath ? { generateBuildId: () => 'actae-demo' } : {}),
}

module.exports = nextConfig
