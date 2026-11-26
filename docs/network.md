# Network behavior

The community build makes no outbound connections. This page states exactly
what that means and how to verify it.

## Runtime

A fresh install binds a local HTTP/WebSocket port and talks only to its own
embedded PostgreSQL process over a local socket.

- No telemetry, analytics, crash reporting, or update checks.
- No fonts, scripts, or third-party requests from the dashboard.
- No control-plane client, enrollment, or license check (see
  [control-plane-split.md](control-plane-split.md)).
- `actae upgrade` performs an explicit HTTPS download from `actae.dev`, but only
  when you run it yourself; the server never calls it.

## Verifying this yourself

```bash
./actae &
# macOS
lsof -i -P -n -a -p "$(pgrep -f 'actae$' | head -1)"
# Linux
ss -tp | grep actae
```

Expected: no rows with a non-loopback peer while the server is idle.

If you find an outbound connection, that is a bug. Report it with the
`SECURITY.md` process or as a normal issue (no secrets attached).
