# Website dashboard demo (`/demo/dashboard/`)

The marketing site's **Dashboard tour** page embeds the *real* Actae dashboard —
the same Next.js app the self-hosted build serves — running entirely in the
browser on recorded demo data, with a **deterministic "live" replay** so a
visitor can watch a run stream in. No account, backend, network, or LLM is
involved.

## How it is assembled

Two static exports, no server:

1. The dashboard (`dashboard/`) is built in **mock mode** with a **base path**:
   `NEXT_PUBLIC_MOCK_MODE=true DASHBOARD_BASE_PATH=/demo/dashboard-app next build`.
2. The export is vendored to
   `saas/frontend/public/demo/dashboard-app/` (committed, so CI and a fresh
   clone work without rebuilding).
3. The site page
   [`saas/frontend/src/app/demo/dashboard/page.tsx`](../saas/frontend/src/app/demo/dashboard/page.tsx)
   embeds it in an `<iframe>` pointing at
   `/demo/dashboard-app/dashboard/flow/?channel=demo-live-run&replay=1`.

Because the dashboard export already uses `trailingSlash` + `output: "export"`,
and Next applies `basePath` to both `Link` navigation and `/_next` asset URLs,
all in-dashboard navigation (Overview, Runs, Events, Forks, Approvals,
Coordination) works as real client-side routing inside the iframe.

**Serving requirement (why the iframe was once blocked).** The site's security
headers must permit same-origin framing of the demo: the SaaS backend serves
`/demo/dashboard-app/…` with `X-Frame-Options: SAMEORIGIN` +
`frame-ancestors 'self'` and includes `'self'` in `frame-src` (see
`saas/backend/src/actae_saas/app.py`, `_relax_frame_headers`). Without this the
browser shows *"This content is blocked. Contact the site owner to fix the
issue."* Any other host serving the site must do the same.

## Rebuilding the vendored export

The dashboard UI changes rarely, but when it does:

```bash
cd saas/frontend
npm run build:demo-dashboard          # builds dashboard/ and re-vendors the export
```

`prebuild` runs this automatically on a dev/release machine. It **skips in CI**
(so the saas-frontend job stays fast and uses the committed copy) unless
`ACTAE_BUILD_DEMO=1`. Env overrides:

| Var | Effect |
|-----|--------|
| `ACTAE_SKIP_DEMO_BUILD=1` | Skip; use the committed copy. |
| `ACTAE_BUILD_DEMO=1` | Force a build even in CI. |
| `ACTAE_DEMO_STRICT=1` | Fail the build on error (default: warn + keep the committed copy). |

**Staleness guard (no CI job):** the script stamps the export with
`DEMO_BUILD.json` = `{ schema, dashboardSourceHash, basePath, mock }`, where the
hash covers the dashboard's build inputs (`dashboard/src` + build config, via
`scripts/dashboard-source-hash.mjs`). A vitest test
(`src/app/demo/dashboard-vendored.test.ts`, part of the normal `npm test`)
recomputes the hash and fails if it does not match — so a dashboard change that
was not re-vendored is caught by the existing suite, not a new workflow.

## Mock mode (what makes it work offline)

`dashboard/src/lib/api.ts` short-circuits every read the dashboard makes when
`NEXT_PUBLIC_MOCK_MODE=true`, backed by `dashboard/src/lib/mock-data.ts`. The
demo family is `agent-research-run` (baseline) with `agent-research-run-exp-a`
(GPT-5.5 prompt fork), `agent-research-run-exp-b` (Claude 3.5 fork), and
`agent-research-run-exp-a-sub`. The extended mock branches cover:

- `listChannels` (with display names), `queryEvents` (filters `MOCK_EVENTS` +
  `MOCK_FORK_EVENTS` + `MOCK_APPROVAL_EVENTS`), `listWakeups` (one pending
  approval deadline);
- the experiment platform: `listExperiments`, `getExperiment`,
  `rankExperiment` (a scored/ranked/promotable group), `compareChannels`
  (a realistic combined state/config/tools/output/metrics diff),
  `setChannelOutcome`, `promoteChannel`;
- coordination: `listExecutionGroups`, `listExecutionGroupMembers`,
  `listExecutionGroupMessages`, `getExecutionGroupFork`,
  `promoteExecutionGroupMember`, `getCausalGraph` (one group with members,
  causal messages and a completed counterfactual fork).

Overview / Runs / Flow / Events already had dedicated mock paths. **Every page
in the sidebar is populated** — Approvals shows pending/approved/rejected/expired
requests, and Coordination shows the group + fork. The Runs list also reflects
the replayed run: the demo channels are derived from the flow store's live
events, so the run appears (and its counts grow) as the replay streams.

## Deterministic "live" replay

`dashboard/src/lib/demo-scenario.ts` is a fixed, recorded dry-run timeline
(baseline run → saved boundary at cursor 3 → fork → fact-check → completion):
same steps, cursors, **timestamps** and pacing on every visit, so the demo is
identical and reproducible (the "dry-run" is the point). It mirrors
`examples/research_agent/demo.py --dry-run`.

The replay targets two **dedicated, otherwise-empty channels**
(`demo-live-run`, `demo-live-run-fork`) that are intentionally absent from
`MOCK_EVENTS`, so it builds the timeline from empty — no duplication with the
recorded dataset, and the timeline visibly grows.

`dashboard/src/components/DemoReplayDock.tsx` (rendered from the root layout,
**mock mode only**) streams the scenario one step at a time. It injects via
`useFlowStore.injectDemoEvent`, a mock-specific action that mirrors
`addLiveEvent`'s merge/dedupe **without requiring a WebSocket connection**
(`addLiveEvent` is a no-op unless `isLive`, which is false in the demo), and
sets a `demoLive` flag so the timeline shows a `DEMO` badge instead of a
misleading "offline" state. Controls: **Run demo / Pause / Resume / Restart**
with a progress bar; `resetDemoReplay` clears the demo channels for an
idempotent restart, and the `demoLive` flag is cleared on completion (so the
`DEMO` badge disappears once the run is finished history). It auto-starts when
the URL carries `?replay=1` (the embed adds it). Event ids are stable
(`demo-<channel>-<cursor>-<type>`).

Regression coverage: `dashboard/src/lib/demo-scenario.test.ts` (stable ids,
deterministic increasing timestamps, dedicated channels) and the
`demo replay injection` block in `flow-store.test.ts` (injects without a live
connection, dedupes, resets).

## Boundaries

- **Read-only and synthetic.** It exercises the dashboard's *rendering* and
  navigation against mock data. It is not the production authentication, RBAC,
  or a real instance; no requests leave the browser.
- **Not a substitute for a real run.** Visitors who want a real run download
  the free community build and point it at their own agent.
- The old bespoke `DashboardDemo` React re-implementation (and its `.saas-dashboard-*`
  styling) was removed once the real dashboard was embedded.
- **Committed artifact churn.** The vendored export is committed (so dev, CI
  and a fresh clone work without the dashboard toolchain). The demo build pins a
  fixed `generateBuildId`, but Next's chunk contents are not byte-reproducible,
  so re-vendoring after a dashboard change rewrites the affected chunk files —
  expected, and caught/nudged by the staleness test rather than a CI job.

## Tests

- Dashboard: `dashboard/src/lib/demo-scenario.test.ts` (timeline shape,
  cumulative offsets, stable/unique event ids) plus the existing mock-data and
  page suites.
- Site: `seo-metadata.test.ts` (canonical for `/demo/dashboard/`) and
  `dashboard-vendored.test.ts` (export present + base-path assets + **not
  stale**: `DEMO_BUILD.json` hash matches the dashboard source).
- Browser smoke (manual/CI-optional): load `/demo/dashboard/`, assert the
  iframe renders the dashboard, the replay streams events, and sidebar
  navigation to Forks works.
