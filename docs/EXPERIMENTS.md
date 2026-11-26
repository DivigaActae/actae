# Experiment Platform

First-class experiment groups turn forks into an experiment primitive: a
baseline run plus N variant forks with declared parameter deltas, ranked
by recorded scores, with a "what changed?" comparison and promotion.

## Groups

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/api/v1/experiments` | Create a group (`{name, description?, baseline_channel_id?}`); the baseline is auto-added as a `baseline` member. |
| `GET` | `/api/v1/experiments` | List groups (with members). |
| `GET` | `/api/v1/experiments/{group_id}` | One group with members. |
| `POST` | `/api/v1/experiments/{group_id}/members` | Add a fork `{channel_id, role?, declared_delta?}` (role `baseline`\|`variant`). |
| `GET` | `/api/v1/experiments/{group_id}/rank` | Members sorted by `result_score` DESC (nulls last), joined with `outcome`/`result_score`. |

Schema (migration v19):
- `experiment_groups (group_id UUID PK, name, description, baseline_channel_id, status, created_at, updated_at)`
- `experiment_group_members (group_id FK ON DELETE CASCADE, channel_id, role, declared_delta JSONB, created_at, PK (group_id, channel_id))`

## Fork outcomes & scoring

`PATCH /api/v1/channels/{channel_id}/outcome` with
`{"outcome": "promoted"|"rejected"|"inconclusive"|"crashed", "score"?: number}`.
Stored on `channel_metadata.outcome` / `result_score` (REAL — f32 precision;
compare with tolerance). `AgentSession._crash` auto-records `crashed`.
Ranking uses `result_score` desc.

## Promotion (make the winner canonical)

`POST /api/v1/channels/{child_id}/promote` — an **append-only merge**:

1. Resolves the parent from lineage; roots are rejected (400).
2. Copies the child's latest state onto the parent at its next per-channel
   cursor.
3. Records a `run.promoted` event on the parent (`depends_on` = the child's
   last event id) carrying the child provenance.
4. Marks the child `outcome = "promoted"`.

Returns `{parent_channel_id, parent_cursor}`. The parent's history is never
rewritten — promotion appends.

## "What changed?" comparison

`GET /api/v1/channels/compare?left={channel}&right={channel}` returns a
combined document:

- `state_diff` — the full `diff_channels` result (state diff + per-side
  divergence + honest truncation).
- `manifest_diff` — structural diff of the two fork manifests.
- `tool_diff` — the tool-execution ledgers diffed by `key_name` (result
  compared structurally; `added`/`removed`/`changed`).
- `output_left` / `output_right` / `output_changed` — final outputs from
  `experiment_metadata.result.output`, else the last event's `payload.output`.
- `metrics_delta` — latency/token/cost deltas from
  `experiment_metadata.metrics`.

## Retention & GC

`DELETE /api/v1/channels/{channel_id}?recursive=true|false`:

- Non-recursive deletes of a channel with children → `409` (use `recursive`).
- Soft-deletes (`channel_metadata.deleted_at`) and purges the physical rows
  (`agent_events`, `channel_states`, `channel_cursors`, `tool_executions`,
  `channel_steps`, `experiment_group_members`).
- The `channel_metadata` row is kept so idempotent replays of deleted forks
  resolve to `410 gone` instead of resurrecting a half-purged channel.

## SDK surface

- Python: `create_experiment`, `list_experiments`, `get_experiment`,
  `add_experiment_member`, `rank_experiment`, `set_outcome`, `promote_channel`,
  `delete_channel`, `compare_channels`.
- Go: `CreateExperiment`, `ListExperiments`, `AddExperimentMember`,
  `RankExperiment`, `SetOutcome`, `PromoteChannel`, `DeleteChannel`,
  `CompareChannels`.

## Guardrails

- Scores are f32 (REAL column) — round/diff with tolerance.
- `rank` returns the baseline as a member; sorting is score-desc, nulls-last,
  then created_at ascending, then channel_id ascending for ties. The final
  `channel_id` tie-break makes the order **deterministic** — `created_at` alone
  is not a stable key because two members inserted within the same Postgres
  `NOW()` microsecond compare equal (previously the tie order was undefined).
- Promotion is idempotent-friendly (a second promote re-records the parent
  state + a new `run.promoted`); prefer a single promote per decision.

## Testing

`tests/test_experiments.py` (14 live tests) covers the full HTTP surface:
create/list/get (incl. malformed group ids → 404, empty-name → 400), baseline
auto-member, variant `declared_delta` round-trip and role defaulting,
member-upsert semantics, outcome/score recording, ranking order (score desc,
nulls last, deterministic tie-break), promotion (winner → parent with a
`run.promoted` event), root-promote → 400, unknown-channel promote → 404, and
non-recursive delete of a parent with children → 409.
