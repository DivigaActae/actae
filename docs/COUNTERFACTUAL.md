# Counterfactual Debugging with Actae

How to turn one failing run into a falsifiable experiment in minutes, and
prove which fix worked — with the structural state diff.

## The pitch

> One failing run → two forks with different context → a diff that proves
> which one fixes it.

This is Actae's version of the "time-travel debugging" story (the demo
LangSmith runs in its marketing): because every run is an immutable event
log plus versioned state snapshots, you can rewind to the failure, fork,
change the context, and compare **state**, not just logs.

## The workflow (plain English)

1. **A run fails.** The channel (`my-agent`) has an event log and a saved
   state snapshot right before the failure.
2. **Fork twice from the failure boundary.** `fork(source, "fix-a")` and
   `fork(source, "fix-b")` — both forks start from the exact state the
   failing run had at the point you picked. Use `at_cursor` to fork from the
   *pre-failure* snapshot (an exact-boundary fork), or `0` for the latest.
3. **Change the context on each fork.** Fork A retries with more
   retries; fork B falls back to a different tool. Each fork saves its
   own state as it runs.
4. **Diff the two forks' states.** `diff_states("fix-a", "fix-b")`
   returns exactly what each fork knew that the other didn't: added /
   removed / changed keys, the shared ancestor, and the divergence cursor.
   If B's state is "closer to the goal" (e.g. the error key is gone, the
   plan field is filled), you have **falsifiable evidence B fixed it** —
   not a vibes-based guess from skimming logs.

## Why this is different from an event-log diff

| | Event-log diff (CompareView today) | Structural state diff (new) |
|---|---|---|
| What it compares | sequences of event types | the actual JSON state |
| Answers | "did the runs diverge, and at which event?" | "what knowledge/context did one fork have that the other lacked?" |
| Falsifiability | weak — two runs can emit different events yet converge to the same state | strong — the state is the outcome |

## SDK sketch

```python
# The failing run's channel is "my-agent". Fork two fixes from its latest
# state (or pass at_cursor to fork at the pre-failure snapshot).
client.fork("my-agent", "fix-a", at_cursor=0, display_name="Fix A: more retries",
            reason="increase retries", operation_id=...)
client.fork("my-agent", "fix-b", at_cursor=0, display_name="Fix B: fallback tool",
            reason="try fallback path", operation_id=...)

# ... run each fork, each save_state() as it goes ...

# Prove which fix worked by diffing their final states.
diff = client.diff_states("fix-a", "fix-b")
for entry in diff["entries"]:
    print(entry["kind"], ".".join(entry["path"]), "→", entry.get("right"))
```

The dashboard surfaces the same thing: open a fork in the Flow view, pick a
sibling fork in the **State Diff** section of the Inspector, and see the
added/removed/changed state rendered as a tree against the shared ancestor.

## Conventions that make the demo reproducible

- Store a `{"hypothesis": ..., "baseline_run": <root>}` object in
  `experiment_metadata` when forking (it round-trips through
  `fork`/`update_metadata` and is shown in the fork tree).
- Save state at every agent "step" (the SDK's `AgentSession.step` does this
  automatically) so fork boundaries are always exact.
- Use `operation_id` for idempotent forks — re-running the experiment can
  never duplicate forks. Standard idempotency: an identical replay returns
  the original child; request drift is `409 idempotency_conflict`.

## Reproducibility (post-audit hardening)

Every fork now carries an **immutable receipt** (`fork()` return /
`get_fork_receipt` / `GET /channels/{id}/receipt`) with the requested and
resolved boundaries, the copied source-state version + SHA-256 fingerprint,
`restorable`, and a **reproducibility grade**:

| Grade | Meaning |
|---|---|
| `state_exact` | The inherited state is an exact, restorable checkpoint at the requested boundary. |
| `context_exact` | `state_exact` AND a full manifest (model + environment + dependencies + seed) was supplied. |
| `execution_replayable` | `state_exact` AND the source has a tool-execution ledger. |
| `best_effort` | Everything else (approximate boundary, lineage-only, missing manifest). |

`diff_states` reports **per-side divergence** (`left_diverged_at_cursor` /
`right_diverged_at_cursor`) — the fork edge where each fork departed the
shared chain — and honest truncation (`truncated`, `entry_count_total`). A
decision trail's `boundary` is the immutable fork-time snapshot, so it stays
correct even when the parent advanced after the fork.

## Where the code lives

- Server: `GET /api/v1/channels/diff` and `GET /api/v1/channels/{id}/trail`
  (`actae/src/events/store.rs` — `diff_channels` / `get_decision_trail`).
- SDKs: `diff_states` / `decision_trail` (Python + Go).
- Dashboard: `StateDiffPanel` + `DecisionTrailPanel` in the Inspector, plus
  the `CompareView` "what changed?" tabs.
- See `docs/FORK_RUNTIME.md` §9 for the full response schemas.
