# Actae reliability evidence — reliability-90e1046a

- **Schema:** `actae.reliability.v1`
- **Generated:** 2026-10-07T16:09:40.281739+00:00
- **Endpoint:** http://127.0.0.1:8002
- **Server version:** 0.1.0
- **Verdict:** **PASS** (41/41 checks)

## crash — PASS (7/7)
- [x] worker dies for real (exit 137) — returncode=137
- [x] the external effect applied exactly once — count=1
- [x] both recorded steps survived the crash — n=2
- [x] resume returns a started session — status=started
- [x] resume recovers the real step count after a hard kill — step_count=2 (want 2)
- [x] re-claim after crash replays the recorded result — status=replayed
- [x] the effect did not fire again on recovery — count=1

## fork — PASS (6/6)
- [x] auto replays an inherited effect instead of re-firing it — status=replayed
- [x] auto does not start the tool on the child
- [x] block refuses a new irreversible call
- [x] the blocked boundary is persisted as an event
- [x] intervention is exposed on the fork session
- [x] intervention is recorded on fork metadata

## coordination — PASS (4/4)
- [x] consumer A claims the whole batch — n=5
- [x] consumer B cannot double-claim A's active range — n=0
- [x] acked work is not redelivered — n=0
- [x] a lapsed lease's unacked work is redelivered — first=3 reclaimed=3

## stale_worker — PASS (5/5)
- [x] worker A claims the effect — status=claimed
- [x] worker B reclaims after the lease lapses — status=reclaimed
- [x] the stale owner's complete is fenced (execution_not_owned)
- [x] a heartbeated lease is not stolen (in_progress) — status=in_progress
- [x] bare channel writes are not fenced (documented boundary) — n=2 actors=['stale-worker', 'worker-a'] — fence side effects via claim_execution

## atomicity — PASS (8/8)
- [x] transition worker dies for real (exit 137) — returncode=137
- [x] exactly one event committed — n=1
- [x] state snapshot committed with the event — state={'cursor': 1, 'state': {'done': True, 'step': 3}}
- [x] snapshot cursor equals the event cursor — event_cursor=1 state_cursor=1
- [x] the recovery re-send replays the original event — replayed_id=264ce4e3-f498-4aee-885a-c3134c2177e6 original_id=264ce4e3-f498-4aee-885a-c3134c2177e6
- [x] no duplicate event was written on recovery — n=1
- [x] a reused operation_id with different content is rejected
- [x] no transition leaves neither event nor snapshot — events=0 state=None

## long_tool — PASS (7/7)
- [x] the owner claims the long tool — status=claimed
- [x] a tool that outlives its lease is reclaimable — status=reclaimed
- [x] the stale owner's completion is fenced — owner=['fenced']
- [x] the non-idempotent downstream fired twice (the documented lease cost) — applications=2
- [x] a heartbeated long tool is not reclaimed — status=in_progress
- [x] the heartbeating owner completes normally — owner=['completed']
- [x] the downstream fired once when the lease was held — applications=1

## session_fence — PASS (4/4)
- [x] a second live owner cannot start
- [x] a lapsed lease can be reclaimed by a new owner
- [x] the stale owner's step is rejected
- [x] the rejected step appended no event — 6->6
