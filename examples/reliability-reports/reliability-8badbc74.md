# Actae reliability evidence — reliability-8badbc74

- **Schema:** `actae.reliability.v1`
- **Generated:** 2026-09-21T08:26:29.108279+00:00
- **Endpoint:** http://localhost:8002
- **Server version:** 0.1.0
- **Verdict:** **PASS** (30/30 checks)

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
- [x] the recovery re-send replays the original event — replayed_id=f68d0612-b16e-4005-b311-e947bae1e848 original_id=f68d0612-b16e-4005-b311-e947bae1e848
- [x] no duplicate event was written on recovery — n=1
- [x] a reused operation_id with different content is rejected
- [x] no transition leaves neither event nor snapshot — events=0 state=None
