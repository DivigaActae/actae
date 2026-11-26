# Production on-call and incident ownership

Before accepting customer payloads, a named person must explicitly accept each
role; repository authorship is not acceptance.

Day-to-day monitoring surface: the operator admin panel (`/admin` via SSH
tunnel, `docs/ADMIN_PANEL.md`) for eyes-on state, and the `monitor.sh`
watchdog pushing transition alerts to the `SAAS_ALERT_WEBHOOK_URL`
destination below.

| Role | Named owner | Contact/pager | Accepted at |
|---|---|---|---|
| Primary on-call | Pending | `SAAS_ALERT_WEBHOOK_URL` destination | — |
| Secondary/escalation | Pending | Separate reachable channel | — |
| Security incident lead | Pending | Private security channel | — |
| Customer communications | Pending | `/status/` and email | — |

Severity 1: data exposure/loss, all control-plane access down, or multiple
customer runtimes down. Acknowledge within 15 minutes, publish an initial
status within 30, preserve logs/evidence, and rotate affected secrets. Severity
2: degraded billing/provisioning or one tenant unavailable; acknowledge within
one hour. Test the paging route monthly and perform quarterly restore and
node-loss drills. No single person should be the only reachable responder for
GA.
