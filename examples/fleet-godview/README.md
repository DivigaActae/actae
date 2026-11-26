# Fleet god-view example

This example is intentionally read-only. It lists routable instances, queries
the same channel across them, prints partial failures, and demonstrates how a
short-lived stream ticket is rotated after a disconnect. Organization tokens
stay in the server-side process; browser code receives only a one-use ticket.

Required environment for the Python example:

```text
ACTAE_FLEET_URL=https://fleet.example
ACTAE_ORG_TOKEN=...        # server-side only
ACTAE_ORG_ID=org-123
ACTAE_INSTANCE_ID=instance-123
ACTAE_STREAM_TICKET=...    # optional, issued by SaaS
```

Run with `python python_godview.py`. To test a deployed gateway, use the
opt-in suite: `ACTAE_FLEET_E2E=1 pytest tests/test_fleet_gateway_e2e.py -v`.
