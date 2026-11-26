# Refund agent demo

[Watch the 100-second recording](../../docs/demo/actae-refund-demo.mp4) — crash, recovery, dashboard, fork.

A production-shaped case: a customer refund that hits an external payment
provider, a worker killed inside the ambiguous window (effect applied, result
not recorded), recovery, and a counterfactual fork with a different refund
amount.

Everything here runs locally. The payment/email provider is a mock with a
strict idempotency contract, so you can count exactly how many times a
customer was charged.

## Run it

```bash
# 1. Start the community build (prints the API key; the SDK picks it up
#    from $ACTAE_CONFIG_DIR/client.json)
ACTAE_PORT=8022 ACTAE_DATA_DIR=/tmp/actae-demo-data \
  ACTAE_CONFIG_DIR=/tmp/actae-demo-cfg ./actae

# 2. Start the sandbox provider
python3 -u examples/refund_agent/payment_service.py --port 9099

# 3. In another shell
cd examples/refund_agent
export ACTAE_CONFIG_DIR=/tmp/actae-demo-cfg
export PYTHON=/path/to/venv/bin/python   # venv with sdks/python installed

./run --mode run --crash     # provider applies the refund, worker dies (137)
./run --mode recover         # reclaimed, same idempotency key, provider dedupes
./run --mode fork --amount 1225 --full 4900
```

## What the output proves

```
[ledger] claim reclaimed (key=refund:4471)
[ledger] previous worker died mid-call; ownership moved with a fencing token
[provider] refund re_0001 applied: €49.00
[ledger] completed — result recorded
...
[audit] charges applied for #4471: 1
[audit] provider deduplicated retries: 1
```

- The effect landed **before** the crash and the worker never recorded it.
- On restart the ledger reclaims the execution with a fencing token and
  presents the **same idempotency key**, so the provider returns the recorded
  result instead of charging again.
- The fork is a counterfactual branch: the parent run is untouched, the child
  inherits the prefix and the state, and the confirmation email is replayed
  rather than resent.

## Honest limits

- The provider is a sandbox, not a real PSP. The duplicate-prevention guarantee
  holds only because it honors `Idempotency-Key`; a downstream that ignores the
  key will double-apply. That negative result is measured in
  [`durability_benchmark.py`](../durability_benchmark.py).
- The fork's second refund is a new business operation (new key), not a retry.
  Both branches exist side by side; promoting one is a human decision.

## Files

| File | Purpose |
|---|---|
| `agent.py` | the agent: read order → refund → email → complete |
| `payment_service.py` | mock provider with idempotency and an effect log |
| `run` | small wrapper so the commands stay short |
