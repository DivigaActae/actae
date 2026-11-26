# Execution Integrity (Tamper-Evident Ledger)

Every event is chained to the previous event on its channel with a SHA-256
hash (audit selling point 3):

```text
hash = sha256(channel_id | cursor | event_type | canonical_payload | prev_hash)
```

- `canonical_payload` is the sonic-rs serialization of the event payload.
- `prev_hash` is the previous event's hash on the channel (`""`/NULL for the
  first event).
- Both `hash` and `prev_hash` are stored on the row inside the SAME
  transaction that inserts the event, so a partial write can never produce a
  broken chain.

## What it detects

Any mutation of a recorded event — payload, event type, cursor, the chain
link itself, or a row deletion that breaks the sequence — is detected by
verification. Events recorded before migration v21 have NULL hashes and are
reported as `unverified_events` (they predate the ledger; the chain begins at
the first post-migration event on each channel).

## API

`GET /api/v1/events/verify/{channel_id}` (read-authorized):

```json
{
  "channel_id": "run-123",
  "valid": false,
  "verified_events": 41,
  "unverified_events": 0,
  "broken_at_cursor": 42,
  "first_bad_hash": "5729c58044d5d47353b23bd20d40f62fa6eb2a767c0744f6332a7bee67e856f7"
}
```

`valid` is false and `broken_at_cursor`/`first_bad_hash` are set when an
event fails either the content check (recomputed hash != stored hash) or the
link check (`prev_hash` != previous event's hash).

## Coverage

- Rust integration: `verify_chain_detects_tampering` (payload mutation is
  caught; the first event still verifies), `publish_registers_channel_and_hash_chain_verifies`.
- Live parity: `test_verify_chain_and_close_channel_live`.
- Combined with provenance-aware retention (fork boundaries and
  `depends_on` parents are never purged; purged rows are archived), the
  ledger keeps regulated-industry evidence intact end to end.

## Note

`hash`/`prev_hash` are internal integrity fields — they are not exposed on
the public `Event` type. The verification endpoint is the supported way to
assert ledger integrity.
