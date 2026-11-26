# Python SDK Reference

Distributed execution groups, durable waits, ownership fencing, causal graphs, and Group Fork policies/receipts are documented in [Execution Groups](EXECUTION_GROUPS.md).

The consolidated Python SDK reference is maintained at:

[sdks/python/SDK_REFERENCE.md](../sdks/python/SDK_REFERENCE.md)

`SDK_REFERENCE.md` is a manually curated single-file snapshot intended for
LLM prompt injection. Keep it in sync whenever the SDK surface changes
(`sdks/python/README.md` is the human-facing source of truth). The canonical
API documentation lives in the source docstrings:

```bash
cat sdks/python/src/actae_client/client.py \
    sdks/python/src/actae_client/types.py \
    sdks/python/src/actae_client/errors.py \
    sdks/python/src/actae_client/session.py \
    sdks/python/src/actae_client/adapters/base.py \
    sdks/python/src/actae_client/adapters/langgraph.py \
    sdks/python/src/actae_client/adapters/crewai.py \
    sdks/python/src/actae_client/adapters/langchain.py
```

After any SDK change, run `python3 -m pytest sdks/python/tests/` and update
`sdks/python/README.md` + `sdks/python/SDK_REFERENCE.md` together.
