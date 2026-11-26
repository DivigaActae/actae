# Execution Groups and Group Fork demo

This walkthrough uses a real Actae server and the Python SDK. It demonstrates:

1. Creating a group with two independently leased agents.
2. Sending and acknowledging a durable causal message.
3. Reading the persisted causal DAG.
4. Receiving a new delivery through `GroupMemberSession.stream_ws()`.
5. Forking at an exact event boundary with a frozen research agent.
6. Blocking a counterfactual write, promoting the agent to reactive, and recording a live divergence receipt.

Start Actae first:

```bash
cd actae
cargo run
```

Then run:

```bash
pip install -e sdks/python
python examples/execution_groups/demo.py
```

The script uses unique IDs on every run, so it does not mutate or delete prior
demo data. Set `ACTAE_URL`, `ACTAE_WS_URL`, and `ACTAE_API_KEY` for another
instance. The dashboard's **Coordination → Execution Groups** page will show
the group, colored agents, messages, causal graph, and fork receipt while the
demo runs.
