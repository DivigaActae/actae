# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

import asyncio
import os

from actae_client import FleetClient


async def main() -> None:
    async with FleetClient.from_env() as fleet:
        instances = await fleet.instances()
        print("instances:", instances)
        instance = os.environ["ACTAE_INSTANCE_ID"]
        result = await fleet.query(instance, channel_ids=[os.getenv("ACTAE_CHANNEL_ID", "run")], limit=20)
        print("events:", len(result.get("events", result.get("items", []))))
        if result.get("errors"):
            print("partial failures:", result["errors"])

        ticket = os.getenv("ACTAE_STREAM_TICKET")
        if ticket:
            async for frame in fleet.stream(instance_id=instance, ticket=ticket, reconnect=False):
                print("stream:", frame)
                break


if __name__ == "__main__":
    asyncio.run(main())
