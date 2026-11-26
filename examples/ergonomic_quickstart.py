# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Run with ACTAE_URL and ACTAE_API_KEY set."""

import asyncio

from actae_client import Actae


actae = Actae.from_env()


@actae.effect("order:{order_id}:receipt", name="create_receipt")
async def create_receipt(order_id: str, amount: int):
    # Replace with the real external call. A retry replays this JSON result.
    return {"receipt_id": "receipt-{}".format(order_id), "amount": amount}


async def main():
    async with actae.run("order-42", framework="example"):
        receipt = await create_receipt("42", 2500)
        print(receipt)
    await actae.close()


if __name__ == "__main__":
    asyncio.run(main())
