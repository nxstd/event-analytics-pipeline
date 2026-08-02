import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from app.models import Event, StatsGroup


async def create_clickhouse_client(
    host: str, port: int, database: str, username: str, password: str
) -> AsyncClient:
    return await clickhouse_connect.get_async_client(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
    )


class ClickHouseStore:
    def __init__(self, client: AsyncClient) -> None:
        self.client = client

    async def ping(self) -> bool:
        result = await self.client.query("SELECT 1")
        return bool(result.result_rows and result.result_rows[0][0] == 1)

    async def insert_event(self, event: Event) -> None:
        await self.client.insert(
            "events",
            [[
                event.event_id,
                event.type,
                event.amount,
                event.currency,
                event.status.value,
                event.created_at,
                datetime.now(UTC),
            ]],
            column_names=[
                "event_id",
                "type",
                "amount",
                "currency",
                "status",
                "created_at",
                "processed_at",
            ],
        )

    async def get_stats(self) -> list[StatsGroup]:
        result = await self.client.query(
            """
            SELECT type, status, currency, count() AS events_count, sum(amount) AS total_amount
            FROM events
            GROUP BY type, status, currency
            ORDER BY type, status, currency
            """
        )
        groups = []
        for row in result.result_rows:
            currency = row[2].decode() if isinstance(row[2], bytes) else str(row[2]).rstrip("\\0")
            groups.append(
                StatsGroup(
                    type=row[0],
                    status=row[1],
                    currency=currency,
                    events_count=row[3],
                    total_amount=Decimal(row[4]),
                )
            )
        return groups

    async def close(self) -> None:
        close_result: Any = self.client.close()
        if asyncio.iscoroutine(close_result):
            await close_result
