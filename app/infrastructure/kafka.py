import json
import logging
from collections.abc import Mapping
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )

    async def start(self) -> None:
        await self._producer.start()
        logger.info("Kafka producer started")

    async def stop(self) -> None:
        await self._producer.stop()
        logger.info("Kafka producer stopped")

    async def publish(self, event_id: str, payload: bytes) -> None:
        await self._producer.send_and_wait(self.topic, value=payload, key=event_id.encode("utf-8"))

    async def ping(self) -> bool:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        try:
            await admin.start()
            await admin.list_topics()
            return True
        finally:
            await admin.close()


def decode_message(value: bytes) -> Mapping[str, Any]:
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise ValueError("Kafka message must contain a JSON object")
    return decoded
