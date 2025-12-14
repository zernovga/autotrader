import asyncio
import json
from typing import AsyncGenerator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import UnknownTopicOrPartitionError


def serialize_json(data: dict | list | str | int | float | None) -> bytes:
    return json.dumps(data).encode("utf-8")


def deserialize_json(data: bytes):
    return json.loads(data.decode("utf-8"))


class KafkaProducerWrapper:
    def __init__(self, config):
        self._config = config
        self._producer: AIOKafkaProducer | None = None

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._config.bootstrap_servers,
            value_serializer=serialize_json,
        )
        await self._producer.start()

    async def send(self, topic: str, value):
        if not self._producer:
            raise RuntimeError("Producer is not started")

        for _ in range(5):
            try:
                await self._producer.send_and_wait(topic, value)
                return
            except UnknownTopicOrPartitionError:
                await asyncio.sleep(0.5)

        raise RuntimeError(f"Topic {topic} not available")

    async def stop(self):
        if self._producer:
            await self._producer.stop()


class KafkaConsumerWrapper:
    def __init__(self, config, topics: list[str], group_id: str):
        self._config = config
        self._topics = topics
        self._group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._config.bootstrap_servers,
            value_deserializer=deserialize_json,
            enable_auto_commit=True,
            group_id=self._group_id,
        )
        await self._consumer.start()

    async def consume(self) -> AsyncGenerator[dict, None]:
        """Асинхронный генератор, отдающий сообщения."""
        if not self._consumer:
            raise RuntimeError("Consumer is not started")

        try:
            async for msg in self._consumer:
                yield msg.value
        finally:
            await self._consumer.stop()

    async def stop(self):
        if self._consumer:
            await self._consumer.stop()
