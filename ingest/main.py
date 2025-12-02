import asyncio
import json

from aiokafka import AIOKafkaProducer

from common.config import settings


class TinkoffSandboxStreamer:
    def __init__(self):
        pass

    async def stream(self):
        pass


class KafkaProducerWrapper:
    def __init__(self, servers: str):
        self.servers = servers
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.servers,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        await self.producer.start()

    async def send(self, topic: str, value: dict):
        await self.producer.send_and_wait(topic, value)


async def main():
    streamer: Streamer = TinkoffSandboxStreamer()
    producer = KafkaProducerWrapper(settings.kafka.bootstrap_servers)

    await producer.start()

    async for msg in streamer.stream():
        await producer.send(settings.kafka.topic_raw, msg)


if __name__ == "__main__":
    asyncio.run(main())
