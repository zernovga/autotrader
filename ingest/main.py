import asyncio
import json
from typing import Protocol

from aiokafka import AIOKafkaProducer

from common.config import settings
from common.logging_config import configure_logging

log = configure_logging("ingest")


class DemoStreamer:
    async def stream(self):
        while True:
            yield {"data": "streamer test message"}
            await asyncio.sleep(2)


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
    print("starting main")

    streamer = DemoStreamer()
    producer = KafkaProducerWrapper(settings.kafka.bootstrap_servers)

    await producer.start()

    async for msg in streamer.stream():
        log.info("raw event", payload=msg)
        await producer.send(settings.kafka.topic_raw, msg)


if __name__ == "__main__":
    log.info("started")

    asyncio.run(main())
