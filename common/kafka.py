import json

from aiokafka import AIOKafkaProducer


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

        await self._producer.send_and_wait(topic, value)

    async def stop(self):
        if self._producer:
            await self._producer.stop()
