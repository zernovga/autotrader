import asyncio

from common.config import settings
from common.kafka import KafkaConsumerWrapper, KafkaProducerWrapper
from common.logging_config import configure_logging

from .streamers import StreamerMeta

log = configure_logging("ingest")


class IngestService:
    def __init__(self) -> None:
        self.producer = KafkaProducerWrapper(settings.kafka)
        self.consumer = KafkaConsumerWrapper(
            config=settings.kafka,
            topics=["ingest.request"],
            group_id="ingest",
        )
        self.running_streamers = dict()

    async def run(self):
        await self.producer.start()
        await self.consumer.start()

        log.info("IngestService started")

        try:
            async for msg in self.consumer.consume():
                log.info("Received ingest request", msg=msg)
                if msg["streamer"] not in StreamerMeta.streamers:
                    log.info("Unknown streamer", streamer=msg["streamer"])
                    continue

                name = f"{msg['streamer']}-{'-'.join(map(str, msg['params'].values()))}"
                if name in self.running_streamers:
                    log.info("Streamer already running", figi=msg["figi"])
                    continue

                params = msg["params"]

                streamer = StreamerMeta.streamers[msg["streamer"]](**params)

                asyncio.create_task(streamer.run())

        except Exception as e:
            log.exception("ingest error", exception=str(e))
        finally:
            await self.close()

    async def close(self):
        log.info("Shutting down IngestService...")
        await self.consumer.stop()


# async def main():
#     print("starting main")

#     figi = "BBG0013HRTL0"

#     producer = KafkaProducerWrapper(settings.kafka)

#     streamer = TinkoffSandboxStreamer(figi)

#     await producer.start()
#     log.info("connecting to client")
#     while True:
#         async with AsyncClient(settings.tinkoff_token) as client:
#             try:
#                 await streamer.ensure_market_open(client)

#                 async for msg in client.market_data_stream.market_data_stream(
#                     streamer.stream()
#                 ):
#                     try:
#                         log.info("raw event", payload=msg)

#                         data = {
#                             "figi": figi,
#                             "time": msg.candle.time.isoformat(),
#                             "last_trade": msg.candle.last_trade_ts.isoformat(),
#                             "open": float(quotation_to_decimal(msg.candle.open)),
#                             "high": float(quotation_to_decimal(msg.candle.high)),
#                             "low": float(quotation_to_decimal(msg.candle.low)),
#                             "close": float(quotation_to_decimal(msg.candle.close)),
#                         }
#                         log.info("normalized event", payload=data)

#                         await producer.send(settings.kafka.topic_raw, data)

#                     except AioRequestError as e:
#                         log.error("request error", exception=str(e))
#                     except Exception as e:
#                         log.error("error", exception=str(e))

#             except AioRequestError as e:
#                 log.error("request error", exception=str(e))
#             except Exception as e:
#                 log.error("error", exception=str(e))


# async def main_demo():
#     from datetime import datetime
#     from random import random

#     print("starting main demo")

#     figi = "DEMO"

#     producer = KafkaProducerWrapper(settings.kafka)

#     await producer.start()
#     log.info("connecting to client")

#     while True:
#         try:
#             data = {
#                 "figi": figi,
#                 "time": datetime.now().isoformat(),
#                 "last_trade": datetime.now().isoformat(),
#                 "open": 20 * random(),
#                 "high": 20 * random(),
#                 "low": 20 * random(),
#                 "close": 20 * random(),
#             }
#             log.info("normalized event", payload=data)

#             await producer.send(settings.kafka.topic_raw, data)
#             await asyncio.sleep(1)

#         except Exception as e:
#             log.error("error", exception=str(e))


async def main():
    service = IngestService()
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
