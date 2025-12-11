import asyncio
import uuid
from decimal import Decimal

from common.config import settings
from common.kafka import KafkaConsumerWrapper, KafkaProducerWrapper
from common.logging_config import configure_logging
from tinkoff.invest import GetAccountsResponse, OrderDirection, OrderType
from tinkoff.invest.async_services import (
    OrderExecutionReportStatus,
    OrderIdType,
    PostOrderAsyncRequest,
    PostOrderAsyncResponse,
    PriceType,
    TimeInForceType,
)
from tinkoff.invest.sandbox.async_client import AsyncSandboxClient
from tinkoff.invest.utils import decimal_to_quotation

log = configure_logging("orders")


class OrdersService:
    def __init__(self):
        self.consumer = KafkaConsumerWrapper(
            config=settings.kafka,
            topics=["orders.new"],
            group_id="orders",
        )
        self.producer = KafkaProducerWrapper(config=settings.kafka)
        self.active_order = None

    async def start(self):
        await self.consumer.start()
        await self.producer.start()

        async with AsyncSandboxClient(settings.tinkoff_token) as client:
            response: GetAccountsResponse = await client.users.get_accounts()
            account, *_ = response.accounts
            self.account_id = account.id
            log.info("account selected", account=self.account_id)

    async def run(self):
        log.info("OrderService started")

        async for msg in self.consumer.consume():
            log.info("new order", order=msg)

            if self.active_order is None and msg["signal"] == "BUY":  # type: ignore
                async with AsyncSandboxClient(settings.tinkoff_token) as client:
                    order_id = str(uuid.uuid4())
                    request = PostOrderAsyncRequest(
                        instrument_id=msg["figi"],  # type: ignore
                        quantity=1,
                        price=decimal_to_quotation(Decimal(msg["price"])),  # type: ignore
                        direction=OrderDirection.ORDER_DIRECTION_BUY,
                        account_id=self.account_id,
                        order_type=OrderType.ORDER_TYPE_MARKET,
                        order_id=order_id,
                        time_in_force=TimeInForceType.TIME_IN_FORCE_FILL_OR_KILL,
                        price_type=PriceType.PRICE_TYPE_CURRENCY,
                        confirm_margin_trade=False,
                    )
                    log.info("order request formed", request=request)
                    response: PostOrderAsyncResponse = (
                        await client.orders.post_order_async(request)
                    )
                    log.info("order response", payload=response)
                    state = await client.orders.get_order_state(
                        account_id=self.account_id,
                        order_id="cb4702eb-d999-4510-afb4-ed1b88713036",
                        order_id_type=OrderIdType.ORDER_ID_TYPE_REQUEST,
                    )
                    log.info("order state", state=state)
                    if (
                        state.execution_report_status
                        == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
                    ):
                        self.active_order = order_id
            elif self.active_order is not None and msg["signal"] == "SELL":  # type: ignore
                async with AsyncSandboxClient(settings.tinkoff_token) as client:
                    order_id = str(uuid.uuid4())
                    request = PostOrderAsyncRequest(
                        instrument_id=msg["figi"],  # type: ignore
                        quantity=1,
                        price=decimal_to_quotation(Decimal(msg["price"])),  # type: ignore
                        direction=OrderDirection.ORDER_DIRECTION_SELL,
                        account_id=self.account_id,
                        order_type=OrderType.ORDER_TYPE_MARKET,
                        order_id=order_id,
                        time_in_force=TimeInForceType.TIME_IN_FORCE_FILL_OR_KILL,
                        price_type=PriceType.PRICE_TYPE_CURRENCY,
                        confirm_margin_trade=False,
                    )
                    log.info("order request formed", request=request)
                    response: PostOrderAsyncResponse = (
                        await client.orders.post_order_async(request)
                    )
                    log.info("order response", payload=response)
                    state = await client.orders.get_order_state(
                        account_id=self.account_id,
                        order_id="cb4702eb-d999-4510-afb4-ed1b88713036",
                        order_id_type=OrderIdType.ORDER_ID_TYPE_REQUEST,
                    )
                    log.info("order state", state=state)
                    if (
                        state.execution_report_status
                        == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
                    ):
                        self.active_order = None

    async def close(self):
        log.info("Shutting down OrdersService...")
        async with AsyncSandboxClient(settings.tinkoff_token) as client:
            orders = client.orders.get_orders(account_id=self.account_id)
            log.info("open orders", orders=orders)

        await self.consumer.stop()
        await self.producer.stop()


async def main():
    service = OrdersService()

    await service.start()

    try:
        await service.run()
    except Exception as e:
        log.exception(
            "OrdersService error",
            exception=str(e),
        )
    finally:
        await service.close()


if __name__ == "__main__":
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())
