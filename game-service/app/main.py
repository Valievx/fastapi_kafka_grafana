import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.v1 import router
from infrastructure.kafka.producer import kafka_producer
from infrastructure.clickhouse.client import clickhouse_client
from lifespan import game_consumers


@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()
    await clickhouse_client.connect()

    await asyncio.gather(*(consumer.start() for consumer in game_consumers))

    yield

    await asyncio.gather(*(consumer.stop() for consumer in game_consumers))

    await clickhouse_client.close()
    await kafka_producer.stop()


def create_app() -> FastAPI:
    app = FastAPI(
        debug=True,
        docs_url="/api/docs",
        lifespan=lifespan
    )
    app.include_router(router.router)
    return app
