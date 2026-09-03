from contextlib import asynccontextmanager

from infrastructure.redis.client import redis_client
from infrastructure.kafka.producer import kafka_producer
from lifespan import horoscope_consumer
from api.v1 import router

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app):
    await kafka_producer.start()
    await horoscope_consumer.start()

    yield

    await horoscope_consumer.stop()
    await kafka_producer.stop()
    await redis_client.close()


def create_app() -> FastAPI:
    app = FastAPI(
        debug=True,
        docs_url="/api/docs",
        lifespan=lifespan
    )
    app.include_router(router.router)
    return app
