from application.handlers.horoscope import HoroscopeHandler
from infrastructure.kafka.consumer import KafkaConsumer
from infrastructure.redis.client import redis_client
from infrastructure.repositories.task_repository import TaskRepository

task_repository = TaskRepository(redis=redis_client.client)
horoscope_handler = HoroscopeHandler(repository=task_repository)
horoscope_consumer = KafkaConsumer(group_id="horoscope-service", handler=horoscope_handler.handle)
