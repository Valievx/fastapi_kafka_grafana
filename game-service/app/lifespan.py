from application.handlers.game_event_create import GameEventHandler
from infrastructure.kafka.consumer import KafkaConsumer
from infrastructure.repositories.game_event_repository import GameEventRepository
from infrastructure.clickhouse.client import clickhouse_client

CONSUMER_COUNT = 4

game_event_repository = GameEventRepository(client=clickhouse_client)
game_event_handler = GameEventHandler(repository=game_event_repository)


game_consumers = [
    KafkaConsumer(
        consumer_id=i,
        topic="game-events",
        group_id="game-service",
        handler=game_event_handler.handle,
    )
    for i in range(CONSUMER_COUNT)
]
