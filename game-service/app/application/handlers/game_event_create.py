from schemas.event import EventSchema
from infrastructure.repositories.game_event_repository import GameEventRepository


class GameEventHandler:

    def __init__(self, repository: GameEventRepository):
        self.repository = repository

    async def handle(self, event: dict):
        event = EventSchema.model_validate(event)
        await self.repository.save(event)
