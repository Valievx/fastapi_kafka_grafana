from schemas.event import EventSchema
from infrastructure.repositories.game_event_repository import GameEventRepository


class GameEventHandler:

    def __init__(self, repository: GameEventRepository):
        self.repository = repository

    async def handle(self, events: list[dict]):
        events = [EventSchema.model_validate(event) for event in events]
        await self.repository.save(events)
