from schemas.event import EventSchema
from infrastructure.clickhouse.client import ClickHouseClient


class GameEventRepository:

    def __init__(self, client: ClickHouseClient):
        self.client = client

    async def save(self, event: EventSchema):
        self.client.client.insert(
            "game_events",
            [[
                str(event.event_id),
                event.event_type,
                event.time,
                event.text,
            ]],
            column_names=[
                "event_id",
                "event_type",
                "time",
                "text",
            ],
        )
