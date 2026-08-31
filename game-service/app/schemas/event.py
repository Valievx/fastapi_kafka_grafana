import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class EventSchema(BaseModel):
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_type: str
    time: datetime = Field(default_factory=datetime.now)
    text: str
