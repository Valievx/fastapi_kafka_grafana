import uuid

from pydantic import BaseModel, Field


class HoroscopeRequestSchema(BaseModel):
    request_id: uuid.UUID = Field(default_factory=uuid.uuid4)
