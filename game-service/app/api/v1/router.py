from fastapi import  APIRouter
from fastapi.responses import JSONResponse

from schemas.event import EventSchema
from infrastructure.kafka.producer import kafka_producer

router = APIRouter(prefix="/api/v1")


@router.post(path="/send-event")
async def send_event(data: EventSchema):
    await kafka_producer.send(data)
    return JSONResponse(content={"event_id": str(data.event_id)}, status_code=200)
