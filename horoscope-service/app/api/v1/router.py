from uuid import UUID

from fastapi import APIRouter, HTTPException

from schemas.horoscope import HoroscopeRequestSchema
from infrastructure.kafka.producer import kafka_producer
from lifespan import task_repository


router = APIRouter(prefix="/api/v1/horoscope")


@router.post(path="/")
async def create_horoscope(data: HoroscopeRequestSchema):
    await task_repository.create(request_id=data.request_id)
    await kafka_producer.send(data, "horoscope-event")
    return {"request_id": str(data.request_id)}


@router.get(path="/task/{request_id}")
async def get_horoscope_status(request_id: UUID):
    task = await task_repository.get(request_id)

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "request_id": str(request_id),
        "status": task.get("status"),
        "result": (
            int(task["result"])
            if task.get("result")
            else None
        ),
        "error": task.get("error"),
    }
