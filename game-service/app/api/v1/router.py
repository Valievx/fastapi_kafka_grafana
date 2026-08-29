import time

from fastapi import  APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/v1")


@router.get(path="/")
async def process():
    time.sleep(2)
    return JSONResponse(content={"status": True}, status_code=200)
