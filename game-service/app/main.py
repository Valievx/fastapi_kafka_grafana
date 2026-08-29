from fastapi import FastAPI

from api.v1 import router


def create_app() -> FastAPI:
    app = FastAPI(
        debug=True,
        docs_url="/api/docs"
    )
    app.include_router(router.router)
    return app
