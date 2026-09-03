import pathlib

from pydantic_settings import BaseSettings

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    DEBUG: bool

    # Kafka
    KAFKA_URL: str

    # Redis
    REDIS_URL: str

    class Config:
        env_file = BASE_DIR / ".env"
        env_file_encoding = "utf-8"


settings = Settings()
