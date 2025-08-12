import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL","")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL","INFO")
    POLL_SECONDS: int = int(os.getenv("POLL_SECONDS","20"))
    SUBREDDITS: str | None = os.getenv("SUBREDDITS")
    FOURCHAN_BOARDS: str = os.getenv("FOURCHAN_BOARDS","biz")
    SERVICE_NAME: str = os.getenv("SERVICE_NAME","service")

    class Config:
        env_file = ".env"

