import os
from pathlib import Path
from typing import Any, Optional, Union

from pydantic_settings import BaseSettings


class Config(BaseSettings):
    ENV: Optional[str] = ""
    HOST: Optional[str] = ""
    PORT: Optional[int] = 0
    PROJECT_NAME: Optional[str] = ""
    MONGODB_URL: Optional[str] = ""
    COLLECTION_NAME: Optional[str] = ""
    RABBITMQ_USERNAME:Optional[str] = ""
    RABBITMQ_PASSWORD:Optional[str] = ""
    RABBITMQ_HOST:Optional[str] = ""
    RABBITMQ_PORT:Optional[str] = ""
    MONGO_HOST:Optional[str] = ""
    MONGO_PORT:Optional[str] = ""
    MONGO_USER:Optional[str] = ""
    MONGO_DB:Optional[str] = ""
    MONGO_PASSWORD:Optional[str] = ""
    SCAN_SETTING_URL:Optional[str] = ""

    def __init__(self, _env_file: Union[Path, str, None], _env_file_encoding: Optional[str] = "", _secrets_dir: Union[Path, str, None] = None, **values: Any) -> None:
        super().__init__(_env_file=_env_file, _env_file_encoding=_env_file_encoding, _secrets_dir=_secrets_dir, **values)
        self.MONGODB_URL = self.get_connection_string()

    @classmethod
    def get_connection_string(cls):
        MONGODB_URL = os.getenv("MONGODB_URL", "")
        if MONGODB_URL:
            return MONGODB_URL

        MONGO_HOST = os.getenv("MONGO_HOST",)
        MONGO_PORT = os.getenv("MONGO_PORT")
        MONGO_USER = os.getenv("MONGO_USER")
        MONGO_DB = os.getenv("MONGO_DB")
        MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")

        return f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"


class LocalConfig(Config):
    pass


class ProductionConfig(Config):
    DEBUG: bool = False


def get_config():
    return Config(".env")


config = get_config()
