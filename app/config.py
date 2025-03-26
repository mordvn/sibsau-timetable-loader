from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False
    ENTITIES_FETCH_INTERVAL: int = 43200
    ANTI_DDOS_FETCH_INTERVAL: int = 0
    MONGODB_URI: str
    RABBITMQ_URI: str
    START_GROUP_ID: int = 1
    END_GROUP_ID: int = 20000
    START_PROFESSOR_ID: int = 1
    END_PROFESSOR_ID: int = 20000

    class Config:
        env_file = ".env"


settings = Settings()
