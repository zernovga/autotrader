from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", case_sensitive=False
    )


class KafkaSettings(CommonSettings):
    bootstrap_servers: str = "kafka:29092"
    topic_raw: str = "market.raw"


class StreamSourceSettings(CommonSettings):
    source_url: str = "wss://example.com/market-stream"


class Settings(CommonSettings):
    kafka: KafkaSettings = KafkaSettings()
    stream: StreamSourceSettings = StreamSourceSettings()

    tinkoff_token: str = ""


settings = Settings()
