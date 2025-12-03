from pydantic_settings import BaseSettings, SettingsConfigDict


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",  # можно поддерживать .env
        env_file_encoding="utf-8",
        extra="ignore",
    )


class KafkaSettings(CommonSettings):
    bootstrap_servers: str = "kafka:29092"
    topic_raw: str = "market.raw"


class StreamSourceSettings(CommonSettings):
    source_url: str = "wss://example.com/market-stream"


class Settings(CommonSettings):
    kafka: KafkaSettings = KafkaSettings()
    stream: StreamSourceSettings = StreamSourceSettings()


settings = Settings()
