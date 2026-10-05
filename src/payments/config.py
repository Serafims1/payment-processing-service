from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    api_key: SecretStr
    database_url: SecretStr
    rabbitmq_url: SecretStr
    webhook_timeout: float = 5
    retry_base: float = 1
    relay_interval: float = 1
