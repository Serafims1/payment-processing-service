from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    api_key: SecretStr
    database_url: SecretStr
    rabbitmq_url: SecretStr
    webhook_timeout: float = Field(default=5, gt=0)
    retry_base: float = Field(default=1, ge=0)
    relay_interval: float = Field(default=1, gt=0)
