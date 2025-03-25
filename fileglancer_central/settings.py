from typing import List, Dict, Optional
from functools import cache
import sys

from pathlib import Path
from pydantic import HttpUrl, BaseModel
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource
)
from loguru import logger

class Target(BaseModel):
    name: str
    browseable: bool = True
    client: str = "aioboto"
    options: Dict[str,str] = {}


class Settings(BaseSettings):
    """ Settings can be read from a settings.yaml file, 
        or from the environment, with environment variables prepended 
        with "fgc_" (case insensitive). The environment variables can
        be passed in the environment or in a .env file. 
    """

    log_level: str = 'DEBUG'
    db_url: str = 'sqlite:///fileglancer.db'
    confluence_url: HttpUrl = 'https://wikis.janelia.org'
    confluence_token: str
    jira_url: HttpUrl = 'https://issues.hhmi.org/issues'
    jira_token: str

    model_config = SettingsConfigDict(
        yaml_file="config.yaml",
        env_file='.env',
        env_prefix='fgc_',
        env_nested_delimiter="__",
        env_file_encoding='utf-8'
    )

    def __init__(self, **data) -> None:
        try:
            super().__init__(**data)
        except ValueError as e:
            if "confluence_token" in str(e):
                logger.error("Confluence token is required but not provided. Please set FGC_CONFLUENCE_TOKEN environment variable or add it to config.yaml")
            raise e

  
    @classmethod
    def settings_customise_sources(  # noqa: PLR0913
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


@cache
def get_settings():
    return Settings()
