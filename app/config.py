from __future__ import annotations

import os
from enum import Enum
from typing import Optional

import yaml
from pydantic import BaseModel


class FilterType(str, Enum):
    substring = "substring"
    regex = "regex"


class ReadMode(str, Enum):
    expand = "expand"
    scroll = "scroll"
    load = "load"


class FilterConfig(BaseModel):
    type: FilterType = FilterType.substring
    pattern: str


class FeedDefaults(BaseModel):
    check_interval: int = 3600
    max_articles: int = 500
    read_mode: ReadMode = ReadMode.expand


class FeedConfig(BaseModel):
    name: str
    url: str
    check_interval: Optional[int] = None
    max_articles: Optional[int] = None
    read_mode: Optional[ReadMode] = None
    filters: list[FilterConfig] = []


class AppConfig(BaseModel):
    defaults: FeedDefaults = FeedDefaults()
    filters: list[FilterConfig] = []
    feeds: list[FeedConfig] = []


_config: Optional[AppConfig] = None


def load_config(path: Optional[str] = None) -> AppConfig:
    global _config
    if path is None:
        path = os.environ.get("CONFIG_PATH", "config.yaml")
    with open(path) as f:
        data = yaml.safe_load(f)
    _config = AppConfig.model_validate(data or {})
    return _config


def get_config() -> AppConfig:
    if _config is None:
        return load_config()
    return _config
