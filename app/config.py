from __future__ import annotations

import os
import re
from enum import Enum
from typing import Optional

import yaml
from pydantic import BaseModel, field_validator, model_validator

# Category for articles no rule matched and whose feed has no fallback category
OTHER_CATEGORY = "Other"


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
    read_mode: ReadMode = ReadMode.expand


class CategoryConfig(BaseModel):
    name: str
    # Case-insensitive regexes matched against the title and the feed's own tags
    patterns: list[str] = []
    # Case-insensitive regexes matched against the article URL (e.g. "/sport/")
    link_patterns: list[str] = []

    @field_validator("patterns", "link_patterns")
    @classmethod
    def _patterns_compile(cls, patterns: list[str]) -> list[str]:
        for p in patterns:
            try:
                re.compile(p)
            except re.error as exc:
                raise ValueError(f"invalid regex {p!r}: {exc}") from exc
        return patterns


class FeedConfig(BaseModel):
    name: str
    url: str
    check_interval: Optional[int] = None
    read_mode: Optional[ReadMode] = None
    filters: list[FilterConfig] = []
    # Used when no category rule matches an article from this feed
    category: Optional[str] = None


class AppConfig(BaseModel):
    defaults: FeedDefaults = FeedDefaults()
    max_articles: int = 2000
    filters: list[FilterConfig] = []
    # Checked in order; the first category with a matching pattern wins
    categories: list[CategoryConfig] = []
    feeds: list[FeedConfig] = []

    @model_validator(mode="after")
    def _feed_categories_exist(self) -> "AppConfig":
        known = {c.name for c in self.categories} | {OTHER_CATEGORY}
        for fc in self.feeds:
            if fc.category and fc.category not in known:
                raise ValueError(f"feed {fc.name!r} uses undefined category {fc.category!r}")
        return self

    def category_names(self) -> list[str]:
        """All category names in display order, with the catch-all last."""
        names = [c.name for c in self.categories if c.name != OTHER_CATEGORY]
        return names + [OTHER_CATEGORY]


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
