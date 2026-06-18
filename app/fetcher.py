from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Optional

import feedparser
from apscheduler.schedulers.background import BackgroundScheduler

from .config import AppConfig, FeedConfig, FilterConfig, FilterType
from .db import Article, Feed, get_session

_sse_queues: list[asyncio.Queue] = []
_event_loop: Optional[asyncio.AbstractEventLoop] = None
_scheduler = BackgroundScheduler(daemon=True)


def is_filtered(title: str, filters: list[FilterConfig]) -> bool:
    for f in filters:
        if f.type == FilterType.substring:
            if f.pattern.lower() in title.lower():
                return True
        elif f.type == FilterType.regex:
            if re.search(f.pattern, title, re.IGNORECASE):
                return True
    return False


def fetch_feed(feed_config: FeedConfig, engine, global_filters: list[FilterConfig], max_articles: int) -> None:
    parsed = feedparser.parse(feed_config.url)

    combined_filters = global_filters + feed_config.filters

    with get_session(engine) as session:
        feed = session.query(Feed).filter(Feed.url == feed_config.url).first()
        if not feed:
            return

        existing_guids = {
            row[0] for row in session.query(Article.guid).filter(Article.feed_id == feed.id)
        }

        new_count = 0
        for entry in parsed.entries:
            guid = entry.get("id") or entry.get("link", "")
            if not guid or guid in existing_guids:
                continue

            content = None
            if hasattr(entry, "content") and entry.content:
                content = entry.content[0].get("value")

            published_at = datetime.now(timezone.utc)
            if entry.get("published_parsed"):
                try:
                    published_at = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                except Exception:
                    pass

            title = entry.get("title", "(no title)")
            filtered = is_filtered(title, combined_filters)

            session.add(Article(
                feed_id=feed.id,
                guid=guid,
                title=title,
                link=entry.get("link"),
                published_at=published_at,
                summary=entry.get("summary"),
                content=content,
                filtered=filtered,
            ))
            existing_guids.add(guid)
            if not filtered:
                new_count += 1

        # Prune oldest articles beyond max_articles
        total = session.query(Article).filter(Article.feed_id == feed.id).count()
        if total > max_articles:
            oldest_ids = [
                row[0] for row in session.query(Article.id)
                .filter(Article.feed_id == feed.id)
                .order_by(Article.published_at.asc())
                .limit(total - max_articles)
            ]
            session.query(Article).filter(Article.id.in_(oldest_ids)).delete(synchronize_session=False)

        feed.last_fetched_at = datetime.now(timezone.utc)

    if new_count > 0 and _event_loop:
        asyncio.run_coroutine_threadsafe(_broadcast(new_count), _event_loop)


async def _broadcast(count: int) -> None:
    msg = json.dumps({"new_articles": count})
    for q in _sse_queues:
        await q.put(msg)


def sync_feeds(config: AppConfig, engine) -> None:
    configured_urls = {fc.url for fc in config.feeds}
    with get_session(engine) as session:
        for fc in config.feeds:
            interval = fc.check_interval or config.defaults.check_interval
            mode = (fc.read_mode or config.defaults.read_mode).value
            feed = session.query(Feed).filter(Feed.url == fc.url).first()
            if feed:
                feed.name = fc.name
                feed.check_interval = interval
                feed.read_mode = mode
            else:
                session.add(Feed(name=fc.name, url=fc.url, check_interval=interval, read_mode=mode))

        session.query(Feed).filter(Feed.url.notin_(configured_urls)).delete(synchronize_session=False)


def start_scheduler(config: AppConfig, engine) -> None:
    sync_feeds(config, engine)

    for fc in config.feeds:
        interval = fc.check_interval or config.defaults.check_interval
        max_art = fc.max_articles or config.defaults.max_articles
        _scheduler.add_job(
            fetch_feed,
            "interval",
            seconds=interval,
            args=[fc, engine, config.filters, max_art],
            id=f"feed_{fc.url}",
            replace_existing=True,
            next_run_time=datetime.now(),
        )

    _scheduler.start()


def stop_scheduler() -> None:
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
