from __future__ import annotations

import asyncio
import json
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import feedparser
import nh3
from apscheduler.schedulers.background import BackgroundScheduler

from .config import AppConfig, FeedConfig, FilterConfig, FilterType
from .db import Article, Feed, HiddenFeed, PushSubscription, ReadArticle, get_session

_sse_queues: list[asyncio.Queue] = []
_event_loop: Optional[asyncio.AbstractEventLoop] = None
_scheduler = BackgroundScheduler(daemon=True)
_vapid_private_key: Optional[str] = None
_vapid_claims: Optional[dict] = None
_push_engine = None

FETCH_TIMEOUT = 30  # seconds per feed HTTP request

# Tags and attributes allowed in sanitised feed HTML
_ALLOWED_TAGS = {
    "a", "b", "i", "em", "strong", "code", "pre", "blockquote",
    "p", "br", "hr", "div", "span",
    "ul", "ol", "li",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "img", "figure", "figcaption",
    "table", "thead", "tbody", "tr", "th", "td",
}
_ALLOWED_ATTRS: dict[str, set[str]] = {
    "a":   {"href", "title", "target"},  # "rel" managed by link_rel parameter
    "img": {"src", "alt", "title", "width", "height", "style"},
    "*":   {"class", "style"},
}


def _sanitise_html(html: Optional[str]) -> Optional[str]:
    """Strip unsafe tags/attributes and block non-http(s) URLs in feed HTML."""
    if not html:
        return html
    return nh3.clean(
        html,
        tags=_ALLOWED_TAGS,
        attributes=_ALLOWED_ATTRS,
        url_schemes={"http", "https"},
        link_rel="noopener noreferrer",
    )


def _safe_url(url: Optional[str]) -> Optional[str]:
    """Return url only if it uses http or https; reject javascript: and other schemes."""
    if url and url.startswith(("http://", "https://")):
        return url
    return None


def _fetch_parsed(url: str, etag: Optional[str], modified: Optional[str]) -> feedparser.FeedParserDict:
    """
    Fetch a feed URL with a hard timeout and conditional-GET headers.
    Returns a FeedParserDict; sets result['status'] = 304 for Not Modified.
    """
    req = urllib.request.Request(url)
    req.add_header("User-Agent", feedparser.USER_AGENT)
    if etag:
        req.add_header("If-None-Match", etag)
    if modified:
        req.add_header("If-Modified-Since", modified)

    try:
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            status = resp.status
            headers = dict(resp.headers)
            body = resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 304:
            result = feedparser.FeedParserDict()
            result["status"] = 304
            result["entries"] = []
            return result
        raise

    result = feedparser.parse(body, response_headers=headers)
    result["status"] = status
    return result


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
    combined_filters = global_filters + feed_config.filters

    with get_session(engine) as session:
        feed = session.query(Feed).filter(Feed.url == feed_config.url).first()
        if not feed:
            return

        parsed = _fetch_parsed(feed_config.url, feed.http_etag, feed.http_modified)

        # Persist updated caching headers for the next request
        if getattr(parsed, "etag", None):
            feed.http_etag = parsed.etag
        if getattr(parsed, "modified", None):
            feed.http_modified = parsed.modified

        # 304 Not Modified — nothing to process
        if parsed.get("status") == 304:
            feed.last_fetched_at = datetime.now(timezone.utc)
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
                content = _sanitise_html(entry.content[0].get("value"))

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
                link=_safe_url(entry.get("link")),
                published_at=published_at,
                summary=_sanitise_html(entry.get("summary")),
                content=content,
                filtered=filtered,
            ))
            existing_guids.add(guid)
            if not filtered:
                new_count += 1

        # Prune oldest articles; clean up dependent rows first (SQLite FK enforcement is off)
        total = session.query(Article).filter(Article.feed_id == feed.id).count()
        if total > max_articles:
            oldest_ids = [
                row[0] for row in session.query(Article.id)
                .filter(Article.feed_id == feed.id)
                .order_by(Article.published_at.asc())
                .limit(total - max_articles)
            ]
            session.query(ReadArticle).filter(
                ReadArticle.article_id.in_(oldest_ids)
            ).delete(synchronize_session=False)
            session.query(Article).filter(
                Article.id.in_(oldest_ids)
            ).delete(synchronize_session=False)

        feed.last_fetched_at = datetime.now(timezone.utc)

    if new_count > 0:
        if _event_loop and not _event_loop.is_closed():
            asyncio.run_coroutine_threadsafe(_broadcast(new_count), _event_loop)
        if _vapid_private_key and _push_engine:
            _send_push(new_count, feed_config.name)


def _send_push(count: int, feed_name: str) -> None:
    try:
        from pywebpush import WebPushException, webpush
    except ImportError:
        return

    body = f"{count} new article{'s' if count > 1 else ''} from {feed_name}"
    payload = json.dumps({"title": "📰 News", "body": body})

    with get_session(_push_engine) as session:
        subs = session.query(PushSubscription).all()
        to_delete = []
        for sub in subs:
            try:
                webpush(
                    subscription_info={
                        "endpoint": sub.endpoint,
                        "keys": {"p256dh": sub.p256dh, "auth": sub.auth},
                    },
                    data=payload,
                    vapid_private_key=_vapid_private_key,
                    vapid_claims=_vapid_claims,
                    timeout=10,
                )
            except WebPushException as exc:
                if exc.response is not None and exc.response.status_code in (404, 410):
                    to_delete.append(sub)
            except Exception:
                pass
        for sub in to_delete:
            session.delete(sub)


async def _broadcast(count: int) -> None:
    msg = json.dumps({"new_articles": count})
    for q in list(_sse_queues):  # snapshot so concurrent disconnects don't corrupt iteration
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

        # Delete feeds removed from config; cascade through dependent rows manually
        # because SQLite FK enforcement is disabled and bulk delete skips ORM cascades.
        feeds_to_delete = session.query(Feed).filter(Feed.url.notin_(configured_urls)).all()
        for feed in feeds_to_delete:
            article_ids = [
                row[0] for row in session.query(Article.id).filter(Article.feed_id == feed.id)
            ]
            if article_ids:
                session.query(ReadArticle).filter(
                    ReadArticle.article_id.in_(article_ids)
                ).delete(synchronize_session=False)
            session.query(HiddenFeed).filter(
                HiddenFeed.feed_id == feed.id
            ).delete(synchronize_session=False)
            session.delete(feed)  # ORM cascade removes articles


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
