from __future__ import annotations

import asyncio
import json
import logging
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import feedparser
import nh3
from apscheduler.schedulers.background import BackgroundScheduler

from .config import OTHER_CATEGORY, AppConfig, FeedConfig, FilterConfig, FilterType
from .db import (
    Article, Feed, HiddenCategory, HiddenFeed, PushSubscription, ReadArticle, Token,
    get_session, reclaim_space,
)

logger = logging.getLogger(__name__)

# Each entry is (token, queue); token="" for clients without a cookie.
_sse_queues: list[tuple[str, asyncio.Queue]] = []
_event_loop: Optional[asyncio.AbstractEventLoop] = None
_scheduler = BackgroundScheduler(daemon=True)
_vapid_private_key = None  # py_vapid.Vapid instance, set in main.lifespan
_vapid_claims: Optional[dict] = None
_push_engine = None

FETCH_TIMEOUT = 30  # seconds per feed HTTP request
# Let the push service hold the message while the device is offline/dozing
# (pywebpush defaults to 0, which makes FCM drop it if the phone is asleep).
PUSH_TTL = 12 * 3600

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


# Tracking pixels: <img> sized 0 or 1 in either dimension (e.g. golem.de's
# 1×1 cpx.php). Matched on nh3's output, which always double-quotes attributes.
_TRACKING_PIXEL_RE = re.compile(r'<img\b[^>]*?\s(?:width|height)="[01]"[^>]*>', re.IGNORECASE)


def _strip_tracking_pixels(html: Optional[str]) -> Optional[str]:
    if not html:
        return html
    return _TRACKING_PIXEL_RE.sub("", html)


def _sanitise_html(html: Optional[str]) -> Optional[str]:
    """Strip unsafe tags/attributes, tracking pixels and non-http(s) URLs in feed HTML."""
    if not html:
        return html
    return _strip_tracking_pixels(nh3.clean(
        html,
        tags=_ALLOWED_TAGS,
        attributes=_ALLOWED_ATTRS,
        url_schemes={"http", "https"},
        link_rel="noopener noreferrer",
    ))


def strip_stored_tracking_pixels(engine) -> None:
    """One-off cleanup for articles stored before tracking pixels were stripped on fetch."""
    with get_session(engine) as session:
        rows = session.query(Article).filter(
            Article.summary.contains("<img") | Article.content.contains("<img")
        )
        for article in rows:
            article.summary = _strip_tracking_pixels(article.summary)
            article.content = _strip_tracking_pixels(article.content)


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


# (category name, regex for title + tags, regex for the link); either regex may be None
CategoryRules = list[tuple[str, Optional[re.Pattern], Optional[re.Pattern]]]


def _combine(patterns: list[str]) -> Optional[re.Pattern]:
    if not patterns:
        return None
    return re.compile("|".join(f"(?:{p})" for p in patterns), re.IGNORECASE)


def compile_category_rules(config: AppConfig) -> CategoryRules:
    """One combined case-insensitive regex per category and field, in config order."""
    return [
        (c.name, _combine(c.patterns), _combine(c.link_patterns))
        for c in config.categories if c.patterns or c.link_patterns
    ]


def categorize(
    title: str, tags: Optional[str], link: Optional[str], rules: CategoryRules, fallback: Optional[str],
) -> str:
    text = f"{title}\n{tags}" if tags else title
    for name, text_regex, link_regex in rules:
        if text_regex and text_regex.search(text):
            return name
        if link_regex and link and link_regex.search(link):
            return name
    return fallback or OTHER_CATEGORY


def recategorize_articles(config: AppConfig, engine, rules: CategoryRules) -> None:
    """Re-apply the category rules to stored articles, so config edits take effect retroactively."""
    fallback_by_url = {fc.url: fc.category for fc in config.feeds}
    with get_session(engine) as session:
        rows = session.query(
            Article.id, Article.title, Article.tags, Article.link, Article.category, Feed.url,
        ).join(Feed)
        for article_id, title, tags, link, current, feed_url in rows.all():
            category = categorize(title, tags, link, rules, fallback_by_url.get(feed_url))
            if category != current:
                session.query(Article).filter(Article.id == article_id).update(
                    {Article.category: category}, synchronize_session=False
                )


def _entry_tags(entry) -> Optional[str]:
    terms = [t.get("term") for t in entry.get("tags", []) if t.get("term")]
    return ", ".join(terms) or None


def fetch_feed(
    feed_config: FeedConfig, engine, global_filters: list[FilterConfig], max_articles: int,
    category_rules: CategoryRules,
) -> None:
    """max_articles is a global cap enforced across all feeds combined, not per feed."""
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

        feed_id = feed.id
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
            tags = _entry_tags(entry)
            link = _safe_url(entry.get("link"))

            session.add(Article(
                feed_id=feed.id,
                guid=guid,
                title=title,
                link=link,
                published_at=published_at,
                summary=_sanitise_html(entry.get("summary")),
                content=content,
                filtered=filtered,
                tags=tags,
                category=categorize(title, tags, link, category_rules, feed_config.category),
            ))
            existing_guids.add(guid)
            if not filtered:
                new_count += 1

        # Prune oldest articles across all feeds; clean up dependent rows first
        # (SQLite FK enforcement is off)
        total = session.query(Article).count()
        pruned = total > max_articles
        if pruned:
            oldest_ids = [
                row[0] for row in session.query(Article.id)
                .order_by(Article.published_at.asc(), Article.id.asc())
                .limit(total - max_articles)
            ]
            session.query(ReadArticle).filter(
                ReadArticle.article_id.in_(oldest_ids)
            ).delete(synchronize_session=False)
            session.query(Article).filter(
                Article.id.in_(oldest_ids)
            ).delete(synchronize_session=False)

        feed.last_fetched_at = datetime.now(timezone.utc)

    # Deleted rows only free pages inside the file; reclaim them so the
    # database file actually shrinks (needs auto_vacuum=INCREMENTAL, set in db.py).
    if pruned:
        reclaim_space(engine)

    if new_count > 0:
        if _event_loop and not _event_loop.is_closed():
            asyncio.run_coroutine_threadsafe(_broadcast(new_count), _event_loop)
        if _vapid_private_key and _push_engine:
            import threading
            threading.Thread(target=_send_push, args=(feed_id,), daemon=True).start()


def _unread_for_token(token: str, session) -> int:
    token_obj = session.get(Token, token)
    if not token_obj:
        return 0
    read_ids_subq = (
        session.query(ReadArticle.article_id)
        .filter(ReadArticle.token == token)
        .scalar_subquery()
    )
    hidden_subq = (
        session.query(HiddenFeed.feed_id)
        .filter(HiddenFeed.token == token)
        .scalar_subquery()
    )
    hidden_cat_subq = (
        session.query(HiddenCategory.category)
        .filter(HiddenCategory.token == token)
        .scalar_subquery()
    )
    query = session.query(Article).filter(
        Article.filtered.is_(False),
        ~Article.id.in_(read_ids_subq),
        ~Article.feed_id.in_(hidden_subq),
        ~Article.category.in_(hidden_cat_subq),
    )
    if token_obj.watermark_at:
        query = query.filter(Article.published_at > token_obj.watermark_at)
    if token_obj.reading_article_id is not None:
        query = query.filter(Article.id != token_obj.reading_article_id)
    return query.count()


def _send_push(feed_id: int) -> None:
    try:
        from pywebpush import WebPushException, webpush
    except ImportError:
        return

    with get_session(_push_engine) as session:
        subs = session.query(PushSubscription).all()
        to_delete = []
        hiding = {
            row[0] for row in session.query(HiddenFeed.token).filter(HiddenFeed.feed_id == feed_id)
        }
        pushed_tokens = set()
        for sub in subs:
            if sub.token in hiding:
                continue  # new articles are from a feed this user hides
            token_obj = session.get(Token, sub.token) if sub.token else None
            if token_obj and token_obj.last_pushed_at:
                continue  # already notified; wait until the user comes back
            unread = _unread_for_token(sub.token, session) if sub.token else 0
            if unread == 0:
                continue

            body = f"{unread} unread article{'s' if unread > 1 else ''}"
            payload = json.dumps({"title": "📰 News", "body": body})
            try:
                webpush(
                    subscription_info={
                        "endpoint": sub.endpoint,
                        "keys": {"p256dh": sub.p256dh, "auth": sub.auth},
                    },
                    data=payload,
                    vapid_private_key=_vapid_private_key,
                    vapid_claims=dict(_vapid_claims),  # webpush mutates it (aud per endpoint)
                    ttl=PUSH_TTL,
                    timeout=10,
                )
                if token_obj:
                    pushed_tokens.add(token_obj)
            except WebPushException as exc:
                if exc.response is not None and exc.response.status_code in (404, 410):
                    to_delete.append(sub)
                else:
                    logger.warning("Push failed for %s: %s", sub.endpoint[:60], exc)
            except Exception:
                logger.exception("Push failed for %s", sub.endpoint[:60])
        for sub in to_delete:
            session.delete(sub)
        now = datetime.now(timezone.utc)
        for token_obj in pushed_tokens:
            token_obj.last_pushed_at = now


async def _broadcast(count: int) -> None:
    """Broadcast new-article notification to every connected client (all tokens)."""
    msg = json.dumps({"new_articles": count})
    for _tok, q in list(_sse_queues):
        await q.put(msg)


async def broadcast_to_token(token: str, payload: dict) -> None:
    """Send a read-state update only to queues belonging to this token."""
    msg = json.dumps(payload)
    for tok, q in list(_sse_queues):
        if tok == token:
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
    strip_stored_tracking_pixels(engine)
    category_rules = compile_category_rules(config)
    recategorize_articles(config, engine, category_rules)

    for fc in config.feeds:
        interval = fc.check_interval or config.defaults.check_interval
        _scheduler.add_job(
            fetch_feed,
            "interval",
            seconds=interval,
            args=[fc, engine, config.filters, config.max_articles, category_rules],
            id=f"feed_{fc.url}",
            replace_existing=True,
            next_run_time=datetime.now(),
        )

    _scheduler.start()


def stop_scheduler() -> None:
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
