from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding, NoEncryption, PrivateFormat, PublicFormat,
)
from fastapi import Cookie, FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import load_config
from .db import Article, Feed, HiddenFeed, PushSubscription, ReadArticle, Token, get_engine, get_session
from .fetcher import _sse_queues, broadcast_to_token, start_scheduler, stop_scheduler
import app.fetcher as fetcher_module

PAGE_SIZE = 20

config = load_config()
engine = get_engine()


def _resolve_git_commit() -> str:
    sha = os.environ.get("GIT_COMMIT", "")
    if sha:
        return sha[:7]
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "dev"


GIT_COMMIT = _resolve_git_commit()

BASE_DIR = Path(__file__).parent


# ── VAPID keys ────────────────────────────────────────────────────────────────

def _load_or_create_vapid_keys() -> tuple[str, str]:
    """Return (private_key_pem, public_key_base64url), generating and persisting if needed."""
    db_path = os.environ.get("DB_PATH", "data/news.db")
    keys_path = Path(os.path.dirname(db_path)) / "vapid_keys.json"
    if keys_path.exists():
        data = json.loads(keys_path.read_text())
        return data["private_key"], data["public_key"]

    private_key = ec.generate_private_key(ec.SECP256R1())
    private_pem = private_key.private_bytes(
        Encoding.PEM, PrivateFormat.TraditionalOpenSSL, NoEncryption()
    ).decode()
    pub_bytes = private_key.public_key().public_bytes(
        Encoding.X962, PublicFormat.UncompressedPoint
    )
    public_b64url = base64.urlsafe_b64encode(pub_bytes).rstrip(b"=").decode()

    keys_path.parent.mkdir(parents=True, exist_ok=True)
    keys_path.write_text(json.dumps({"private_key": private_pem, "public_key": public_b64url}))
    return private_pem, public_b64url


VAPID_PRIVATE_KEY, VAPID_PUBLIC_KEY = _load_or_create_vapid_keys()
VAPID_CONTACT = os.environ.get("VAPID_CONTACT", "mailto:noreply@localhost")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _time_ago(dt: Optional[datetime]) -> str:
    if dt is None:
        return "unknown"
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    diff = now - dt
    s = int(diff.total_seconds())
    if s < 60:
        return "just now"
    if s < 3600:
        return f"{s // 60}min ago"
    if s < 86400:
        return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"


templates.env.filters["time_ago"] = _time_ago


@asynccontextmanager
async def lifespan(app: FastAPI):
    fetcher_module._event_loop = asyncio.get_running_loop()
    fetcher_module._vapid_private_key = VAPID_PRIVATE_KEY
    fetcher_module._vapid_claims = {"sub": VAPID_CONTACT}
    fetcher_module._push_engine = engine
    start_scheduler(config, engine)
    yield
    stop_scheduler()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/sw.js")
async def service_worker():
    content = (BASE_DIR / "static" / "sw.js").read_text()
    return Response(content, media_type="application/javascript",
                    headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-cache"})


# ── token helpers ─────────────────────────────────────────────────────────────

def _ensure_token(token_val: Optional[str], session, response: Optional[Response] = None) -> Token:
    if token_val:
        token_obj = session.get(Token, token_val)
        if token_obj:
            token_obj.last_seen_at = datetime.now(timezone.utc)
            return token_obj

    new_token = str(uuid.uuid4())
    token_obj = Token(token=new_token)
    session.add(token_obj)
    session.flush()
    if response:
        response.set_cookie("news_token", new_token, max_age=60 * 60 * 24 * 3650, httponly=True, samesite="lax")
    return token_obj


def _is_read(article: Article, token_obj: Optional[Token], read_ids: set[int]) -> bool:
    if token_obj is None:
        return False
    if article.id in read_ids:
        return True
    if token_obj.watermark_at and article.published_at:
        pub = article.published_at
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        wm = token_obj.watermark_at
        if wm.tzinfo is None:
            wm = wm.replace(tzinfo=timezone.utc)
        return pub <= wm
    return False


def _hidden_feed_subq(token: str, session):
    return (
        session.query(HiddenFeed.feed_id)
        .filter(HiddenFeed.token == token)
        .scalar_subquery()
    )


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, news_token: Optional[str] = Cookie(default=None)):
    response = templates.TemplateResponse(request, "index.html", {"git_commit": GIT_COMMIT})
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session, response)
        response.set_cookie("news_token", token_obj.token, max_age=60 * 60 * 24 * 3650, httponly=True, samesite="lax")
    return response


@app.get("/articles", response_class=HTMLResponse)
async def articles(
    request: Request,
    offset: int = 0,
    watermark_shown: int = 0,
    news_token: Optional[str] = Cookie(default=None),
):
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)

        read_ids: set[int] = {
            r.article_id
            for r in session.query(ReadArticle).filter(ReadArticle.token == token_obj.token)
        }

        hidden_subq = _hidden_feed_subq(token_obj.token, session)
        rows = (
            session.query(Article, Feed)
            .join(Feed)
            .filter(Article.filtered.is_(False), ~Article.feed_id.in_(hidden_subq))
            .order_by(Article.published_at.desc(), Article.id.desc())
            .offset(offset)
            .limit(PAGE_SIZE + 1)
            .all()
        )

        has_more = len(rows) > PAGE_SIZE
        rows = rows[:PAGE_SIZE]

        items = []
        divider_idx = None
        for i, (article, feed) in enumerate(rows):
            is_read = _is_read(article, token_obj, read_ids)
            if not watermark_shown and is_read and divider_idx is None:
                divider_idx = i
            items.append({"article": article, "feed": feed, "is_read": is_read})

        next_watermark_shown = 1 if (watermark_shown or divider_idx is not None) else 0

        return templates.TemplateResponse(
            request,
            "_articles.html",
            {
                "items": items,
                "divider_idx": divider_idx,
                "has_more": has_more,
                "next_offset": offset + len(items),
                "watermark_shown": next_watermark_shown,
            },
        )


@app.get("/articles/prepend", response_class=HTMLResponse)
async def articles_prepend(
    request: Request,
    since_id: int = 0,
    news_token: Optional[str] = Cookie(default=None),
):
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)

        read_ids: set[int] = {
            r.article_id
            for r in session.query(ReadArticle).filter(ReadArticle.token == token_obj.token)
        }

        hidden_subq = _hidden_feed_subq(token_obj.token, session)
        rows = (
            session.query(Article, Feed)
            .join(Feed)
            .filter(Article.filtered.is_(False), Article.id > since_id, ~Article.feed_id.in_(hidden_subq))
            .order_by(Article.published_at.desc(), Article.id.desc())
            .limit(PAGE_SIZE)
            .all()
        )

        items = [
            {"article": a, "feed": f, "is_read": _is_read(a, token_obj, read_ids)}
            for a, f in rows
        ]

        return templates.TemplateResponse(request, "_articles_prepend.html", {"items": items})


@app.get("/next-unread")
async def next_unread_article(news_token: Optional[str] = Cookie(default=None)):
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)
        read_ids_subq = (
            session.query(ReadArticle.article_id)
            .filter(ReadArticle.token == token_obj.token)
            .scalar_subquery()
        )
        hidden_subq = _hidden_feed_subq(token_obj.token, session)
        query = session.query(Article).filter(
            Article.filtered.is_(False),
            ~Article.id.in_(read_ids_subq),
            ~Article.feed_id.in_(hidden_subq),
        )
        if token_obj.watermark_at:
            query = query.filter(Article.published_at > token_obj.watermark_at)
        article = query.order_by(Article.published_at.asc(), Article.id.asc()).first()
        return {"id": article.id if article else None}


@app.get("/read-state")
async def read_state(ids: str = "", news_token: Optional[str] = Cookie(default=None)):
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)
        watermark = token_obj.watermark_at.isoformat() + "Z" if token_obj.watermark_at else None

        id_list = [int(p) for p in ids.split(",") if p.strip().lstrip("-").isdigit()]
        if id_list:
            read_ids = [
                r.article_id for r in
                session.query(ReadArticle).filter(
                    ReadArticle.token == token_obj.token,
                    ReadArticle.article_id.in_(id_list),
                )
            ]
        else:
            read_ids = []

        return {"read_ids": read_ids, "watermark": watermark}


@app.get("/unread-count")
async def unread_count(news_token: Optional[str] = Cookie(default=None)):
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)

        read_ids_subq = (
            session.query(ReadArticle.article_id)
            .filter(ReadArticle.token == token_obj.token)
            .scalar_subquery()
        )
        hidden_subq = _hidden_feed_subq(token_obj.token, session)
        query = session.query(Article).filter(
            Article.filtered.is_(False),
            ~Article.id.in_(read_ids_subq),
            ~Article.feed_id.in_(hidden_subq),
        )
        if token_obj.watermark_at:
            query = query.filter(Article.published_at > token_obj.watermark_at)

        return {"unread": query.count()}


@app.get("/feeds")
async def list_feeds(news_token: Optional[str] = Cookie(default=None)):
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)
        hidden_ids = {
            row[0] for row in session.query(HiddenFeed.feed_id)
            .filter(HiddenFeed.token == token_obj.token)
        }
        feeds = session.query(Feed).order_by(Feed.name).all()
        result = []
        for feed in feeds:
            count = (
                session.query(Article)
                .filter(Article.feed_id == feed.id, Article.filtered.is_(False))
                .count()
            )
            result.append({
                "id": feed.id,
                "name": feed.name,
                "url": feed.url,
                "check_interval": feed.check_interval,
                "read_mode": feed.read_mode,
                "last_fetched_at": feed.last_fetched_at.isoformat() + "Z" if feed.last_fetched_at else None,
                "article_count": count,
                "hidden": feed.id in hidden_ids,
            })
        return result


@app.post("/feeds/{feed_id}/toggle")
async def toggle_feed_hidden(feed_id: int, news_token: Optional[str] = Cookie(default=None)):
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)
        existing = session.get(HiddenFeed, (token_obj.token, feed_id))
        if existing:
            session.delete(existing)
            return {"hidden": False}
        session.add(HiddenFeed(token=token_obj.token, feed_id=feed_id))
        return {"hidden": True}


@app.get("/status")
async def status():
    try:
        jobs = fetcher_module._scheduler.get_jobs()
        next_runs = [j.next_run_time for j in jobs if j.next_run_time]
        if not next_runs:
            return {"next_refresh_in": None}
        soonest = min(next_runs)
        now = datetime.now(soonest.tzinfo)
        seconds = max(0, int((soonest - now).total_seconds()))
        return {"next_refresh_in": seconds}
    except Exception:
        return {"next_refresh_in": None}


@app.post("/read/{article_id}")
async def mark_read(article_id: int, news_token: Optional[str] = Cookie(default=None)):
    token_str: str = ""
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)
        token_str = token_obj.token
        existing = session.get(ReadArticle, (token_str, article_id))
        if not existing:
            session.add(ReadArticle(token=token_str, article_id=article_id))
    await broadcast_to_token(token_str, {"read_article": article_id})
    return Response(status_code=204)


@app.post("/watermark")
async def set_watermark(news_token: Optional[str] = Cookie(default=None)):
    token_str: str = ""
    watermark_ts: datetime
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)
        token_str = token_obj.token
        watermark_ts = datetime.now(timezone.utc)
        token_obj.watermark_at = watermark_ts
    await broadcast_to_token(token_str, {"watermark": watermark_ts.isoformat()})
    return RedirectResponse("/", status_code=303)


@app.get("/token/export")
async def token_export(news_token: Optional[str] = Cookie(default=None)):
    return {"token": news_token or ""}


@app.post("/token/import")
async def token_import(response: Response, token: str = Form(...)):
    token = token.strip()
    try:
        uuid.UUID(token)
    except ValueError:
        return Response("Invalid token format", status_code=400)

    with get_session(engine) as session:
        existing = session.get(Token, token)
        if not existing:
            session.add(Token(token=token))

    response = RedirectResponse("/", status_code=303)
    response.set_cookie("news_token", token, max_age=60 * 60 * 24 * 3650, httponly=True, samesite="lax")
    return response


@app.post("/token/new")
async def token_new():
    new_token = str(uuid.uuid4())
    with get_session(engine) as session:
        session.add(Token(token=new_token))
    response = RedirectResponse("/", status_code=303)
    response.set_cookie("news_token", new_token, max_age=60 * 60 * 24 * 3650, httponly=True, samesite="lax")
    return response


@app.get("/vapid-public-key")
async def vapid_public_key():
    return {"public_key": VAPID_PUBLIC_KEY}


@app.post("/push/subscribe")
async def push_subscribe(request: Request, news_token: Optional[str] = Cookie(default=None)):
    data = await request.json()
    endpoint = data.get("endpoint", "")
    keys = data.get("keys", {})
    p256dh = keys.get("p256dh", "")
    auth = keys.get("auth", "")
    if not endpoint or not p256dh or not auth:
        return Response("Missing fields", status_code=400)
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)
        existing = session.query(PushSubscription).filter(
            PushSubscription.endpoint == endpoint
        ).first()
        if existing:
            existing.token = token_obj.token
            existing.p256dh = p256dh
            existing.auth = auth
        else:
            session.add(PushSubscription(
                token=token_obj.token, endpoint=endpoint, p256dh=p256dh, auth=auth,
            ))
    return {"ok": True}


@app.delete("/push/subscribe")
async def push_unsubscribe(request: Request):
    data = await request.json()
    endpoint = data.get("endpoint", "")
    if not endpoint:
        return Response("Missing endpoint", status_code=400)
    with get_session(engine) as session:
        session.query(PushSubscription).filter(
            PushSubscription.endpoint == endpoint
        ).delete(synchronize_session=False)
    return {"ok": True}


@app.get("/events")
async def sse_events(news_token: Optional[str] = Cookie(default=None)):
    token_str = news_token or ""

    async def generator():
        q: asyncio.Queue = asyncio.Queue()
        entry = (token_str, q)
        _sse_queues.append(entry)
        try:
            yield "data: {}\n\n"
            while True:
                try:
                    data = await asyncio.wait_for(q.get(), timeout=30)
                    yield f"data: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            if entry in _sse_queues:
                _sse_queues.remove(entry)

    return StreamingResponse(generator(), media_type="text/event-stream")
