from __future__ import annotations

import asyncio
import json
import os
import subprocess
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import Cookie, FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import load_config
from .db import Article, Feed, ReadArticle, Token, get_engine, get_session
from .fetcher import _sse_queues, start_scheduler, stop_scheduler
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
    start_scheduler(config, engine)
    yield
    stop_scheduler()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


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
        response.set_cookie("news_token", new_token, max_age=60 * 60 * 24 * 3650, httponly=False, samesite="lax")
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


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, news_token: Optional[str] = Cookie(default=None)):
    response = templates.TemplateResponse(request, "index.html", {"git_commit": GIT_COMMIT})
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session, response)
        response.set_cookie("news_token", token_obj.token, max_age=60 * 60 * 24 * 3650, httponly=False, samesite="lax")
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

        rows = (
            session.query(Article, Feed)
            .join(Feed)
            .filter(Article.filtered.is_(False))
            .order_by(Article.published_at.desc())
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

        rows = (
            session.query(Article, Feed)
            .join(Feed)
            .filter(Article.filtered.is_(False), Article.id > since_id)
            .order_by(Article.published_at.desc())
            .limit(PAGE_SIZE)
            .all()
        )

        items = [
            {"article": a, "feed": f, "is_read": _is_read(a, token_obj, read_ids)}
            for a, f in rows
        ]

        return templates.TemplateResponse(request, "_articles_prepend.html", {"items": items})


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
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)
        existing = session.get(ReadArticle, (token_obj.token, article_id))
        if not existing:
            session.add(ReadArticle(token=token_obj.token, article_id=article_id))
    return Response(status_code=204)


@app.post("/watermark")
async def set_watermark(news_token: Optional[str] = Cookie(default=None)):
    with get_session(engine) as session:
        token_obj = _ensure_token(news_token, session)
        token_obj.watermark_at = datetime.now(timezone.utc)
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
    response.set_cookie("news_token", token, max_age=60 * 60 * 24 * 3650, httponly=False, samesite="lax")
    return response


@app.post("/token/new")
async def token_new():
    new_token = str(uuid.uuid4())
    with get_session(engine) as session:
        session.add(Token(token=new_token))
    response = RedirectResponse("/", status_code=303)
    response.set_cookie("news_token", new_token, max_age=60 * 60 * 24 * 3650, httponly=False, samesite="lax")
    return response


@app.get("/events")
async def sse_events():
    async def generator():
        q: asyncio.Queue = asyncio.Queue()
        _sse_queues.append(q)
        try:
            yield "data: {}\n\n"
            while True:
                try:
                    data = await asyncio.wait_for(q.get(), timeout=30)
                    yield f"data: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            if q in _sse_queues:
                _sse_queues.remove(q)

    return StreamingResponse(generator(), media_type="text/event-stream")
