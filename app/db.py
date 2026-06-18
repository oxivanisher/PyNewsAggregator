from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer, String, Text,
    UniqueConstraint, create_engine, text,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship


class Base(DeclarativeBase):
    pass


class Feed(Base):
    __tablename__ = "feeds"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False, unique=True)
    check_interval = Column(Integer, nullable=False)
    read_mode = Column(String, nullable=False, default="expand")
    last_fetched_at = Column(DateTime, nullable=True)
    http_etag = Column(String, nullable=True)
    http_modified = Column(String, nullable=True)

    articles = relationship("Article", back_populates="feed", cascade="all, delete-orphan")


class Article(Base):
    __tablename__ = "articles"
    __table_args__ = (UniqueConstraint("feed_id", "guid"),)

    id = Column(Integer, primary_key=True)
    feed_id = Column(Integer, ForeignKey("feeds.id"), nullable=False)
    guid = Column(String, nullable=False)
    title = Column(String, nullable=False)
    link = Column(String, nullable=True)
    published_at = Column(DateTime, nullable=False)
    summary = Column(Text, nullable=True)
    content = Column(Text, nullable=True)
    filtered = Column(Boolean, default=False, nullable=False)

    feed = relationship("Feed", back_populates="articles")


class Token(Base):
    __tablename__ = "tokens"

    token = Column(String, primary_key=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_seen_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    watermark_at = Column(DateTime, nullable=True)

    read_articles = relationship("ReadArticle", back_populates="token_obj", cascade="all, delete-orphan")


class ReadArticle(Base):
    __tablename__ = "read_articles"

    token = Column(String, ForeignKey("tokens.token"), primary_key=True)
    article_id = Column(Integer, ForeignKey("articles.id"), primary_key=True)
    read_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    token_obj = relationship("Token", back_populates="read_articles")


class HiddenFeed(Base):
    __tablename__ = "hidden_feeds"

    token = Column(String, ForeignKey("tokens.token", ondelete="CASCADE"), primary_key=True)
    feed_id = Column(Integer, ForeignKey("feeds.id", ondelete="CASCADE"), primary_key=True)


def _migrate(engine) -> None:
    """Add columns that didn't exist in earlier versions of the schema."""
    migrations = [
        ("feeds", "http_etag", "TEXT"),
        ("feeds", "http_modified", "TEXT"),
    ]
    with engine.connect() as conn:
        for table, column, definition in migrations:
            existing = {row[1] for row in conn.execute(text(f"PRAGMA table_info({table})"))}
            if column not in existing:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {definition}"))
        conn.commit()


def get_engine():
    db_path = os.environ.get("DB_PATH", "data/news.db")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    _migrate(engine)
    return engine


@contextmanager
def get_session(engine):
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
