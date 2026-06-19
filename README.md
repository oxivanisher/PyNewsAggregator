# PyNewsAggregator

A self-hosted, containerized RSS/Atom feed reader. No accounts, no database server — just a YAML config file and a SQLite database.

## Features

- Configure feeds in a YAML file — no UI settings
- Global and per-feed headline filters (substring and regex)
- Per-feed configurable polling interval with a global default
- Timeline view, newest first, with infinite scroll
- Click to expand full article content inline; click again to collapse
- Three read modes: on expand, on scroll-past, or on load
- Read-state sync across devices via an exportable/importable token
- "You were here" watermark divider in the timeline
- Real-time banner when new articles arrive (Server-Sent Events)
- Mobile-friendly, dark UI
- SQLite storage — no external database required

## Quick start

```bash
cp config.example.yaml config.yaml
# edit config.yaml to add your feeds

docker compose -f docker-compose.example.yml up -d
```

Then open [http://localhost:8000](http://localhost:8000).

## Configuration

All settings live in `config.yaml` (mounted into the container as read-only). See [`config.example.yaml`](config.example.yaml) for a fully annotated example.

### Top-level keys

| Key | Description |
|-----|-------------|
| `defaults.check_interval` | Feed poll interval in seconds (default: `3600`) |
| `defaults.max_articles` | Articles retained per feed before oldest are pruned (default: `500`) |
| `defaults.read_mode` | How articles are marked read: `expand`, `scroll`, or `load` (default: `expand`) |
| `filters` | List of global headline filters (applied to all feeds) |
| `feeds` | List of feed sources |

### Filter format

```yaml
filters:
  - type: substring   # or: regex
    pattern: "sponsored"
```

### Feed format

```yaml
feeds:
  - name: My Feed
    url: https://example.com/feed.xml
    check_interval: 1800   # optional override
    max_articles: 200       # optional override
    read_mode: scroll       # optional override
    filters:               # optional extra filters for this feed only
      - type: substring
        pattern: "some noise"
```

### Read modes

| Mode | Behaviour |
|------|-----------|
| `expand` | Marked read when you click to open the article (default) |
| `scroll` | Marked read automatically as the article scrolls into view |
| `load` | Marked read immediately when it appears in the timeline |

## Device sync

Your read state is stored server-side, keyed by a token that lives in your browser cookie.

- Click **⇄ Token** in the header to open the token manager
- **Export**: copy the token string and paste it into another device
- **Import**: paste a token from another device to share its read history
- Multiple browsers/devices using the same token share the same read state

## Running locally (without Docker)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp config.example.yaml config.yaml
# edit config.yaml

DB_PATH=data/news.db CONFIG_PATH=config.yaml uvicorn app.main:app --reload
```

## Building locally with Docker

```bash
cp config.example.yaml config.yaml
docker compose up --build
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_PATH` | `config.yaml` | Path to the YAML config file |
| `DB_PATH` | `data/news.db` | Path to the SQLite database file |

## GitHub Container Registry

Images are built automatically on every push to `main` and on version tags (`v*`), for both `linux/amd64` and `linux/arm64`:

```
ghcr.io/oxivanisher/pynewsaggregator:latest
```
