import feedparser
import hashlib
import logging
from datetime import datetime
from typing import List, Dict, Optional
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)

DEFAULT_FEEDS = {
    "Yahoo Finance":  "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US",
    "Reuters":        "https://feeds.reuters.com/reuters/businessNews",
    "BBC Business":   "https://feeds.bbci.co.uk/news/business/rss.xml",
    "CNBC":           "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "MarketWatch":    "https://feeds.marketwatch.com/marketwatch/topstories/",
}

def _parse_date(entry):
    for attr in ("published", "updated"):
        raw = getattr(entry, attr, None)
        if raw:
            try:
                return parsedate_to_datetime(raw).replace(tzinfo=None)
            except Exception:
                pass
    return None

def _make_guid(entry, source):
    raw = getattr(entry, "id", None) or getattr(entry, "link", None) or entry.title
    return hashlib.sha256(f"{source}::{raw}".encode()).hexdigest()

def fetch_feed(name, url, max_articles=50):
    articles = []
    try:
        logger.info(f"[Extractor] Fetching {name}")
        feed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
        for entry in feed.entries[:max_articles]:
            title = getattr(entry, "title", "").strip()
            if not title:
                continue
            summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
            articles.append({
                "guid":         _make_guid(entry, name),
                "title":        title,
                "summary":      summary[:2000],
                "link":         getattr(entry, "link", ""),
                "source":       name,
                "published_at": _parse_date(entry),
            })
    except Exception as exc:
        logger.error(f"[Extractor] Failed {name}: {exc}")
    return articles

def extract_all_feeds(feeds=None, max_per_feed=50):
    feeds = feeds or DEFAULT_FEEDS
    seen, all_articles = set(), []
    for name, url in feeds.items():
        for article in fetch_feed(name, url, max_per_feed):
            if article["guid"] not in seen:
                seen.add(article["guid"])
                all_articles.append(article)
    logger.info(f"[Extractor] Total unique articles: {len(all_articles)}")
    return all_articles