import logging
from datetime import datetime, timedelta
from typing import List, Dict
from sqlalchemy.orm import Session
from models.database import Article, SentimentResult, PipelineRun, get_session, init_db, get_engine

logger = logging.getLogger(__name__)

class SentimentLoader:
    def __init__(self, db_url=None):
        self.engine = get_engine(db_url)
        init_db(self.engine)
        self.SessionLocal = get_session(self.engine)

    def get_existing_guids(self, guids):
        with self.SessionLocal() as session:
            rows = session.query(Article.guid).filter(Article.guid.in_(guids)).all()
            return {r.guid for r in rows}

    def load_articles(self, articles):
        existing = self.get_existing_guids([a["guid"] for a in articles])
        new = [a for a in articles if a["guid"] not in existing]
        if not new:
            return []
        with self.SessionLocal() as session:
            for a in new:
                session.add(Article(
                    guid=a["guid"], title=a["title"], summary=a.get("summary",""),
                    link=a.get("link",""), source=a["source"],
                    published_at=a.get("published_at"), processed=False,
                ))
            session.commit()
        logger.info(f"[Loader] Inserted {len(new)} new articles.")
        return new

    def load_sentiment_results(self, classified):
        if not classified:
            return 0
        guids = [c["guid"] for c in classified]
        with self.SessionLocal() as session:
            rows = session.query(Article.id, Article.guid).filter(Article.guid.in_(guids)).all()
            guid_to_id = {r.guid: r.id for r in rows}
        inserted = 0
        with self.SessionLocal() as session:
            for c in classified:
                aid = guid_to_id.get(c["guid"])
                if not aid:
                    continue
                session.add(SentimentResult(
                    article_id=aid, article_guid=c["guid"], title=c["title"],
                    source=c["source"], sentiment_label=c["sentiment_label"],
                    sentiment_score=c["sentiment_score"],
                    inference_latency_ms=c.get("inference_latency_ms"),
                    model_name=c.get("model_name",""),
                    published_at=c.get("published_at"),
                ))
                inserted += 1
            session.query(Article).filter(Article.guid.in_(guids)).update(
                {"processed": True}, synchronize_session="fetch"
            )
            session.commit()
        logger.info(f"[Loader] Inserted {inserted} sentiment results.")
        return inserted

    def start_run(self, run_id):
        with self.SessionLocal() as session:
            run = PipelineRun(run_id=run_id, status="running")
            session.add(run)
            session.commit()

    def finish_run(self, run_id, stats, status="success", error=None):
        with self.SessionLocal() as session:
            run = session.query(PipelineRun).filter_by(run_id=run_id).first()
            if run:
                run.finished_at = datetime.utcnow()
                run.status = status
                run.articles_fetched = stats.get("fetched", 0)
                run.articles_new = stats.get("new", 0)
                run.articles_classified = stats.get("classified", 0)
                run.positive_count = stats.get("positive", 0)
                run.negative_count = stats.get("negative", 0)
                run.avg_latency_ms = stats.get("avg_latency_ms")
                run.error_message = error
                session.commit()

    def get_recent_results(self, limit=50, source=None):
        with self.SessionLocal() as session:
            q = session.query(SentimentResult).order_by(SentimentResult.analyzed_at.desc())
            if source:
                q = q.filter(SentimentResult.source == source)
            rows = q.limit(limit).all()
            return [{"id": r.id, "title": r.title, "source": r.source,
                     "sentiment_label": r.sentiment_label,
                     "sentiment_score": round(r.sentiment_score, 4),
                     "analyzed_at": r.analyzed_at.isoformat() if r.analyzed_at else None,
                     "inference_latency_ms": r.inference_latency_ms} for r in rows]

    def get_stats(self):
        with self.SessionLocal() as session:
            total = session.query(SentimentResult).count()
            positive = session.query(SentimentResult).filter_by(sentiment_label="POSITIVE").count()
            negative = session.query(SentimentResult).filter_by(sentiment_label="NEGATIVE").count()
            sources = session.query(SentimentResult.source).distinct().all()
            return {"total_articles": total, "positive": positive, "negative": negative,
                    "positive_pct": round(positive/total*100, 1) if total else 0,
                    "sources": [s[0] for s in sources]}

    def get_pipeline_runs(self, limit=10):
        with self.SessionLocal() as session:
            rows = session.query(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(limit).all()
            return [{"run_id": r.run_id, "status": r.status,
                     "articles_fetched": r.articles_fetched,
                     "articles_classified": r.articles_classified,
                     "avg_latency_ms": r.avg_latency_ms,
                     "started_at": r.started_at.isoformat() if r.started_at else None} for r in rows]