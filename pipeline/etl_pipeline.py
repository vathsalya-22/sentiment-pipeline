import argparse, logging, os, time, uuid
from datetime import datetime
from statistics import mean
import schedule
from pipeline.extractor import extract_all_feeds
from pipeline.transformer import classify_batch
from pipeline.loader import SentimentLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_pipeline(db_url=None):
    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    logger.info(f"[Pipeline] Starting run: {run_id}")
    loader = SentimentLoader(db_url)
    loader.start_run(run_id)
    stats = {}
    try:
        raw_articles = extract_all_feeds()
        stats["fetched"] = len(raw_articles)
        existing = loader.get_existing_guids([a["guid"] for a in raw_articles])
        new_articles = [a for a in raw_articles if a["guid"] not in existing]
        stats["new"] = len(new_articles)
        if not new_articles:
            logger.info("[Pipeline] No new articles.")
            loader.finish_run(run_id, stats)
            return stats
        classified = classify_batch(new_articles)
        stats["classified"] = len(classified)
        stats["positive"] = sum(1 for c in classified if c["sentiment_label"] == "POSITIVE")
        stats["negative"] = sum(1 for c in classified if c["sentiment_label"] == "NEGATIVE")
        latencies = [c["inference_latency_ms"] for c in classified if c.get("inference_latency_ms")]
        stats["avg_latency_ms"] = round(mean(latencies), 2) if latencies else None
        loader.load_articles(new_articles)
        loader.load_sentiment_results(classified)
        loader.finish_run(run_id, stats, status="success")
        logger.info(f"[Pipeline] Done: {stats}")
        return stats
    except Exception as exc:
        logger.error(f"[Pipeline] FAILED: {exc}", exc_info=True)
        loader.finish_run(run_id, stats, status="failed", error=str(exc))
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()
    if args.schedule:
        run_pipeline()
        schedule.every(args.interval).minutes.do(run_pipeline)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_pipeline()