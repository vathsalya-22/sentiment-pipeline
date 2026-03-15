import time
import logging
import os
from transformers import pipeline as hf_pipeline

logger = logging.getLogger(__name__)
MODEL_NAME = os.getenv("MODEL_NAME", "distilbert-base-uncased-finetuned-sst-2-english")
_classifier = None

def get_classifier():
    global _classifier
    if _classifier is None:
        logger.info(f"[Transformer] Loading model: {MODEL_NAME}")
        _classifier = hf_pipeline(
            task="text-classification",
            model=MODEL_NAME,
            truncation=True,
            max_length=512,
        )
    return _classifier

def classify_batch(articles, batch_size=32):
    clf = get_classifier()
    texts = []
    for a in articles:
        combined = f"{a.get('title','')}. {a.get('summary','')[:200]}".strip(". ")
        texts.append(combined[:512])

    results = []
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i:i+batch_size]
        batch_articles = articles[i:i+batch_size]
        t0 = time.perf_counter()
        batch_results = clf(batch_texts)
        latency_ms = (time.perf_counter() - t0) * 1000 / len(batch_texts)
        for article, res in zip(batch_articles, batch_results):
            results.append({
                **article,
                "sentiment_label":      res["label"],
                "sentiment_score":      round(res["score"], 6),
                "inference_latency_ms": round(latency_ms, 2),
                "model_name":           MODEL_NAME,
            })
        logger.info(f"[Transformer] Batch {i//batch_size+1}: {len(batch_texts)} articles | {latency_ms:.1f}ms each")
    return results