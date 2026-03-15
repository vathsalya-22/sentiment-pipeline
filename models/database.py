from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()

class Article(Base):
    __tablename__ = "articles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    guid = Column(String(512), unique=True, nullable=False)
    title = Column(String(512), nullable=False)
    summary = Column(Text, nullable=True)
    link = Column(String(1024), nullable=True)
    source = Column(String(128), nullable=False)
    published_at = Column(DateTime, nullable=True)
    ingested_at = Column(DateTime, default=datetime.utcnow)
    processed = Column(Boolean, default=False)

class SentimentResult(Base):
    __tablename__ = "sentiment_results"
    id = Column(Integer, primary_key=True, autoincrement=True)
    article_id = Column(Integer, nullable=False)
    article_guid = Column(String(512), nullable=False)
    title = Column(String(512), nullable=False)
    source = Column(String(128), nullable=False)
    sentiment_label = Column(String(16), nullable=False)
    sentiment_score = Column(Float, nullable=False)
    inference_latency_ms = Column(Float, nullable=True)
    model_name = Column(String(128), nullable=False)
    analyzed_at = Column(DateTime, default=datetime.utcnow)
    published_at = Column(DateTime, nullable=True)

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(64), unique=True, nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    articles_fetched = Column(Integer, default=0)
    articles_new = Column(Integer, default=0)
    articles_classified = Column(Integer, default=0)
    positive_count = Column(Integer, default=0)
    negative_count = Column(Integer, default=0)
    avg_latency_ms = Column(Float, nullable=True)
    status = Column(String(16), default="running")
    error_message = Column(Text, nullable=True)

def get_engine(db_url=None):
    url = db_url or os.getenv("DATABASE_URL") or "sqlite:///./financial_sentiment.db"
    connect_args = {"check_same_thread": False} if "sqlite" in url else {}
    return create_engine(url, connect_args=connect_args, echo=False)

def init_db(engine=None):
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(bind=engine)
    print("[DB] Tables initialized.")
    return engine

def get_session(engine=None):
    if engine is None:
        engine = get_engine()
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)