import os
from datetime import datetime
from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pipeline.loader import SentimentLoader

DB_URL = os.getenv("DATABASE_URL", "sqlite:///./financial_sentiment.db")
app = FastAPI(title="Financial Sentiment API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
loader = SentimentLoader(DB_URL)

DASHBOARD = """<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>Financial Sentiment Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0}
header{background:#1e293b;padding:1.5rem 2rem;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #334155}
header h1{font-size:1.4rem;color:#38bdf8}
.container{max-width:1200px;margin:0 auto;padding:2rem}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:1rem;margin-bottom:2rem}
.card{background:#1e293b;border-radius:12px;padding:1.5rem;border:1px solid #334155}
.card .label{font-size:.75rem;color:#94a3b8;text-transform:uppercase}
.card .value{font-size:2rem;font-weight:700;margin-top:.3rem}
.pos{color:#34d399}.neg{color:#f87171}.neu{color:#38bdf8}
.btn{background:#0ea5e9;color:#fff;border:none;padding:.6rem 1.4rem;border-radius:8px;cursor:pointer;font-size:.9rem;margin-bottom:1.5rem}
table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:12px;overflow:hidden}
th{background:#0f172a;padding:.8rem 1rem;text-align:left;font-size:.75rem;color:#64748b;text-transform:uppercase}
td{padding:.75rem 1rem;border-top:1px solid #334155;font-size:.85rem;max-width:400px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.badge{display:inline-block;padding:.2rem .6rem;border-radius:999px;font-size:.7rem;font-weight:600}
.bp{background:#064e3b;color:#34d399}.bn{background:#450a0a;color:#f87171}
</style></head>
<body>
<header><h1>📈 Financial Sentiment Dashboard</h1><span id="ts">Loading...</span></header>
<div class="container">
<div class="stats">
  <div class="card"><div class="label">Total Articles</div><div class="value neu" id="s-total">—</div></div>
  <div class="card"><div class="label">Positive</div><div class="value pos" id="s-pos">—</div></div>
  <div class="card"><div class="label">Negative</div><div class="value neg" id="s-neg">—</div></div>
  <div class="card"><div class="label">Positive %</div><div class="value pos" id="s-pct">—</div></div>
</div>
<button class="btn" onclick="run()">▶ Run ETL Now</button>
<table><thead><tr><th>Source</th><th>Title</th><th>Sentiment</th><th>Score</th><th>Time</th></tr></thead>
<tbody id="tbody"><tr><td colspan="5" style="text-align:center;color:#64748b">Loading...</td></tr></tbody>
</table></div>
<script>
async function loadStats(){const r=await fetch('/api/stats');const d=await r.json();
  document.getElementById('s-total').textContent=d.total_articles.toLocaleString();
  document.getElementById('s-pos').textContent=d.positive.toLocaleString();
  document.getElementById('s-neg').textContent=d.negative.toLocaleString();
  document.getElementById('s-pct').textContent=d.positive_pct+'%';
  document.getElementById('ts').textContent='Updated: '+new Date().toLocaleTimeString();}
async function loadResults(){const r=await fetch('/api/results?limit=50');const data=await r.json();
  const tb=document.getElementById('tbody');
  if(!data.length){tb.innerHTML='<tr><td colspan="5" style="text-align:center;color:#64748b">No data yet — run the pipeline!</td></tr>';return;}
  tb.innerHTML=data.map(row=>{
    const p=row.sentiment_label==='POSITIVE';
    return`<tr><td>${row.source}</td><td title="${row.title}">${row.title}</td>
    <td><span class="badge ${p?'bp':'bn'}">${row.sentiment_label}</span></td>
    <td>${(row.sentiment_score*100).toFixed(1)}%</td>
    <td>${row.analyzed_at?new Date(row.analyzed_at).toLocaleString():'—'}</td></tr>`;}).join('');}
async function run(){await fetch('/api/run',{method:'POST'});setTimeout(()=>{loadStats();loadResults();},4000);}
loadStats();loadResults();setInterval(()=>{loadStats();loadResults();},60000);
</script></body></html>"""

@app.get("/", response_class=HTMLResponse)
def dashboard(): return HTMLResponse(content=DASHBOARD)

@app.get("/health")
def health(): return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/results")
def results(limit: int = Query(default=50, le=500), source: str = Query(default=None)):
    return loader.get_recent_results(limit=limit, source=source)

@app.get("/api/stats")
def stats(): return loader.get_stats()

@app.get("/api/runs")
def runs(limit: int = Query(default=10)): return loader.get_pipeline_runs(limit=limit)

@app.post("/api/run")
def trigger(background_tasks: BackgroundTasks):
    def _run():
        from pipeline.etl_pipeline import run_pipeline
        run_pipeline(DB_URL)
    background_tasks.add_task(_run)
    return {"message": "Pipeline triggered", "timestamp": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
