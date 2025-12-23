from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT = REPO_ROOT / "data" / "outputs"

app = FastAPI(title="SciSciNet Dartmouth Networks API")

# CORS for local frontend dev (Vite default port 5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=False,  # avoid "*" + credentials issues; enable only if you truly need cookies/auth
    allow_methods=["*"],
    allow_headers=["*"],
)

def read_json(path: Path) -> dict:
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

@app.get("/api/papers_graph")
def papers_graph() -> dict:
    return read_json(OUT / "papers_graph.json")

@app.get("/api/authors_graph")
def authors_graph() -> dict:
    return read_json(OUT / "authors_graph.json")

@app.get("/api/t2_timeline")
def t2_timeline() -> dict:
    return read_json(OUT / "t2_timeline.json")


@app.get("/api/t2_patent_counts_by_year")
def t2_patent_counts_by_year() -> dict:
    return read_json(OUT / "t2_patent_counts_by_year.json")    