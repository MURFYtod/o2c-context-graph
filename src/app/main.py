from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

from app.services.bootstrap_data import ensure_dataset_present
from app.services.data_loader import load_csvs_to_sqlite
from app.services.graph_service import build_context_graph
from app.services.query_service import answer_question


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "data" / "o2c.db"
STATIC_DIR = BASE_DIR / "app" / "static"
load_dotenv(BASE_DIR / ".env")

app = FastAPI(title="Order-to-Cash Context Graph")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class ChatRequest(BaseModel):
    question: str


@app.on_event("startup")
def startup_ingest() -> None:
    ensure_dataset_present(DATA_DIR, BASE_DIR)
    inputs = list(DATA_DIR.rglob("*.csv")) + list(DATA_DIR.rglob("*.jsonl"))
    if inputs:
        load_csvs_to_sqlite(DATA_DIR, DB_PATH)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/api/reload")
def reload_data() -> dict:
    tables = load_csvs_to_sqlite(DATA_DIR, DB_PATH)
    return {"ok": True, "loaded_tables": tables}


@app.get("/api/graph")
def get_graph() -> dict:
    result = build_context_graph(DB_PATH)
    return {"nodes": result.nodes, "edges": result.edges, "counts": result.counts}


@app.post("/api/chat")
def chat(req: ChatRequest) -> dict:
    return answer_question(DB_PATH, req.question)
