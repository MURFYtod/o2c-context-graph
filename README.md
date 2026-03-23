# Order-to-Cash Context Graph (Assignment)

This project implements a graph-based data modeling + conversational query system for fragmented business data (orders, deliveries, invoices, payments, and related entities).

## What is implemented

- Graph construction from CSV dataset into a context graph
- Graph visualization UI with node expansion/pan/inspect
- LLM-assisted natural language to SQL query flow (OpenRouter free tier supported)
- Data-backed answers from SQLite execution results
- Guardrails to reject off-domain prompts

## Architecture

- **Backend**: FastAPI
- **Storage**: SQLite (loaded from CSVs)
- **Graph modeling**: NetworkX in-memory graph with typed node/edge relationships
- **Frontend**: static HTML/JS using `vis-network` for graph visualization
- **LLM translation**:
  - Primary: OpenRouter API (`OPENROUTER_API_KEY`)
  - Fallback: deterministic SQL templates for key example questions

## Project structure

- `app/main.py` : API + static UI entrypoint
- `app/services/data_loader.py` : CSV ingestion and table normalization
- `app/services/graph_service.py` : graph modeling and relationship derivation
- `app/services/query_service.py` : NL query guardrails + SQL generation + execution
- `app/static/` : graph + chat UI
- `data/` : place dataset CSV files here

## Setup

1. Create and activate a virtual env
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Download dataset (recommended):
   - `python scripts/download_dataset.py`
   - This downloads the Google Drive dataset and copies CSVs into `data/`
4. Alternative manual way:
   - Put dataset files (`.csv` or `.jsonl`) under `data/` (nested folders are supported)
5. Optional (for dynamic NL-to-SQL via LLM):
   - copy `.env.example` to `.env`
   - set `OPENROUTER_API_KEY`
6. Run:
   - `uvicorn app.main:app --reload --port 8001`
7. Open:
   - `http://127.0.0.1:8001`

## Deploy (Render - Free Tier)

1. Push this repository to GitHub.
2. In Render, click **New +** -> **Blueprint**.
3. Select your GitHub repo (Render will detect `Task/render.yaml`).
4. In service environment variables, set:
   - `OPENROUTER_API_KEY` = your key
   - (optional) `OPENROUTER_MODEL` override
5. Deploy.

Notes:
- On first startup, the app auto-downloads the dataset from Google Drive if `data/` is empty.
- Startup may take longer on first deploy due to data bootstrap + SQLite load.

## Expected input naming

The loader maps common file names to these canonical tables:

- `orders`
- `order_items`
- `deliveries`
- `invoices`
- `payments`
- `customers`
- `products`
- `address`

Aliases like `sales_order_headers`, `sales_order_items`, `billing_document_headers`, `outbound_delivery_headers`, `payments_accounts_receivable`, and singular forms are normalized automatically where possible.

## Guardrails behavior

If the user asks unrelated questions (e.g., creative writing, general knowledge), the assistant returns:

`This system is designed to answer questions related to the provided dataset only.`

## Example supported queries

- Which products are associated with the highest number of billing documents?
- Trace the full flow of a given billing document.
- Identify sales orders with incomplete flow.

## Notes for submission

For final submission, add:
- deployed demo URL
- public GitHub repo URL
- exported AI session transcripts/logs
