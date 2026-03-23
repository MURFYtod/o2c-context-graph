from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from typing import Iterable

import pandas as pd


CANONICAL_TABLES = [
    "orders",
    "order_items",
    "deliveries",
    "invoices",
    "payments",
    "customers",
    "products",
    "address",
]


def _normalize_name(file_name: str) -> str:
    lower = file_name.lower().replace(" ", "_").replace("-", "_")
    stem = Path(lower).stem
    aliases = {
        "sales_orders": "orders",
        "sales_order_headers": "orders",
        "order": "orders",
        "order_item": "order_items",
        "sales_order_items": "order_items",
        "delivery": "deliveries",
        "outbound_delivery_headers": "deliveries",
        "outbound_delivery_items": "delivery_items",
        "invoice": "invoices",
        "billing": "invoices",
        "billing_document_headers": "invoices",
        "billing_document_items": "invoice_items",
        "payment": "payments",
        "payments_accounts_receivable": "payments",
        "journal_entry_items_accounts_receivable": "journal_entries",
        "customer": "customers",
        "business_partners": "customers",
        "customer_company_assignments": "customer_company_assignments",
        "customer_sales_area_assignments": "customer_sales_area_assignments",
        "product": "products",
        "product_descriptions": "product_descriptions",
        "addresses": "address",
        "business_partner_addresses": "address",
    }
    return aliases.get(stem, stem)


def discover_input_files(data_dir: Path) -> Iterable[Path]:
    csvs = list(data_dir.rglob("*.csv"))
    jsonls = list(data_dir.rglob("*.jsonl"))
    return sorted(csvs + jsonls)


def _read_input_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".jsonl":
        return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported input file: {path}")


def _logical_name(path: Path) -> str:
    stem = path.stem.lower()
    if stem.startswith("part-") or stem.startswith("part_"):
        return path.parent.name
    return path.name


def _sanitize_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].apply(
            lambda v: json.dumps(v, ensure_ascii=True)
            if isinstance(v, (dict, list))
            else v
        )
    return df


def load_csvs_to_sqlite(data_dir: Path, sqlite_path: Path) -> list[str]:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    loaded_tables: set[str] = set()
    table_frames: dict[str, list[pd.DataFrame]] = {}

    for input_file in discover_input_files(data_dir):
        table_name = _normalize_name(_logical_name(input_file))
        df = _read_input_file(input_file)
        if df.empty:
            continue
        df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]
        table_frames.setdefault(table_name, []).append(df)

    with sqlite3.connect(sqlite_path) as conn:
        for table_name, frames in table_frames.items():
            combined = pd.concat(frames, ignore_index=True)
            combined = _sanitize_for_sqlite(combined)
            combined.to_sql(table_name, conn, if_exists="replace", index=False)
            loaded_tables.add(table_name)
    return sorted(loaded_tables)
