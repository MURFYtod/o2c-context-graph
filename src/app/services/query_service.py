from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

import requests


SYSTEM_PROMPT = """
You are a domain-safe SQL assistant for an Order-to-Cash dataset.
You only answer questions related to these business entities: orders, order_items, deliveries, invoices, payments, customers, products, address.

Rules:
1) If the prompt is outside this domain, return JSON {"reject": true, "reason": "..."}.
2) Otherwise return JSON {"reject": false, "sql": "...", "answer_template": "..."}.
3) SQL must be read-only SELECT and valid SQLite SQL.
4) Use only existing tables and columns provided in schema.
5) Keep SQL concise; use LIMIT when feasible.
"""


def _off_topic(text: str) -> bool:
    t = text.lower()
    domain_words = [
        "order",
        "delivery",
        "invoice",
        "billing",
        "payment",
        "customer",
        "product",
        "journal",
        "sales",
        "flow",
    ]
    return not any(w in t for w in domain_words)


def _extract_billing_document(text: str) -> str | None:
    match = re.search(r"\bbilling\s+document\s+(\d{6,})\b", text.lower())
    if match:
        return match.group(1)
    return None


def _schema_description(conn: sqlite3.Connection) -> str:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    lines: list[str] = []
    for (table,) in tables:
        cols = conn.execute(f"PRAGMA table_info([{table}])").fetchall()
        col_names = ", ".join(c[1] for c in cols)
        lines.append(f"{table}: {col_names}")
    return "\n".join(lines)


def _call_openrouter_json(user_query: str, schema: str) -> dict[str, Any] | None:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return None
    model = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.1-8b-instruct:free")
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Schema:\n{schema}\n\nQuestion: {user_query}\nReturn JSON only."},
        ],
        "temperature": 0,
    }
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=45,
    )
    if resp.status_code >= 400:
        return None
    content = resp.json()["choices"][0]["message"]["content"]
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, flags=re.DOTALL)
        if match:
            return json.loads(match.group(0))
    return None


def _fallback_sql(user_query: str) -> dict[str, Any]:
    q = user_query.lower()
    if "highest number of billing" in q or ("products" in q and "billing" in q):
        return {
            "reject": False,
            "sql": """
                SELECT oi.material AS product, COUNT(DISTINCT ii.billingdocument) AS billing_docs
                FROM order_items oi
                JOIN delivery_items di
                  ON di.referencesddocument = oi.salesorder
                 AND di.referencesddocumentitem = oi.salesorderitem
                JOIN invoice_items ii
                  ON ii.referencesddocument = di.deliverydocument
                 AND ii.material = oi.material
                GROUP BY oi.material
                ORDER BY billing_docs DESC
                LIMIT 10
            """,
            "answer_template": "Top products by billing document count.",
        }
    if "broken" in q or "incomplete" in q:
        return {
            "reject": False,
            "sql": """
                SELECT o.salesorder,
                       MAX(CASE WHEN di.deliverydocument IS NOT NULL THEN 1 ELSE 0 END) AS has_delivery,
                       MAX(CASE WHEN ii.billingdocument IS NOT NULL THEN 1 ELSE 0 END) AS has_invoice
                FROM orders o
                LEFT JOIN order_items oi ON oi.salesorder = o.salesorder
                LEFT JOIN delivery_items di
                  ON di.referencesddocument = oi.salesorder
                 AND di.referencesddocumentitem = oi.salesorderitem
                LEFT JOIN invoice_items ii
                  ON ii.referencesddocument = di.deliverydocument
                GROUP BY o.salesorder
                HAVING has_delivery != has_invoice
                LIMIT 50
            """,
            "answer_template": "Sales orders with broken flow.",
        }
    if "trace" in q and ("billing" in q or "invoice" in q):
        return {
            "reject": False,
            "sql": """
                SELECT i.billingdocument,
                       di.deliverydocument,
                       oi.salesorder,
                       oi.salesorderitem,
                       p.accountingdocument AS journal_entry
                FROM invoices i
                LEFT JOIN invoice_items ii ON ii.billingdocument = i.billingdocument
                LEFT JOIN delivery_items di ON di.deliverydocument = ii.referencesddocument
                LEFT JOIN order_items oi
                  ON oi.salesorder = di.referencesddocument
                 AND oi.salesorderitem = di.referencesddocumentitem
                LEFT JOIN journal_entries je ON je.referencedocument = i.billingdocument
                LEFT JOIN payments p ON p.accountingdocument = je.accountingdocument
                LIMIT 20
            """,
            "answer_template": "Invoice flow trace rows.",
        }
    return {"reject": True, "reason": "This system is designed to answer dataset-domain questions only."}


def answer_question(sqlite_path: Path, user_query: str) -> dict[str, Any]:
    if _off_topic(user_query):
        return {"rejected": True, "answer": "This system is designed to answer questions related to the provided dataset only."}

    with sqlite3.connect(sqlite_path) as conn:
        billing_doc = _extract_billing_document(user_query)
        lowered = user_query.lower()
        if billing_doc and ("trace" in lowered or "full flow" in lowered):
            sql = f"""
                SELECT i.billingdocument,
                       di.deliverydocument,
                       oi.salesorder,
                       oi.salesorderitem,
                       p.accountingdocument AS journal_entry
                FROM invoices i
                LEFT JOIN invoice_items ii ON ii.billingdocument = i.billingdocument
                LEFT JOIN delivery_items di ON di.deliverydocument = ii.referencesddocument
                LEFT JOIN order_items oi
                  ON oi.salesorder = di.referencesddocument
                 AND oi.salesorderitem = di.referencesddocumentitem
                LEFT JOIN journal_entries je ON je.referencedocument = i.billingdocument
                LEFT JOIN payments p ON p.accountingdocument = je.accountingdocument
                WHERE CAST(i.billingdocument AS TEXT) = '{billing_doc}'
                LIMIT 50
            """.strip()
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description] if cursor.description else []
            return {
                "rejected": False,
                "answer": f"Full flow for billing document {billing_doc}. Returned {len(rows)} rows.",
                "sql": sql,
                "columns": columns,
                "rows": rows[:100],
            }

        schema = _schema_description(conn)
        model_output = _call_openrouter_json(user_query, schema) or _fallback_sql(user_query)
        if model_output.get("reject"):
            return {"rejected": True, "answer": "This system is designed to answer questions related to the provided dataset only."}
        sql = str(model_output.get("sql", "")).strip()
        if not sql.lower().startswith("select"):
            return {"rejected": True, "answer": "Unsafe query rejected."}
        try:
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description] if cursor.description else []
        except sqlite3.Error as exc:
            return {"rejected": False, "answer": f"Query could not be executed on the current dataset schema: {exc}", "sql": sql}
        answer = str(model_output.get("answer_template", "Query executed successfully."))
        return {
            "rejected": False,
            "answer": f"{answer} Returned {len(rows)} rows.",
            "sql": sql,
            "columns": columns,
            "rows": rows[:100],
        }
