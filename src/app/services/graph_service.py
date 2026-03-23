from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import networkx as nx
import pandas as pd


@dataclass
class GraphBuildResult:
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    counts: dict[str, int]


def _first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def build_context_graph(sqlite_path: Path) -> GraphBuildResult:
    g = nx.DiGraph()
    counts = defaultdict(int)
    with sqlite3.connect(sqlite_path) as conn:
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)["name"].tolist()
        dfs = {}
        for table in tables:
            dfs[table] = pd.read_sql_query(f"SELECT * FROM [{table}] LIMIT 3000", conn)

    def add_entity_nodes(table: str, id_candidates: list[str], label_col: str | None = None) -> None:
        df = dfs.get(table)
        if df is None or df.empty:
            return
        id_col = _first_existing(df, id_candidates)
        if not id_col:
            return
        for _, row in df.iterrows():
            entity_id = str(row[id_col])
            if entity_id == "nan":
                continue
            node_id = f"{table}:{entity_id}"
            label = entity_id if not label_col or label_col not in df.columns else str(row[label_col])
            g.add_node(node_id, entity=table, entity_id=entity_id, label=label)
            counts[table] += 1

    add_entity_nodes("orders", ["salesorder", "order_id", "sales_order", "po_id", "id"])
    add_entity_nodes("order_items", ["salesorderitem", "order_item_id", "item_id", "id"])
    add_entity_nodes("deliveries", ["deliverydocument", "delivery_id", "delivery_doc", "id"])
    add_entity_nodes("invoices", ["billingdocument", "invoice_id", "billing_doc", "billing_document", "id"])
    add_entity_nodes("payments", ["accountingdocument", "payment_id", "journal_entry", "id"])
    add_entity_nodes("customers", ["customer", "businesspartner", "customer_id", "id"], label_col="businesspartnername")
    add_entity_nodes("products", ["product", "material", "product_id", "material_id", "sku", "id"], label_col="product")
    add_entity_nodes("address", ["addressid", "address_id", "id"])

    def join_rel(
        left_table: str,
        left_id_candidates: list[str],
        right_table: str,
        right_id_candidates: list[str],
        via_cols: list[str],
        relation_name: str,
    ) -> None:
        ldf = dfs.get(left_table)
        rdf = dfs.get(right_table)
        if ldf is None or rdf is None or ldf.empty or rdf.empty:
            return
        l_id = _first_existing(ldf, left_id_candidates)
        r_id = _first_existing(rdf, right_id_candidates)
        key = _first_existing(ldf, via_cols)
        if not l_id or not r_id or not key or key not in rdf.columns:
            return
        keys = set(rdf[key].astype(str).tolist())
        for _, row in ldf.iterrows():
            key_val = str(row.get(key))
            if key_val in keys:
                left_node = f"{left_table}:{row[l_id]}"
                right_matches = rdf[rdf[key].astype(str) == key_val]
                for _, r in right_matches.iterrows():
                    right_node = f"{right_table}:{r[r_id]}"
                    if g.has_node(left_node) and g.has_node(right_node):
                        g.add_edge(left_node, right_node, relation=relation_name)

    join_rel(
        "orders",
        ["salesorder", "order_id", "sales_order", "po_id", "id"],
        "order_items",
        ["salesorderitem", "order_item_id", "item_id", "id"],
        ["salesorder", "order_id", "sales_order", "po_id"],
        "has_item",
    )

    delivery_items_df = dfs.get("delivery_items")
    if delivery_items_df is not None and not delivery_items_df.empty:
        item_df = dfs.get("order_items")
        if item_df is not None and not item_df.empty:
            for _, r in delivery_items_df.iterrows():
                so = str(r.get("referencesddocument"))
                soi = str(r.get("referencesddocumentitem"))
                ddoc = str(r.get("deliverydocument"))
                if so == "nan" or soi == "nan" or ddoc == "nan":
                    continue
                item_match = item_df[
                    (item_df.get("salesorder", pd.Series(dtype=str)).astype(str) == so)
                    & (item_df.get("salesorderitem", pd.Series(dtype=str)).astype(str) == soi)
                ]
                for _, oi in item_match.iterrows():
                    src = f"order_items:{oi.get('salesorderitem')}"
                    dst = f"deliveries:{ddoc}"
                    if g.has_node(src) and g.has_node(dst):
                        g.add_edge(src, dst, relation="fulfilled_by")

    invoice_items_df = dfs.get("invoice_items")
    if invoice_items_df is not None and not invoice_items_df.empty and delivery_items_df is not None and not delivery_items_df.empty:
        for _, inv in invoice_items_df.iterrows():
            ref_doc = str(inv.get("referencesddocument"))
            bill = str(inv.get("billingdocument"))
            if ref_doc == "nan" or bill == "nan":
                continue
            src = f"deliveries:{ref_doc}"
            dst = f"invoices:{bill}"
            if g.has_node(src) and g.has_node(dst):
                g.add_edge(src, dst, relation="billed_by")

    journal_df = dfs.get("journal_entries")
    payments_df = dfs.get("payments")
    if journal_df is not None and not journal_df.empty and payments_df is not None and not payments_df.empty:
        pay_docs = set(payments_df.get("accountingdocument", pd.Series(dtype=str)).astype(str).tolist())
        for _, jr in journal_df.iterrows():
            ref_bill = str(jr.get("referencedocument"))
            acc_doc = str(jr.get("accountingdocument"))
            if ref_bill == "nan" or acc_doc == "nan":
                continue
            if acc_doc in pay_docs:
                src = f"invoices:{ref_bill}"
                dst = f"payments:{acc_doc}"
                if g.has_node(src) and g.has_node(dst):
                    g.add_edge(src, dst, relation="paid_by")

    for pair in [("orders", "customers"), ("deliveries", "customers"), ("orders", "address"), ("order_items", "products")]:
        lt, rt = pair
        ldf, rdf = dfs.get(lt), dfs.get(rt)
        if ldf is None or rdf is None or ldf.empty or rdf.empty:
            continue
        keys = [c for c in ldf.columns if c in rdf.columns and (c.endswith("_id") or c in {"customer", "businesspartner", "product", "material"})]
        if not keys:
            continue
        l_id = _first_existing(ldf, ["salesorder", "salesorderitem", "deliverydocument", "order_id", "sales_order", "po_id", "order_item_id", "item_id", "delivery_id", "delivery_doc", "id"])
        r_id = _first_existing(rdf, ["customer", "businesspartner", "addressid", "product", "material", "customer_id", "address_id", "product_id", "material_id", "sku", "id"])
        if not l_id or not r_id:
            continue
        for k in keys:
            grouped = rdf.groupby(k).first().reset_index()
            indexed = {str(row[k]): row for _, row in grouped.iterrows()}
            for _, row in ldf.iterrows():
                key = str(row[k])
                if key in indexed:
                    src = f"{lt}:{row[l_id]}"
                    dst = f"{rt}:{indexed[key][r_id]}"
                    if g.has_node(src) and g.has_node(dst):
                        g.add_edge(src, dst, relation=f"references_{k}")

    nodes = [{"id": node, **attrs} for node, attrs in g.nodes(data=True)]
    edges = [{"source": src, "target": dst, **attrs} for src, dst, attrs in g.edges(data=True)]
    return GraphBuildResult(nodes=nodes, edges=edges, counts=dict(counts))
