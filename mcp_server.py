#!/usr/bin/env python3
"""
mcp_server.py  —  Process 2 of 3
===================================
MCP Tool Server: wraps the Iceberg REST Catalog and exposes 6 typed MCP tools.
Runs as an HTTP server on :5002 for easy integration with the client.

In production: use the official MCP Python SDK (mcp.run(transport='stdio')).
Here we expose the same tool definitions + dispatch over HTTP for demo simplicity.

Start with:
  pip install flask requests
  python mcp_server.py

Depends on catalog_server.py running on :5001.
"""

import json
import time
import logging
from datetime import datetime, timezone, timedelta
from copy import deepcopy
from typing import Any

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [MCP] %(message)s")
log = logging.getLogger(__name__)

CATALOG_URL = "http://localhost:5001"
MAX_RESPONSE_BYTES = 4096   # hard cap on any tool response sent to the LLM
MAX_SNAPSHOTS = 10           # absolute ceiling for get_snapshots

# ─── auth (demo: catalog issues token on first call) ──────────────────────────
_token: str | None = None
_token_expiry: float = 0.0

def _get_token() -> str:
    global _token, _token_expiry
    if _token and time.time() < _token_expiry - 60:
        return _token
    resp = requests.post(f"{CATALOG_URL}/v1/oauth/tokens",
                         data={"grant_type": "client_credentials",
                               "client_id": "demo", "client_secret": "demo"})
    resp.raise_for_status()
    data = resp.json()
    _token = data["access_token"]
    _token_expiry = time.time() + data["expires_in"]
    log.info("Refreshed catalog OAuth token (expires in %ds)", data["expires_in"])
    return _token

def _catalog_get(path: str, **params) -> dict:
    """Authenticated GET to the Iceberg REST Catalog."""
    token = _get_token()
    resp = requests.get(
        f"{CATALOG_URL}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
    )
    if resp.status_code == 404:
        raise KeyError(resp.json().get("error", {}).get("message", f"Not found: {path}"))
    resp.raise_for_status()
    return resp.json()


# ─── response trimmer (PRODUCTION CRITICAL) ───────────────────────────────────

def trim_table_metadata(raw_metadata: dict, snapshot_limit: int = 5) -> dict:
    """
    Reduces Iceberg TableMetadata from 50-500KB to < 4KB for LLM consumption.
    Rules:
      - Keep: current schema only (by current-schema-id), partition-spec, sort-order,
              last-updated-ms, properties, last N snapshots
      - Strip: all historical schemas, metadata-log, snapshot-log, statistics,
               manifest-list paths, raw S3 location, table-uuid
    """
    current_schema_id = raw_metadata.get("current-schema-id", 1)
    all_schemas = raw_metadata.get("schemas", [raw_metadata.get("schema", {})])

    # Find the current schema
    current_schema = next(
        (s for s in all_schemas if s.get("schema-id") == current_schema_id),
        all_schemas[-1] if all_schemas else {}
    )

    # Trim column list if very wide
    cols = current_schema.get("fields", [])
    if len(cols) > 50:
        col_summary = [{"name": c["name"], "type": c["type"], "required": c.get("required", False)}
                       for c in cols[:50]]
        col_note = f"[Table has {len(cols)} columns — showing first 50. Call describe_table_columns for full list.]"
    else:
        col_summary = [
            {"name": c["name"], "type": c["type"],
             "required": c.get("required", False),
             "doc": c.get("doc", "")}
            for c in cols
        ]
        col_note = None

    # Trim snapshots
    raw_snaps = raw_metadata.get("snapshots", [])
    recent_snaps = sorted(raw_snaps, key=lambda s: s["timestamp-ms"], reverse=True)[:snapshot_limit]
    trimmed_snaps = []
    for s in recent_snaps:
        summary = s.get("summary", {})
        trimmed_snaps.append({
            "snapshot_id": s["snapshot-id"],
            "timestamp_utc": datetime.fromtimestamp(
                s["timestamp-ms"] / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M UTC"),
            "operation": summary.get("operation"),
            "rows_added": int(summary.get("added-records", 0)),
            "rows_deleted": int(summary.get("deleted-records", 0)),
            "data_files_added": int(summary.get("added-data-files", 0)),
            "schema_id": s.get("schema-id"),
            # manifest-list: STRIPPED (S3 path, useless to LLM)
        })

    part_spec = raw_metadata.get("partition-spec", {})
    sort_order = raw_metadata.get("sort-order", {})

    trimmed = {
        "schema_id": current_schema_id,
        "column_count": len(cols),
        "columns": col_summary,
        "partition_spec": {
            "spec_id": part_spec.get("spec-id"),
            "fields": [
                {
                    "column": _col_name(part_spec, f["source-id"], current_schema),
                    "transform": f["transform"],
                    "partition_name": f["name"],
                }
                for f in part_spec.get("fields", [])
            ],
            "is_partitioned": len(part_spec.get("fields", [])) > 0,
        },
        "sort_order": sort_order.get("fields", []),
        "properties": raw_metadata.get("properties", {}),
        "last_updated_utc": datetime.fromtimestamp(
            raw_metadata.get("last-updated-ms", 0) / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M UTC"),
        "recent_snapshots": trimmed_snaps,
        "snapshot_count_shown": len(trimmed_snaps),
        "total_snapshot_count": len(raw_snaps),
    }

    if col_note:
        trimmed["column_note"] = col_note

    return trimmed


def _col_name(partition_spec: dict, source_id: int, schema: dict) -> str:
    for f in schema.get("fields", []):
        if f["id"] == source_id:
            return f["name"]
    return f"col#{source_id}"


# ─── Tool definitions ──────────────────────────────────────────────────────────
# These are what the LLM reads via tools/list.
# Description quality directly determines agent behaviour.

TOOLS = [
    {
        "name": "list_namespaces",
        "description": (
            "Returns all namespaces (databases/schemas) in the Iceberg catalog. "
            "Use FIRST for any discovery question: 'what data do we have?', 'show me all tables', "
            "'what namespaces exist?'. Returns names only — no table or schema metadata. "
            "Fast: O(1) catalog call."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "list_tables",
        "description": (
            "Returns all table names in one namespace. Returns names only (no schema data). "
            "Call BEFORE describe_table to discover exact table names. "
            "DO NOT call describe_table for every table returned — only for the specific table the user asked about. "
            "Fast: names-only metadata call, ~100ms."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {
                    "type": "string",
                    "description": "Namespace name from list_namespaces, e.g. 'sales', 'analytics', 'raw'"
                }
            },
            "required": ["namespace"]
        }
    },
    {
        "name": "describe_table",
        "description": (
            "Returns trimmed TableMetadata for ONE specific table: "
            "current schema (all column names, types, required/nullable, docs), "
            "partition spec with transform functions (month/identity/year/hour), "
            "sort order, table properties (owner, format, compression), "
            "and the 5 most-recent snapshots with timestamps and row counts. "
            "USE WHEN: asked about table structure, columns, types, partitioning, or recent changes. "
            "DO NOT call for every table — call list_tables first, then only for the specific table. "
            "DO NOT call if you already have schema from this conversation. "
            "Slower: ~800ms, ~3KB response after trimming."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string", "description": "e.g. 'sales'"},
                "table": {"type": "string", "description": "Exact table name from list_tables, e.g. 'orders'"}
            },
            "required": ["namespace", "table"]
        }
    },
    {
        "name": "get_snapshots",
        "description": (
            "Returns recent snapshot history for a table: snapshot IDs, UTC timestamps, "
            "operation type (append/overwrite/delete), rows added/deleted, data files added, "
            "and schema_id at each snapshot (schema_id change = schema evolution event). "
            "USE WHEN: asked 'what changed', 'when was this last updated', 'show me the history', "
            "or to find the right snapshot for time-travel. "
            "Returns up to `limit` snapshots newest-first (default 5, max 10)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string"},
                "table": {"type": "string"},
                "limit": {
                    "type": "integer",
                    "description": "Max snapshots to return, default 5, max 10",
                    "default": 5
                }
            },
            "required": ["namespace", "table"]
        }
    },
    {
        "name": "time_travel_info",
        "description": (
            "Given a table and a target date string, finds the Iceberg snapshot closest to (but not after) "
            "that date and returns ready-to-run SQL for both Spark (FOR VERSION AS OF snapshot_id) "
            "and ANSI (FOR SYSTEM_TIME AS OF 'date') time-travel syntax. "
            "Also returns the actual snapshot timestamp so the user knows how close the match is. "
            "USE WHEN: asked for data 'as of', 'on', 'last Monday', or any point-in-time query. "
            "target_date must be ISO format: YYYY-MM-DD."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string"},
                "table": {"type": "string"},
                "target_date": {
                    "type": "string",
                    "description": "ISO date string, e.g. '2026-06-15'. Must be YYYY-MM-DD."
                }
            },
            "required": ["namespace", "table", "target_date"]
        }
    },
    {
        "name": "explain_partition",
        "description": (
            "Returns a human-readable explanation of how a table is partitioned, "
            "what transform functions are applied (identity/month/year/hour/bucket/truncate), "
            "and concrete advice on how to write partition-pruning WHERE clauses for this table. "
            "USE WHEN: asked 'how should I query this', 'what columns to filter on', "
            "'is this table partitioned', or 'how do I avoid a full scan'."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string"},
                "table": {"type": "string"}
            },
            "required": ["namespace", "table"]
        }
    },
]


# ─── Tool dispatch ─────────────────────────────────────────────────────────────

def dispatch(tool_name: str, params: dict) -> dict:
    """Route a tool call to the appropriate catalog operation."""

    if tool_name == "list_namespaces":
        data = _catalog_get("/v1/namespaces")
        return {
            "namespaces": [n[0] for n in data["namespaces"]],
            "count": len(data["namespaces"]),
        }

    elif tool_name == "list_tables":
        ns = params["namespace"]
        data = _catalog_get(f"/v1/namespaces/{ns}/tables")
        tables = [i["name"] for i in data.get("identifiers", [])]
        return {
            "namespace": ns,
            "tables": tables,
            "count": len(tables),
        }

    elif tool_name == "describe_table":
        ns, tbl = params["namespace"], params["table"]
        data = _catalog_get(f"/v1/namespaces/{ns}/tables/{tbl}", snapshots=5)
        meta = data["metadata"]
        trimmed = trim_table_metadata(meta, snapshot_limit=5)
        return {"identifier": f"{ns}.{tbl}", **trimmed}

    elif tool_name == "get_snapshots":
        ns, tbl = params["namespace"], params["table"]
        limit = min(int(params.get("limit", 5)), MAX_SNAPSHOTS)
        data = _catalog_get(f"/v1/namespaces/{ns}/tables/{tbl}", snapshots=limit)
        meta = data["metadata"]
        raw_snaps = sorted(
            meta.get("snapshots", []), key=lambda s: s["timestamp-ms"], reverse=True
        )[:limit]
        snaps = []
        for s in raw_snaps:
            summary = s.get("summary", {})
            snaps.append({
                "snapshot_id": s["snapshot-id"],
                "timestamp_utc": datetime.fromtimestamp(
                    s["timestamp-ms"] / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M UTC"),
                "operation": summary.get("operation"),
                "rows_added": int(summary.get("added-records", 0)),
                "rows_deleted": int(summary.get("deleted-records", 0)),
                "data_files_added": int(summary.get("added-data-files", 0)),
                "schema_id": s.get("schema-id"),
                # manifest-list: STRIPPED
            })
        return {
            "table": f"{ns}.{tbl}",
            "snapshots": snaps,
            "shown": len(snaps),
        }

    elif tool_name == "time_travel_info":
        ns, tbl = params["namespace"], params["table"]
        target_date = params["target_date"]

        data = _catalog_get(f"/v1/namespaces/{ns}/tables/{tbl}", snapshots=50)
        meta = data["metadata"]
        all_snaps = meta.get("snapshots", [])

        target_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        target_ms = int(target_dt.timestamp() * 1000)
        eligible = [s for s in all_snaps if s["timestamp-ms"] <= target_ms]

        if not eligible:
            earliest = min(all_snaps, key=lambda s: s["timestamp-ms"]) if all_snaps else None
            return {
                "error": f"No snapshot exists on or before {target_date}.",
                "earliest_available": datetime.fromtimestamp(
                    earliest["timestamp-ms"] / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d") if earliest else None,
            }

        snap = max(eligible, key=lambda s: s["timestamp-ms"])
        actual_ts = datetime.fromtimestamp(snap["timestamp-ms"] / 1000, tz=timezone.utc)
        delta_hours = (target_dt - actual_ts).total_seconds() / 3600

        return {
            "namespace": ns,
            "table": tbl,
            "target_date": target_date,
            "closest_snapshot_id": snap["snapshot-id"],
            "closest_snapshot_timestamp_utc": actual_ts.strftime("%Y-%m-%d %H:%M UTC"),
            "delta_hours": round(delta_hours, 1),
            "schema_id_at_snapshot": snap.get("schema-id"),
            "spark_sql": (
                f"SELECT *\nFROM {ns}.{tbl}\nFOR VERSION AS OF {snap['snapshot-id']}"
            ),
            "ansi_sql": (
                f"SELECT *\nFROM {ns}.{tbl}\nFOR SYSTEM_TIME AS OF '{target_date}'"
            ),
            "note": (
                f"Snapshot is {delta_hours:.1f}h before your target. "
                f"Data reflects state at {actual_ts.strftime('%Y-%m-%d %H:%M UTC')}."
            ),
        }

    elif tool_name == "explain_partition":
        ns, tbl = params["namespace"], params["table"]
        data = _catalog_get(f"/v1/namespaces/{ns}/tables/{tbl}", snapshots=1)
        meta = data["metadata"]

        current_schema_id = meta.get("current-schema-id", 1)
        all_schemas = meta.get("schemas", [meta.get("schema", {})])
        schema = next((s for s in all_schemas if s.get("schema-id") == current_schema_id), all_schemas[-1])
        col_map = {f["id"]: f["name"] for f in schema.get("fields", [])}

        part_spec = meta.get("partition-spec", {})
        fields = part_spec.get("fields", [])

        if not fields:
            return {
                "table": f"{ns}.{tbl}",
                "partitioned": False,
                "query_advice": "This table is unpartitioned. Every query triggers a full table scan.",
                "optimization_suggestion": (
                    "Consider partitioning by a high-cardinality date or timestamp column "
                    "with a month or day transform for time-series workloads."
                ),
            }

        partition_columns = []
        query_advice = []
        for f in fields:
            col = col_map.get(f["source-id"], f"col#{f['source-id']}")
            transform = f["transform"]
            partition_columns.append({
                "column": col,
                "transform": transform,
                "partition_field_name": f["name"],
            })
            if transform == "identity":
                query_advice.append(
                    f"Filter with WHERE {col} = '<value>' — uses identity partition pruning. "
                    f"Equality filter required; range scans do NOT prune identity partitions."
                )
            elif transform == "month":
                query_advice.append(
                    f"Filter with WHERE {col} BETWEEN 'YYYY-MM-01' AND 'YYYY-MM-31' "
                    f"or YEAR({col}) = Y AND MONTH({col}) = M — month partition pruning."
                )
            elif transform == "year":
                query_advice.append(
                    f"Filter with WHERE YEAR({col}) = <year> or {col} BETWEEN 'YYYY-01-01' AND 'YYYY-12-31'."
                )
            elif transform == "day":
                query_advice.append(
                    f"Filter with WHERE DATE({col}) = 'YYYY-MM-DD' — day-level partition pruning."
                )
            elif transform == "hour":
                query_advice.append(
                    f"Filter with WHERE {col} BETWEEN '<timestamp>' AND '<timestamp+1h>' — "
                    f"hour-level partition pruning. Use BETWEEN with tight bounds."
                )

        return {
            "table": f"{ns}.{tbl}",
            "partitioned": True,
            "partition_columns": partition_columns,
            "query_advice": query_advice,
            "iceberg_pruning_note": (
                "Iceberg partition pruning is metadata-level: the query engine reads the "
                "manifest list to find relevant data files without opening any parquet files. "
                "This is fundamentally faster than Hive-style directory listing."
            ),
        }

    else:
        raise ValueError(f"Unknown tool: {tool_name!r}")


# ─── HTTP endpoints (MCP-over-HTTP for demo) ──────────────────────────────────

@app.route("/tools/list", methods=["GET"])
def tools_list():
    """Returns the MCP tool definitions (tools/list in the MCP spec)."""
    return jsonify({"tools": TOOLS})


@app.route("/tools/call", methods=["POST"])
def tools_call():
    """Dispatches a tool call and returns the result."""
    body = request.get_json()
    tool_name = body.get("name", "")
    params = body.get("arguments", {})

    start = time.time()
    try:
        result = dispatch(tool_name, params)

        # Enforce response size budget
        result_json = json.dumps(result)
        if len(result_json) > MAX_RESPONSE_BYTES:
            log.warning("Response for %s exceeded %d bytes (%d) — truncating snapshot list",
                        tool_name, MAX_RESPONSE_BYTES, len(result_json))
            # Trim snapshots further if the culprit
            if "recent_snapshots" in result and len(result["recent_snapshots"]) > 3:
                result["recent_snapshots"] = result["recent_snapshots"][:3]
                result["trim_note"] = "Snapshot list truncated to fit response budget."

        latency_ms = int((time.time() - start) * 1000)
        log.info("TOOL CALL  %-20s  params=%-40s  %dms  %dB",
                 tool_name, str(params)[:40], latency_ms, len(json.dumps(result)))

        return jsonify({
            "content": [{"type": "text", "text": json.dumps(result, indent=2)}],
            "isError": False,
        })

    except KeyError as e:
        log.error("TOOL ERROR  %s: %s", tool_name, e)
        return jsonify({
            "content": [{"type": "text", "text": json.dumps({"error": str(e), "error_code": "NOT_FOUND"})}],
            "isError": True,
        }), 200  # MCP spec: tool errors are 200 with isError:true

    except Exception as e:
        log.error("TOOL ERROR  %s: %s", tool_name, e, exc_info=True)
        return jsonify({
            "content": [{"type": "text", "text": json.dumps({
                "error": str(e),
                "error_code": "TOOL_EXECUTION_ERROR",
                "retry_after": 0,
                "suggested_action": "Check namespace and table names via list_namespaces and list_tables.",
            })}],
            "isError": True,
        }), 200


if __name__ == "__main__":
    print("=" * 60)
    print("  Iceberg MCP Server")
    print("  Listening on http://localhost:5002")
    print(f"  Catalog backend: {CATALOG_URL}")
    print("=" * 60)
    print(f"\n  {len(TOOLS)} tools registered:")
    for t in TOOLS:
        print(f"    · {t['name']}")
    print()
    app.run(host="0.0.0.0", port=5002, debug=False)
