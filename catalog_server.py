#!/usr/bin/env python3
"""
catalog_server.py  —  Process 1 of 3
======================================
A Flask server that mimics the Apache Iceberg REST Catalog spec.
Serves realistic mock TableMetadata for the demo.

Start with:
  pip install flask
  python catalog_server.py

Runs on http://localhost:5001
All endpoints follow the Iceberg REST Catalog spec (apache/iceberg#rest-catalog).
"""

import json
import time
from datetime import datetime, timezone, timedelta
from copy import deepcopy
from flask import Flask, jsonify, abort, request

app = Flask(__name__)

# ─── helpers ──────────────────────────────────────────────────────────────────

def ts(days_ago: int = 0, hours_ago: int = 0) -> int:
    delta = timedelta(days=days_ago, hours=hours_ago)
    return int((datetime.now(timezone.utc) - delta).timestamp() * 1000)


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ─── catalog data ──────────────────────────────────────────────────────────────

CATALOG = {
    "sales": {
        "orders": {
            "format-version": 2,
            "table-uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "location": "s3://datalake-prod/warehouse/sales/orders",
            "last-updated-ms": ts(days_ago=1, hours_ago=2),
            "current-schema-id": 3,
            "schemas": [
                {
                    "schema-id": 1,
                    "fields": [
                        {"id": 1, "name": "order_id",     "type": "long",          "required": True},
                        {"id": 2, "name": "customer_id",  "type": "long",          "required": True},
                        {"id": 3, "name": "order_date",   "type": "date",          "required": True},
                        {"id": 4, "name": "total_amount", "type": "decimal(18,2)", "required": True},
                        {"id": 5, "name": "status",       "type": "string",        "required": True,
                         "doc": "enum: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED"},
                        {"id": 6, "name": "region",       "type": "string",        "required": True},
                    ]
                },
                {
                    "schema-id": 2,
                    "fields": [
                        {"id": 1, "name": "order_id",     "type": "long",          "required": True},
                        {"id": 2, "name": "customer_id",  "type": "long",          "required": True},
                        {"id": 3, "name": "order_date",   "type": "date",          "required": True},
                        {"id": 4, "name": "total_amount", "type": "decimal(18,2)", "required": True},
                        {"id": 5, "name": "status",       "type": "string",        "required": True,
                         "doc": "enum: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED, RETURNED"},
                        {"id": 6, "name": "region",       "type": "string",        "required": True},
                    ]
                },
                {
                    "schema-id": 3,
                    "fields": [
                        {"id": 1, "name": "order_id",            "type": "long",          "required": True},
                        {"id": 2, "name": "customer_id",         "type": "long",          "required": True},
                        {"id": 3, "name": "order_date",          "type": "date",          "required": True},
                        {"id": 4, "name": "total_amount",        "type": "decimal(18,2)", "required": True},
                        {"id": 5, "name": "status",              "type": "string",        "required": True,
                         "doc": "enum: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED, RETURNED"},
                        {"id": 6, "name": "region",              "type": "string",        "required": True},
                        {"id": 7, "name": "delivery_partner_id", "type": "string",        "required": False,
                         "doc": "Added in schema v3 — nullable for pre-partner orders"},
                    ]
                },
            ],
            "partition-spec": {
                "spec-id": 1,
                "fields": [
                    {"source-id": 3, "field-id": 1000, "transform": "month",    "name": "order_date_month"},
                    {"source-id": 6, "field-id": 1001, "transform": "identity", "name": "region"},
                ]
            },
            "sort-order": {
                "order-id": 1,
                "fields": [{"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-last"}]
            },
            "properties": {
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.target-file-size-bytes": "134217728",
                "owner": "data-platform-team",
                "write.metadata.delete-after-commit.enabled": "true",
                "write.metadata.previous-versions-max": "10",
            },
            "snapshots": [
                {
                    "snapshot-id": 8847263910,
                    "timestamp-ms": ts(days_ago=3, hours_ago=8),
                    "parent-snapshot-id": 8847201445,
                    "schema-id": 3,
                    "summary": {
                        "operation": "append",
                        "added-records": "142891",
                        "added-files-size": "1847293012",
                        "added-data-files": "47",
                        "changed-partition-count": "8",
                    },
                    "manifest-list": "s3://datalake-prod/metadata/orders/snap-8847263910.avro",
                },
                {
                    "snapshot-id": 8847201445,
                    "timestamp-ms": ts(days_ago=5, hours_ago=15),
                    "parent-snapshot-id": 8846998812,
                    "schema-id": 3,
                    "summary": {
                        "operation": "overwrite",
                        "added-records": "0",
                        "deleted-records": "3201",
                        "changed-partition-count": "12",
                        "total-records": "2847293",
                    },
                    "manifest-list": "s3://datalake-prod/metadata/orders/snap-8847201445.avro",
                },
                {
                    "snapshot-id": 8846998812,
                    "timestamp-ms": ts(days_ago=7, hours_ago=1),
                    "parent-snapshot-id": 8846712003,
                    "schema-id": 2,
                    "summary": {
                        "operation": "append",
                        "added-records": "98334",
                        "added-files-size": "982736010",
                        "added-data-files": "31",
                    },
                    "manifest-list": "s3://datalake-prod/metadata/orders/snap-8846998812.avro",
                },
                {
                    "snapshot-id": 8846712003,
                    "timestamp-ms": ts(days_ago=9, hours_ago=16),
                    "parent-snapshot-id": 8846400120,
                    "schema-id": 2,
                    "summary": {
                        "operation": "append",
                        "added-records": "211042",
                        "added-files-size": "2103847612",
                        "added-data-files": "68",
                    },
                    "manifest-list": "s3://datalake-prod/metadata/orders/snap-8846712003.avro",
                },
                {
                    "snapshot-id": 8846400120,
                    "timestamp-ms": ts(days_ago=14, hours_ago=9),
                    "parent-snapshot-id": None,
                    "schema-id": 1,
                    "summary": {
                        "operation": "append",
                        "added-records": "175829",
                        "added-files-size": "1758290123",
                        "added-data-files": "55",
                    },
                    "manifest-list": "s3://datalake-prod/metadata/orders/snap-8846400120.avro",
                },
            ],
            "snapshot-log": [
                {"snapshot-id": 8846400120, "timestamp-ms": ts(days_ago=14, hours_ago=9)},
                {"snapshot-id": 8846712003, "timestamp-ms": ts(days_ago=9, hours_ago=16)},
                {"snapshot-id": 8846998812, "timestamp-ms": ts(days_ago=7, hours_ago=1)},
                {"snapshot-id": 8847201445, "timestamp-ms": ts(days_ago=5, hours_ago=15)},
                {"snapshot-id": 8847263910, "timestamp-ms": ts(days_ago=3, hours_ago=8)},
            ],
        },

        "customers": {
            "format-version": 2,
            "table-uuid": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
            "location": "s3://datalake-prod/warehouse/sales/customers",
            "last-updated-ms": ts(days_ago=2, hours_ago=11),
            "current-schema-id": 1,
            "schemas": [{
                "schema-id": 1,
                "fields": [
                    {"id": 1, "name": "customer_id", "type": "long",          "required": True},
                    {"id": 2, "name": "email",        "type": "string",        "required": True},
                    {"id": 3, "name": "signup_date",  "type": "date",          "required": True},
                    {"id": 4, "name": "country",      "type": "string",        "required": True},
                    {"id": 5, "name": "tier",         "type": "string",        "required": False,
                     "doc": "enum: FREE, PRO, ENTERPRISE"},
                    {"id": 6, "name": "ltv_usd",      "type": "decimal(12,2)", "required": False},
                ]
            }],
            "partition-spec": {"spec-id": 1, "fields": [
                {"source-id": 4, "field-id": 1000, "transform": "identity", "name": "country"}
            ]},
            "properties": {"owner": "crm-team"},
            "snapshots": [
                {"snapshot-id": 7712000011, "timestamp-ms": ts(days_ago=2, hours_ago=11),
                 "schema-id": 1,
                 "summary": {"operation": "append", "added-records": "42130"}, },
                {"snapshot-id": 7710000007, "timestamp-ms": ts(days_ago=9, hours_ago=4),
                 "schema-id": 1,
                 "summary": {"operation": "append", "added-records": "38901"}},
            ],
        },

        "products": {
            "format-version": 2,
            "table-uuid": "c3d4e5f6-a7b8-9012-cdef-123456789012",
            "location": "s3://datalake-prod/warehouse/sales/products",
            "last-updated-ms": ts(days_ago=15, hours_ago=3),
            "current-schema-id": 1,
            "schemas": [{
                "schema-id": 1,
                "fields": [
                    {"id": 1, "name": "product_id", "type": "long",          "required": True},
                    {"id": 2, "name": "sku",         "type": "string",        "required": True},
                    {"id": 3, "name": "category",    "type": "string",        "required": True},
                    {"id": 4, "name": "price_usd",   "type": "decimal(10,2)", "required": True},
                    {"id": 5, "name": "is_active",   "type": "boolean",       "required": True},
                ]
            }],
            "partition-spec": {"spec-id": 0, "fields": []},
            "properties": {"owner": "catalog-team"},
            "snapshots": [
                {"snapshot-id": 5501234567, "timestamp-ms": ts(days_ago=15, hours_ago=3),
                 "schema-id": 1,
                 "summary": {"operation": "append", "added-records": "8941"}},
            ],
        },
    },

    "analytics": {
        "daily_revenue": {
            "format-version": 2,
            "table-uuid": "d4e5f6a7-b8c9-0123-defa-234567890123",
            "location": "s3://datalake-prod/warehouse/analytics/daily_revenue",
            "last-updated-ms": ts(hours_ago=3),
            "current-schema-id": 1,
            "schemas": [{
                "schema-id": 1,
                "fields": [
                    {"id": 1, "name": "report_date",    "type": "date",          "required": True},
                    {"id": 2, "name": "region",          "type": "string",        "required": True},
                    {"id": 3, "name": "revenue_usd",     "type": "decimal(18,2)", "required": True},
                    {"id": 4, "name": "order_count",     "type": "long",          "required": True},
                    {"id": 5, "name": "avg_order_value", "type": "decimal(10,2)", "required": False},
                ]
            }],
            "partition-spec": {"spec-id": 1, "fields": [
                {"source-id": 1, "field-id": 1000, "transform": "year", "name": "report_date_year"}
            ]},
            "properties": {"owner": "analytics-team", "refresh": "daily"},
            "snapshots": [
                {"snapshot-id": 9900000003, "timestamp-ms": ts(hours_ago=3),
                 "schema-id": 1,
                 "summary": {"operation": "overwrite", "added-records": "7", "deleted-records": "7"}},
            ],
        },

        "funnel_events": {
            "format-version": 2,
            "table-uuid": "e5f6a7b8-c9d0-1234-efab-345678901234",
            "location": "s3://datalake-prod/warehouse/analytics/funnel_events",
            "last-updated-ms": ts(hours_ago=1),
            "current-schema-id": 2,
            "schemas": [
                {"schema-id": 1, "fields": [
                    {"id": 1, "name": "event_id",   "type": "string",      "required": True},
                    {"id": 2, "name": "user_id",    "type": "string",      "required": True},
                    {"id": 3, "name": "event_type", "type": "string",      "required": True},
                    {"id": 4, "name": "event_ts",   "type": "timestamptz", "required": True},
                    {"id": 5, "name": "session_id", "type": "string",      "required": False},
                ]},
                {"schema-id": 2, "fields": [
                    {"id": 1, "name": "event_id",   "type": "string",      "required": True},
                    {"id": 2, "name": "user_id",    "type": "string",      "required": True},
                    {"id": 3, "name": "event_type", "type": "string",      "required": True},
                    {"id": 4, "name": "event_ts",   "type": "timestamptz", "required": True},
                    {"id": 5, "name": "session_id", "type": "string",      "required": False},
                    {"id": 6, "name": "ab_variant", "type": "string",      "required": False,
                     "doc": "Added in schema v2 for A/B experiment tracking"},
                ]},
            ],
            "partition-spec": {"spec-id": 1, "fields": [
                {"source-id": 4, "field-id": 1000, "transform": "hour", "name": "event_ts_hour"}
            ]},
            "properties": {"owner": "growth-team"},
            "snapshots": [
                {"snapshot-id": 4412000099, "timestamp-ms": ts(hours_ago=1),
                 "schema-id": 2,
                 "summary": {"operation": "append", "added-records": "58291"}},
                {"snapshot-id": 4411000044, "timestamp-ms": ts(days_ago=2),
                 "schema-id": 1,
                 "summary": {"operation": "append", "added-records": "41823"}},
            ],
        },
    },

    "raw": {
        "clickstream": {
            "format-version": 2,
            "table-uuid": "f6a7b8c9-d0e1-2345-fabc-456789012345",
            "location": "s3://datalake-prod/warehouse/raw/clickstream",
            "last-updated-ms": ts(hours_ago=0),
            "current-schema-id": 1,
            "schemas": [{
                "schema-id": 1,
                "fields": [
                    {"id": 1, "name": "row_id",    "type": "string",      "required": True},
                    {"id": 2, "name": "page",       "type": "string",      "required": True},
                    {"id": 3, "name": "referrer",   "type": "string",      "required": False},
                    {"id": 4, "name": "user_agent", "type": "string",      "required": False},
                    {"id": 5, "name": "ip_hash",    "type": "string",      "required": True},
                    {"id": 6, "name": "ts",         "type": "timestamptz", "required": True},
                ]
            }],
            "partition-spec": {"spec-id": 1, "fields": [
                {"source-id": 6, "field-id": 1000, "transform": "hour", "name": "ts_hour"}
            ]},
            "properties": {"owner": "infra-team", "retention-days": "90"},
            "snapshots": [
                {"snapshot-id": 3301000055, "timestamp-ms": ts(hours_ago=0),
                 "schema-id": 1,
                 "summary": {"operation": "append", "added-records": "1204938"}},
            ],
        }
    }
}


# ─── REST Catalog endpoints ────────────────────────────────────────────────────

@app.route("/v1/config")
def config():
    return jsonify({"defaults": {}, "overrides": {}})


@app.route("/v1/oauth/tokens", methods=["POST"])
def oauth_tokens():
    # In a real catalog this validates client_id/secret.
    # Here we return a demo token immediately.
    return jsonify({
        "access_token": "demo_token_iceberg_rest_catalog",
        "token_type": "Bearer",
        "expires_in": 3600,
        "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
    })


@app.route("/v1/namespaces")
def list_namespaces():
    return jsonify({
        "namespaces": [[ns] for ns in sorted(CATALOG.keys())]
    })


@app.route("/v1/namespaces/<namespace>")
def get_namespace(namespace):
    if namespace not in CATALOG:
        abort(404, description=f"Namespace '{namespace}' not found")
    return jsonify({
        "namespace": [namespace],
        "properties": {"location": f"s3://datalake-prod/warehouse/{namespace}"}
    })


@app.route("/v1/namespaces/<namespace>/tables")
def list_tables(namespace):
    if namespace not in CATALOG:
        abort(404, description=f"Namespace '{namespace}' not found")
    return jsonify({
        "identifiers": [
            {"namespace": [namespace], "name": tbl}
            for tbl in sorted(CATALOG[namespace].keys())
        ]
    })


@app.route("/v1/namespaces/<namespace>/tables/<table>")
def load_table(namespace, table):
    if namespace not in CATALOG:
        abort(404, description=f"Namespace '{namespace}' not found")
    if table not in CATALOG[namespace]:
        abort(404, description=f"Table '{namespace}.{table}' not found")

    meta = deepcopy(CATALOG[namespace][table])

    # Apply snapshot limit if requested
    snap_limit = request.args.get("snapshots", type=int)
    if snap_limit is not None and "snapshots" in meta:
        meta["snapshots"] = sorted(
            meta["snapshots"], key=lambda s: s["timestamp-ms"], reverse=True
        )[:snap_limit]

    return jsonify({
        "metadata-location": f"s3://datalake-prod/metadata/{namespace}/{table}/v3.metadata.json",
        "metadata": meta,
        "config": {},
    })


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": {"message": str(e), "type": "NoSuchTableException", "code": 404}}), 404


if __name__ == "__main__":
    print("=" * 60)
    print("  Iceberg REST Catalog Server (Mock)")
    print("  Listening on http://localhost:5001")
    print("=" * 60)
    print(f"\n  Namespaces: {list(CATALOG.keys())}")
    for ns, tables in CATALOG.items():
        print(f"  {ns}: {list(tables.keys())}")
    print()
    app.run(host="0.0.0.0", port=5001, debug=False)
