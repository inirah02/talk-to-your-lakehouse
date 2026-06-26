# Talk to Your Lakehouse: Iceberg MCP Demo

This repository contains a local demo for a talk on using the Model Context Protocol to make an Apache Iceberg lakehouse queryable through natural language.

The demo shows how an LLM can use typed tools to discover namespaces, inspect Iceberg table metadata, reason over snapshots, generate time travel SQL, and explain partition pruning without directly reading the full catalog metadata payload.

## What this demo includes

This repo has three small Python processes:

1. `catalog_server.py`

   A mock Apache Iceberg REST Catalog server running on port `5001`. It serves realistic table metadata for demo namespaces such as `sales`, `analytics`, and `raw`.

2. `mcp_server.py`

   A lightweight MCP-style tool server running on port `5002`. It wraps the catalog API and exposes typed tools for the LLM, including namespace discovery, table listing, table description, snapshot history, time travel SQL generation, and partition explanation.

3. `client.py`

   An interactive terminal client that connects to Groq, loads the tools from the MCP server, and lets the model call those tools while answering lakehouse questions.

## Architecture

```text
User question
    |
    v
client.py
    |
    | Groq tool calling
    v
mcp_server.py
    |
    | Authenticated REST calls
    v
catalog_server.py
    |
    v
Mock Iceberg table metadata
```

## Why this exists

Most lakehouse workflows still expect engineers to manually inspect catalogs, table schemas, snapshots, partitions, and metadata files. This demo explores what changes when an LLM is not asked to guess, but is instead given small, typed tools over the lakehouse control plane.

The goal is not to replace the query engine. The goal is to reduce the friction around discovery, debugging, schema inspection, and query planning.

## Demo capabilities

The agent can answer questions such as:

```text
What namespaces and tables are in this lakehouse?
```

```text
Tell me about the orders table: schema, partitioning, and recent activity.
```

```text
What changed in the orders table in the last two weeks?
```

```text
I need to query the orders data as it was last Monday. Give me the time travel SQL.
```

```text
How should I write efficient Spark SQL against the orders table to avoid full scans?
```

## Repository structure

```text
.
├── catalog_server.py     # Mock Iceberg REST Catalog server
├── mcp_server.py         # MCP-style tool server over the catalog
├── client.py             # Groq-powered terminal client
├── requirements.txt      # Python dependencies
└── README.md
```

## Prerequisites

Use Python 3.10 or later.

You also need a Groq API key for the interactive client.

Create a key from the Groq console, then export it before running the client:

```bash
export GROQ_API_KEY="your_groq_api_key_here"
```

## Setup

Clone the repo:

```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
```

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Run the demo

Open three terminal windows.

Terminal 1: start the mock Iceberg REST Catalog.

```bash
python catalog_server.py
```

Expected service:

```text
http://localhost:5001
```

Terminal 2: start the MCP tool server.

```bash
python mcp_server.py
```

Expected service:

```text
http://localhost:5002
```

Terminal 3: start the interactive client.

```bash
export GROQ_API_KEY="your_groq_api_key_here"
python client.py
```

The client will show suggested demo questions. You can type one of the numbers or ask your own question.

## Available MCP tools

The MCP server exposes these tools to the client:

| Tool | Purpose |
| --- | --- |
| `list_namespaces` | Lists available catalog namespaces |
| `list_tables` | Lists tables inside a namespace |
| `describe_table` | Returns trimmed schema, partition, property, and snapshot metadata for one table |
| `get_snapshots` | Returns recent Iceberg snapshot history |
| `time_travel_info` | Finds the closest snapshot for a target date and returns SQL |
| `explain_partition` | Explains partition transforms and query pruning strategy |

## Design notes

The demo intentionally keeps the catalog local so the talk can focus on the control-plane pattern instead of cloud setup.

The MCP server trims table metadata before sending it to the LLM. This is important because real Iceberg metadata can be large, noisy, and full of file paths that are not useful for conversational reasoning.

The client uses model tool calling instead of prompting the model with raw metadata. This makes the agent behavior easier to inspect because every tool call and tool result is printed in the terminal.

