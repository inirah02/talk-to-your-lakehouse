#!/usr/bin/env python3
"""
client.py  —  Process 3 of 3
================================
Interactive terminal client that uses the Groq API (free tier) with
real tool-use (function calling) over the Iceberg MCP Server.

Setup:
  pip install groq requests
  export GROQ_API_KEY="your_key_here"   # get free key at console.groq.com

  Then in 3 terminal windows:
    1.  python catalog_server.py
    2.  python mcp_server.py
    3.  python client.py

Model: llama-3.3-70b-versatile (Groq free tier, native tool_use support)
"""

import json
import os
import sys
import time
import textwrap
from datetime import datetime, timezone, timedelta

import requests

try:
    from groq import Groq
except ImportError:
    print("ERROR: groq not installed. Run:  pip install groq")
    sys.exit(1)

# ─── config ───────────────────────────────────────────────────────────────────

MCP_URL = "http://localhost:5002"
MODEL   = "llama-3.3-70b-versatile"   # Groq free tier, supports tool_use

SYSTEM_PROMPT = """You are an expert Apache Iceberg data platform assistant with deep knowledge of:
- Apache Iceberg table format (v1 and v2), snapshots, partition specs, and schema evolution
- SQL query optimisation for lakehouse architectures
- Time-travel queries using both snapshot-id (FOR VERSION AS OF) and timestamp (FOR SYSTEM_TIME AS OF)
- Iceberg partition pruning mechanics: how transforms (identity/month/year/hour/bucket) affect query plans

You have access to a live Iceberg REST Catalog via tools. Use them to answer questions accurately.

TOOL USAGE RULES:
1. ALWAYS call list_namespaces first if the user asks a discovery question without a specific table.
2. ALWAYS call list_tables(namespace) before describe_table — never guess table names.
3. DO NOT call describe_table for every table — only for the specific table the user asked about.
4. For 'what changed' or 'history' questions: call get_snapshots, look for schema_id changes,
   then optionally call describe_table once to get the column diff.
5. For time-travel questions: call get_snapshots to understand the timeline,
   then call time_travel_info to get the ready-to-use SQL.
6. If a tool returns an error, explain it clearly and suggest the corrective action.

When presenting SQL: format it in a code block. When presenting schemas: use a table format.
Be precise about Iceberg internals — this audience is technical (Iceberg Bengaluru Meetup).
"""

# ─── terminal colours ─────────────────────────────────────────────────────────

class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    CYAN   = "\033[36m"
    GREEN  = "\033[32m"
    YELLOW = "\033[33m"
    RED    = "\033[31m"
    MAGENTA= "\033[35m"
    BLUE   = "\033[34m"
    WHITE  = "\033[37m"
    BG_DARK= "\033[40m"

def banner(text: str, color=C.CYAN) -> None:
    width = 70
    print(f"\n{color}{C.BOLD}{'─' * width}{C.RESET}")
    print(f"{color}{C.BOLD}  {text}{C.RESET}")
    print(f"{color}{C.BOLD}{'─' * width}{C.RESET}")

def section(label: str, color=C.DIM) -> None:
    print(f"\n{color}{'·' * 3} {label}{C.RESET}")

def tool_call_display(name: str, params: dict) -> None:
    params_str = json.dumps(params, separators=(',', ':'))
    print(f"\n  {C.YELLOW}{C.BOLD}⚙  TOOL CALL{C.RESET}  {C.YELLOW}{name}{C.RESET}"
          f"{C.DIM}({params_str}){C.RESET}")

def tool_result_display(name: str, result: dict, latency_ms: int) -> None:
    result_str = json.dumps(result, indent=2)
    # Summarise for display (don't dump the whole thing)
    lines = result_str.split("\n")
    preview_lines = lines[:20]
    if len(lines) > 20:
        preview_lines.append(f"  ... ({len(lines) - 20} more lines)")
    preview = "\n".join(preview_lines)
    print(f"\n  {C.GREEN}✓  TOOL RESULT{C.RESET}  {C.DIM}{name}  [{latency_ms}ms]{C.RESET}")
    for line in preview.split("\n"):
        print(f"  {C.DIM}{line}{C.RESET}")


# ─── MCP tool fetcher ─────────────────────────────────────────────────────────

def fetch_mcp_tools() -> list[dict]:
    """Fetch tool definitions from the MCP server."""
    try:
        resp = requests.get(f"{MCP_URL}/tools/list", timeout=5)
        resp.raise_for_status()
        return resp.json()["tools"]
    except Exception as e:
        print(f"{C.RED}ERROR: Cannot connect to MCP server at {MCP_URL}: {e}{C.RESET}")
        print(f"{C.DIM}Make sure mcp_server.py is running on port 5002{C.RESET}")
        sys.exit(1)


def call_mcp_tool(tool_name: str, arguments: dict) -> dict:
    """Dispatch a tool call to the MCP server."""
    start = time.time()
    try:
        resp = requests.post(
            f"{MCP_URL}/tools/call",
            json={"name": tool_name, "arguments": arguments},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        latency_ms = int((time.time() - start) * 1000)

        # MCP returns content[] array; extract the text
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")

        result = json.loads(text) if text else {}
        return result, latency_ms, data.get("isError", False)

    except Exception as e:
        latency_ms = int((time.time() - start) * 1000)
        return {"error": str(e)}, latency_ms, True


# ─── Groq tool-use agent loop ─────────────────────────────────────────────────

class IcebergAgent:
    def __init__(self, api_key: str):
        self.client = Groq(api_key=api_key)
        self.tools = fetch_mcp_tools()
        self.conversation: list[dict] = []
        self.session_stats = {"turns": 0, "tool_calls": 0, "total_latency_ms": 0}

        section(f"Loaded {len(self.tools)} MCP tools from {MCP_URL}", C.GREEN)
        for t in self.tools:
            print(f"    {C.GREEN}·{C.RESET}  {t['name']}")

    def _groq_tools(self) -> list[dict]:
        """Convert MCP tool definitions to Groq's tool format."""
        return [
            {
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t["description"],
                    "parameters": t["inputSchema"],
                },
            }
            for t in self.tools
        ]

    def chat(self, user_message: str) -> str:
        """Run one user turn through the full agent loop."""
        self.session_stats["turns"] += 1
        self.conversation.append({"role": "user", "content": user_message})

        messages = [{"role": "system", "content": SYSTEM_PROMPT}] + self.conversation
        tool_calls_this_turn = 0

        # ── Agent loop: LLM → tool call → inject result → repeat ──────────────
        while True:
            section(f"Calling Groq [{MODEL}] …", C.DIM)

            response = self.client.chat.completions.create(
                model=MODEL,
                messages=messages,
                tools=self._groq_tools(),
                tool_choice="auto",
                temperature=0.1,       # low temp for deterministic catalog queries
                max_tokens=2048,
            )

            msg = response.choices[0].message

            # If no tool call, we have the final answer
            if not msg.tool_calls:
                final_text = msg.content or ""
                self.conversation.append({"role": "assistant", "content": final_text})
                return final_text

            # Process all tool calls in this response
            tool_results = []
            for tc in msg.tool_calls:
                fn = tc.function
                tool_name = fn.name
                try:
                    arguments = json.loads(fn.arguments)
                except json.JSONDecodeError:
                    arguments = {}

                tool_call_display(tool_name, arguments)
                result, latency_ms, is_error = call_mcp_tool(tool_name, arguments)
                tool_result_display(tool_name, result, latency_ms)

                tool_calls_this_turn += 1
                self.session_stats["tool_calls"] += 1
                self.session_stats["total_latency_ms"] += latency_ms

                tool_results.append({
                    "tool_call_id": tc.id,
                    "role": "tool",
                    "name": tool_name,
                    "content": json.dumps(result),
                })

            # Add the assistant's tool-use message + all tool results to messages
            messages.append({"role": "assistant", "content": None, "tool_calls": [
                {"id": tc.id, "type": "function",
                 "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                for tc in msg.tool_calls
            ]})
            messages.extend(tool_results)

    def print_stats(self) -> None:
        avg_lat = (self.session_stats["total_latency_ms"] / max(self.session_stats["tool_calls"], 1))
        print(f"\n{C.DIM}Session stats: "
              f"{self.session_stats['turns']} turns · "
              f"{self.session_stats['tool_calls']} tool calls · "
              f"avg tool latency {avg_lat:.0f}ms{C.RESET}")


# ─── Main REPL ────────────────────────────────────────────────────────────────

DEMO_QUERIES = [
    "What namespaces and tables are in this lakehouse?",
    "Tell me about the orders table — schema, partitioning, and recent activity.",
    "What changed in the orders table in the last two weeks? Any schema evolution?",
    "I need to query the orders data as it was last Monday. Give me the time-travel SQL.",
    "How should I write efficient Spark SQL against the orders table to avoid full scans?",
    "Compare the schema of orders and customers tables.",
]

def main():
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        print(f"\n{C.RED}ERROR: GROQ_API_KEY environment variable not set.{C.RESET}")
        print(f"{C.DIM}Get a free key at https://console.groq.com → API Keys → Create key.{C.RESET}")
        print(f"{C.DIM}Then run:  export GROQ_API_KEY='gsk_...'  and restart this script.{C.RESET}\n")
        sys.exit(1)

    banner("Talk to Your Lakehouse — Iceberg MCP Demo", C.CYAN)
    print(f"  {C.DIM}Model:      {MODEL}{C.RESET}")
    print(f"  {C.DIM}MCP server: {MCP_URL}{C.RESET}")
    print(f"  {C.DIM}Session:    {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}{C.RESET}")

    agent = IcebergAgent(api_key=api_key)

    print(f"\n{C.DIM}Suggested queries (copy-paste or type your own):{C.RESET}")
    for i, q in enumerate(DEMO_QUERIES, 1):
        print(f"  {C.DIM}{i}. {q}{C.RESET}")

    print(f"\n{C.DIM}Type 'quit' or Ctrl-C to exit.{C.RESET}\n")

    while True:
        try:
            user_input = input(f"{C.BOLD}{C.WHITE}You › {C.RESET}").strip()
        except (KeyboardInterrupt, EOFError):
            print(f"\n{C.DIM}Exiting.{C.RESET}")
            agent.print_stats()
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            agent.print_stats()
            break

        # Shortcut: type a number to pick a demo query
        if user_input.isdigit() and 1 <= int(user_input) <= len(DEMO_QUERIES):
            user_input = DEMO_QUERIES[int(user_input) - 1]
            print(f"  {C.DIM}→ {user_input}{C.RESET}")

        section("Agent turn", C.MAGENTA)
        t0 = time.time()
        try:
            answer = agent.chat(user_input)
            elapsed = time.time() - t0
            banner(f"Answer  [{elapsed:.1f}s]", C.GREEN)
            # Pretty-print the answer with wrapping
            for line in answer.split("\n"):
                if line.startswith("```"):
                    print(f"{C.CYAN}{line}{C.RESET}")
                elif line.startswith("#"):
                    print(f"{C.BOLD}{line}{C.RESET}")
                else:
                    print(textwrap.fill(line, width=80) if len(line) > 80 else line)
            print()
        except Exception as e:
            print(f"\n{C.RED}ERROR: {e}{C.RESET}\n")


if __name__ == "__main__":
    main()
