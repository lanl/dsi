#!/usr/bin/env python

from mcp.server.fastmcp import FastMCP
from tools.ai_search.mcp.dsi_tools import get_db_tool, query_dsi_tool
import argparse


mcp = FastMCP(
    "dsi-tools",
    host="127.0.0.1",
    port=8000,
    streamable_http_path="/mcp",   # <- this is what your client URL should use
)


@mcp.tool()
def get_db_info(db_path: str):
    tables, schema, desc = get_db_tool(db_path)
    return {"tables": tables, "schema": schema, "description": desc}


@mcp.tool()
def query_db(db_path: str, query_str: str):
    rows = query_dsi_tool(query_str=query_str, db_path=db_path)
    return {"rows": rows, "row_count": len(rows)}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DSI MCP server")
    p.add_argument(
        "--transport",
        default="streamable-http",
        help="Transport for MCP server (e.g., streamable-http, stdio). Default: streamable-http",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    mcp.run(transport=args.transport)


# Run as:
# python tools/ai_search/mcp/dsi_mcp_server.py
# or
# python -m tools.ai_search.mcp.dsi_mcp_server