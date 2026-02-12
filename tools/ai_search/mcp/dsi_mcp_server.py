from mcp.server.fastmcp import FastMCP
from tools.ai_search.mcp.dsi_tools import get_db_tool, query_dsi_tool


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


if __name__ == "__main__":
    mcp.run(transport="streamable-http")


# Run as:
# python tools/ai_search/mcp/dsi_mcp_server.py