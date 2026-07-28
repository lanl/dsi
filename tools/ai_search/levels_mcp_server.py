"""LEVELS MCP server — desktop-reachable DSI tools via Streamable HTTP.

Use this server when connecting from
MCP Inspector or another desktop client by machine IP.

Run (from tools/ai_search)::

    python levels_mcp_server.py
    python levels_mcp_server.py --host 127.0.0.1   # local-only

Then in another terminal::

    npx @modelcontextprotocol/inspector

In Inspector, choose transport **Streamable HTTP** and connect to
``http://127.0.0.1:8000/mcp`` or ``http://<your-ip>:8000/mcp``.
"""

from __future__ import annotations

import argparse
import socket
from typing import Any, Dict, List, Optional, Union

from mcp.server.fastmcp import FastMCP
from typing_extensions import Annotated

from tool_utils import *


def _local_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="LEVELS MCP server (DSI tools)")
    p.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind address (default 0.0.0.0 for desktop/IP access)",
    )
    p.add_argument("--port", type=int, default=8000, help="Port (default 8000)")
    p.add_argument(
        "--transport",
        default="streamable-http",
        help="MCP transport (default streamable-http)",
    )
    return p.parse_args()


mcp = FastMCP(
    "levels-dsi-tools",
    host="0.0.0.0",
    port=8000,
    streamable_http_path="/mcp",
)


@mcp.tool()
def load_dsi_tool(
    path: Annotated[str, "the path to the DSI object to load"],
    run_path: Annotated[str, "the path this code is being run from"] = "",
    master_db_folder: Annotated[
        str,
        "the folder containing the master database, used to resolve relative paths when loading new databases",
    ] = "",
) -> Union[Dict[str, Any], str]:
    """Load a DSI object from the path and add information to the context for the llm to use."""
    return load_dsi(path=path, run_path=run_path, master_db_folder=master_db_folder)


@mcp.tool()
def query_dsi_tool(
    query_str: Annotated[str, "the SQL query to run on DSI object"],
    db_path: Annotated[str, "the absolute path to the DSI database to query"],
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Execute a SQL query on a DSI object."""
    return query_dsi(query_str=query_str, db_path=db_path)


@mcp.tool()
def python_repl_tool(
    code: Annotated[
        str,
        "Python code to execute. All generated outputs (plots, data, files) must be written to disk.",
    ],
) -> str:
    """Executes arbitrary Python code. The executed code must save any plots or files to disk, not return them directly."""
    return python_repl(code=code)


@mcp.tool()
def download_file_tool(
    url: Annotated[str, "a string containing the URL of the file to download."],
    output_path: Annotated[str, "the local path where the file should be saved."] = ".",
) -> str:
    """Download a file from a URL and save it locally."""
    return download_file(url=url, output_path=output_path)


@mcp.tool()
def arxiv_search_tool(
    query: Annotated[str, "Topic or keywords to search for."],
    max_results: Annotated[int, "How many papers to return (default 5)."] = 10,
) -> List[Dict[str, Any]]:
    """Search arXiv.org for research papers related to a given topic."""
    return arxiv_search(query=query, max_results=max_results)


@mcp.tool()
def wikipedia_search_tool(
    query: Annotated[str, "Search query or topic."],
    max_results: Annotated[int, "Max number of results from each source."] = 5,
) -> List[Dict[str, Any]]:
    """Search Wikipedia for information about the search query."""
    return wikipedia_search(query=query, max_results=max_results)


@mcp.tool()
def web_search_tool(
    query: Annotated[str, "Search query"],
    max_results: Annotated[int, "Max number of results to return"] = 5,
) -> List[Dict[str, Any]]:
    """Perform a web search on the topics and return structured search results with title, snippet, and URL."""
    return web_search(query=query, max_results=max_results)


@mcp.tool()
def send_email_tool(
    recipient: Annotated[str, "The destination email address."],
    subject: Annotated[str, "The subject line of the email."],
    body: Annotated[str, "The plain-text body of the email."],
    smtp_host: Annotated[
        str, 'The SMTP server hostname. Defaults to "smtp.example.com".'
    ] = "smtp.example.com",
    smtp_port: Annotated[
        int,
        "The SMTP server port (typically 587 for TLS, 465 for SSL). Defaults to 587.",
    ] = 587,
    username: Annotated[
        Optional[str], "SMTP authentication username, or None if not required."
    ] = None,
    password: Annotated[
        Optional[str], "SMTP authentication password, or None if not required."
    ] = None,
) -> str:
    """Send an email using SMTP."""
    return send_email(
        recipient=recipient,
        subject=subject,
        body=body,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        username=username,
        password=password,
    )


@mcp.tool()
def upload_paper_tool(
    path: Annotated[str, "the path of the DSI object to load"],
) -> Dict[str, str]:
    """Upload a local PDF to OpenAI Files API and return identifiers."""
    return upload_paper(path=path)


if __name__ == "__main__":
    args = parse_args()
    mcp.settings.host = args.host
    mcp.settings.port = args.port
    path = mcp.settings.streamable_http_path
    lines = [
        f"LEVELS MCP server listening on {args.host}:{args.port}",
        f"  local:  http://127.0.0.1:{args.port}{path}",
    ]
    if args.host in ("0.0.0.0", "::"):
        lines.append(f"  LAN:    http://{_local_ip()}:{args.port}{path}")
    lines.append(f"Transport: {args.transport}")
    lines.append(
        "Inspector: npx @modelcontextprotocol/inspector  "
        "(Streamable HTTP → URL above)"
    )
    print("\n".join(lines), flush=True)
    mcp.run(transport=args.transport)
