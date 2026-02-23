from __future__ import annotations
import sys
import sys
from typing import Any, Dict, List, Optional, Union
from mcp.server.fastmcp import FastMCP
import argparse

from typing import Any, Dict, List, Optional, Union
from typing_extensions import Annotated

from tool_utils import *


# mcp = FastMCP("dsi-explorer-tools")
mcp = FastMCP(
    "dsi-tools",
    host="127.0.0.1",
    port=8000,
    streamable_http_path="/mcp",   # <- this is what your client URL should use
)


@mcp.tool()
def load_dsi_tool(
    path: Annotated[str, "the path to the DSI object to load"],
    run_path: Annotated[str, "the path this code is being run from"] = "",
    master_db_folder: Annotated[str, "the folder containing the master database, used to resolve relative paths when loading new databases"] = "",
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
    code: Annotated[str, "Python code to execute. All generated outputs (plots, data, files) must be written to disk."],
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
    smtp_host: Annotated[str, "The SMTP server hostname. Defaults to \"smtp.example.com\"."] = "smtp.example.com",
    smtp_port: Annotated[int, "The SMTP server port (typically 587 for TLS, 465 for SSL). Defaults to 587."] = 587,
    username: Annotated[Optional[str], "SMTP authentication username, or None if not required."] = None,
    password: Annotated[Optional[str], "SMTP authentication password, or None if not required."] = None,
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
    mcp_transport = "streamable-http"
    if sys.argv[1:]:
        mcp_transport = sys.argv[1]

    mcp.run(transport=mcp_transport)
