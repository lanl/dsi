import asyncio
import json
from langchain_mcp_adapters.client import MultiServerMCPClient


URL = "http://127.0.0.1:8000/mcp"
DB_PATH = "/Users/pascalgrosset/projects/dsi/tools/ai_search/data/oceans_11/ocean_11_datasets.db"
#DB_PATH = "/home/pascalgrosset/projects/dsi/tools/ai_search/data/oceans_11/ocean_11_datasets.db"
QUERY_OK = "SELECT * FROM genesis_datacard LIMIT 3"


def unwrap_text_json(blocks):
    # Adapter variants:
    # 1) raw JSON string: '{ ... }'
    # 2) list of content blocks: [{"type":"text","text":"{...}"}]
    # 3) dict already parsed (sometimes)
    if isinstance(blocks, str):
        return json.loads(blocks)
    if isinstance(blocks, dict):
        return blocks
    if isinstance(blocks, list) and blocks:
        first = blocks[0]
        if isinstance(first, dict) and "text" in first:
            return json.loads(first["text"])
    raise TypeError(f"Unexpected tool return: {type(blocks)} {blocks!r}")


async def main():
    client = MultiServerMCPClient(
        connections={"dsi-tools": {"transport": "streamable_http", "url": URL}}
    )

    tools = await client.get_tools()
    tool_by_name = {t.name: t for t in tools}
    print("TOOLS:", list(tool_by_name.keys()))

    info_raw = await tool_by_name["get_db_info"].ainvoke({"db_path": DB_PATH})
    info = unwrap_text_json(info_raw)

    print("\nget_db_info:")
    print("tables_count:", len(info["tables"]))
    print("first_tables:", info["tables"][:10])
    print("has_schema:", bool(info["schema"]))
    print("desc_len:", len(info["description"]))

    res_raw = await tool_by_name["query_db"].ainvoke({"db_path": DB_PATH, "query_str": QUERY_OK})
    res = unwrap_text_json(res_raw)

    print("\nquery_db:")
    print("row_count:", res["row_count"])
    if res["rows"]:
        print("first_row:", json.dumps(res["rows"][0], indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
    
    
# Run as:
# python tools/ai_search/mcp/test_mcp.py