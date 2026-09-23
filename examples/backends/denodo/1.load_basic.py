# examples/backends/denodo/1.load_basic.py
"""
Basic Denodo Data Catalog search: one POST call, one table.

Requires the settings described in README.md ("Configuration"): the base URL
plus the OAuth client settings, from environment variables or
~/.denodo/config.json. The first request opens a browser to log in.
"""

from dsi.dsi import DSI

def main():
    # Search every view's custom properties for the term "CUI"
    dsi = DSI(
        backend_name="Denodo",
        params={"keywords": "CUI", "search_in": ["properties"], "match": "substring"},
    )

    print("\nTable List:")
    dsi.list()

    print("\nTable Summary:")
    dsi.summary()

    print("\nSchema:")
    print(dsi.schema())          # no arguments: returns this backend's schema

    dsi.close()

if __name__ == "__main__":
    main()
