# examples/navdat/1.load_basic.py
"""
Basic NAVDAT search using an author filter.
"""

from dsi.dsi import DSI


def main():
    # Initialize NAVDAT backend with a basic author search.
    # `authors` is confirmed (2026-08-24) to filter both the `samples` and
    # `citations` tables. `size` controls the samples page size (single
    # page only - see README "Known Limitations").
    dsi = DSI(
        backend_name="NAVDAT",
        params={"authors": "Walker", "size": 10}
    )

    print("\nTable List:")
    dsi.list()

    print("\nTable Summary:")
    dsi.summary()

    dsi.close()


if __name__ == "__main__":
    main()