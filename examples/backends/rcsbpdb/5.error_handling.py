import pandas as pd

from dsi.dsi import DSI


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        params={
            "identifiers": [
                "1CBS",
                "NOT_A_PDB_ID",
            ]
        },
        silence_messages=True,
    )

    print("\nLoaded one valid PDB ID and one invalid identifier.")

    print("\nAvailable tables:")
    dsi.list()

    errors_df = dsi.get_table("errors", collection=True)

    print("\nErrors table:")
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        None,
        "display.max_colwidth",
        None,
    ):
        print(errors_df)

    print("\nFind skipped records:")
    dsi.find("status = skipped")

    dsi.close()


if __name__ == "__main__":
    main()