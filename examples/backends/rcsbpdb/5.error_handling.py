import pandas as pd

from dsi.dsi import DSI


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        silence_messages=True,
    )

    errors_df = dsi.query(
        {"identifiers": ["NOT_A_PDB_ID"]},
        collection=True,
    )
    errors_table = errors_df.attrs["table_name"]

    print("\nRetrieved an error table for an invalid PDB identifier.")
    print(f"Cached table: {errors_table}")

    print("\nAvailable tables:")
    dsi.list()

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

    print("\nFind skipped records in cached error data:")
    dsi.query("status = skipped")

    dsi.close()


if __name__ == "__main__":
    main()