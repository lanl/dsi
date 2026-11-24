from dsi.dsi import DSI


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        silence_messages=True,
    )

    datasets_df = dsi.query(
        {"keywords": "genomics", "limit": 5},
        collection=True,
    )

    print("\nRetrieved RCSBPDB dataset-discovery results.")
    print(f"Cached table: {datasets_df.attrs['table_name']}")

    print("\nAvailable tables:")
    dsi.list()

    print("\nBackend summary:")
    dsi.summary()

    dsi.close()


if __name__ == "__main__":
    main()