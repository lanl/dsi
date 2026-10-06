
## 1.load_basic.py

from dsi.dsi import DSI


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"keywords": "genomics", "limit": 5},
        silence_messages=True,
    )

    print("\nLoaded RCSBPDB backend using keyword search.")

    print("\nAvailable tables:")
    dsi.list()

    print("\nBackend summary:")
    dsi.summary()

    dsi.close()


if __name__ == "__main__":
    main()