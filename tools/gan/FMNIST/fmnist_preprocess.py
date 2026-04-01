from dsi.dsi import DSI

store = DSI("fmnist.db")
store.read("fmnist.pq", "Parquet", "fashion")