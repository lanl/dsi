from dsi.dsi import DSI

store = DSI("mnist.db")
store.read("mnist.pq", "Parquet", "mnisttrain")