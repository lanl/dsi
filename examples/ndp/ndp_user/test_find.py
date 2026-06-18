from dsi.dsi import DSI

# Initialize NDP backend
dsi = DSI(
    backend_name="NDP",
    keywords="temperature",
    limit=5
)

# ✅ CORRECT: Use find() with pandas query syntax
dsi.find("num_resources > 2")
dsi.find("num_resources < 2")
dsi.find('organization == "Oceans11 - LANL"')

# ❌ WRONG: Don't use query() - will raise NotImplementedError
# dsi.query("SELECT * FROM datasets WHERE num_resources > 2")