from pathlib import Path

from dsi.dsi import DSI

output_db = Path("rcsbpdb_repro_empty_errors.db")
if output_db.exists():
    output_db.unlink()

dsi = DSI(
    backend_name="RCSBPDB",
    params={"identifiers": ["1CBS", "10.2210/pdb4hhb/pdb"]},
    silence_messages=True,
)

print("\nTables before process:")
dsi.list()

print("\nErrors table before process:")
dsi.display("errors")

print("\nTrying to write to SQLite...")
dsi.process("sqlite", str(output_db))

print(f"\nSQLite file written to: {output_db}")
dsi.close()
