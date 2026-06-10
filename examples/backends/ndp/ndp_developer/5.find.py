# examples/ndp/ndp_developer/5.find.py
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "climate ocean", "limit": 10}
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    tables_found = backend.find_table("datasets")
    columns_found = backend.find_column("title")
    cells_found = backend.find_cell("climate")
    
    if verbose:
        print(f"\n=== Tables matching 'datasets': {len(tables_found)} ===")
        for v in tables_found:
            print(f"  {v.t_name}")
            print(f"    Columns ({len(v.c_name)}): {', '.join(v.c_name[:5])}...")
        
        print(f"\n=== Columns matching 'title': {len(columns_found)} ===")
        for v in columns_found[:3]:
            print(f"  {v.t_name}.{v.c_name[0]}")
            print(f"    Sample values: {v.value[:2]}")
        
        print(f"\n=== Cells matching 'climate': {len(cells_found)} ===")
        for v in cells_found[:5]:
            preview = str(v.value)[:60] + "..." if len(str(v.value)) > 60 else str(v.value)
            print(f"  {v.t_name}.{v.c_name[0]}[{v.row_num}]: {preview}")
    
    terminal.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)