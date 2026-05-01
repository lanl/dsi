"""
Small wwPDB backend demo.

Run from the repository root with something like:

python -m examples.wwpdb.wwpdb_demo
"""

from dsi.backends.wwpdb import WWPDB


def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def show_rows(label, rows, limit=5):
    print(f"\n{label}: {len(rows)} row(s)")
    for row in rows[:limit]:
        print(row)


def show_backend(label, backend):
    print_section(label)

    print("\nSummary:")
    print(backend.summary())

    print("\nTable names:")
    print(backend.get_table_names())

    print("\nDatasets schema:")
    print(backend.get_schema("datasets"))

    datasets = backend.get_table("datasets")
    resources = backend.get_table("resources")
    errors = backend.get_table("errors")

    print("\nCompact dataset view:")
    for row in datasets[:5]:
        print(
            "-",
            row.get("dataset_id"),
            "|",
            row.get("experimental_method"),
            "|",
            row.get("release_date"),
            "|",
            row.get("title"),
        )

    print("\nCompact resource view:")
    for row in resources[:5]:
        print(
            "-",
            row.get("resource_id"),
            "|",
            row.get("dataset_id"),
            "|",
            row.get("format"),
            "|",
            row.get("download_url"),
        )

    show_rows("Errors", errors, limit=5)


def demo_identifier_mode():
    with WWPDB(
        identifiers=["1CBS", "10.2210/pdb4hhb/pdb"],
        auto_load=True,
    ) as backend:
        show_backend("1. Identifier-driven mode: PDB ID + wwPDB DOI", backend)


def demo_query_param_mode():
    with WWPDB(
        params={"keywords": "hemoglobin", "limit": 5},
        auto_load=True,
    ) as backend:
        show_backend("2. Query-driven mode: params keyword search", backend)

        print("\nSearch loaded in-memory tables for 'hemoglobin':")
        matches = backend.find("hemoglobin")
        print(f"Found {len(matches)} matching cell(s).")
        for match in matches[:5]:
            print(match.to_dict())


def demo_query_artifacts_mode():
    with WWPDB(auto_load=False) as backend:
        backend.query_artifacts({"keywords": "retinoic acid", "limit": 5})
        show_backend("3. DSI-facing query_artifacts(params) mode", backend)


def demo_error_handling_mode():
    with WWPDB(
        identifiers=["not-a-pdb-id"],
        auto_load=True,
    ) as backend:
        show_backend("4. Error handling mode: invalid identifier", backend)


def demo_excel_style_batch_input():
    """
    This demonstrates the current role of the old Excel file.

    The backend no longer reads Excel directly. In the old prototype,
    an Excel DOI column was read into a list. That list can still be
    passed to WWPDB as batch identifiers.
    """
    print_section("5. Excel-style batch input demonstration")

    excel_like_doi_column = [
        "10.2210/pdb1cbs/pdb",
        "10.2210/pdb4hhb/pdb",
    ]

    print("\nExcel DOI column would become this Python list:")
    print(excel_like_doi_column)

    with WWPDB(identifiers=excel_like_doi_column, auto_load=True) as backend:
        show_backend("Excel-style DOI list passed as identifiers", backend)


def main():
    demo_identifier_mode()
    demo_query_param_mode()
    demo_query_artifacts_mode()
    demo_error_handling_mode()
    demo_excel_style_batch_input()


if __name__ == "__main__":
    main()