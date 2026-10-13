# examples/scout/2.hpc_source.py
"""
Register the LANL Oceans11 catalog as an HPC/url source (not the dedicated Oceans11
webserver backend) and query it with DSIScout, demonstrating the HPC catalog-walk path:
pull the referenced database, auto-detect its schema via hpc_schema_crosswalk.json, and
filter+merge its primary table into fed_results.
"""

from dsi.dsi_scout import DSIScout


def main():
    scout = DSIScout()
    scout.add_source(
        location_type="url",
        location="oceans11.lanl.gov",
        path="https://oceans11.lanl.gov/dataCatalog/oceans11.db",
        submitter_name="example",
    )

    print("\nRegistered sources:")
    for source in scout.list_sources():
        print(f"  {source}")

    scout.query(keyword="heat", limit=5)

    print("\nTable List:")
    scout.list()

    print("\nfed_results:")
    df = scout.get_table("fed_results", collection=True)
    print(df[["fed_source", "fed_id", "title", "authors"]].to_string())

    scout.close()


if __name__ == "__main__":
    main()
