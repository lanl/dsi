# examples/scout/3.mixed_sources.py
"""
Register a webserver backend (OSTI) and an HPC catalog source (the LANL Oceans11
catalog, reached as a plain url source) together at construction time, and run one query
across all three - the core DSIScout value: a single federated result spanning both
webserver APIs and HPC catalog chains.
"""

from dsi.dsi_scout import DSIScout


def main():
    scout = DSIScout(
        sources=[
            "OSTI",
            {
                "location_type": "url",
                "location": "oceans11.lanl.gov",
                "path": "https://oceans11.lanl.gov/dataCatalog/oceans11.db",
                "submitter_name": "example",
            },
        ],
        keyword="heat",
        limit=5,
    )

    print("\nTable List:")
    scout.list()

    print("\nfed_results by source:")
    df = scout.get_table("fed_results", collection=True)
    print(df["fed_source"].value_counts())

    print("\nSummary:")
    scout.summary("fed_results")

    scout.close()


if __name__ == "__main__":
    main()
