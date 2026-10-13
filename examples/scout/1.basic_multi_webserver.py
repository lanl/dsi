# examples/scout/1.basic_multi_webserver.py
"""
Federate an keyword search across two webserver backends (NDP and OSTI) with DSIScout.


"""

from dsi.dsi_scout import DSIScout


def main():
    scout = DSIScout()
    scout.add_source(webserver_name="NDP")
    scout.add_source(webserver_name="OSTI")

    print("\nRegistered sources:")
    for source in scout.list_sources():
        print(f"  {source}")

    scout.query(keyword="quic-fire", limit=5)

    print("\nTable List:")
    scout.list()

    print("\nfed_results by source:")
    df = scout.get_table("fed_results", collection=True)
    print(df["fed_source"].value_counts())

    scout.close()


if __name__ == "__main__":
    main()
