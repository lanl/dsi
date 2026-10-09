# examples/backends/denodo/2.search_parameters.py
"""
The two search parameters that shape a query:
  search_in - where to look   (name, description, properties,
                               column_names, column_descriptions)
  match     - how to match    (substring, all_words, any_words)

Authenticates once, then reuses that token for every query.
"""

from dsi.dsi import DSI

def row_count(dsi):
    return len(dsi.get_table("denodo_search_results", collection=True))

def main():
    # The first query authenticates; keep its token for the rest
    first = DSI(backend_name="Denodo",
                params={"keywords": "area", "search_in": ["name"]})
    token = first.main_backend_obj.token
    print(f"\nsearch_in=['name']                 'area' -> {row_count(first)} views")
    first.close()

    for scope in ["description", "properties", "column_names", "column_descriptions"]:
        dsi = DSI(backend_name="Denodo", token=token,
                  params={"keywords": "area", "search_in": [scope]})
        print(f"search_in=['{scope}']  'area' -> {row_count(dsi)} views")
        dsi.close()

    # The three matching modes, on a two-word phrase
    print()
    for match in ["substring", "all_words", "any_words"]:
        dsi = DSI(backend_name="Denodo", token=token,
                  params={"keywords": "area type", "search_in": ["name"], "match": match})
        print(f"match='{match}'   'area type' -> {row_count(dsi)} views")
        dsi.close()

    # An empty keyword returns the whole catalog
    whole = DSI(backend_name="Denodo", token=token, params={"keywords": ""})
    print(f"\nkeywords=''  (whole catalog) -> {row_count(whole)} views")
    whole.close()

if __name__ == "__main__":
    main()
