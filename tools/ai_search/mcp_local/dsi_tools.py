import io
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from contextlib import redirect_stdout, redirect_stderr
import pandas as pd

from dsi.dsi import DSI
_NULL = io.StringIO()  # to hide DSI outputs

def load_db_description(db_path: str) -> str:
    """Load the database description from a YAML file when provided with the path to a DSI database.

    Arg:
        db_path (str): the absolute path of the DSI database
        
    Returns:
        str: message indicating success or failure
    """

    try:
        # The description file is expected to be in the same directory as the database, with the same name but ending in '_description.yaml'
        db_description_path = db_path.rsplit(".", 1)[0] + '_description.yaml'
        
        with open(db_description_path, "r") as f:
            db_description = f.read()

        return str(db_description)
    except:
        return ""


def check_db_valid(db_path: str) -> bool:
    """Check if the provided path points to a valid DSI database.

    Arg:
        db_path (str): the absolute path of the DSI database
        
    Returns:
        bool: True if the database is valid, False otherwise
    """
    
    if not os.path.exists(db_path):
        return False
    else:
        try:
            # with open(os.devnull, "w") as fnull:
            #     with redirect_stdout(fnull), redirect_stderr(fnull):
            with redirect_stdout(_NULL), redirect_stderr(_NULL):
                temp_store = DSI(db_path, check_same_thread=False)
                temp_tables = temp_store.list(True) # force things to fail if the table is empty
                temp_store.close()
                    
        except Exception as e:
            return False

    return True
      
    
def query_dsi_tool(query_str: str, db_path: str) ->list:
    """Execute a SQL query on a DSI object

    Arg:
        query_str (str): the SQL query to run on DSI object
        db_path (str): the absolute path to the DSI database to query

    Returns:
        collection: the results of the query
    """
    
    _store = None
    try:
        _store = DSI(db_path, check_same_thread=False)

        # with open(os.devnull, "w") as fnull:
        #     with redirect_stdout(fnull), redirect_stderr(fnull):
        with redirect_stdout(_NULL), redirect_stderr(_NULL):
            df = _store.query(query_str, collection=True)
                
        if df is None:
            return []
        return df.to_dict(orient="records")
    
    except Exception as e:   
        return []
    
    finally:
        if _store is not None:
            try:
                with redirect_stdout(_NULL), redirect_stderr(_NULL):
                    _store.close()
            except Exception:
                pass


def get_db_tool(db_path: str) -> tuple[list, dict, str]:
    """Load the database information (tables and schema) from a DSI database.

    Arg:
        db_path (str): the absolute path of the DSI database    
        
    Returns:
        list: the list of tables in the database
        dict: the schema of the database
        str: the description of the database (if available, otherwise empty string)
    """
    
    tables = []
    schema = {}
    desc = ""
    
    if check_db_valid(db_path) == False:
        return tables, schema, desc
    
    
    try:
        # with open(os.devnull, "w") as fnull:
        #     with redirect_stdout(fnull), redirect_stderr(fnull):
        with redirect_stdout(_NULL), redirect_stderr(_NULL):
            _dsi_store = DSI(db_path, check_same_thread=False)
            tables = _dsi_store.list(True)
            schema = _dsi_store.schema()
            desc = load_db_description(db_path)
            _dsi_store.close()
            
        return tables, schema, desc

    except Exception as e:
        return tables, schema, desc


def test_dsi_tools():
    db_path = "/Users/pascalgrosset/projects/dsi/tools/ai_search/data/oceans_11/ocean_11_datasets.db"
    
    tables, schema, desc = get_db_tool(db_path)
    print("Tables:", tables)
    print("Schema:", json.dumps(schema, indent=2))
    print("Description:", desc)
    
    query = "SELECT * FROM alexandria_project LIMIT 5"
    results = query_dsi_tool(query, db_path)
    print("Query Results:", json.dumps(results, indent=2))
    
    
    query = "SELECT * FROM alexandria_ LIMIT 5"
    results = query_dsi_tool(query, db_path)
    print("Query Results:", json.dumps(results, indent=2))
    
    
    db_path = "/Users/pascalgrosset/projects/dsi/tools/ai_search/data/oceans_11/nif.db"
    tables, schema, desc = get_db_tool(db_path)
    print("Tables:", tables)
    print("Schema:", json.dumps(schema, indent=2))
    print("Description:", desc)


def main() -> None:
    test_dsi_tools()


if __name__ == "__main__":
    main()