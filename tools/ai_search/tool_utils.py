from __future__ import annotations

import os
import io
import requests
import smtplib
import feedparser
import logging


from contextlib import redirect_stdout, redirect_stderr
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from typing_extensions import Annotated

from langchain_core.tools import tool
from langchain_core.tools import tool
from langchain_openai import OpenAI
from langchain_experimental.utilities import PythonREPL
from openai import OpenAI

from dsi.dsi import DSI

_NULL = io.StringIO()  # to hide DSI outputs
logger = logging.getLogger(__name__)

########################################################################
#### Utility functions

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
        logger.info(f"load_db_description :: db_description_path: {db_description_path}")
        
        with open(db_description_path, "r") as f:
            db_desc = f.read()

        return str(db_desc)
    except:
        logger.info(f"load_db_description :: Failed: No database description file found at  {db_description_path}")
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
            with redirect_stdout(_NULL), redirect_stderr(_NULL):
                temp_store = DSI(db_path, check_same_thread=False)
                temp_tables = temp_store.list(True) # force things to fail if the table is empty
                temp_store.close()
                    
        except Exception as e:
            return False

    return True



def get_db_info(db_path: str) -> tuple[list, dict, str]:
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
        with redirect_stdout(_NULL), redirect_stderr(_NULL):
            _dsi_store = DSI(db_path, check_same_thread=False)
            tables = _dsi_store.list(True)
            schema = _dsi_store.schema()
            desc = load_db_description(db_path)
            _dsi_store.close()
             
        return tables, schema, desc

    except Exception as e:
        return tables, schema, desc
    
    
    
def get_db_abs_path(db_path: str, run_path: str) -> [str, str]:
    """Get the absolute path of a DSI database given its path.

    Arg:
        db_path (str): the path of the DSI database (can be relative or absolute)
        run_path (str): the path of the codebase to resolve relative paths against
        
    Returns:
        str: the absolute path of the DSI database
        str: the absolute path of the folder containing the DSI database
    """
    
    p = Path(db_path)
    if not p.is_absolute():
        master_database_path = str( (Path(run_path) / db_path).expanduser() )
        master_db_folder = "/".join(master_database_path.split("/")[:-1]) + '/'
    else:
        master_database_path = db_path
        master_db_folder = "/".join(master_database_path.split("/")[:-1]) + '/'
        
    return master_database_path, master_db_folder



########################################################################
#### Tools Definition


def load_dsi(path: str, run_path: str = "", master_db_folder: str = "") -> dict:
    """Load a DSI object from the path and add information to the context for the llm to use.

    Arg:
        path (str): the path to the DSI object to load
        run_path (str): the path this code is being run from
        master_db_folder (str): the folder containing the master database, used to resolve relative paths when loading new databases
        
    Returns:
        str: message indicating success or failure
    """

    master_database_previously_set = True
    if master_db_folder == "":
        master_database_previously_set = False
        print("No master database loaded.")
        print(run_path)
        # the ai is loading the master database for the first time
        master_database_path, master_db_folder = get_db_abs_path(path, run_path)
        data_path = master_database_path.strip()
    else:
        p = Path(path)
        if not p.is_absolute():
            db_path = str(master_db_folder + '/' + path)
        else:
            db_path = path

        data_path = db_path.strip()
        
        
    logger.info(f"\n\n!!!load_dsi :: loading database at: {data_path}\n\n")


    if not check_db_valid(data_path):
        return f"Failed to load DSI database at: {data_path}. Please check the path and ensure it points to a valid DSI .db file."
            
    try:
        _, _db_schema, _db_description = get_db_info(data_path)
        _current_db_abs_path = data_path

        if master_database_previously_set == False:
            return {
                "the current working database path (current_db_abs_path) is": _current_db_abs_path,
                "the master database path (master_database_path) is": master_database_path,
                "the master databse folder (master_db_folder) is": master_db_folder,
                "the current databse schema is": _db_schema,
                "the database description is": _db_description
            }
        else:
            return {
                "the current working database path (current_db_abs_path) is": _current_db_abs_path,
                "the current databse schema is": _db_schema,
                "the database description is": _db_description
            }
        
        
    except Exception as e:
        logger.debug(f"Failed to extract database information. Error: {repr(e)}")
        return "Failed to load database information"
    


def query_dsi(query_str: str, db_path: str) ->dict:
    """Execute a SQL query on a DSI object

    Arg:
        query_str (str): the SQL query to run on DSI object
        db_path (str): the absolute path to the DSI database to query

    Returns:
        collection: the results of the query
    """
    
    logger.info(f"\n\n!!!query_dsi_tool :: running on {query_str}\n\n")
    
    #print(f"query {query_str}, db_path: {db_path}")
    
    _store = None
    try:
        # with open(os.devnull, "w") as fnull:
        #     with redirect_stdout(fnull), redirect_stderr(fnull):
        with redirect_stdout(_NULL), redirect_stderr(_NULL):
            _store = DSI(db_path, check_same_thread=False)
            df = _store.query(query_str, collection=True)
                
        if df is None:
            return {}
        return df.to_dict(orient="records")
    
    except Exception as e:   
        return {}
    
    finally:
        if _store is not None:
            try:
                # with open(os.devnull, "w") as fnull:
                #     with redirect_stdout(fnull), redirect_stderr(fnull):
                with redirect_stdout(_NULL), redirect_stderr(_NULL):
                    _store.close()
            except Exception:
                pass



def python_repl(code: Annotated[str, "Python code to execute. All generated outputs (plots, data, files) must be written to disk."]) -> str:
    """Executes arbitrary Python code. 
    The executed code must save any plots or files to disk, not return them directly.
    Returns the stdout/stderr logs as a string.

    Arg:
        code (str): the code to run

    Returns:
        str: error message
    """

    logger.info(f"\n\n!!!python_repl_tool :: running on {code}\n\n")
    
    python_repl = PythonREPL()
    try:
        result = python_repl.run(code)  # returns captured stdout; plots display only if backend and show() cooperate
    except BaseException as e:
        return f"Failed to execute. Error: {repr(e)}"
    return f"Successfully executed:\n```python\n{code}\n```\nStdout:\n{result}"



def download_file(url: Annotated[str, "web link for the file"], output_path: Annotated[str, "local path to save the file"]=".") -> str:
    """ Download a file from a URL and save it locally.

    Arg:
        url (str): a string containing the URL of the file to download.
        output_path (str): the local path where the file should be saved.
    Returns:
        Confirmation message with the saved file path.
    """

    logger.info(f"\n\n!!!download_file_tool :: running: {url}\n\n")

    try:
        # Download
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Ensure directory exists
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write file to disk
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return f"File successfully downloaded to: {output_path}"

    except Exception as e:
        return f"Error downloading file: {str(e)}"




def arxiv_search(query: str, max_results: int = 10) -> list:
    """
    Search arXiv.org for research papers related to a given topic.

    Args:
        query (str): Topic or keywords to search for.
        max_results (int): How many papers to return (default 5).

    Returns:
        list: A list of dictionaries with paper metadata:
              title, authors, published date, summary, and link.
    """

    logger.info(f"\n\n!!!arxiv_search_tool :: running query: {query}\n\n")

    # Build arXiv API URL
    url = (
        "http://export.arxiv.org/api/query?search_query="
        + query.replace(" ", "+")
        + f"&start=0&max_results={max_results}"
    )

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        return [{"error": f"Failed to fetch from arXiv: {str(e)}"}]

    parsed = feedparser.parse(response.text)

    results = []
    for entry in parsed.entries:
        results.append({
            "title": entry.title,
            "authors": [a.name for a in entry.authors],
            "published": entry.published,
            "summary": entry.summary,
            "link": entry.link,
        })

    return results



def wikipedia_search(query: str, max_results: int = 5) -> list:
    """
    Search Wikipedia for information about the search query
    
    Args:
        query (str): Search query or topic.
        max_results (int): Max number of results from each source.
    
    Returns:
        list of {title, snippet, url, source}
    """

    logger.info(f"\n\n!!!wikipedia_search_tool :: running query: {query}\n\n")

    results = []
    try:
        wiki_url = "https://en.wikipedia.org/w/api.php"
        wiki_params = {
            "action": "query",
            "list": "search",
            "srsearch": query,
            "format": "json"
        }
        wiki_headers = {
            "User-Agent": "Mozilla/5.0 (compatible; CombinedSearchTool/1.0; +https://example.com)"
        }

        wiki_response = requests.get(
            wiki_url, params=wiki_params, headers=wiki_headers, timeout=10
        )
        wiki_response.raise_for_status()

        wiki_data = wiki_response.json()

        for item in wiki_data.get("query", {}).get("search", [])[:max_results]:
            title = item["title"]
            url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
            snippet = item.get("snippet", "").replace("<span class=\"searchmatch\">", "").replace("</span>", "")

            results.append({
                "title": title,
                "snippet": snippet,
                "url": url,
                "source": "wikipedia"
            })

    except Exception as e:
        results.append({"error": f"Wikipedia error: {str(e)}"})

    return results



def web_search(query: str, max_results: int = 5) -> list:
    """
    Perform a web search on the topics and returns structured search results with title, snippet, and URL.

    Args:
        query (str): Search query
        max_results (int): Max number of results to return

    Returns:
        list of {title, snippet, url}
    """

    logger.info(f"\n\n!!!web_search_tool :: running query: {query}\n\n")

    search_url = "https://duckduckgo.com/html/"
    params = {"q": query}

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ddg_web_search_tool/1.0)"
    }

    try:
        r = requests.get(search_url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
    except Exception as e:
        return [{"error": f"Request failed: {str(e)}"}]

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    for a in soup.select(".result__a"):
        if len(results) >= max_results:
            break

        title = a.get_text()
        url = a.get("href")

        snippet_elem = a.find_parent("div", class_="result").select_one(".result__snippet")
        snippet = snippet_elem.get_text() if snippet_elem else ""

        results.append({
            "title": title,
            "snippet": snippet,
            "url": url,
            "source": "duckduckgo"
        })

    if not results:
        return [{"message": "No results found"}]

    return results



def send_email(recipient: str, subject: str, body: str, smtp_host: str = "smtp.example.com", smtp_port: int = 587,
               username: str = None, password: str = None):
    """
    Send an email using SMTP.

    Args:
        recipient (str): The destination email address.
        subject (str): The subject line of the email.
        body (str): The plain-text body of the email.
        smtp_host (str, optional): The SMTP server hostname. Defaults to "smtp.example.com".
        smtp_port (int, optional): The SMTP server port (typically 587 for TLS, 465 for SSL). Defaults to 587.
        smtp_port (int, optional): The SMTP server port (typically 587 for TLS, 465 for SSL). Defaults to 587.
        username (str, optional): SMTP authentication username, or None if not required.
        password (str, optional): SMTP authentication password, or None if not required.

    Returns:

        str: A confirmation message or an error description.
    """

    logger.info(f"\n\n!!!send_email_tool :: running query to {recipient}\n\n")
    
    try:
        # Build MIME message
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = username if username else "noreply@example.com"
        msg["To"] = recipient
        
        logger.debug(f"send_email_tool :: SMTP server: {smtp_host}:{smtp_port}, From: {msg['From']}, To: {msg['To']}")
        logger.debug(f"send_email_tool :: \n Email subject: {subject}, \n body: {body}")
        

        # Connect to SMTP server
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()  # upgrade connection to TLS

        # Authenticate if credentials provided
        if username and password:
            server.login(username, password)

        # Send the message
        server.sendmail(msg["From"], [recipient], msg.as_string())
        server.quit()

        return f"Email sent to {recipient}."

    except Exception as e:
        return f"Failed to send email: {e}"




def upload_paper(path: str) -> Dict[str, str]:
    """
    Upload a local PDF to OpenAI Files API and return identifiers.
    Intended for later use as an input_file in Responses API.
    
    Args:
        path (str): Local filesystem path ending in .pdf.
    
    Returns:
        Dict: {file_id: str, filename: str}
    """
    
    client = OpenAI()
    
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    if not path.lower().endswith(".pdf"):
        raise ValueError("This tool expects a .pdf file path.")

    f = client.files.create(
        file=open(path, "rb"),
        purpose="user_data",  # recommended for files you plan to use as model inputs
    )
    return {"file_id": f.id, "filename": os.path.basename(path)}



########################################################################
#### Tools Wrapper

@tool()
def load_dsi_tool(
    path: Annotated[str, "the path to the DSI object to load"],
    run_path: Annotated[str, "the path this code is being run from"] = "",
    master_db_folder: Annotated[str, "the folder containing the master database, used to resolve relative paths when loading new databases"] = "",
) -> Union[Dict[str, Any], str]:
    """Load a DSI object from the path and add information to the context for the llm to use."""
    return load_dsi(path=path, run_path=run_path, master_db_folder=master_db_folder)


@tool()
def query_dsi_tool(
    query_str: Annotated[str, "the SQL query to run on DSI object"],
    db_path: Annotated[str, "the absolute path to the DSI database to query"],
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Execute a SQL query on a DSI object."""
    return query_dsi(query_str=query_str, db_path=db_path)


@tool()
def python_repl_tool(
    code: Annotated[str, "Python code to execute. All generated outputs (plots, data, files) must be written to disk."],
) -> str:
    """Executes arbitrary Python code. The executed code must save any plots or files to disk, not return them directly."""
    return python_repl(code=code)


@tool()
def download_file_tool(
    url: Annotated[str, "a string containing the URL of the file to download."],
    output_path: Annotated[str, "the local path where the file should be saved."] = ".",
) -> str:
    """Download a file from a URL and save it locally."""
    return download_file(url=url, output_path=output_path)


@tool()
def arxiv_search_tool(
    query: Annotated[str, "Topic or keywords to search for."],
    max_results: Annotated[int, "How many papers to return (default 5)."] = 10,
) -> List[Dict[str, Any]]:
    """Search arXiv.org for research papers related to a given topic."""
    return arxiv_search(query=query, max_results=max_results)


@tool()
def wikipedia_search_tool(
    query: Annotated[str, "Search query or topic."],
    max_results: Annotated[int, "Max number of results from each source."] = 5,
) -> List[Dict[str, Any]]:
    """Search Wikipedia for information about the search query."""
    return wikipedia_search(query=query, max_results=max_results)


@tool()
def web_search_tool(
    query: Annotated[str, "Search query"],
    max_results: Annotated[int, "Max number of results to return"] = 5,
) -> List[Dict[str, Any]]:
    """Perform a web search and return structured results."""
    return web_search(query=query, max_results=max_results)


@tool()
def send_email_tool(
    recipient: Annotated[str, "The destination email address."],
    subject: Annotated[str, "The subject line of the email."],
    body: Annotated[str, "The plain-text body of the email."],
    smtp_host: Annotated[str, "The SMTP server hostname. Defaults to \"smtp.example.com\"."] = "smtp.example.com",
    smtp_port: Annotated[int, "The SMTP server port (typically 587 for TLS, 465 for SSL). Defaults to 587."] = 587,
    username: Annotated[Optional[str], "SMTP authentication username, or None if not required."] = None,
    password: Annotated[Optional[str], "SMTP authentication password, or None if not required."] = None,
) -> str:
    """Send an email using SMTP."""
    return send_email(
        recipient=recipient,
        subject=subject,
        body=body,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        username=username,
        password=password,
    )


@tool()
def upload_paper_tool(
    path: Annotated[str, "the path of the DSI object to load"],
) -> Dict[str, str]:
    """Upload a local PDF to OpenAI Files API and return identifiers."""
    return upload_paper(path=path)