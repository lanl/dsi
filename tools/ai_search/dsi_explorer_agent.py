import argparse
import io
import logging
import os
import random
import requests
import smtplib
import sqlite3
import sys
import time
import json
import feedparser
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown as RichMarkdown
from contextlib import redirect_stdout, redirect_stderr


from bs4 import BeautifulSoup
from contextlib import redirect_stdout
from datetime import datetime
from email.mime.text import MIMEText
from getpass import getpass
from IPython.display import Markdown, display
from pathlib import Path
from typing import Annotated, Dict, Any
from typing_extensions import TypedDict, Annotated

from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.tools import tool
from langchain_core.tools import Tool
from langchain_openai import ChatOpenAI, OpenAI
from langchain_experimental.utilities import PythonREPL


from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from openai import OpenAI

from dsi.dsi import DSI

########################################################################
#### Global Variables and settings

# for local history in command line
try:
    import readline
    histfile = os.path.expanduser("~/.dsi_explorer_history")

except ImportError:
    readline = None  # Windows fallback (no history)


# Setup global logger
logger = logging.getLogger("global_logger")
logger.setLevel(logging.DEBUG)
logger.propagate = False    



########################################################################
#### Utility functions

def redirect_stdout_to_logger(logger: logging.Logger, level=logging.INFO) -> io.StringIO:
    """Capture all print() output within the context and redirect it to the logger.

    Args:
        logger (logging.Logger): The logger to which the output should be redirected.
        level (int): The logging level to use for the redirected output.

    Returns:
        context manager that redirects stdout to the logger
    """

    buffer = io.StringIO()

    class _LogWriter(io.StringIO):
        # override write to pass text directly to logger
        def write(self, text):
            text = text.strip()
            if text:
                logger.log(level, text)
            return len(text)

    return redirect_stdout(_LogWriter())




def extract_message_texts(result) -> str:
    """Extract readable message content from either a dict or list of LangGraph messages."""
    if isinstance(result, dict) and "messages" in result:
        messages = result["messages"]
    elif isinstance(result, list):
        messages = result
    else:
        messages = []

    lines = []
    for m in messages:
        if hasattr(m, "content"):
            text = m.content
        elif isinstance(m, dict) and "content" in m:
            text = m["content"]
        else:
            text = m

        # Ensure text is a string (ToolMessage content can be list/dict)
        if isinstance(text, (list, dict)):
            text = json.dumps(text, indent=2, default=str)
        else:
            text = str(text)

        text = text.replace("\\n", "\n")
        lines.append(text.strip())

    return "\n\n".join(lines)



def load_db_description(db_path: str) -> str:
    """Load the database description from a YAML file when provided with the path to a DSI database.

    Arg:
        db_path (str): the absolute path of the DSI database
        
    Returns:
        str: message indicating success or failure
    """
    
    logger.info(f"\n\n!!!load_db_description :: running on {db_path}\n\n")

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
            with open(os.devnull, "w") as fnull:
                with redirect_stdout(fnull), redirect_stderr(fnull):
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
        with open(os.devnull, "w") as fnull:
            with redirect_stdout(fnull), redirect_stderr(fnull):
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
#### Tools Available

@tool
def load_dsi_tool(path: str, run_path: str = "", master_db_folder: str = "") -> dict:
    """Load a DSI object from the path and add information to the context for the llm to use.

    Arg:
        path (str): the path to the DSI object to load
        run_path (str): the path this code is being run from
        master_db_folder (str): the folder containing the master database, used to resolve relative paths when loading new databases
        
    Returns:
        str: message indicating success or failure
    """


    logger.info(f"\n\n!!!load_dsi :: running on: {path} at {run_path} \n\n")
    
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
    


@tool
def query_dsi_tool(query_str: str, db_path: str) ->dict:
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
        with open(os.devnull, "w") as fnull:
            with redirect_stdout(fnull), redirect_stderr(fnull):
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
                with open(os.devnull, "w") as fnull:
                    with redirect_stdout(fnull), redirect_stderr(fnull):
                        _store.close()
            except Exception:
                pass



@tool
def python_repl_tool(code: Annotated[str, "Python code to execute. All generated outputs (plots, data, files) must be written to disk."]) -> str:
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



@tool
def download_file_tool(url: Annotated[str, "web link for the file"], output_path: Annotated[str, "local path to save the file"]=".") -> str:
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



@tool
def arxiv_search_tool(query: str, max_results: int = 10) -> list:
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



@tool
def wikipedia_search_tool(query: str, max_results: int = 5) -> list:
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



@tool
def web_search_tool(query: str, max_results: int = 5) -> list:
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



@tool
def send_email_tool(recipient: str, subject: str, body: str, smtp_host: str = "smtp.example.com", smtp_port: int = 587,
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




@tool
def upload_paper_tool(path: str) -> Dict[str, str]:
    """
    Upload a local PDF to OpenAI Files API and return identifiers.
    Intended for later use as an input_file in Responses API.
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


# Register the tools
tools = [query_dsi_tool, 
         python_repl_tool, 
         load_dsi_tool, 
         download_file_tool, 
         arxiv_search_tool,
         wikipedia_search_tool,
         web_search_tool,
         send_email_tool,
         upload_paper_tool
        ]


########################################################################
#### AI Functions

class State(TypedDict):
    """State for the DSIExplorer agent graph."""
    messages: Annotated[list, add_messages]
    response: str
    metadata: Dict[str, Any]



class DSIExplorerWorker:
    """A simple DSI worker that executes tasks based on the prompt and messages."""
    
    def __init__(self, llm, prompt=None, name="worker"):
        self.prompt = prompt
        self.name = name  # add a name for easier tracing
        self.llm = llm.bind_tools(tools)
        
        logger.debug(f"Initialized DSIExplorerWorker with name: {self.name}, prompt: {self.prompt}, llm: {self.llm}")

    
    def __call__(self, state):
        messages = state["messages"]

        conversation = [SystemMessage(content=self.prompt)] + messages

        logger.debug(f"\n\nDSIExplorerWorker.__call__ :: '{self.name}' ai invoked with: {extract_message_texts(conversation)} \n")

        response = self.llm.invoke(conversation)

        return {
            "messages": messages + [response],
            "response": response.content,
            "metadata": response.response_metadata
        }
    


def should_call_tools(state: State) -> str:
    """Decide whether to call tools or continue.
    
    Arg:
        state (State): the current state of the graph   
        
    Returns:
        str: "call_tools" or "continue"
    """
    
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and last.tool_calls:
        return "call_tools"
    
    return "continue"



class DSIExplorer:
    """DSIExplorer agent for data analysis and execution."""
    
    def __init__(self, llm, db_index_name:str="", output_mode:str="jupyter"):
        self.db_tables = []
        self.db_schema = ""
        self.db_description = ""
        
        self.master_db_folder = ""
        self.master_database_path = ""
        self.current_db_abs_path = ""

        self.output_mode = output_mode
        self.run_path = os.getcwd()
        
        self.wrsk_path = self.create_workspace()
        self.create_checkpoint()

        self.thread_id = str(random.randint(1, 20000))


        # set LLM
        if llm is None:
            try:
                self.llm = ChatOpenAI(model="gpt-5.1", temperature=0.1, request_timeout=120)
            except Exception as e:
                print(f"[ERROR] Failed to initialize ChatOpenAI LLM. Error: {repr(e)}")
                sys.exit(1)
        else:
            self.llm = llm
        

        # setup logging to file
        log_file = os.path.join(self.wrsk_path, "run.log")
        file_handler = logging.FileHandler(log_file, mode="w")
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter( "%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        logger.info("!!!!!\n!!!!!Starting DSIExplorer agent initialization.")
        logger.info(f"Workspace created at: {self.wrsk_path}")   
        logger.info(f"Checkpoint database created with thread id: {self.thread_id}")

        

        self.load_master_db(db_index_name)
        self.create_graph()
        
        logger.info("DSI Explorer agent initialization complete.")
        
        

    def load_master_db(self, db_index_name: str) -> None:
        """Load the  master dataset from the given path.
        
        Arg:
            db_index_name (str): the path to the DSI object
        """
        
        if db_index_name == "":        
            print("No DSI database provided. Please load one")
            return

        _master_datbase_path, _master_db_folder = get_db_abs_path(db_index_name, self.run_path)
        absolute_db_path = _master_datbase_path

        logger.debug("master_db_folder: " + _master_db_folder)
        
        if check_db_valid(absolute_db_path):
            self.db_tables, self.db_schema, self.db_description = get_db_info(absolute_db_path)
            
            # set the values now that we know things are correct
            self.current_db_abs_path =  absolute_db_path
            self.master_database_path = absolute_db_path
            self.master_db_folder = _master_db_folder


            logger.debug("master_database_path: " + self.master_database_path)
            logger.debug("master_db_folder: " + self.master_db_folder)
        else:        
            print("No valid DSI database provided. Please load one")
            #sys.exit(1)
        


    def create_workspace(self)-> str:
        """Create a workspace to store files and logs for the agent interaction
        
        Returns:
            str: the absolute path to the workspace
        """
        
        now = datetime.now()
        datetime_str = now.strftime("%Y_%m_%d__%H_%M")
        workspace_name = "dsi_explorer_agent__" +  datetime_str
        absolute_workspace_path = os.path.abspath(workspace_name)
        print(f"Creating workspace at: {absolute_workspace_path}")

        # Create the woking directory and switch to it
        os.makedirs(absolute_workspace_path, exist_ok=True)
        os.chdir(absolute_workspace_path)  # changes the global working directory
        
        return absolute_workspace_path



    def create_checkpoint(self):
        """Create a checkpointing database for the agent."""
        
        # Setup checkpointing
        db_path = "checkpoint.db"
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.checkpointer = SqliteSaver(conn)
        


    def create_graph(self) -> None:
        """Create the DSIExplorer agent graph."""
        
        worker_prompt = f"""
        You are a data-analysis agent who can write python code, SQL queries, and generate plots to answer user questions based on the data available in a DSI object.
        Use the load_dsi_tool tool to load DSI files that have a .db extension
        Use query_dsi_tool to run SQL queries on it
        When a user asks for data or dataset or ... you have, do NOT list the schema or metadata information you have about tables. Query the DSI objects for data and list the data in the tables.
        
        You can:
        - write and execute Python code,
        - compose SQL statements but ONLY select; no update or delete
        - generate plots and diagrams,
        - analyze and summarize data.

        Requirements:
        - Planning: Think carefully about the problem, but **do not show your reasoning**.
        - Data:
            - Always use the provided tools when available — never simulate results.
            - Never fabricate or assume sample data. Query or compute real values only.
            - When creating plots or files, always save them directly to disk (do not embed inline output).
            - Do not infer or assume any data beyond what is provided by the tools.
        - Keep all responses concise and focused on the requested task.
        - Only load a dataset when explicitly asked by a user
        - Do not restate the prompt or reasoning; just act and report the outcome briefly.
        - All downloaded files, generated plots, and outputs must be saved to  {self.wrsk_path}.
        """
        
        dsi_explorer_executor = DSIExplorerWorker(self.llm, worker_prompt)
        
        logger.info("Worker agent created.")
        logger.debug("Worker prompt: " + worker_prompt)


        tool_node = ToolNode(tools) 
        logger.info("Tool node created.")


        graph_builder = StateGraph(State)
        graph_builder.add_node("dsi_explorer_executor", dsi_explorer_executor)
        graph_builder.add_node("tools", tool_node)

        graph_builder.add_edge(START, "dsi_explorer_executor")
        
        graph_builder.add_conditional_edges(
            "dsi_explorer_executor",
            should_call_tools,
            {
                "call_tools": "tools",
                "continue": END,
            },
        )

        graph_builder.add_edge("tools", "dsi_explorer_executor")
        self.app = graph_builder.compile(checkpointer=self.checkpointer)
        logger.info("Graph compiled.")



    def craft_message(self, human_msg):
        """Craft the message with context if available."""

        # global db_schema
        # global db_description

        base_system_context = f"""
            The following phrases all refer to this same database:
            - "master database"
            - "master dataset"
            - "DSIExplorer master database"
            - "Diana dataset

            When the user asks to reload, refresh, reset, reinitialize, restart, or 
            the **master database**, interpret that as a request to reload the 
            DSIExplorer master database at {self.master_database_path} using the tool load_dsi_tool. 
            Do no reload or load the master dataset unless explicitly asked by the user.
            
            The the path this code is being run from (run_path) is {self.run_path}
        """


        # Build remaining dynamic system context parts
        system_parts = [base_system_context]
        
        if self.current_db_abs_path != "":
            system_parts.append("The current working database path (current_db_abs_path) is: " + self.current_db_abs_path)
            
        if self.master_database_path != "":
            system_parts.append("The master database path (master_database_path) is: " + self.master_database_path)
            
        if self.db_schema != "":
            system_parts.append("The schema of the dataset loaded: " + self.db_schema)

        if self.db_description != "":
            system_parts.append("Dataset description: " + self.db_description)

        # Combine
        if system_parts:
            system_message = SystemMessage(content="\n\n".join(system_parts))
            messages = [system_message, HumanMessage(content=human_msg)]
        else:
            messages = [HumanMessage(content=human_msg)]


        # clear
        self.db_schema = ""
        self.db_description = ""
        self.master_database_path = ""
        self.current_db_abs_path = ""
        

        return {"messages": messages}

    
    
    def ask(self, user_query) -> None:
        """Ask a question to the DSI Explorer agent.

        Arg:
            user_query (str): the user question
        """

        logger.info("\n\n------------------------------------------------")
        logger.info(f"ask :: User query: {user_query}")

        start = time.time()

        msg = self.craft_message(user_query)
        logger.debug(f"ask :: Crafted message: {extract_message_texts(msg)}")

        result = self.app.invoke(
            msg,
            config={"configurable": {"thread_id": self.thread_id}}
        )

        # Get and display the cleaned output
        response_text = result["response"] 
        cleaned_output = response_text.strip()
        if self.output_mode == "jupyter":
            display(Markdown(cleaned_output))
        elif self.output_mode == "console":
            console = Console()
            md = RichMarkdown(cleaned_output)
            console.print(md)
            #print("\n" + cleaned_output)
        else:
            print(cleaned_output)


        elapsed = time.time() - start
        total_tokens = str(result["metadata"].get("token_usage", {}).get("total_tokens", 0)).strip()

        print(f"\nQuery took: {elapsed:.2f} seconds, total tokens used: {total_tokens}.\n")

        logger.debug(f"ask :: Elapsed time: {elapsed:.2f} seconds")
        logger.debug("ask :: Final result messages: " + extract_message_texts(result))




def main():    
    """Main function to run the DSI Explorer CLI."""

    console = Console()

    # Create the argument parser
    parser = argparse.ArgumentParser(description="Simple CLI for DSIExplorer")
    parser.add_argument("db", help="Path to a DSI .db file to load.")
    args = parser.parse_args()

    # If no DB provided, exit immediately
    if args.db is None:
        print("[ERROR] No database file provided.\nUsage: python dsi_explorer_agent.py path/to/database.db")
        sys.exit(1)

    # Validate DB file exists
    if not os.path.exists(args.db):
        print(f"[ERROR] Database file not found: {args.db}")
        return

    # Make it fancy
    banner = Panel.fit(
        "[bold cyan]DSI EXPLORER CLI[/bold cyan]",
        border_style="cyan",
        padding=(0, 2),
    )
    console.print(banner)


    # Use chat GPT-5.1 model by default
    model = ChatOpenAI(model="gpt-5.1", max_tokens=100000, timeout=None, max_retries=2)

    # Initialize the explorer in console output mode
    explorer = DSIExplorer(model, db_index_name=args.db, output_mode="console")

    
    def show_help():
        print("\nAvailable commands:")
        print("  .clear         Clear the screen")
        print("  .help          Show this help message")
        print("  .history       Show command history")
        print("  .exit          Quit the CLI")
        print("\nAll other text is sent to the DSI Explorer.")
        print("Example:")
        print('  Tell me the data you have.\n')

    def clear_screen():
        # Windows
        if os.name == 'nt':
            os.system('cls')
        # macOS/Linux/Unix
        else:
            os.system('clear')

    def rlinput(prompt: str) -> str:
        # Use Rich to print the prompt, but without newline
        console.print(prompt, end="")

        # Let readline manage only the user input part
        return input()


    print("\nType a question and press Enter.")
    show_help()


    # Interactive loop
    while True:
        try:
            user_input = rlinput("[bold cyan]AI-DSI> [/bold cyan]").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            break

        if not user_input:
            continue

        # Special commands
        if user_input == ".exit":
            print("Goodbye.\n")
            break

        if user_input == ".clear":
            clear_screen()
            continue

        if user_input == ".help":
            show_help()
            continue

        if user_input == ".history":
            if readline:
                for i in range(1, readline.get_current_history_length() + 1):
                    console.print(f"[dim]{i}[/dim]: {readline.get_history_item(i)}")
            else:
                console.print("[yellow]History not available on this platform.[/yellow]")
            continue

        # Normal query
        explorer.ask(user_input)


if __name__ == "__main__":
    main()
