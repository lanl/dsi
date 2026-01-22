import feedparser
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
import atexit
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown as RichMarkdown


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


# define some global variables
dsi_store = None
db_schema = ""
db_description = None
master_db_folder = ""



########################################################################
#### Utility functions

def check_chatgpt_api_key(env_var="OPENAI_API_KEY") -> bool:
    """ Check if the ChatGPT/OpenAI API key is set in the environment.

    Arg:
        env_var (str): the environment variable name to check

    Returns:    
        bool: True if the API key is set, False otherwise
    """

    api_key = os.getenv(env_var)

    if not api_key:
        return False
    else:
        return True


def get_chatgpt_api_key() -> None:
    """Capture the ChatGPT/OpenAI API key from user input and store it in the environment variable."""

    api_key = getpass("Please enter your ChatGPT/OpenAI API key: ").strip()

    if not api_key:
        raise ValueError("No API key provided.")

    # store for the current process
    env_var = "OPENAI_API_KEY"
    os.environ[env_var] = api_key
    print(f"API key stored in environment variable '{env_var}' for this session.")



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
    """Extract readable message content from either a dict or list of LangGraph messages. 
       Used for logging: CHATGPT Generated but seems to work!
    
    Arg:
        result (dict or list): the result containing messages
        
    Returns:
        str: concatenated message contents
    """
    
    # Case 1: result is a dict that contains messages
    if isinstance(result, dict) and "messages" in result:
        messages = result["messages"]
        
    # Case 2: result is already a list of messages
    elif isinstance(result, list):
        messages = result
        
    else:
        messages = []

    lines = []
    for m in messages:
        # Handle LangGraph message objects
        if hasattr(m, "content"):
            text = m.content
        # Handle plain dicts with 'content' key
        elif isinstance(m, dict) and "content" in m:
            text = m["content"]
        else:
            text = str(m)

        # Unescape literal newlines
        text = text.replace("\\n", "\n")
        lines.append(text.strip())

    return "\n\n".join(lines)



def load_db_description(db_path: str) -> str:
    """Load the database description from a YAML file when provided with the path to a DSI database.

    Arg:
        db_path (str): the path of the DSI database
    Returns:
        str: message indicating success or failure
    """

    logger.info(f"\n\n!!!load_db_description :: running on {db_path}\n\n")

    global db_description

    try:
        db_description_path = db_path.rsplit(".", 1)[0] + '_description.yaml'
        logger.info(f"load_db_description :: db_description_path: {db_description_path}")
        
        with open(db_description_path, "r") as f:
            db_description = f.read()

        return "Successfully loaded database description."
    except:
        db_description = None
        logger.info(f"load_db_description :: Failed: No database description file found at  {db_description_path}")
        return "Failed to load database description."



########################################################################
#### Tools Available

def load_dsi(path: str) -> str:
    """Load a DSI object from the path and add information to the context for the llm to use.

    Arg:
        path (str): the path to the DSI object
        
    Returns:
        str: message indicating success or failure
    """

    logger.info(f"\n\n!!!load_dsi :: running on: {path}\n\n")
    
    global dsi_store
    global db_schema
    global master_db_folder
    

    p = Path(path)
    if not p.is_absolute():
        db_path = str(master_db_folder + '/' + path)
    else:
        db_path = path

    data_path = db_path.strip()
    
    logger.info(f"\n\n!!!load_dsi :: loading database at: {db_path}\n\n")

    
    # Check if the path exists and there is data
    
    if not os.path.exists(data_path):
        return f"Failed to extract database information at: {data_path}"
    
    else:
        try:
            with redirect_stdout_to_logger(logger, level=logging.DEBUG):
                temp_store = DSI(data_path, backend_name = "Sqlite", check_same_thread=False)
                temp_tables = temp_store.list(True) # force things to fail if the table is empty
                temp_store.close()
        except Exception as e:
            logger.debug(f"load_dsi :: Failed to extract database information at: {data_path}. Error: {repr(e)}")
            return f"Failed to extract database information at: {data_path}"
            
            
    # if the above works, actually load the DSI object
    try:
        with redirect_stdout_to_logger(logger, level=logging.DEBUG):
            if dsi_store is not None:
                dsi_store.close()

            dsi_store = DSI(data_path, backend_name = "Sqlite", check_same_thread=False)
            tables = dsi_store.list(True)
            schema = dsi_store.schema()


        # Append the database information to the context
        logger.info(f"""load_dsi :: Loading {dsi_store}; the DSI Object has a database that has {len(tables)} tables: {tables}.
        The schema of the database is {schema} \n""")
        
        db_schema += f"""The DSI Object has a database that has {len(tables)} tables: {tables}.
            The schema of the database is {schema} \n"""
        
            
        # load the database description if available
        try:
            load_db_description(data_path)
        except Exception as e:
            logger.debug("load_dsi :: No master database description to load.")
        
        
        return "Successfully loaded DSI object and extracted database information"
        
    except Exception as e:
        logger.debug(f"Failed to extract database information. Error: {repr(e)}")
        return "Failed to extract database information"
    
    
load_dsi_tool = Tool(
    name="load_dsi_tool",
    description="Load a DSI object from a path and update LLM context.",
    func=load_dsi,
)



@tool
def query_dsi_tool(query_str: str) -> list:
    """Execute a SQL query on a DSI object

    Arg:
        query_str (str): the SQL query to run on DSI object

    Returns:
        collection: the results of the query
    """

    logger.info(f"\n\n!!!query_dsi_tool :: running on {query_str}\n\n")
    
    global dsi_store   
    s = dsi_store

    with redirect_stdout_to_logger(logger, level=logging.DEBUG):
        df = s.query(query_str, collection=True)
        
    if df is None:
        return {}

    return df.to_dict(orient="records")



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



# Register the tools
tools = [query_dsi_tool, 
         python_repl_tool, 
         load_dsi_tool, 
         download_file_tool, 
         arxiv_search_tool,
         wikipedia_search_tool,
         web_search_tool,
         send_email_tool
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
        self.llm = llm
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
    
    def __init__(self, db_index_name:str="", output_mode:str="jupyter"):
        self.store = None
        self.tables = None
        self.schema = None
        self.msg = {}
        self.master_datbase_path = ""
        self.output_mode = output_mode

        global master_db_folder
        
        # Get absolute path of dataset
        relative_db_path = Path(db_index_name)
        absolute_db_path = str(relative_db_path.resolve())
        
        self.wrsk_path = self.create_workspace()
        self.create_checkpoint()

        self.thread_id = str(random.randint(1, 20000))
        
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

        master_db_folder = "/".join(absolute_db_path.split("/")[:-1]) + '/'
        logger.debug("master_db_folder: " + master_db_folder)

        self.load_master_db(absolute_db_path)
        self.create_graph()
        
        logger.info("DSI Explorer agent initialization complete.")
        print(f"Dataset {db_index_name} has been loaded.\nThe DSI Data Explorer agent is ready.")
        


    def load_master_db(self, master_db_path: str) -> None:
        """Load the  master dataset from the given path.
        
        Arg:
            path (str): the path to the DSI object
        """
        
        if master_db_path != "":
            logger.info(f"load_master :: Using database index: {master_db_path}")
            output_msg = load_dsi(master_db_path)
            if "Failed" in output_msg:
                print(f"[ERROR] A valid DSI file is needed. {master_db_path}. We will now exit!")
                sys.exit(1)

            #load_db_description(master_db_path)
            self.master_datbase_path = master_db_path
        else:        
            print("No DSI database provided. We will now exit!")
            sys.exit(1)
        


    def create_workspace(self)-> str:
        """Create a workspace to store files and logs for the agent interaction
        
        Returns:
            str: the absolute path to the workspace
        """
        
        now = datetime.now()
        datetime_str = now.strftime("%Y_%m_%d__%H_%M")
        workspace_name = "dsi_explorer_agent__" +  datetime_str
        absolute_workspace_path = os.path.abspath(workspace_name)

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
        The currently loaded DSI object is stored in dsi_store global variable. Use query_dsi_tool to run SQL queries on it.
        When a user asks for data or dataset or ... you have, do NOT list the schema or metadata information you have about tables. Query the DSI objects for data and list the data in the tables.
        
        You can:
        - write and execute Python code,
        - compose SQL statements,
        - generate plots and diagrams,
        - analyze and summarize data.

        The dsi_explorer master dataset is avilable at {self.master_datbase_path} in case you need to reload it.

        Requirements:
        - Planning: Think carefully about the problem, but **do not show your reasoning**.
        - Data:
            - Always use the provided tools when available — never simulate results.
            - Never fabricate or assume sample data. Query or compute real values only.
            - When creating plots or files, always save them directly to disk (do not embed inline output).
            - Do not infer or assume any data beyond what is provided by the tools.
        - Keep all responses concise and focused on the requested task.
        - Do not restate the prompt or reasoning; just act and report the outcome briefly.
        """
        
        llm = ChatOpenAI(model="gpt-5.1", temperature=0.1, request_timeout=120)
        dsi_explorer_executor = DSIExplorerWorker(llm, worker_prompt)
        
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

        global db_schema
        global db_description

        base_system_context = f"""
            The following phrases all refer to this same database:
            - "master database"
            - "master dataset"
            - "DSIExplorer master database"
            - "Diana dataset

            When the user asks to reload, refresh, reset, reinitialize, restart, or 
            the **master database**, interpret that as a request to reload the 
            DSIExplorer master database using the tool load_dsi_tool("{self.master_datbase_path}"),
            load the last dataset in the context.

            Do no reload or load the master dataset unless explicitly asked by the user.
        """

        # Build remaining dynamic system context parts
        system_parts = [base_system_context]

        if db_schema != "":
            system_parts.append("You have this dataset loaded: " + db_schema)

        if db_description is not None:
            system_parts.append("Dataset description: " + db_description)

        # Combine
        if system_parts:
            system_message = SystemMessage(content="\n\n".join(system_parts))
            messages = [system_message, HumanMessage(content=human_msg)]
        else:
            messages = [HumanMessage(content=human_msg)]

        # clear
        db_schema = ""
        db_description = None
        

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

    # Initialize the explorer in console output mode
    explorer = DSIExplorer(db_index_name=args.db, output_mode="console")

    
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
            user_input = rlinput("[bold cyan]URDA-DSI> [/bold cyan]").strip()
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
