import argparse
import io
import logging
import os
import random
import sqlite3
import asyncio
import sys
import time
import json
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown as RichMarkdown
from langchain_mcp_adapters.client import MultiServerMCPClient

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
from langgraph.checkpoint.memory import MemorySaver
from langchain_openai import ChatOpenAI, OpenAI


from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from openai import OpenAI

from dsi.dsi import DSI
from tool_utils import *


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

_NULL = io.StringIO()  # do not close this

########################################################################
#### Utility functions

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


def run_async(coro):
    """
    Run an async coroutine from sync code.
    - In scripts: uses asyncio.run()
    - In Jupyter: uses the existing event loop (via nest_asyncio)
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import nest_asyncio
        nest_asyncio.apply()
        return asyncio.get_event_loop().run_until_complete(coro)
    else:
        return asyncio.run(coro)
    
    
########################################################################
#### AI Functions

class State(TypedDict):
    """State for the DSIExplorer agent graph."""
    messages: Annotated[list, add_messages]
    response: str
    metadata: Dict[str, Any]


class DSIExplorerWorker:
    """A simple DSI worker that executes tasks based on the prompt and messages."""
    
    def __init__(self, llm, prompt=None, tools=[], name="worker"):
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
    
    def __init__(self, llm, db_index_name:str="", output_mode:str="jupyter", use_mcp:bool=False, mcp_transport:str="streamable-http", mcp_url: str=""):
        self.db_tables = []
        self.db_schema = ""
        self.db_description = ""
        
        self.master_db_folder = ""
        self.master_database_path = ""
        self.current_db_abs_path = ""

        self.output_mode = output_mode
        self.run_path = os.getcwd()
        self.use_mcp = use_mcp
        self.mcp_transport = mcp_transport
        self.mcp_url = mcp_url
        
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
        
        # Register the tools
        if self.use_mcp:
            client = MultiServerMCPClient(
                connections={"dsi-tools": {"transport": self.mcp_transport, "url": self.mcp_url}}
            )
            
            dsi_tools = run_async(client.get_tools())
            tool_by_name = {t.name: t for t in dsi_tools}

            print("MCP Tools:", list(tool_by_name.keys()))
        
        else:
            dsi_tools = [
                query_dsi_tool, 
                python_repl_tool, 
                load_dsi_tool, 
                download_file_tool, 
                arxiv_search_tool,
                wikipedia_search_tool,
                web_search_tool,
                send_email_tool,
                upload_paper_tool
            ]

        dsi_explorer_executor = DSIExplorerWorker(self.llm, worker_prompt, tools=dsi_tools, name="dsi_explorer_executor")
        
        logger.info("Worker agent created.")
        logger.debug("Worker prompt: " + worker_prompt)


        tool_node = ToolNode(tools=dsi_tools) 
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
        
        memory = MemorySaver()   
        self.app = graph_builder.compile(checkpointer=memory)
        
        #self.app = graph_builder.compile(checkpointer=self.checkpointer)
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

        if self.use_mcp:
            result = run_async(self.app.ainvoke(
                self.craft_message(user_query),
                config={"configurable": {"thread_id": self.thread_id}},
            ))
        else:
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
