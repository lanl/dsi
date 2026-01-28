from dsi_explorer_agent import DSIExplorer
from langchain_openai import ChatOpenAI
from pathlib import Path
import os

# Get the data
current_file = Path(__file__).resolve()
current_dir = current_file.parent
dataset_path = str( current_dir / "data/oceans_11/ocean_11_datasets.db" )

llm_model = ChatOpenAI(
    model="gpt-5.1", 
    api_key=os.environ["OPENAI_API_KEY"],
)

ai = DSIExplorer(llm=llm_model, db_index_name=dataset_path, output_mode="console")

print("\nQuery: Tell me about the datasets you have.")
ai.ask("Tell me about the datasets you have.")

print("\nQuery: Do you have any implosion data?")
ai.ask("Do you have any implosion data?")

print("\nQuery: Can you find some arxiv papers related to this?")
ai.ask("can you find some arxiv papers related to this")