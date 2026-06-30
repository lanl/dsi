# DSI Explorer
DSI Explorer is an agent designed to interact with [DSI](https://github.com/lanl/dsi) (Data Science Infrastructure) project to allow users to explore data. In the example provided, in the data folder, users can explore data from [Oceans11](https://oceans11.lanl.gov/).

The capabilities are:
 - browsing data
 - simple visualization
 - arxiv searches
 - web searches
 - wikipedia searches.  


## Install
Install DSI using the instructions [here](https://lanl.github.io/dsi/installation.html)
From DSI
```bash
python -m pip install -r tools/ai_search/requirements.txt
```

## Running


### Jupyter notebook
The interface to DSI Explorer is Jupyter Lab. Before launching Jupyter Lab, you need to register your current environment to Jupyter Lab as follows:
```bash
python -m ipykernel install --user --name mydsi --display-name "mydsi"
```

Then, launch Jupyter lab as follows:
```bash
jupyter lab&
```

From Jupyter, open: ``tools/ai_search/dsi_data_explorer.ipynb```

Then, you need to pick the venv_ursa kernel. From the menu bar at the top:```Kernel -> Change Kernel ...```, choose ```mydsi```.




### Terminal
From the terminal, type:
python src/tools/ai_search/dsi_explorer_agent.py \<name of the dataset\>  
Example:

```bash
$ python tools/ai_search/dsi_explorer_agent.py tools/ai_search/data/oceans_11/ocean_11_datasets.db
```

This will produce:
```bash
╭────────────────────╮
│  DSI EXPLORER CLI  │
╰────────────────────╯
Dataset tools/ai_search/data/oceans_11/ocean_11_datasets.db has been loaded.
The DSI Data Explorer agent is ready.

Type a question and press Enter.

Available commands:
  .clear         Clear the screen
  .help          Show this help message
  .history       Show command history
  .exit          Quit the CLI

All other text is sent to the DSI Explorer.
Example:
  Tell me the data you have.

AIDSI> 
```
 and you will then use the command line ```URSA-DSI>```to interact with the database.


**Note:**  
An example of dataset is available in the data folder. Ideally, each dataset should have a yaml file, similar to ```data/ocean_11_datasets_descriptions.yaml```, that accompanies it. The name of the yaml file should be  ```<name-of-dababase>_description.yaml``` and the agent will read it in directly.