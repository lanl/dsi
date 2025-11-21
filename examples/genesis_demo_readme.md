# DSI Genesis Demo

## Setup DSI
The only pre-requirements is python. For LANL people, the best way is through easyIt software, requesting anaconda, and then installing it though LANL Self Service. For external people, any anaconda downloaded from the web will do. 

These steps under "Setup DSI" needs to be done only once.


### Get DSI
```bash
git clone https://github.com/lanl/dsi.git #Get the repo
cd dsi
git checkout ai_dev # change to branch for the ai demo
```

### Install the python libraries for the demo 

We will setup a virtual environment to use the packages

```bash
python -m venv venv_dsi_genesis        # create a virtual environment
source venv_dsi_genesis/bin/activate   # activate it
```

First let's install the dependecies of DSI
```bash
python -m pip install -r requirements.txt           # installs the basic requirements     
python -m pip install -r requirements.extras.txt    # installs extra dependencies
python -m pip install --upgrade pip                 # good practice
```

Now, let's install DSI
```bash
python -m pip install .
```

Finrally, register the Jupyter Environment with jupyter lab so that the packages are visible in Jupyter
```bash
python -m ipykernel install --user --name=venv_dsi_genesis 
```

### Using / Launching the dmoe
 Note: if starting from a new run, do: source venv_dsi_genesis/bin/activate to go in the DSI environment

```bash
jupyter notebook 
```

From Jupyter notebook
- Open the notebook: click on examples/genesis_wildfire.ipynb
- Choose the kernel to run: from the menubar "Kernel" -> "Change Kernel..." and pick venv_dsi_genesis from the dropdown
- Clean the notebook:  from the menubar "Kernel" -> "CRestart Kernel and Clear output of all cells ..."

Now you should be ready to run.

