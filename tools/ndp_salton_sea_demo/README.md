# NDP / DSI / Pelican Seismic Demo Notebook

## Step 1 — Ensure you have a python environement and DSI installed

Using Python's built-in `venv` module:

```bash
python3 -m venv mydsi
source mydsi/bin/activate
```

```bash
git clone git@github.com:lanl/dsi.git
cd dsi
```

Install DSI and its required dependencies:

```bash
pip install . -r requirements.txt
```

## Step 3 — Install the Notebook's Python Libraries

These additional libraries are required specifically for this example:

```bash
pip install -r requirements.txt
```

## Step 4 — Install the Pelican OSDF Client

> **Important:** Do not skip this step.

Run the Pelican installation script:

```bash
python installPelican.py
```

## Step 5 — Run the Notebook

Start JupyterLab:

```bash
jupyter-lab
```

When JupyterLab opens in your browser, navigate to the seismic demo notebook, open it, and run the notebook cells.
