# Exploring Myoglobin Mutations with DSI and RCSB PDB

This notebook demonstrates how the DSI RCSB PDB backend can be used to retrieve
and analyze experimentally determined protein structures from the RCSB Protein
Data Bank.

The user story focuses on a set of myoglobin mutant structures and shows how DSI
can be used as the data-access layer for a downstream structural biology
workflow.

## User Story

As a structural biology researcher, I want to retrieve related myoglobin mutant
structures from the RCSB Protein Data Bank so that I can compare their
three-dimensional structures and examine how mutations affect structural
similarity.

## Scientific Question

How structurally similar are the selected myoglobin mutant proteins, and what
differences can be observed after aligning their C-alpha atoms?

The notebook uses the following RCSB PDB structures:

- `1CH1`
- `1CH2`
- `1CH3`
- `1CH5`

## Workflow

**RCSB PDB IDs → DSI RCSB PDB Backend → datasets/resources → mmCIF files →
BioPython structural parsing → C-alpha alignment → RMSD comparison →
visualization**

The notebook demonstrates:

- retrieving protein structure metadata through the DSI RCSB PDB backend;
- inspecting the normalized `datasets` and `resources` tables;
- accessing the corresponding mmCIF structure files;
- parsing structures with BioPython;
- aligning C-alpha atoms from related myoglobin structures;
- calculating pairwise RMSD values;
- visualizing structural differences with plots; and
- displaying protein structures with py3Dmol.

---

## Step 1 — Create and Activate a Python Environment

Using Python's built-in `venv` module:

```bash
python3 -m venv mydsi
source mydsi/bin/activate
```

On Windows:

```bash
mydsi\Scripts\activate
```

---

## Step 2 — Clone and Install DSI

Clone the DSI repository:

```bash
git clone git@github.com:lanl/dsi.git
cd dsi
```

Install DSI and its core dependencies:

```bash
pip install -e .
```

---

## Step 3 — Install the Notebook Dependencies

Install the libraries required specifically for this RCSB PDB user story:

```bash
pip install -r requirements.txt
```

These packages provide the notebook environment, structural biology tools,
data-processing libraries, plotting support, and 3D visualization used in the
example.

---

## Step 4 — Start JupyterLab

From the activated environment, run:

```bash
jupyter-lab
```

When JupyterLab opens in your browser, navigate to:

```text
RCSBPDB_User_Story.ipynb
```

Open the notebook and run the cells from top to bottom.

---

## Step 5 — Run the User Story

The notebook follows this sequence:

1. Initialize the DSI RCSB PDB backend.
2. Retrieve the selected myoglobin mutant records.
3. Inspect the normalized `datasets` and `resources` tables.
4. Access the associated mmCIF structure files.
5. Parse the structures using BioPython.
6. Select corresponding C-alpha atoms.
7. Perform structural alignment.
8. Calculate RMSD values between structures.
9. Compare the structures using plots and a pairwise RMSD heatmap.
10. Visualize the protein structures interactively with py3Dmol.

---

## Expected Outputs

The notebook produces:

- DSI-normalized metadata tables for the selected RCSB PDB structures;
- downloaded or accessible mmCIF structure files;
- C-alpha RMSD values for structural comparison;
- RMSD comparison plots;
- a pairwise RMSD heatmap; and
- interactive 3D protein structure visualizations.

> **Note:** py3Dmol visualizations are interactive inside Jupyter but become
> static or may not render fully when the notebook is exported to PDF.

---

## Role of DSI

DSI serves as the data-access and organization layer in this workflow.

The RCSB PDB backend provides a consistent way to retrieve repository metadata
and associated structure resources, while downstream scientific libraries
perform the structural analysis.

In this example:

- **DSI / RCSB PDB backend** — discovers and organizes protein structure data.
- **BioPython** — parses mmCIF files and performs structural alignment.
- **NumPy / pandas** — support numerical and tabular analysis.
- **Matplotlib / Seaborn** — visualize RMSD results.
- **py3Dmol** — provides interactive 3D protein structure visualization.

This separation allows the repository-access workflow to remain independent
from the scientific analysis performed after the structures are retrieved.

---

## Summary

This user story demonstrates an end-to-end workflow in which DSI provides
structured access to RCSB PDB data and passes the retrieved scientific resources
to standard Python structural-biology tools for analysis.

**Discover → Retrieve → Parse → Align → Compare → Visualize**
