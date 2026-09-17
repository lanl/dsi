# Exploring Warren-Associated Geoscience Metadata with DSI and NAVDAT

This notebook demonstrates how the DSI NAVDAT backend can be used to retrieve
and analyze sample and citation metadata from the EarthChem PetDB v4 API.

The user story uses an author query for `Warren` and examines the geographic
distribution, publication history, analysis types, and analytical methods
represented in the returned records.

## User Story

As a geoscientist evaluating federated EarthChem metadata, I want to retrieve
sample and citation records matching the author name `Warren`, examine their
geographic distribution, and summarize their analytical methods so that I can
understand the scope of records returned through the DSI NAVDAT backend.

## Scientific Question

What geographic regions, publications, analysis types, and analytical methods
are represented in PetDB records returned for the author query `Warren`?

Because the search is surname-based, the returned records may be associated
with multiple researchers named Warren. The notebook therefore treats the
results as a Warren-matched search cohort rather than records belonging to one
specific researcher.

## Workflow

**Author query → DSI NAVDAT backend → samples and citations → coordinate
validation → geographic and metadata analysis → visualization**

The notebook demonstrates:

- querying the NAVDAT backend through DSI;
- inspecting the normalized `samples` and `citations` tables;
- validating latitude and longitude values;
- mapping the geographic distribution of returned samples;
- identifying the largest returned location groups;
- reviewing unique citations and publication years; and
- summarizing analysis types and analytical methods.

---

## Step 1 — Create and Activate a Python Environment

From Terminal on macOS or Linux, create a Python 3.11 virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

---

## Step 2 — Clone and Install DSI

Clone the DSI repository if it is not already available:

```bash
git clone git@github.com:lanl/dsi.git
cd dsi
```

Install DSI and its core dependencies in editable mode:

```bash
python -m pip install -e .
```

Return to the `geoscience_navdat_demo` directory before continuing.

---

## Step 3 — Install the Notebook Dependencies

Install the packages required for this user story:

```bash
python -m pip install -r requirements.txt
```

The DSI package itself is installed separately in Step 2 because this example
is intended to run against the checked-out DSI source tree.

---

## Step 4 — Register the Jupyter Kernel

Register the active virtual environment as a Jupyter kernel:

```bash
python -m ipykernel install --user \
  --name navdat-dsi \
  --display-name "Python 3.11 (NAVDAT DSI)"
```

---

## Step 5 — Start JupyterLab

From the `geoscience_navdat_demo` directory, run:

```bash
jupyter-lab
```

Open `NAVDAT_User_Story.ipynb` and select the following kernel:

```text
Python 3.11 (NAVDAT DSI)
```

Run the notebook cells from top to bottom.

---

## Notebook Sequence

The notebook follows this sequence:

1. Define the `Warren` author query.
2. Initialize the read-only DSI NAVDAT backend.
3. Retrieve the normalized `samples` and `citations` tables.
4. Inspect the sample fields required for the analysis.
5. Validate sample coordinates.
6. Map the geographic distribution of the returned samples.
7. Identify the largest returned location groups.
8. Review unique citations and publication years.
9. Summarize analysis types and analytical methods.
10. Interpret the results and document the current limitations.
11. Close the DSI backend.

---

## Expected Outputs

The notebook produces:

- normalized sample and citation tables;
- a summary of retrieved samples, valid coordinates, and location groups;
- a geographic sample-distribution plot;
- a table and chart of the largest location groups;
- a deduplicated citation table;
- a publication-year chart;
- analysis-type and analytical-method summaries; and
- a concise final summary of the returned metadata.

The exact record counts may change because the notebook queries the live PetDB
service.

---

## Role of DSI

DSI provides the repository-access and normalization layer in this workflow.
The NAVDAT backend calls the remote service, traverses the nested responses,
and exposes the results as analysis-ready `samples` and `citations` tables.

In this example:

- **DSI / NAVDAT backend** — retrieves and normalizes repository metadata.
- **pandas** — organizes, filters, deduplicates, and summarizes the tables.
- **NumPy** — supports numerical transformations used in visualization.
- **Matplotlib** — creates the geographic and metadata plots.
- **truststore** — allows the notebook to use the operating system certificate
  store when needed for HTTPS access.

This separation keeps repository access independent from the scientific
analysis performed after retrieval.

---

## Current Limitations

- Pagination is not currently implemented, so the notebook analyzes the API
  page returned by the backend.
- The API response does not provide a confirmed key for connecting an
  individual sample directly to a citation record.
- A surname query may match multiple researchers.
- The backend returns metadata rather than measured geochemical values.
- Results and response behavior may change because PetDB is a live service.

---

## Summary

This user story demonstrates an end-to-end geoscience metadata workflow in
which DSI retrieves and normalizes EarthChem PetDB records for downstream
analysis with standard Python tools.

The results also demonstrate why broad repository searches must be validated:
the `Warren` surname query returns a geographically diverse collection
associated with several Warren researchers rather than one narrowly defined
regional collection.

**Query → Retrieve → Normalize → Validate → Analyze → Visualize**
