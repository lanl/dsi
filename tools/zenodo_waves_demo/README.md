# Retrieving a Reproducible Scientific Software Release with DSI and Zenodo

This example demonstrates how the DSI Zenodo backend can retrieve a versioned
scientific software release from Zenodo and expose both record-level metadata and
the files attached to that record.

The user story uses a versioned **LANL WAVES** release to demonstrate a
reproducibility-oriented workflow in which DSI identifies the exact software
record, exposes its archived source resource, and provides the checksum and
download URL needed for downstream verification.

## User Story

As a computational researcher, I want to retrieve a specific archived version of
LANL scientific software from Zenodo through DSI so that I can identify the exact
software release, citation metadata, and source archive needed to support a
reproducible computational workflow.

## Reproducibility Question

Can DSI retrieve a specific citable WAVES software release from Zenodo and
identify the exact archived source artifact associated with that release?

## Workflow

**Software DOI → DSI Zenodo backend → datasets/resources → source archive →
download → checksum verification → archive inspection**

The notebook demonstrates:

- retrieval of a Zenodo software record by DOI;
- inspection of normalized `datasets` and `resources` tables;
- identification of the archived source-code resource;
- use of the DSI-provided download URL in a downstream Python step;
- checksum verification of the downloaded artifact; and
- inspection of the archived software-release contents.

> **Note:** The DSI Zenodo backend is read-only and does not automatically save
> attached research files to disk. It exposes resource metadata and the
> `download_url`; the notebook performs the actual file download with `requests`.

---

## Step 1 — Create and Activate a Python Environment

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

```bash
git clone git@github.com:lanl/dsi.git
cd dsi
pip install -e .
```

---

## Step 3 — Install the Notebook Dependencies

From the `zenodo_waves_demo` directory:

```bash
pip install -r requirements.txt
```

The notebook uses pandas for tabular inspection and requests for the downstream
file download. Checksum verification, ZIP inspection, and path handling use
Python standard-library modules.

---

## Step 4 — Start JupyterLab

```bash
jupyter-lab
```

Open:

```text
Zenodo_User_Story.ipynb
```

and run the cells from top to bottom.

---

## Step 5 — Run the User Story

The notebook follows this sequence:

1. Select a versioned LANL WAVES release by DOI.
2. Initialize the DSI Zenodo backend.
3. Retrieve the normalized `datasets` and `resources` tables.
4. Inspect software-release metadata such as DOI, version, creators, license,
   and publication date.
5. Identify the archived source ZIP from the `resources` table.
6. Use the exposed `download_url` to retrieve the archive.
7. Compare the downloaded file's MD5 checksum with the checksum reported by
   Zenodo.
8. Inspect the archive contents and summarize the file types present.

---

## Expected Outputs

The notebook produces:

- a normalized DSI `datasets` row for the selected WAVES release;
- a normalized `resources` row for its archived source ZIP;
- the source archive's file name, format, size, checksum, and download URL;
- a locally downloaded ZIP archive;
- an MD5 checksum comparison showing whether the downloaded file matches the
  Zenodo metadata; and
- a short inspection of the software archive contents.

---

## Role of DSI

DSI serves as the repository-access and organization layer in this workflow.

The Zenodo backend converts repository-specific API responses into two stable
DSI tables:

- **`datasets`** — record-level metadata for the Zenodo research object.
- **`resources`** — file-level metadata for resources attached to that record.

In this example:

- **DSI / Zenodo backend** — retrieves and normalizes the software record and
  attached resource metadata.
- **requests** — downloads the selected archive using the URL exposed by DSI.
- **hashlib** — verifies the downloaded archive checksum.
- **zipfile / pathlib** — inspect and manage the local archive.
- **pandas** — displays and summarizes metadata and archive contents.

This separation keeps repository access independent from the downstream
software-reproducibility workflow.

---

## Summary

This example demonstrates how DSI can connect a persistent Zenodo DOI to the
exact archived scientific-software resource associated with that release.

**Identify → Retrieve → Resolve → Download → Verify → Inspect**
