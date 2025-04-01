# HALO VISUALIZATION AND ANALYSIS

## About 
Dark matter halos and galaxies are fundamental to understanding cosmic structure formation. Halos represent the backbone of cosmic structure, while galaxies within them drive key research areas such as star formation, evolution, and interactions. 

Visualizing the evolution and clustering of these structures in ensemble cosmology simulations is vital for uncovering the dynamics shaping the universe. Comparative analyses of visualized simulation data under varied parameters allow researchers to:
- Identify models that best match observational data.
- Study the impact of different inputs on the evolution of halos and galaxies.

This project provides a streamlined workflow for halo and galaxy visualization and analysis, specifically designed for **HACC simulations**. The workflow includes three key components:
1. **Meta Database Creation**
2. **Halos and Galaxies Extraction**
3. **Cinema Database Creation**
4. **Post-hoc Analysis with Cinema**

Detailed instructions for each component are outlined below.

---

## Part 1: Meta Database Creation

This section guides you through creating a metadata database using SQLite3 to efficiently organize and manage your simulation data.

For examples of using DSI to create a metadata database, refer to the HACC reader in the DSI repository: [DSI HACC Reader](https://github.com/lanl/dsi/tree/hacc_dev/examples/hacc).

### Steps to Create a Metadata Database

1. **Prerequirement**
HACC simulations are stored in the GenericIO format. You must compile [GenericIO](https://git.cels.anl.gov/hacc/genericio) to work with these files.
- Follow the instructions provided in the GenericIO Repository to compile GenericIO.
- -After compiling, update the second line in the readHalos.py file to point to your own GenericIO path. Modify the path to: "genericio/legacy_python/"

2. **Run the Script**
To create a metadata database, use the `prepare_database.py` script located in the **DBGenerator** folder. 
```bash
python3 prepare_database.py --suite_paths /path1 /path2 ... --output_db_path /output_path
```

Parameters:
- --suite_paths: Specifies the paths to the directories containing the simulation datasets. You can include multiple paths separated by spaces.
- --output_db_path: Defines the path where the generated meta-database will be saved.

Example:

```bash
python3 prepare_database.py --suite_paths /data/suite1 /data/suite2 --output_db_path /output/metadata.db
```

---

##  Part 2: Halos and Galaxies Extraction  

### Prerequirement

To extract and save halos and galaxies, the following libraries are required.
(1) [GenericIO](https://git.cels.anl.gov/hacc/genericio): replace the second line in extractParticles.py to point to your own GenericIO path. Modify the path to: "genericio/legacy_python/"

(2) [PyEVTK](https://github.com/paulo-herrera/PyEVTK): pip install pyevtk

### Halos and Galaxies Extraction
The two steps for extracting halos and galaxies including (1) finding which halos and galaxies to extract by their properties (2) extract and save halo and galaxies as a VTU file. 

The script `DataExtraction/extraction_example.py` demonstrates how to retrieve essential halo and galaxy information using SQL commands first and then process extraction.  

#### Step 1: Finding Halo and Galaxy Properties  

Before performing extraction, it is important to determine which region, halo, or galaxy needs to be extracted.  
This can be done efficiently using the metadata database created with DSI.  

For example, the following SQL query selects all rows from the `halos` table where `halo_rank = 0` (i.e., the largest halo):  

```sql
SELECT * FROM halos WHERE halo_rank = 0;
```

#### Step 2: Extraction and Saving  

There are two methods for extracting halos and galaxies:  

1. **Extracting a subregion** by defining a center and radius.  
2. **Extracting specific halos and their associated galaxies** based on predefined tags.  

The extraction functions are located in `DataExtraction/extractParticles.py`.  

To use other extraction functions, modify line 67 in `extraction_example.py`: [View the file here](https://github.com/lanl/dsi/blob/5523f1bc989b2387d8ed37c595376faf20867ad3/HACC/HACCHaloVis/DataExtraction/extraction_example.py#L67).  

#### Extracting a Subregion  

This method places a cubic region centered at a specified point, with a side length of `2 × radius`.  
All particles within this region are extracted.  

**Relevant functions:**  
- `extractFromBighaloparticlesByRegion`  
- `extractFromGalaxyparticlesByRegion`  

#### Extracting Specific Halos & Galaxies  

In this method, halos and galaxies are extracted based on predefined tags.  
All particles belonging to a selected halo or galaxy are included in the extraction.  

**Relevant functions:**  
- `extractFromBighaloparticles`  
- `extractFromGalaxyparticles`  

### Parallel using MPI 

For extracting multiple halos and galaxies efficiently, use `haloExtractionMPI.py`, which leverages MPI for parallel processing.  
This script requires the `mpi4py` package.  

Ensure `mpi4py` is installed before running the script:  

```bash
pip install mpi4py
```
---



##  Part 3: Cinema Database Creation  

Creating visualizations is an essential step in generating a Cinema database for post-hoc exploration.  
There are two methods for creating visualizations:  

1. **Using ParaView**  
2. **Using a VTK-based renderer**  

### Handling HACC Simulation Data  

HACC simulation data is stored in the **GenericIO** format. However, ParaView has issues importing the legacy Python library for GenericIO.  
To work around this, after extracting a subregion or specific halos, the extracted data must first be saved as a separate **VTU file** before being loaded into ParaView for visualization.  

On the other hand, when using a **VTK-based renderer** or another Python-based renderer, the legacy Python library for GenericIO can be imported directly.  
This allows the extracted data to be used for rendering without additional conversion steps.  

The following sections provide examples for using **ParaView** or a **VTK-based renderer** for visualization.  

### ParaView  

The extracted halos and their associated galaxies should be saved in the following directory structure in order to use the script directly:  

```md
suite_name 
├── run0
│   ├── ts0
│   ├── ├── halo_0.vtu
│   ├── ├── halo_1.vtu
│   ├── ts1
│   ├── ├── halo_0.vtu
│   ├── ├── halo_1.vtu
├── run1
│   ├── ts0
│   ├── ├── halo_0.vtu
│   ├── ├── halo_1.vtu
│   ├── ts1
│   ├── ├── halo_0.vtu
│   ├── ├── halo_1.vtu
```

### VTK Renderer 


