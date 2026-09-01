# Cloverleaf Ensemble Dataset

---
language:
  - en
datacard_version: "1.0"
dataset_id: "LA-UR-25-31939"
name: "Cloverleaf Data Artifacts for ArtIMis LDRD"
tags:
- project:Artimis
- type:dataset
- science:physics
- risk:general

license: GPL-3.0
license_name: "GNU General Public License v3.0"
license_link: "https://www.gnu.org/licenses/gpl-3.0.html"

authors:
  - name: "Ayan Biswas"
    affiliation: "Los Alamos National Laboratory"
  - name: "Terece Turton"
    affiliation: "Los Alamos National Laboratory"    
sponsor_orgs:
  - "DOE:LANL"
research_orgs:
  - "Los Alamos National Laboratory"
dataset_type: "ND"
description: |
  This ensemble of 4 runs is from the Cloverleaf simulation code, a Lagrangian-Eulerian hydrodynamics solver. The dataset represents 
  computational fluid dynamics simulations for physics applications. 
issue_date: "2025-10-08"
report_number: ""
contact_name: "Ayan Biswas"
contact_email: ""

# Data Access Information
access_url: "https://oceans11.lanl.gov/cloverleaf/"
file_format: "HDF5"

# Security Classification and Sensitivity
classification: "U"
marking: "UUR"
distribution_statement: "Approved for public release; distribution is unlimited."
export_control: "none"
sensitivity_level: "public"

---

## Overview

CloverLeaf is a compact simulation code designed to solve the compressible Euler equations on a Cartesian mesh using an explicit, second-order accurate scheme. In the discretization, each cell holds scalar quantities such as density, energy, and pressure, while the velocity components are stored at the cell corners. This mix of cell-centered and corner-based variables is known as a staggered grid layout. The primary CloverLeaf implementation is two-dimensional.

Four data sets were each split into train, test, and validate subsets. The data is output originally in VTK format but was converted to HDF5 format for ease of AI/ML training purposes.

## Data Structure

### File Organization

```
cloverleaf/
...
data/
├── test/
│   └── test.h5
├── train/
│   └── train.h5
├── validate/
│   └── val.h5
```

### Data Format

- **Primary Format**: HDF5
- **Size**: 177 MB

## Provenance

- **Version**: 1.0
- **Release Date**: 2026-05-09
- **Change Log**: Initial release of ensemble dataset

## Datasheet for Dataset

This section follows the [Datasheets for Datasets](https://arxiv.org/abs/1803.09010) framework for transparent dataset documentation.

### Motivation

- **Purpose**: This dataset of four runs was created to train foundation models. 
- **Creators**: Ayan Biswas, Los Alamos National Laboratory
- **Funding**: U.S. Department of Energy, National Nuclear Security Administration

### Composition

- **Instances**: Each file holds trajectories from 4 runs, separated into train, test and validate. 
- **Instance Count**: 4 ensemble runs, thousands of trajectories 
- **Missing Data**: No missing data expected; complete simulation outputs
- **Confidentiality**: Dataset contains unclassified computational simulation data with no confidential or sensitive information

### Collection Process

- **Acquisition**: Data generated through computational simulation using the Cloverleaf code
- **Sampling**: Ensemble of 4 independent simulation runs with potentially varying initial conditions or parameters
- **Timeframe**: Simulations completed prior to 2025-12-09
- **Ethical Review**: Not applicable (computational simulations, no human subjects or biological data)

### Preprocessing/Cleaning/Labeling

- **Preprocessing**: Data represents direct output from Cloverleaf simulation code
- **Raw Data**: Output files are in native simulation format
- **Software**: CloverLeaf simulation code

### Uses

- **Prior Uses**: To train foundation models for the ArtIMis LDRD DI.
- **Intended Uses**: 
  - Physics research in hydrodynamics
  - Algorithm development and testing
  - Performance benchmarking
  - Uncertainty quantification studies
  - Machine learning training data for physics-informed models
- **Unsuitable Uses**: 
  - Production engineering calculations without proper validation
  - Applications outside the code's verified domain of applicability

### Distribution

- **Distribution Method**: To be determined (institutional repository, data portal, or direct access)
- **Distribution Date**: 2026-05-09

### Maintenance

- **Maintainer**: Ayan Biswas
- **Updates**: Updates will be versioned if additional ensemble runs or corrected data become available
- **Retention**: Dataset maintained according to DOE data management requirements
- **Versioning**: Version number will be incremented for any data updates; previous versions will remain accessible

## Contact Information

For questions, issues, or collaboration inquiries:

- **Name**: Ayan Biswas
- **Organization**: Los Alamos National Laboratory

---

*Last Updated*: 2026-05-09  
*Template Version*: 1.0  
*Schema Version*: 1.0
```