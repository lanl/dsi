---
language:
  - en # ISO language tag
tags:
  - project:genesis # include on all GENESIS project models
  - project:NSEC # include your _short_ model team name e.g. MOAT
  - type:model # use other types include {agent, eval, framework, model, etc...}
  - science:wildfire # what kind of science is this for (e.g., materials, biology, lightsource, fusion, climate, etc.)
  - risk:general # indicates level of risk review {general, reviewed, restricted}
license: BSD-3-Clause # use an SPDX license identifier https://spdx.org/licenses/
base_model: random forest # if fine tuning, include the basemodel url here
datasets:
  - https://github.com/lanl/dsi/blob/main/examples/genesis_demo/wildfiredata.csv
metrics:
  - MSE
---

# Wildfire

The model description provides basic details about the model. This includes the architecture, version, if it was introduced in a paper, if an original implementation is available, the author, and general information about the model. Any copyright should be attributed here. General information about training procedures, parameters, and important disclaimers can also be mentioned in this section. The code is at: https://github.com/lanl/dsi/blob/ai_dev/tools/ai_search/wildfire/wildfire_model.py

*Last Updated*: **2024-11-01**

## Developed by
Megan

## Contributed by
Vedent Iyer

## Model Changelog
+ **2024-10-01** initial public version

## Model short description
Random Forest regression baseline for wildfire burned-area prediction using tabular environmental and ignition features.

## Model description
This resource provides a tabular machine-learning baseline for wildfire modeling, using scikit-learn estimators and GridSearchCV hyperparameter search. The primary configuration trains a Random Forest regressor to predict burned area from five input features: wind_speed, wdir, smois, fuels, and ignition.

The reference implementation:
- Loads the dataset from wildfiredata.csv.
- Encodes features and targets into categorical integer codes (via pandas category encoding).
- Splits the dataset into train/test partitions.
- Runs hyperparameter search with cross-validation using GridSearchCV.
- Evaluates on the held-out test set using mean squared error (MSE).
- Saves evaluation plots (one per metric) as PNG files.

## Finetuned from model (optional)
Not applicable (classical ML model; not a neural fine-tune). The primary estimator is a scikit-learn Random Forest.

## Model Type
Random Forest (scikit-learn), plus optional baselines:
Decision Tree, KNN, Linear Regression, Ridge, Lasso, ElasticNet.

## Inputs and outputs
Inputs:
- Modality: tabular (CSV)
- Required feature columns: wind_speed, wdir, smois, fuels, ignition
- Target column: burned_area
- Preprocessing performed in code:
  - Features are restricted to numeric columns via select_dtypes(include=[np.number]) and missing values are filled with 0.
  - Both features and target are encoded to categorical integer codes.

Outputs:
- Predictions for burned_area (as encoded integer codes in the current implementation).
- Evaluation artifacts:
  - Metric values (e.g., MSE) on the test split.
  - Saved plots (e.g., plots/MSE.png).
  - Best hyperparameters from GridSearchCV per selected model.

## Compute Infrastructure
General CPU-based scikit-learn training; no specialized accelerator is required by the reference implementation.

### Hardware
Not specified (CPU training expected).

### Software
Python with key dependencies:
- numpy
- pandas
- matplotlib
- scikit-learn

Reproducibility artifact example (create requirements.txt):
- pip freeze > requirements.txt

## Papers and Scientific Outputs
None provided.

## Model License
BSD 3-Clause license: https://opensource.org/license/bsd-3-clause

## Contact Info and Model Card Authors
- Megan (developer) — email not provided
- Vedent Iyer (contributor) — email not provided

# Intended Uses

Wildfire analysis

## Intended Use
Baseline regression modeling for wildfire-related tabular datasets, specifically for predicting a burned-area proxy from environmental and ignition features. Intended for experimentation, benchmarking, and demonstration workflows within the GENESIS project context.

### Primary Intended Users
Researchers and engineers evaluating baseline performance and workflows for wildfire prediction tasks.

### Mission Relevance
Supports wildfire science workflows by providing a reproducible baseline model and evaluation pipeline for tabular wildfire datasets.

## Out-of-Scope Use Cases
- Operational fire forecasting or safety-critical deployment without additional validation.
- Use on datasets where the required feature/target columns are absent or incompatible.
- Interpreting outputs as physically calibrated burned-area values without verifying preprocessing and label encoding.

# How to use

This section is the most important for reusability of the model.

## Install Instructions
```bash
pip install numpy pandas matplotlib scikit-learn
```

## Training configuration
Default configuration in run_wildfire_models(...):
- input_csv_path: path to wildfiredata.csv
- test_size: 0.20
- seed: 15
- features: [wind_speed, wdir, smois, fuels, ignition]
- target: burned_area
- pred_type: Regression
- selected_models: e.g., [Random Forest]
- selected_metrics: [MSE]

Hyperparameter search grids (as implemented):
- Decision Tree (regression):
  - max_depth: [1,2,3,4,5,6,7,8,9,10,12,15,17,20,25,30,35,40,45,50]
  - min_samples_split: [2,3,4,5]
  - cv: 5
  - scoring: neg_mean_squared_error
- Random Forest (regression):
  - n_estimators: [50,100,200]
  - max_depth: [5,10,15,20]
  - min_samples_split: [2,3,4,5]
  - scoring: neg_mean_squared_error
- KNN (regression):
  - n_neighbors: [3,4,5,6]
  - weights: [uniform, distance]
  - scoring: neg_mean_squared_error
- Linear Regression:
  - fit_intercept: [true, false]
  - positive: [true, false]
  - cv: 5
  - scoring: neg_mean_squared_error
- Ridge Regression:
  - alpha: [0.001,0.01,0.1,1,10,100]
  - fit_intercept: [true, false]
  - cv: 5
  - scoring: neg_mean_squared_error
- Lasso Regression:
  - alpha: [0.001,0.01,0.1,1,10]
  - fit_intercept: [true, false]
  - max_iter: [1000,5000,10000]
  - cv: 5
  - scoring: neg_mean_squared_error
- ElasticNet:
  - alpha: [0.001,0.01,0.1,1,10]
  - l1_ratio: [0.1,0.3,0.5,0.7,0.9]
  - fit_intercept: [true, false]
  - cv: 5
  - scoring: neg_mean_squared_error

## Inference configuration
Inference uses the best estimator found by GridSearchCV for each selected model and calls predict(x_test). No special decoding parameters apply.

# Code snippets of how to use the model
The code is at: https://github.com/lanl/dsi/blob/ai_dev/tools/ai_search/wildfire/wildfire_model.py
```python
run_wildfire_models(
    input_csv_path="wildfiredata.csv",
    test_size=0.20,
    seed=15,
    features=["wind_speed","wdir","smois","fuels","ignition"],
    target="burned_area",
    pred_type="Regression",
    selected_models=["Random Forest"],
    selected_metrics=["MSE"]
)
```

# Limitations

## Risks
This model is a classical ML baseline and is not a frontier generative model. Primary risks relate to misuse in operational or safety-critical contexts without rigorous validation, data shift monitoring, or calibration. No additional national-security-specific risks were identified in the provided information.

## Limitations
- The current implementation encodes both features and target using categorical integer codes, which may be inappropriate for true continuous regression targets and can change the semantics of the prediction task.
- Evaluation is limited to a single hold-out split and selected metrics; broader benchmarking and uncertainty analysis are not provided.
- Dataset representativeness and potential biases are not documented beyond the provided CSV link.

# Training details

## Training data
- Dataset: https://github.com/lanl/dsi/blob/main/examples/genesis_demo/wildfiredata.csv
- Features used: wind_speed, wdir, smois, fuels, ignition
- Target: burned_area
- Preprocessing:
  - numeric feature selection and missing-value fill (0)
  - categorical encoding to integer codes for both X and y

## Training Procedure
- Train/test split: train_test_split(test_size=0.20, random_state=15)
- Hyperparameter tuning: GridSearchCV per selected model
- Cross-validation: 5-fold for regression grids where specified
- Objective: minimize MSE (via neg_mean_squared_error scoring)
- Best estimator used for final test predictions and metric computation

### Reproducibility Information (optional)
- Random seed used: 15
- Key library: scikit-learn (versions not provided)
- Pipeline: provided code snippet in model card

## Pre-training information
Not applicable (no neural pretraining). Model selection is performed via grid search over scikit-learn hyperparameters.

# Evaluation details
https://github.com/lanl/dsi/blob/main/examples/genesis_demo/wildfiredata.csv

## Evaluation data
https://github.com/lanl/dsi/blob/main/examples/genesis_demo/wildfiredata.csv

## Evaluation Procedure
Hold-out evaluation on the test split created by train_test_split. Metric computation on y_test vs y_pred. Selected metric in the provided configuration: MSE.

## Uncertainty Quantification.
None

## Evaluation results
- Metric: MSE (value not provided in the supplied information).
- Plots: saved to the local plots/ directory (e.g., plots/MSE.png) when the script is executed.

# More Information (optional)
None
