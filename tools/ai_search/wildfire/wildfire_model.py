import numpy as np
import matplotlib.pyplot as plt
import time
import os
import pandas as pd
from pathlib import Path
import re

from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.metrics import (accuracy_score, precision_score, recall_score, f1_score,
                             mean_squared_error, root_mean_squared_error, mean_absolute_error, r2_score)


def plot_metrics_and_save(output_df: pd.DataFrame, 
                          selected_models: list = ["Decision Tree", "KNN"], 
                          selected_metrics: list = ["MSE"] 
                          out_dir="plots", 
                          dpi=300):
    """
    Create one bar chart per metric and save each figure to disk.
    
    Arguments:
        output_df: DataFrame containing the evaluation results for each model, including the computed metrics.
        selected_metrics: List of metric names to plot (e.g., ["MSE", "R²"]).
        selected_models: List of model names corresponding to the rows in output_df (e.g., ["Decision Tree", "KNN"]).
        out_dir: Directory    
    """
    
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    def safe_name(s: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s)).strip("_")

    saved_paths = []

    for colname in selected_metrics:
        fig, ax = plt.subplots()

        ax.bar(selected_models, output_df[colname])

        title = f"{colname} vs ML Models" if len(selected_models) > 1 else f"{colname} vs {selected_models[0]}"
        ax.set_title(title)
        ax.set_ylabel(colname)
        ax.set_xlabel("Model")
        fig.tight_layout()

        filename = out_dir / f"{safe_name(colname)}.png"
        fig.savefig(filename, dpi=dpi, bbox_inches="tight")
        saved_paths.append(str(filename))

        plt.close(fig)

    return saved_paths



def run_wildfire_models(input_csv_path: str, 
                  test_size: float = 0.20, 
                  seed: int = 15, 
                  features: list = ['wind_speed','wdir','smois','fuels','ignition'], 
                  target: str = "burned_area", 
                  pred_type: str = "Regression", 
                  selected_models: list = ["Decision Tree", "KNN"], 
                  selected_metrics: list = ["MSE"]):
    """Runs the specified ML models on the wildfire data and evaluates them based on the selected metrics.
    
    Args:
        input_csv_path (str): The path to the input wildfire dataset CSV file.
        test_size (float): The proportion of the dataset to include in the test split.
        seed (int): The random seed for reproducibility.
        features (list): The list of feature column names to use for training the models.
        target (str): The name of the target column to predict.
        pred_type (str): The type of prediction task ("Classification" or "Regression").
        selected_models (list): The list of ML models to run (e.g., ["Decision Tree", "KNN"]).
        selected_metrics (list): The list of evaluation metrics to compute (e.g., ["MSE", "R²"]).   
    """
    
    # Specify the parameters to run
    df = pd.read_csv(input_csv_path)
    TEST_SIZE = test_size
    SEED = seed
    
    all_models = ["Decision Tree", "Random Forest", "KNN", "Linear Regression", "Ridge Regression", "Lasso Regression", "ElasticNet"]


    # Splits data into X, y based on input features/target.
    # Trains/test the X and y values.

    X = df[features].select_dtypes(include=[np.number]).fillna(0)
    y = df[target]

    # if pred_type == "Classification":
    y = y.astype("category").cat.codes
    X = X.apply(lambda col: col.astype("category").cat.codes)

    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=TEST_SIZE, random_state=SEED)


    start_time = time.time()
    results = []
    for model_name in selected_models:
        grid = None
        if model_name == "Decision Tree":
            param_grid = {'max_depth': [1, 2, 3, 4, 5, 6 ,7, 8, 9, 10, 12, 15, 17, 20, 25, 30, 35, 40, 45, 50],
                        'min_samples_split': [2, 3, 4, 5]}
            if pred_type == "Classification":
                grid = GridSearchCV(DecisionTreeClassifier(), param_grid, scoring='accuracy', error_score='raise')
            else:
                grid = GridSearchCV(DecisionTreeRegressor(random_state=SEED), param_grid, cv=5, scoring='neg_mean_squared_error')
        elif model_name == "Random Forest":
            param_grid = {'n_estimators': [50, 100, 200], 'max_depth': [5, 10, 15, 20], 'min_samples_split': [2, 3, 4, 5]}
            if pred_type == "Classification":
                grid = GridSearchCV(RandomForestClassifier(random_state=SEED), param_grid, scoring='accuracy')
            else:
                grid = GridSearchCV(RandomForestRegressor(random_state=SEED), param_grid, scoring='neg_mean_squared_error')
        elif model_name == "KNN":
            param_grid = {'n_neighbors': [3, 4, 5, 6],
                        'weights': ['uniform', 'distance']}
            if pred_type == "Classification":
                grid = GridSearchCV(KNeighborsClassifier(), param_grid, scoring='accuracy')
            else:
                grid = GridSearchCV(KNeighborsRegressor(), param_grid, scoring='neg_mean_squared_error')
        elif model_name == "Linear Regression" and pred_type == "Regression": # NO CLASSIFICATION 
            param_grid = {'fit_intercept': [True, False], 'positive': [True, False]}
            grid = GridSearchCV(LinearRegression(), param_grid, scoring='neg_mean_squared_error', cv=5) #scoring = r2 or neg mse??

        elif model_name == "Ridge Regression" and pred_type == "Regression": # NO CLASSIFICATION 
            param_grid = {'alpha': [0.001, 0.01, 0.1, 1, 10, 100], 'fit_intercept': [True, False]}
            grid = GridSearchCV(Ridge(), param_grid, scoring='neg_mean_squared_error', cv=5) #scoring = r2 or neg mse??
        
        elif model_name == "Lasso Regression" and pred_type == "Regression": # NO CLASSIFICATION 
            param_grid = {'alpha': [0.001, 0.01, 0.1, 1, 10],  'fit_intercept': [True, False], 'max_iter': [1000, 5000, 10000]}
            grid = GridSearchCV(Lasso(), param_grid, scoring='neg_mean_squared_error', cv=5) #scoring = r2 or neg mse??
        
        elif model_name == "ElasticNet" and pred_type == "Regression": # NO CLASSIFICATION 
            param_grid = {'alpha': [0.001, 0.01, 0.1, 1, 10], 'l1_ratio': [0.1, 0.3, 0.5, 0.7, 0.9], 'fit_intercept': [True, False]}
            grid = GridSearchCV(ElasticNet(), param_grid, scoring='neg_mean_squared_error', cv=5) #scoring = r2 or neg mse??
        
        if grid is None:
            raise RuntimeError(f"Wrong selection of model: {model_name} with {pred_type} prediction")
        
        grid.fit(x_train, y_train)
        y_pred = grid.predict(x_test)

        row = {"Model": model_name}
        if pred_type == "Classification":
            if "Accuracy" in selected_metrics:  row["Accuracy"] = accuracy_score(y_test, y_pred)
            if "Precision" in selected_metrics:  row["Precision"] = precision_score(y_test, y_pred, average="weighted", zero_division=0)
            if "Recall" in selected_metrics:  row["Recall"] = recall_score(y_test, y_pred, average="weighted", zero_division=0)
            if "F1" in selected_metrics:  row["F1"] = f1_score(y_test, y_pred, average="weighted", zero_division=0)
        else:
            if "MSE" in selected_metrics:  row["MSE"] = mean_squared_error(y_test, y_pred)
            if "RMSE" in selected_metrics:  row["RMSE"] = root_mean_squared_error(y_test, y_pred)
            if "MAE" in selected_metrics:  row["MAE"] = mean_absolute_error(y_test, y_pred)
            if "R²" in selected_metrics:  row["R²"] = r2_score(y_test, y_pred)
        
        row["Optimal Hyperparameters"] = grid.best_params_
        results.append(row)

    print(f"Finished in {time.time() - start_time:.1f}s")
    output_df = pd.DataFrame(results)
    
    
    plot_metrics_and_save(output_df, 
                          selected_models=selected_models, 
                          selected_metrics=selected_metrics, 
                          out_dir="plots", 
                          dpi=300)
    
    print("model training and evaluation complete. Plots saved to 'plots' directory.")




