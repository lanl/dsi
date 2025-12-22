import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
import sys
import os
from contextlib import redirect_stdout
import io
import re

from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.metrics import (accuracy_score, precision_score, recall_score, f1_score,
                             mean_squared_error, root_mean_squared_error, mean_absolute_error, r2_score)

from dsi.dsi import DSI

db_filepath = sys.argv[1]
db_type = sys.argv[2]

@st.cache_resource(show_spinner=False)
def get_db():
    fnull = open(os.devnull, 'w')
    with redirect_stdout(fnull):
        return DSI(filename=db_filepath, backend_name=db_type, check_same_thread=False)

st.set_page_config(page_title="DSI ML Emulator", layout="wide")
st.title("DSI ML Emulator")

f1 = io.StringIO()
with redirect_stdout(f1):
    get_db().list()
output = f1.getvalue()

pattern = re.compile(r"Table:\s*(?P<table>\w+).*?- num of rows:\s*(?P<rows>\d+)", re.DOTALL)
tables = [m.group("table") for m in pattern.finditer(output) if int(m.group("rows")) > 5]

if len(tables) == 0:
    st.warning("Emulator requires tables with at least 5 rows. Please try again with a larger dataset.")
    st.stop()

sel_key = "selected_table"
if sel_key not in st.session_state:
    st.session_state[sel_key] = tables[0] if tables else None

st.write("### Select Table (only showing those with more than 5 rows)")
selected = st.radio("Tables", options=tables, horizontal=True, key=sel_key, label_visibility="collapsed")

table = st.session_state[sel_key]
fnull = open(os.devnull, 'w')
with redirect_stdout(fnull):
    df = get_db().get_table(table_name=table, collection=True)

st.write(f"### Preview of {table}")
st.dataframe(df.head())

st.write("### Select target (output) column")
target = st.selectbox("label", df.columns, index=len(df.columns) - 1, label_visibility="collapsed")

st.write("### Select input columns")
features = st.multiselect("label", [c for c in df.columns if c != target], 
                        default=[c for c in df.columns if c != target], label_visibility="collapsed")

col1, col2 = st.columns(2)
with col1:
    st.write("#### Test size (%)")
    TEST_SIZE = st.number_input("Test size (%)", label_visibility="collapsed",
                                min_value=0, max_value=100, value=20, step=1, 
                                help="Percent of data used for testing.") / 100
with col2:
    st.write("#### Random state (seed)")
    SEED = st.number_input("Random state (seed)", label_visibility="collapsed",
                        min_value=0, max_value=100, value=15, step=1, 
                        help="Fixed integer used for reproducible results.")

# st.write("### Select Prediction Type")
# pred_type = st.radio("label", ["Classification", "Regression"], horizontal=True, label_visibility="collapsed")
pred_type = "Regression"

st.write("### Choose ML Models")
all_models = ["Decision Tree", "Random Forest", "KNN"]
if pred_type == "Classification":
    # all_models += ["Naive Bayes"]
    pass
else:
    all_models += ["Linear Regression", "Ridge Regression", "Lasso Regression", "ElasticNet"]
model_btns = [st.checkbox(m) for m in all_models]
selected_models = [name for name, clicked in zip(all_models, model_btns) if clicked]

st.write("### Choose Performance Metrics")

if pred_type == "Classification":
    all_metrics = ["Accuracy", "Precision", "Recall", "F1"]
    metric_btns = [st.checkbox(m) for m in all_metrics]
    metrics = [name for name, clicked in zip(all_metrics, metric_btns) if clicked]
else:
    all_metrics = ["Mean Squared Error", "Root Mean Squared Error", "Mean Absolute Error", "R²"]
    short_names = ["MSE", "RMSE", "MAE", "R²"]
    metric_btns = [st.checkbox(m) for m in all_metrics]
    metrics = [name for name, clicked in zip(short_names, metric_btns) if clicked]

is_valid = (len(selected_models) > 0) and (len(metrics) > 0)
if not is_valid:
    st.info("Select at least one **model** and one **metric** to calculate.")

run = st.button("CALCULATE", type="primary", disabled=not is_valid)

if run:
    start_time = time.time()
    with st.spinner("Running grid search…"):
        X = df[features].select_dtypes(include=[np.number]).fillna(0)
        y = df[target]

        # if pred_type == "Classification":
        y = y.astype("category").cat.codes
        X = X.apply(lambda col: col.astype("category").cat.codes)

        x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=TEST_SIZE, random_state=SEED)

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
                param_grid = {'n_neighbors': [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
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
                if "Accuracy" in metrics:  row["Accuracy"] = accuracy_score(y_test, y_pred)
                if "Precision" in metrics:  row["Precision"] = precision_score(y_test, y_pred, average="weighted", zero_division=0)
                if "Recall" in metrics:  row["Recall"] = recall_score(y_test, y_pred, average="weighted", zero_division=0)
                if "F1" in metrics:  row["F1"] = f1_score(y_test, y_pred, average="weighted", zero_division=0)
            else:
                if "MSE" in metrics:  row["MSE"] = mean_squared_error(y_test, y_pred)
                if "RMSE" in metrics:  row["RMSE"] = root_mean_squared_error(y_test, y_pred)
                if "MAE" in metrics:  row["MAE"] = mean_absolute_error(y_test, y_pred)
                if "R²" in metrics:  row["R²"] = r2_score(y_test, y_pred)
            
            row["Optimal Hyperparameters"] = grid.best_params_
            results.append(row)

    st.success(f"Finished in {time.time() - start_time:.1f}s")
    
    output_df = pd.DataFrame(results)
    st.dataframe(output_df, hide_index = True)

    st.subheader("Model Plots")
    plots_per_row = 3
    rows = (len(metrics) + plots_per_row - 1) // plots_per_row
    
    idx = 0
    for _ in range(rows):
        cols = st.columns(plots_per_row)
        for col_box in cols:
            if idx >= len(metrics):
                break
            colname = metrics[idx]

            fig, ax = plt.subplots()
            colors = plt.cm.viridis(np.linspace(0, 1, len(selected_models)))
            # colors = ["blue", "red", "green", "orange", "brown"]

            ax.bar(selected_models, output_df[colname], color=colors, edgecolor="black")
            if len(selected_models) > 1:
                ax.set_title(f"{colname} vs ML Models")
            else:
                ax.set_title(f"{colname} vs {selected_models[0]}")
            ax.set_ylabel(colname)
            fig.tight_layout()
            col_box.pyplot(fig)            
            idx += 1