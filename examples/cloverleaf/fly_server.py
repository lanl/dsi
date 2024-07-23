# #!/usr/bin/env python3

from dash import Dash, html, dcc
import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import numpy as np
import random

app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

import pandas as pd

df = pd.read_csv("clover_random_test.csv")




perFig = go.Figure()
# int_cols = {"PdV":'mean', "Cell Advection":'mean', "MPI Halo Exchange":'mean', "Self Halo Exchange":'mean', "Momentum Advection":'mean', "Total":'mean'}
# sorted_df = df.sort_values(by=['git_committed_date'], ascending=True).groupby('git_committed_date').agg(int_cols)
# commit_dates = pd.to_datetime(sorted_df.index.to_list()).strftime("%b %d, %Y")
# for col_name in sorted_df.columns:
#     fig.add_trace(go.Scatter(x=commit_dates, y=sorted_df[col_name],
#                         mode='lines', # 'lines' or 'markers'
#                         name=col_name))
sorted_df = df.sort_values(by=['git_committed_date'], ascending=True)
commit_dates = pd.to_datetime(sorted_df['git_committed_date']).dt.strftime("%b-%d,%Y")
for col_name in ["PdV", "Cell Advection", "MPI Halo Exchange", "Self Halo Exchange", "Momentum Advection", "Total"]:
    perFig.add_trace(go.Scatter(x=commit_dates, y=sorted_df[col_name],
                        mode='lines',
                        name=col_name))

perFig.update_traces(mode='lines+markers')
perFig.update_xaxes(showgrid=False)
perFig.update_layout(margin=dict(l=20, r=20, t=0, b=0))


gitFig = go.Figure()
sorted_df = df.sort_values(by=['git_committed_date'], ascending=True)
commit_dates = pd.to_datetime(sorted_df['git_committed_date']).dt.strftime("%b-%d,%Y")
for col_name in ["PdV", "Cell Advection", "MPI Halo Exchange", "Self Halo Exchange", "Momentum Advection", "Total"]:
    gitFig.add_trace(go.Scatter(x=commit_dates, y=sorted_df[col_name],
                        mode='lines',
                        name=col_name))

gitFig.update_traces(mode='lines+markers')
gitFig.update_xaxes(showgrid=False)
gitFig.update_layout(margin=dict(l=20, r=20, t=0, b=0))



app.layout = dbc.Container([
    html.Div([
        html.Div([
            html.H1([
                html.Span("Welcome"),
                html.Br(),
                html.Span("PerfAnalyzer")
            ]),
            html.
            P("Analyze performance accross commit"
              )
        ],
                 style={"vertical-alignment": "top", "height": 260}),
        html.Div([
            html.Div(
                dbc.RadioItems(
                    className='btn-group',
                    inputClassName='btn-check',
                    labelClassName="btn btn-outline-light",
                    labelCheckedClassName="btn btn-light",
                    options=[
                        {"label": "Graph", "value": 1},
                        {"label": "Table", "value": 2}
                    ],
                    value=1,
                    style={'width': '100%'}
                ), style={'width': 206}
            ),
            html.Div(
                dbc.Button(
                    "About",
                    className="btn btn-info",
                    n_clicks=0
                ), style={'width': 104})
        ], style={'margin-left': 15, 'margin-right': 15, 'display': 'flex'}),
        html.Div([
            html.Div([
                html.H2('Unclearable Dropdown:'),
                dcc.Dropdown(
                    options=[
                        {'label': 'Option A', 'value': 1},
                        {'label': 'Option B', 'value': 2},
                        {'label': 'Option C', 'value': 3}
                    ],
                    value=1,
                    clearable=False,
                    optionHeight=40,
                    className='customDropdown'
                )
            ]),
            html.Div([
                html.H2('Unclearable Dropdown:'),
                dcc.Dropdown(
                    options=[
                        {'label': 'Option A', 'value': 1},
                        {'label': 'Option B', 'value': 2},
                        {'label': 'Option C', 'value': 3}
                    ],
                    value=2,
                    clearable=False,
                    optionHeight=40,
                    className='customDropdown'
                )
            ]),
            html.Div([
                html.H2('Clearable Dropdown:'),
                dcc.Dropdown(
                    options=[
                        {'label': 'Option A', 'value': 1},
                        {'label': 'Option B', 'value': 2},
                        {'label': 'Option C', 'value': 3}
                    ],
                    clearable=True,
                    optionHeight=40,
                    className='customDropdown'
                )
            ])
        ], style={'margin-left': 15, 'margin-right': 15, 'margin-top': 30}),
        html.Div(
            html.Img(src='assets/image.svg',
                     style={'margin-left': 15, 'margin-right': 15, 'margin-top': 30, 'width': 310})
        )
    ], style={
        'width': 340,
        'margin-left': 35,
        'margin-top': 35,
        'margin-bottom': 35
    }),
    html.Div(
        [
            html.Div(
                dcc.Graph(
                    figure=perFig
                ), style={'width': 790}),
            html.Div(
                dcc.Graph(
                    figure=gitFig
                ), style={'width': 790,'margin-top': 0}),
            html.Div([
                html.H2('Output 1:'),
                html.Div(className='Output'),
                html.H2('Output 2:'),
                html.Div(html.H3("Selected Value"), className='Output')
            ], style={'width': 198})
        ],
        style={
            'width': 990,
            'margin-top': 20,
            'margin-right': 35,
            'margin-bottom': 0,
            "vertical-alignment": "top"
        })
],
                           fluid=True,
                           style={'display': 'flex'},
                           className='dashboard-container')
if __name__ == "__main__":
    app.run_server(debug=True)