# #!/usr/bin/env python3

import argparse
import sys
from dash import Dash, html, dcc, Input, Output, callback, State
import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
import numpy as np
import random
import pandas as pd
import re
# import git
from github import Github
import requests
import datetime


sorted_df = None
fetched_branches = list()
repo_name = ""
local_cached_data = dict()
local_cached_data['git_nodes'] = dict()

def getGitRepo(user_repo):
    return None

def getGitGraph(user_repo, selected_branches):
    # considering perf_data is already sorted by commit time
    global fetched_branches
    global local_cached_data

    all_nodes_list = list()
    repo = None
    for current_selected_branches in selected_branches:
        if current_selected_branches in local_cached_data['git_nodes']:
            all_nodes_list.extend(local_cached_data['git_nodes'][current_selected_branches])
        else:

            print("Trying to get git graphs")
            # git_graph_max_depth = 8

            # visited_commit_hash = []
            # commit_queue = []
            # nodes_list = []
            # repo = getGitRepo(user_repo)
            # # checked_branches_list = ["master", "feature/CMake", "feature/caliper", "fix_gcc", "parse_fix", "development"]
            # for each_branch in fetched_branches:
            #     if each_branch.name == current_selected_branches:
            #         commit_queue.append({"commit":each_branch.commit,"branch":each_branch.name, "depth":0})
            #         # print(each_branch.commit.commit.committer.date.strftime("%b-%d,%Y(%H:%M)"))

            # while len(commit_queue) > 0:
            #     top_here = commit_queue.pop()
            #     c_commit = top_here["commit"]
            #     if c_commit.commit.sha in visited_commit_hash:
            #         continue
            #     if top_here["depth"] > git_graph_max_depth:
            #         continue
            #     c_branch = top_here["branch"]
            #     visited_commit_hash.append(c_commit.commit.sha)
                
            #     for each_parent in c_commit.commit.parents:
            #         nodes_list.append({"sha":c_commit.commit.sha, 
            #                     "date":c_commit.commit.committer.date,
            #                     "depth":top_here["depth"],
            #                     "branch":top_here["branch"],
            #                     "message":c_commit.commit.message
            #         })
            #         child_commit = repo.get_commit(sha=each_parent.sha)
            #         nodes_list.append({"sha":child_commit.commit.sha, 
            #                         "date":child_commit.commit.committer.date,
            #                         "depth":top_here["depth"]+1,
            #                         "branch":top_here["branch"],
            #                         "message":None
            #         })
            #         if child_commit.commit.sha not in visited_commit_hash:
            #             commit_queue.append({"commit":child_commit,"branch":top_here["branch"], "depth":top_here["depth"]+1})
            nodes_list = [{'sha': '07d783c332e3ac0fb63a743736d2707838b429c9', 'date': datetime.datetime(2022, 12, 24, 14, 26, 8, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'feature/CMake', 'message': 'merge master changes + cmake update'}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': 'feature/CMake', 'message': None}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': 'feature/CMake', 'message': 'file correction'}, {'sha': 'de4876d5a45af5c85100af17188c2fc7d3c831bf', 'date': datetime.datetime(2022, 12, 24, 14, 2, 54, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': 'feature/CMake', 'message': None}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': 'feature/CMake', 'message': 'file correction'}, {'sha': '0fdb917bf10d20363dd8b88d762851908643925b', 'date': datetime.datetime(2021, 8, 9, 12, 23, 14, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': 'feature/CMake', 'message': None}, {'sha': '0fdb917bf10d20363dd8b88d762851908643925b', 'date': datetime.datetime(2021, 8, 9, 12, 23, 14, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': 'feature/CMake', 'message': 'OMP flag update'}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': 'Merge pull request #7 from amd-toolchain-support/aocc_support'}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': 'Merge pull request #7 from amd-toolchain-support/aocc_support'}, {'sha': '158e23d08f73d36f71e144851451955b3ae02dff', 'date': datetime.datetime(2021, 8, 2, 18, 40, 45, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': None}, {'sha': '158e23d08f73d36f71e144851451955b3ae02dff', 'date': datetime.datetime(2021, 8, 2, 18, 40, 45, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': 'adding AOCC compiler support for CloverLeaf_ref'}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': None}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': 'Fix typos in C kernels.'}, {'sha': '89cc919b28f687a25d30b44ddf547201da930c14', 'date': datetime.datetime(2020, 7, 14, 9, 16, 46, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': None}, {'sha': '89cc919b28f687a25d30b44ddf547201da930c14', 'date': datetime.datetime(2020, 7, 14, 9, 16, 46, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': 'Add ivdep in place of previous simd.'}, {'sha': '07fcf4d773ba7626e6ea36c7002f7b2cd7c76b2a', 'date': datetime.datetime(2020, 7, 14, 8, 44, 25, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': None}, {'sha': '07fcf4d773ba7626e6ea36c7002f7b2cd7c76b2a', 'date': datetime.datetime(2020, 7, 14, 8, 44, 25, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': 'Unaligned.'}, {'sha': 'e37e1d7aab99070a65094e784721b4d05fb86444', 'date': datetime.datetime(2020, 7, 14, 8, 13, 22, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': None}, {'sha': 'e37e1d7aab99070a65094e784721b4d05fb86444', 'date': datetime.datetime(2020, 7, 14, 8, 13, 22, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': 'Remove simd directives, causing loops to not vectorise under LLVM/clang.'}, {'sha': '439c8d846ede012c89f7be451763a32dbaa5eb2c', 'date': datetime.datetime(2020, 7, 13, 20, 16, 34, tzinfo=datetime.timezone.utc), 'depth': 9, 'branch': 'feature/CMake', 'message': None}, {'sha': 'de4876d5a45af5c85100af17188c2fc7d3c831bf', 'date': datetime.datetime(2022, 12, 24, 14, 2, 54, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': 'feature/CMake', 'message': 'cleanup before merge'}, {'sha': '37c34c42421f605e73f3698bbbbfe7181e82615e', 'date': datetime.datetime(2020, 5, 6, 14, 48, 4, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': None}, {'sha': '37c34c42421f605e73f3698bbbbfe7181e82615e', 'date': datetime.datetime(2020, 5, 6, 14, 48, 4, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': 'Cleanup build'}, {'sha': '9e4093c80cbdb8ac33497569ad0d217c1e8d386c', 'date': datetime.datetime(2020, 5, 6, 14, 44, 38, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': None}, {'sha': '9e4093c80cbdb8ac33497569ad0d217c1e8d386c', 'date': datetime.datetime(2020, 5, 6, 14, 44, 38, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': 'Initial CMake changes'}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': "Merge remote-tracking branch 'origin/parse_fix' into feature/CMake"}, {'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': "Merge remote-tracking branch 'origin/parse_fix' into feature/CMake"}, {'sha': '8d0cfdebc707e9372384a68635bf607772bc4ba5', 'date': datetime.datetime(2019, 3, 11, 13, 7, 50, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': None}, {'sha': '8d0cfdebc707e9372384a68635bf607772bc4ba5', 'date': datetime.datetime(2019, 3, 11, 13, 7, 50, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': 'Fix error in argument parsing'}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': None}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': 'Update to version 1.3 For Mantevo SC2015'}, {'sha': '4f23dd3beb6376f0b6d4922336943760664d5b9a', 'date': datetime.datetime(2015, 8, 5, 22, 32, 14, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': None}, {'sha': '4f23dd3beb6376f0b6d4922336943760664d5b9a', 'date': datetime.datetime(2015, 8, 5, 22, 32, 14, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': 'Added pack/unpack message methods to qextname list for IBM compilers'}, {'sha': '3822dab44664b151a857da568e4b0974ac807195', 'date': datetime.datetime(2015, 8, 5, 13, 45, 53, tzinfo=datetime.timezone.utc), 'depth': 9, 'branch': 'feature/CMake', 'message': None}, {'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': 'Merge pull request #3 from AndrewMallinson/master_v1.3_UHnosync\n\nMaster v1.3 u hnosync'}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': None}, {'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': 'Merge pull request #3 from AndrewMallinson/master_v1.3_UHnosync\n\nMaster v1.3 u hnosync'}, {'sha': '57875df8f3dcf4ffd79df41d621e6577dc6c2d16', 'date': datetime.datetime(2016, 5, 5, 14, 48, 27, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': None}, {'sha': '57875df8f3dcf4ffd79df41d621e6577dc6c2d16', 'date': datetime.datetime(2016, 5, 5, 14, 48, 27, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': 'updated CHANGE LOG to capture the optimisation to update halo.f90'}, {'sha': 'a144711c6e803e71b9e8df7b515deb62ccfab2a5', 'date': datetime.datetime(2016, 2, 19, 14, 50, 2, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': None}, {'sha': 'a144711c6e803e71b9e8df7b515deb62ccfab2a5', 'date': datetime.datetime(2016, 2, 19, 14, 50, 2, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': 'modified update halo kernel.f90 to reduce sync to one barrier\n\nstill need to test'}, {'sha': 'f274ea05a6c8adde7fafcd6ba614dd845a2852df', 'date': datetime.datetime(2016, 2, 11, 17, 42, 46, tzinfo=datetime.timezone.utc), 'depth': 9, 'branch': 'feature/CMake', 'message': None}]
            local_cached_data['git_nodes'][current_selected_branches] = nodes_list
            all_nodes_list.extend(nodes_list)
            # print(nodes_list)
    print('fetched data from ', selected_branches)
    return all_nodes_list


# Fenwick tree range sum functions 
# =================================
def sum(idx, F):
    running_sum = 0
    while idx > 0:
        running_sum += F[idx]
        right_most_set_bit = (idx & -idx)
        idx -= right_most_set_bit
    return running_sum

def add(idx, X, F):
    while idx < len(F):
        F[idx] += X
        right_most_set_bit = (idx & -idx)
        idx += right_most_set_bit

def point_query(idx, F):
    return sum(idx, F)

def range_update(l, r, X, F):
    # Add X to the element at index l
    add(l, X, F)
    # Subtract X from the element at index (r + 1)
    add(r + 1, -X, F)
# =================================


def generatePerfChart(vlineDate='0'):
    perFig = go.Figure()
    # int_cols = {"PdV":'mean', "Cell Advection":'mean', "MPI Halo Exchange":'mean', "Self Halo Exchange":'mean', "Momentum Advection":'mean', "Total":'mean'}
    # sorted_df = df.sort_values(by=['git_committed_date'], ascending=True).groupby('git_committed_date').agg(int_cols)
    # commit_dates = pd.to_datetime(sorted_df.index.to_list()).strftime("%b %d, %Y")
    # for col_name in sorted_df.columns:
    #     fig.add_trace(go.Scatter(x=commit_dates, y=sorted_df[col_name],
    #                         mode='lines', # 'lines' or 'markers'
    #                         name=col_name))
    sorted_df['git_committed_date']
    commit_dates = pd.to_datetime(sorted_df['git_committed_date']).dt.strftime("%b-%d,%Y(%H:%M)")
    for col_name in ["PdV", "Cell Advection", "MPI Halo Exchange", "Self Halo Exchange", "Momentum Advection", "Total"]:
        perFig.add_trace(go.Scatter(x=commit_dates, y=sorted_df[col_name],
                            mode='lines',
                            name=col_name))

    perFig.update_traces(mode='lines+markers')
    perFig.update_xaxes(showgrid=False)
    perFig.update_layout(margin=dict(l=20, r=20, t=0, b=0),
                        legend=dict(
                            x=100,
                            y=-20
    ))
    # if vlineDate is not None:
    perFig.add_vline(x=vlineDate, line_dash = 'dash')
    return perFig

def generateGitChart(git_nodes):
    if len(git_nodes) == 0:
        fig1 = go.Figure().add_annotation(
            x=2, y=2,
            text="No Data to Display",
            font=dict(family="sans serif",size=25,color="crimson"),
            showarrow=False,
            yshift=10)
        fig1.update_layout(
            xaxis =  { "visible": False },
            yaxis = { "visible": False })
        return fig1
    # gitFig = go.Figure()
    gitFig = make_subplots(rows=2, cols=1,
                    vertical_spacing = 0.05,
                    shared_xaxes=True,
                    row_heights=[0.6, 0.4],
                    subplot_titles=("hello",""))

    branch_depth = dict()
    hash_depth = dict()

    git_nodes_df = pd.DataFrame(git_nodes)#.sort_values(by=['date'], ascending=True)
    commit_dates = pd.to_datetime(git_nodes_df['date']).dt.strftime("%b-%d,%Y(%H:%M)")
    sorted_git_nodes_df = git_nodes_df.sort_values(by=['date'], ascending=True)
    sorted_commit_dates = pd.to_datetime(sorted_git_nodes_df['date']).dt.strftime("%b-%d,%Y(%H:%M)")


    Xe = []
    Ye = []
    unique_git_nodes_dates_df = sorted_git_nodes_df.drop_duplicates().reset_index(drop=True)

    def getNodeWidth(sha, depth):
        if sha not in hash_depth:
            branch_depth[depth] = branch_depth.get(depth, 0) + 1
            hash_depth[sha] = branch_depth[depth]
        return hash_depth[sha]
    
    branch_head_Y = dict()
    def getNodeY(go):
        if go['sha'] not in hash_depth:
            le = unique_git_nodes_dates_df['date'].loc[lambda x: x == go['date']]
            cp = point_query(le.index[0], F)
 
            if go['depth'] == 0:
                branch_head_Y[go['branch']] = cp
            else:
                if go['branch'] in branch_head_Y:
                    cp = cp + branch_head_Y[go['branch']]
            hash_depth[go['sha']] = cp
        return hash_depth[go['sha']]

    
    F = [0] * (len(unique_git_nodes_dates_df.index) + 1)
    for ind in git_nodes_df.index:
        if ind % 2 == 1:
            le = unique_git_nodes_dates_df['date'].loc[lambda x: x == git_nodes_df['date'][ind-1]]
            re = unique_git_nodes_dates_df['date'].loc[lambda x: x == git_nodes_df['date'][ind]]
            # print(git_nodes_df['date'][ind-1], le.index[0], ' == ', end='')
            # print(git_nodes_df['date'][ind], re.index[0])
            range_update(re.index[0]+1, le.index[0]-1, 1, F)

    def make_annotations(font_size=15, font_color='rgb(0,0,0)'):
        annotations = []
        for ind in git_nodes_df.index:
            if git_nodes_df['depth'][ind] == 0:
                annotations.append(
                    dict(
                        text=git_nodes_df['branch'][ind],
                        x=commit_dates[ind],
                        y=getNodeY(git_nodes_df.loc[ind]),
                        xref='x2', yref='y2',
                        font=dict(color=font_color, size=font_size),# textangle=-90,
                        showarrow=True, arrowhead=2, arrowsize=1, arrowwidth=2,
                        align="right", valign="top", xanchor="auto", yanchor="bottom"
                    )
                )
        return annotations
    cd = []
    edges = list()
    XY_ind = 0
    
    for ind in git_nodes_df.index:
        if ind % 2 == 1:
            # le = getNodeWidth(git_nodes_df['sha'][ind-1], git_nodes_df['depth'][ind-1])
            # re = getNodeWidth(git_nodes_df['sha'][ind], git_nodes_df['depth'][ind])

            le = getNodeY(git_nodes_df.loc[ind-1])
            re = getNodeY(git_nodes_df.loc[ind])

            # if git_nodes_df['depth'] == 0:
            #     branch_head_Y[git_nodes_df['branch']] = le

            Xe += [commit_dates[ind-1], commit_dates[ind], None]
            Ye += [le, re, None]
            edges.append(((git_nodes_df['date'][ind-1],XY_ind),(git_nodes_df['date'][ind],XY_ind)))
            cd += [[git_nodes_df['message'][ind-1], git_nodes_df['sha'][ind-1]], [git_nodes_df['message'][ind], git_nodes_df['sha'][ind]], None]
            XY_ind = XY_ind + 1

    gitFig.add_trace(go.Scatter(x=Xe,
                                y=Ye,
                                line_shape='hv',
                                mode='lines+markers',
                                marker=dict(symbol='circle-dot',
                                    size=8,
                                    color='#6175c1',
                                    line=dict(color='rgb(50,50,50)', width=1)
                                    ),
                                name='',
                                hovertemplate =
                                '%{customdata[0]}<extra></extra>',
                                showlegend = False,
                                customdata=cd),
                    row = 2,
                    col = 1,
                    secondary_y = False
            )

    commit_dates_second = pd.to_datetime(sorted_df['git_committed_date']).dt.strftime("%b-%d,%Y(%H:%M)")
    for col_name in ["PdV", "Cell Advection", "MPI Halo Exchange", "Self Halo Exchange", "Momentum Advection", "Total"]:
        gitFig.add_trace(go.Scatter(x=commit_dates_second, y=sorted_df[col_name],
                            mode='lines',
                            name=col_name),
                        row = 1,
                        col = 1,
                        secondary_y = False
                        )

    gitFig.update_traces(mode='lines+markers')
    gitFig.update_xaxes(showgrid=False, categoryorder='array', categoryarray=sorted_commit_dates)
    gitFig.update_yaxes(visible=False, showticklabels=False, row=2, col=1)
    gitFig.update_layout(margin=dict(l=20, r=20, t=0, b=0),
                         annotations=make_annotations(),
                         legend_traceorder="normal",
                         dragmode="select",
                         hovermode="x unified")
    gitFig.update_traces(xaxis='x2')
    return gitFig

@callback(
    Output("graph", "figure"), 
    Input("hovermode", "value"))
def update_hovermode(mode):
    df = px.data.gapminder().query("continent=='Oceania'") # replace with your own data source
    fig = px.line(
        df, x="year", y="lifeExp", color="country", 
        title="Hover over points to see the change")
    fig.update_traces(
        mode="markers+lines", hovertemplate=None)
    fig.update_layout(hovermode=mode)
    return fig

# @callback(
#     Output('perf_graph', 'figure'),
#     Input('git_graph', 'hoverData'))
# def update_perf_graph(hoverData):
#     current_hash = hoverData['points'][0]['customdata']
#     current_time = hoverData['points'][0]['x']
#     return generatePerfChart(current_time)

def main(git_nodes):
    app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
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
                html.H6("Provide user and reository name in the form <user/repo>"),
                html.Div([
                    html.Div(dcc.Input(id='input-on-submit', value='UK-MAC/CloverLeaf_ref', type='text')),
                    html.Button('Get Branches', id='submit-val', n_clicks=0),
                    html.Div(id='container-button-basic',
                            children='Enter a value and press submit')



                    # "Input: ",
                    # dcc.Input(id='input-repo', value='UK-MAC/CloverLeaf_ref', type='text'),
                    # html.Button(id='input-repo-submit', type='submit', children='ok')
                ]),
                # html.Br(),
                # html.Div(id='my-output'),
            ]),
            html.Div([
                html.Div([
                    html.H3('Branches'),
                    html.Div(id='branch-div'),
                    dcc.Dropdown(
                        id="branch_multi_option",
                        options=[
                            {'label':i, 'value':i} for i in fetched_branches
                        ],
                        clearable=True,
                        optionHeight=40,
                        multi=True
                    ),
                    html.Div(id='branch-selection_text',
                            children='Selected branches')
                ])
            ], style={'margin-left': 15, 'margin-right': 15, 'margin-top': 30}),
            # html.Div(
            #     html.Img(src='assets/image.svg',
            #             style={'margin-left': 15, 'margin-right': 15, 'margin-top': 30, 'width': 310})
            # )
        ], style={
            'width': 340,
            'margin-left': 35,
            'margin-top': 35,
            'margin-bottom': 35
        }),
        html.Div(
            [
                # html.Div(
                #     dcc.Graph(
                #         figure=generatePerfChart(),
                #         id='perf_graph'
                #     ), style={'width': 790}),
                html.Div(
                    dcc.Graph(
                        figure=generateGitChart(git_nodes),
                        id='git-graph'
                    ), style={'width': 790,'margin-top': 0}),
                html.Div([
                    html.H3('Selected Commits:'),
                    html.Div(
                        dcc.Graph(
                            id='selected-commits-table'
                        ), style={'width': 790,'margin-top': 0,'margin-left': 0}),
                    html.Button('Run performance with selected commits', id='run-perf-commit', n_clicks=0, disabled=True),
                ], style={'width': 790,'margin-top': 0,'margin-left': 0})
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
    
    





    app.run_server(debug=True)


@callback(
    Output('container-button-basic', 'children'),
    Input('submit-val', 'n_clicks'),
    State('input-on-submit', 'value'),
    prevent_initial_call=True
)
def update_branch_output_text(n_clicks, value):
    repo_name = value
    return 'Fetching branches for {}'.format(
        value
    )

@callback(
    Output('branch_multi_option', 'options'),
    Input('submit-val', 'n_clicks'),
    State('input-on-submit', 'value'),
    prevent_initial_call=True
)
def update_branch_output_lists(n_clicks, value):
    global fetched_branches
    global repo_name
    repo_name = value
    repo = getGitRepo(repo_name)
    fetched_branches = repo.get_branches()
    return [br.name for br in fetched_branches]


@callback(
    Output('git-graph', 'figure'),
    Input('input-on-submit', 'value'),
    Input('branch_multi_option', 'value')
)
def update_branch_selection_output(repo_name, value):
    selected_branches = value
    git_nodes = getGitGraph(repo_name, selected_branches)
    print(selected_branches)
    return generateGitChart(git_nodes)


@callback(
    Output('selected-commits-table', 'figure'),
    Output('run-perf-commit','disabled'),
    Input('git-graph', 'selectedData')
)
def update_git_selection(selection):
    if selection is None:
        fig = go.Figure()
        fig.update_layout(
            margin = {"l":20, "t":0},
            xaxis =  { "visible": False },
            yaxis = { "visible": False },
            annotations = [
                {   
                    "text": "Brush over commit nodes to select",
                    "xref": "paper",
                    "yref": "paper",
                    "showarrow": False,
                    "font": {
                        "family":"sans serif","size": 20,"color":"crimson"
                    }
                }
            ]
        )
        return [fig, True]
        
    dss = pd.DataFrame(selection['points'])


    # unique_git_nodes_dates_df = dss.drop_duplicates(subset=['brand']).reset_index(drop=True)

    msg_list = [x[0] for x in dss['customdata']]
    hash_list = [x[1][:7] for x in dss['customdata']]

    c_df = pd.DataFrame({
        'date(time)':dss['x'].to_list(),
        'hash':hash_list,
        'message': msg_list
    })
    table_df = c_df.dropna().drop_duplicates(subset=['hash']).sort_values(by=['date(time)'], ascending=True)
    fig = go.Figure(data=[go.Table(header=dict(values=list(table_df)),
                 cells=dict(values=[table_df[column].to_list() for column in table_df]))
                     ])
    fig.data[0]['columnwidth'] = [40, 20, 100]
    fig.update_layout(autosize=False, margin = {"l":20, "t":0},)
    return [fig, False]


@callback(
    Input('git-graph', 'selectedData'),
    Input('run-perf-commit', 'n_clicks'),
    prevent_initial_call=True
)
def run_perf_with_commits(selectedData, value):
    print(selectedData)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # parser.add_argument('--testname', help='the test name')
    # parser.add_argument('--gitdir', help='the git directory')
    # args = parser.parse_args()
    # if args.gitdir is None:
    #     parser.print_help()
    #     sys.exit(0)
    # git_repo = git.Repo(args.gitdir)

    df = pd.read_csv("clover_random_test.csv")
    sorted_df = df.sort_values(by=['git_committed_date'], ascending=True)
    git_nodes = getGitGraph(sorted_df["git_repo_name"][0],[])
    main(git_nodes)