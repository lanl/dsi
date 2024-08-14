# #!/usr/bin/env python3

import argparse
import sys
from dash import Dash, html, dcc, Input, Output, dash_table, callback, State, callback_context, no_update
from dash.exceptions import PreventUpdate
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
import diskcache
import os
import git
import subprocess

import parse_clover_output
import code_sensing

fetched_branches = list()
repo_name = ""
local_cached_data = None

class PerfRunner():
    def __init__(self, cwd='/tmp/fly_dsi'):
        self.testname = "perf_runner_test_run"
        self.current_working_directory = cwd
        self.current_git_directory = cwd + '/src'

    def initGitRepo(self, gup='UK-MAC/CloverLeaf_ref'):
        self.git_user_repo = gup
        self.git_http_url = 'https://github.com/' + self.git_user_repo + '.git'
        self.git_ssh_url = 'git@github.com:' + self.git_user_repo + '.git'
        self.updateGitRepo()
    
    def updateGitRepo(self):
        self.git_repo = None
        if os.path.exists(self.current_git_directory) == False:
            os.mkdir(self.current_git_directory)
        try:
            self.git_repo = git.Repo(self.current_git_directory)
        except git.InvalidGitRepositoryError:
            self.git_repo = git.Repo.clone_from(self.git_ssh_url, self.current_git_directory)



def initAndLoadCachedData(workignDir='/tmp/fly_dsi'):
    global local_cached_data
    if not os.path.exists(workignDir):
        print("Cache path not exist. Creating directory.")
        os.makedirs(workignDir)
    print("Loading cached data ...")
    cpath = os.path.join(workignDir, 'dsi_perf.diskCacheIndex')
    local_cached_data = diskcache.Index(cpath)
    if 'git_nodes' not in local_cached_data:
        local_cached_data['git_nodes'] = dict()
        


def getGitRepo(user_repo):
    GITHUB_ACCESS_TOKEN = ""
    g = Github(GITHUB_ACCESS_TOKEN)
    return g.get_repo(user_repo)

def getGitGraph(user_repo, selected_branches):
    # considering perf_data is already sorted by commit time
    global fetched_branches
    global local_cached_data
    cached_data = local_cached_data['git_nodes']

    all_nodes_list = list()
    repo = None
    for current_selected_branches in selected_branches:
        if current_selected_branches in cached_data:
            all_nodes_list.extend(cached_data[current_selected_branches])
        else:

            print("Trying to get git graphs")
            git_graph_max_depth = 8

            visited_commit_hash = []
            commit_queue = []
            nodes_list = []
            repo = getGitRepo(user_repo)
            # checked_branches_list = ["master", "feature/CMake", "feature/caliper", "fix_gcc", "parse_fix", "development"]
            for each_branch in fetched_branches:
                if each_branch.name == current_selected_branches:
                    commit_queue.append({"commit":each_branch.commit,"branch":each_branch.name, "depth":0})
                    # print(each_branch.commit.commit.committer.date.strftime("%b-%d,%Y(%H:%M)"))

            while len(commit_queue) > 0:
                top_here = commit_queue.pop()
                c_commit = top_here["commit"]
                if c_commit.commit.sha in visited_commit_hash:
                    continue
                if top_here["depth"] > git_graph_max_depth:
                    continue
                c_branch = top_here["branch"]
                visited_commit_hash.append(c_commit.commit.sha)
                
                for each_parent in c_commit.commit.parents:
                    nodes_list.append({"sha":c_commit.commit.sha, 
                                "date":c_commit.commit.committer.date,
                                "depth":top_here["depth"],
                                "branch":top_here["branch"],
                                "message":c_commit.commit.message,
                                "cname":c_commit.commit.committer.name
                    })
                    child_commit = repo.get_commit(sha=each_parent.sha)
                    nodes_list.append({"sha":child_commit.commit.sha, 
                                    "date":child_commit.commit.committer.date,
                                    "depth":top_here["depth"]+1,
                                    "branch":top_here["branch"],
                                    "message":None,
                                    "cname":None
                    })
                    if child_commit.commit.sha not in visited_commit_hash:
                        commit_queue.append({"commit":child_commit,"branch":top_here["branch"], "depth":top_here["depth"]+1})
            # nodes_list = [{'sha': '07d783c332e3ac0fb63a743736d2707838b429c9', 'date': datetime.datetime(2022, 12, 24, 14, 26, 8, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'feature/CMake', 'message': 'merge master changes + cmake update', 'cname': 'caxwl'}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': 'feature/CMake', 'message': 'file correction', 'cname': 'caxwl'}, {'sha': 'de4876d5a45af5c85100af17188c2fc7d3c831bf', 'date': datetime.datetime(2022, 12, 24, 14, 2, 54, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': 'feature/CMake', 'message': 'file correction', 'cname': 'caxwl'}, {'sha': '0fdb917bf10d20363dd8b88d762851908643925b', 'date': datetime.datetime(2021, 8, 9, 12, 23, 14, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '0fdb917bf10d20363dd8b88d762851908643925b', 'date': datetime.datetime(2021, 8, 9, 12, 23, 14, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': 'feature/CMake', 'message': 'OMP flag update', 'cname': 'caxwl'}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': 'Merge pull request #7 from amd-toolchain-support/aocc_support', 'cname': 'GitHub'}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': 'Merge pull request #7 from amd-toolchain-support/aocc_support', 'cname': 'GitHub'}, {'sha': '158e23d08f73d36f71e144851451955b3ae02dff', 'date': datetime.datetime(2021, 8, 2, 18, 40, 45, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '158e23d08f73d36f71e144851451955b3ae02dff', 'date': datetime.datetime(2021, 8, 2, 18, 40, 45, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': 'adding AOCC compiler support for CloverLeaf_ref', 'cname': 'mohan002'}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': 'Fix typos in C kernels.', 'cname': 'jdshanks'}, {'sha': '89cc919b28f687a25d30b44ddf547201da930c14', 'date': datetime.datetime(2020, 7, 14, 9, 16, 46, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '89cc919b28f687a25d30b44ddf547201da930c14', 'date': datetime.datetime(2020, 7, 14, 9, 16, 46, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': 'Add ivdep in place of previous simd.', 'cname': 'jdshanks'}, {'sha': '07fcf4d773ba7626e6ea36c7002f7b2cd7c76b2a', 'date': datetime.datetime(2020, 7, 14, 8, 44, 25, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '07fcf4d773ba7626e6ea36c7002f7b2cd7c76b2a', 'date': datetime.datetime(2020, 7, 14, 8, 44, 25, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': 'Unaligned.', 'cname': 'jdshanks'}, {'sha': 'e37e1d7aab99070a65094e784721b4d05fb86444', 'date': datetime.datetime(2020, 7, 14, 8, 13, 22, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': 'e37e1d7aab99070a65094e784721b4d05fb86444', 'date': datetime.datetime(2020, 7, 14, 8, 13, 22, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': 'Remove simd directives, causing loops to not vectorise under LLVM/clang.', 'cname': 'jdshanks'}, {'sha': '439c8d846ede012c89f7be451763a32dbaa5eb2c', 'date': datetime.datetime(2020, 7, 13, 20, 16, 34, tzinfo=datetime.timezone.utc), 'depth': 9, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': 'de4876d5a45af5c85100af17188c2fc7d3c831bf', 'date': datetime.datetime(2022, 12, 24, 14, 2, 54, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': 'feature/CMake', 'message': 'cleanup before merge', 'cname': 'caxwl'}, {'sha': '37c34c42421f605e73f3698bbbbfe7181e82615e', 'date': datetime.datetime(2020, 5, 6, 14, 48, 4, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '37c34c42421f605e73f3698bbbbfe7181e82615e', 'date': datetime.datetime(2020, 5, 6, 14, 48, 4, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': 'feature/CMake', 'message': 'Cleanup build', 'cname': 'Wei'}, {'sha': '9e4093c80cbdb8ac33497569ad0d217c1e8d386c', 'date': datetime.datetime(2020, 5, 6, 14, 44, 38, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '9e4093c80cbdb8ac33497569ad0d217c1e8d386c', 'date': datetime.datetime(2020, 5, 6, 14, 44, 38, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': 'feature/CMake', 'message': 'Initial CMake changes', 'cname': 'Wei'}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': "Merge remote-tracking branch 'origin/parse_fix' into feature/CMake", 'cname': 'Wei'}, {'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': 'feature/CMake', 'message': "Merge remote-tracking branch 'origin/parse_fix' into feature/CMake", 'cname': 'Wei'}, {'sha': '8d0cfdebc707e9372384a68635bf607772bc4ba5', 'date': datetime.datetime(2019, 3, 11, 13, 7, 50, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '8d0cfdebc707e9372384a68635bf607772bc4ba5', 'date': datetime.datetime(2019, 3, 11, 13, 7, 50, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': 'Fix error in argument parsing', 'cname': 'Tom Deakin'}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': 'Update to version 1.3 For Mantevo SC2015', 'cname': 'Oliver Perks'}, {'sha': '4f23dd3beb6376f0b6d4922336943760664d5b9a', 'date': datetime.datetime(2015, 8, 5, 22, 32, 14, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '4f23dd3beb6376f0b6d4922336943760664d5b9a', 'date': datetime.datetime(2015, 8, 5, 22, 32, 14, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': 'Added pack/unpack message methods to qextname list for IBM compilers', 'cname': 'David Beckingsale'}, {'sha': '3822dab44664b151a857da568e4b0974ac807195', 'date': datetime.datetime(2015, 8, 5, 13, 45, 53, tzinfo=datetime.timezone.utc), 'depth': 9, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': 'Merge pull request #3 from AndrewMallinson/master_v1.3_UHnosync\n\nMaster v1.3 u hnosync', 'cname': 'OliverPerks'}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': 'feature/CMake', 'message': 'Merge pull request #3 from AndrewMallinson/master_v1.3_UHnosync\n\nMaster v1.3 u hnosync', 'cname': 'OliverPerks'}, {'sha': '57875df8f3dcf4ffd79df41d621e6577dc6c2d16', 'date': datetime.datetime(2016, 5, 5, 14, 48, 27, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': '57875df8f3dcf4ffd79df41d621e6577dc6c2d16', 'date': datetime.datetime(2016, 5, 5, 14, 48, 27, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': 'feature/CMake', 'message': 'updated CHANGE LOG to capture the optimisation to update halo.f90', 'cname': 'Andrew Mallinson'}, {'sha': 'a144711c6e803e71b9e8df7b515deb62ccfab2a5', 'date': datetime.datetime(2016, 2, 19, 14, 50, 2, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': None, 'cname': None}, {'sha': 'a144711c6e803e71b9e8df7b515deb62ccfab2a5', 'date': datetime.datetime(2016, 2, 19, 14, 50, 2, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': 'feature/CMake', 'message': 'modified update halo kernel.f90 to reduce sync to one barrier\n\nstill need to test', 'cname': 'Andrew Mallinson'}, {'sha': 'f274ea05a6c8adde7fafcd6ba614dd845a2852df', 'date': datetime.datetime(2016, 2, 11, 17, 42, 46, tzinfo=datetime.timezone.utc), 'depth': 9, 'branch': 'feature/CMake', 'message': None, 'cname': None}]
            cached_data[current_selected_branches] = nodes_list
            all_nodes_list.extend(nodes_list)
            # print(nodes_list)
    local_cached_data['git_nodes'] = cached_data
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

def generateGitChart(sorted_df, git_nodes, mk_data=None, perf_filter=list()):
    if git_nodes is None or len(git_nodes) == 0:
        fig1 = go.Figure().add_annotation(
            x=2, y=2,
            text="Select Branches",
            font=dict(family="sans serif",size=25,color="crimson"),
            showarrow=False,
            yshift=10)
        fig1.update_layout(
            xaxis =  { "visible": False },
            yaxis = { "visible": False })
        return fig1
    # gitFig = go.Figure()
    gitFig = make_subplots(rows=3, cols=1,
                    vertical_spacing = 0.01,
                    shared_xaxes=True,
                    row_heights=[0.6, 0.1, 0.3],
                    subplot_titles=("hello",""))

    branch_depth = dict()
    hash_depth = dict()

    git_nodes_df = pd.DataFrame(git_nodes)

    merged_df = git_nodes_df
    if sorted_df is not None:
        merged_df = pd.merge(sorted_df, git_nodes_df, left_on="git_hash", right_on="sha", how="outer")
    combined_all_df = merged_df[merged_df.cname != None].sort_values(by=['date'], ascending=True)
    combined_all_df["formatted_date"] = pd.to_datetime(combined_all_df['date']).dt.strftime("%b-%d,%Y(%H:%M:%S)")
    sorted_git_nodes_df = git_nodes_df.sort_values(by=['date'], ascending=True)

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

    def find_commit_dates(hash):
        return combined_all_df[combined_all_df["sha"] == hash]["formatted_date"].iloc[0]
        
    def make_annotations(font_size=15, font_color='rgb(0,0,0)'):
        annotations = []
        for ind in git_nodes_df.index:
            if git_nodes_df['depth'][ind] == 0:
                annotations.append(
                    dict(
                        text=git_nodes_df['branch'][ind],
                        x=find_commit_dates(git_nodes_df['sha'][ind]),
                        y=getNodeY(git_nodes_df.loc[ind]),
                        xref='x3', yref='y3',
                        font=dict(color=font_color, size=font_size),# textangle=-90,
                        showarrow=True, arrowhead=2, arrowsize=1, arrowwidth=2,
                        align="right", valign="top", xanchor="auto", yanchor="bottom"
                    )
                )
        return annotations
    cd = []
    edges = list()
    XY_ind = 0
    Xe2 = []
    for ind in git_nodes_df.index:
        if ind % 2 == 1:
            # le = getNodeWidth(git_nodes_df['sha'][ind-1], git_nodes_df['depth'][ind-1])
            # re = getNodeWidth(git_nodes_df['sha'][ind], git_nodes_df['depth'][ind])

            le = getNodeY(git_nodes_df.loc[ind-1])
            re = getNodeY(git_nodes_df.loc[ind])

            Xe += [find_commit_dates(git_nodes_df['sha'][ind-1]), find_commit_dates(git_nodes_df['sha'][ind]), None]
            Ye += [le, re, None]
            edges.append(((git_nodes_df['date'][ind-1],XY_ind),(git_nodes_df['date'][ind],XY_ind)))
            cd += [[git_nodes_df['message'][ind-1], git_nodes_df['sha'][ind-1], git_nodes_df['cname'][ind-1]], 
                   [git_nodes_df['message'][ind], git_nodes_df['sha'][ind], git_nodes_df['cname'][ind]], 
                   None]
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
                                'Message: %{customdata[0]}<br>Name: %{customdata[2]}<extra></extra>',
                                showlegend = False,
                                customdata=cd),
                    row = 3,
                    col = 1,
                    secondary_y = False
            )

    if sorted_df is not None and len(sorted_df) > 0:
        for col_name in perf_filter:# ["pdv", "cell_advection", "mpi_halo_exchange", "self_halo_exchange", "momentum_advection", "total"]:
            gitFig.add_trace(go.Scatter(x=combined_all_df["formatted_date"], y=combined_all_df[col_name],
                                mode='lines',
                                name=col_name),
                            row = 1,
                            col = 1,
                            secondary_y = False
                            )


    if mk_data is not None:
        print('mk data found', mk_data['hash'])

        global perf_runner
        global local_cached_data
        cached_data = local_cached_data['code_sensing']
        all_var_list = list()
        f_lines = cached_data[mk_data['hash']][mk_data['ev']][mk_data['var_name']][mk_data['file_name']]
        for each_hash in cached_data:

            if mk_data['ev'] in cached_data[each_hash] and \
                mk_data['var_name'] in cached_data[each_hash][mk_data['ev']] and \
                    mk_data['file_name'] in cached_data[each_hash][mk_data['ev']][mk_data['var_name']]:
                f_list = dict()
                f_list['hash'] = each_hash
                f_list['occ'] = len(cached_data[each_hash][mk_data['ev']][mk_data['var_name']][mk_data['file_name']])
                all_var_list.append(f_list)
    
        all_var_df = pd.DataFrame(all_var_list)

        code_senese_df = pd.merge(all_var_df, git_nodes_df, left_on="hash", right_on="sha", how="outer")
        code_senese_df = code_senese_df.dropna().drop_duplicates(subset=['hash'], keep='first').reset_index(drop=True)
        code_senese_df["formatted_date"] = pd.to_datetime(code_senese_df['date']).dt.strftime("%b-%d,%Y(%H:%M:%S)")
        print(code_senese_df.dropna()['occ'])
        print(code_senese_df.dropna()['formatted_date'])
        gitFig.add_trace(go.Bar(x=code_senese_df.dropna()["formatted_date"], y=code_senese_df.dropna()['occ'],
                                    marker = {'color' : 'black'},
                                    name=mk_data['file_name']),
                            row = 2,
                            col = 1,
                            secondary_y = False
                            )

    
    gitFig.update_traces(mode='lines+markers', row=1, col=1)
    gitFig.update_xaxes(showgrid=False, categoryorder='array', categoryarray=combined_all_df["formatted_date"])
    gitFig.update_xaxes(visible=False, showticklabels=False, row=1, col=1)
    gitFig.update_yaxes(visible=True, showgrid=False, showticklabels=False, row=2, col=1)
    gitFig.update_yaxes(visible=True, showticklabels=False, title="Commits", row=3, col=1)
    gitFig.update_yaxes(type="linear", title="Time (s)", row=1, col=1)
    gitFig.update_layout(margin=dict(l=20, r=20, t=0, b=20),
                         annotations=make_annotations(),
                         legend_traceorder="normal",
                         dragmode="select",
                         hovermode="x unified")
    gitFig.update_traces(xaxis='x3')
    return gitFig

# @callback(
#     Output('perf_graph', 'figure'),
#     Input('git_graph', 'hoverData'))
# def update_perf_graph(hoverData):
#     current_hash = hoverData['points'][0]['customdata']
#     current_time = hoverData['points'][0]['x']
#     return generatePerfChart(current_time)

def main(perf_data, git_nodes):
    app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY], suppress_callback_exceptions=True)
    app.layout = dbc.Container([
        html.Div([
            html.Div([
                html.H3([
                    html.Span("Welcome"),
                    html.Br(),
                    html.Span("PerfAnalyzer")
                ]),
                # html.P("Analyze performance accross commit")
            ], style={"vertical-alignment": "top"}),
            


            html.Div([
                html.Div([
                    html.H6("Provide user and reository name"),
                    html.Div([
                        dcc.Input(id='input-on-submit', value='UK-MAC/CloverLeaf_ref', type='text', style={'width':200}),
                        html.Button('Get Branches', id='submit-val', n_clicks=0),
                        html.Div(id='container-button-basic',
                                children='Enter <user/repo> and press Get Branches')
                    ]),
                ], style={'width': '39%', 'display': 'inline-block'}),
                html.Div([
                    html.Div([
                        html.H6('Branches'),
                        dcc.Dropdown(
                            id="branch-multi-option",
                            options=[
                                {'label':i, 'value':i} for i in fetched_branches
                            ],
                            placeholder="Select Branches",
                            clearable=True,
                            optionHeight=40,
                            multi=True
                        ),
                        # html.Div(id='branch-selection_text', children='Selected branches')
                    ])
                ], style={'width': '59%', 'display': 'inline-block', 'margin-left': 0, 'margin-right': 0, 'vertical-align': 'top'}),
            ],
            style={
                'margin-top': 0,
                'margin-bottom': 10,
                "vertical-alignment": "top"
            }),

            html.Div([
                dcc.Dropdown(
                    id="perf-metric-option",
                    options=[],
                    placeholder="Select performance metric to filter",
                    clearable=True,
                    optionHeight=40,
                    multi=True,
                    style={'margin-bottom': 10, 'width': '100%'}
                ),
                dcc.Loading(
                    [dcc.Graph(figure=generateGitChart(perf_data, git_nodes), id='git-graph')],
                    overlay_style={"visibility":"visible", "opacity": .5, "backgroundColor": "white"},
                    custom_spinner=html.H2(["Loading Performance data", dbc.Spinner(color="primary")]),
                ),
                html.Div(
                    html.Button('Run performance with selected commits', id='run-perf-commit', n_clicks=0, disabled=True
                    ), style={'margin-top': 20,'margin-bottom': 20}),
                html.H4('Selected Commits:'),
                dash_table.DataTable(id='selected-second-commits-table',
                                    columns=[
                                        {'name': 'date(time)', 'id': 'date(time)', 'type': 'text'},
                                        {'name': 'hash', 'id': 'hash', 'type': 'text'},
                                        {'name': 'message', 'id': 'message', 'type': 'text'}
                                    ],
                                    filter_action='native',
                                    editable=False,
                                    row_selectable="multi",
                                    row_deletable=True,
                                    style_cell={'textAlign': 'left'},
                                    style_table={
                                        'height': '250px', 'minHeight': '200px', 'maxHeight': '250px',
                                        'overflow': 'auto',
                                        'font-size': '12px',
                                    },
                                    style_data={
                                        # 'width': '100px', 'minWidth': '100px',
                                        'height': 'auto',
                                        'whiteSpace': 'normal',
                                        'overflow': 'auto',
                                        # 'textOverflow': 'ellipsis',
                                    }
                                     ),
            ], style={'width': 790,'margin-top': 0,'margin-left': 0})
            # html.Div(
            #     html.Img(src='assets/image.svg',
            #             style={'margin-left': 15, 'margin-right': 15, 'margin-top': 30, 'width': 310})
            # )
        ], style={
            'width': '60%',
            'height': '1000px',
            'margin-left': 10,
            'margin-top': 5,
            'margin-right': 10
        }),
        html.Div([
            html.Div([
                html.H4("Search any variable, (regex or plaintext):"),
                dcc.Input(id='custom-var-search', value='pragma, define', type='text', style={'margin-bottom': 10}),
                html.Div(id='search-var-info', children='Choose files types to filter'),
                dcc.Dropdown(
                    options={r"\.c":".c", r"\.cc": ".cc", r"\.py":".py", r"\.f90":".f90", r"\.ipynb": ".ipynb"},
                    value = [r"\.c", r"\.cc",],
                    id="filter-file-multi-options",
                    clearable=True,
                    optionHeight=40,
                    multi=True
                ),
                html.Div(id='file-filter-selection-text', children='Filtered Files', style={'margin-bottom': 10}),
                html.Button('Search', id='submit-var-search', n_clicks=0, disabled=True),
                dash_table.DataTable(id='var-search-table',
                                    columns=[
                                        {'name': 'variable', 'id': 'var_name', 'type': 'text'},
                                        {'name': 'file name', 'id': 'file_name', 'type': 'text'},
                                        # {'name': 'occurance', 'id': 'occ', 'type': 'numeric'}
                                    ],
                                    filter_action='native',
                                    editable=False,
                                    sort_action="native",
                                    row_selectable="multi",
                                    row_deletable=True,
                                    style_cell={'textAlign': 'left'},
                                    style_table={
                                        'height': '250px', 'minHeight': '200px', 'maxHeight': '250px',
                                        'overflow': 'auto',
                                        'font-size': '12px',
                                    },
                                    style_data={
                                        # 'width': '100px', 'minWidth': '100px', 
                                        'maxWidth': '100px',
                                        'whiteSpace': 'normal',
                                        'overflow': 'auto',
                                        # 'textOverflow': 'ellipsis',
                                    }
                                     ),
                # html.Div(id='var-results-table', style={'margin-top': 10}),
                html.Div(id='code-view', style={'margin-top': 10,
                                                'height': '1000px', 'maxHeight': '1000px',
                                                'overflow': 'auto',
                                                }),
            ], style={'margin-left': 0, 'margin-top': 5}),
        ], style={
            'width': '40%',
            'margin-left': 0,
        }),
    ],
    fluid=True,
    style={'display': 'flex'},
    className='dashboard-container')

    app.run_server(debug=True)


@callback(
    # Output('click-data', 'children'),
    Input('git-graph', 'clickData'))
def display_click_data(clickData):
    print(clickData)


@callback(
    Output('var-search-table', 'data'),
    Input('submit-var-search', 'n_clicks'),
    State('custom-var-search', 'value'),
    Input('filter-file-multi-options', 'value'),
    prevent_initial_call=True
)
def update_var_result_table(n_clicks, search_var, filtered_files):
    changed_id = [p['prop_id'] for p in callback_context.triggered][0]
    if 'submit-var-search' in changed_id:


        global perf_runner
        global local_cached_data
        if 'code_sensing' not in local_cached_data:
            local_cached_data['code_sensing'] = dict()
        cached_data = local_cached_data['code_sensing']

        perf_runner.updateGitRepo()
        candidate_commit_hash = perf_runner.git_repo.head.object.hexsha
        cached_data[candidate_commit_hash] = cached_data.get(candidate_commit_hash, dict())

        all_var_list = list()
        for each_var in search_var.split(","):
            ev = each_var.rstrip()
            if ev not in cached_data[candidate_commit_hash]:
                var_dict = code_sensing.recursive_customized_match([ev], filtered_files, perf_runner.current_git_directory)
                cached_data[candidate_commit_hash][ev] = cached_data[candidate_commit_hash].get(ev, dict())
                cached_data[candidate_commit_hash][ev].update(var_dict)

            each_hash = candidate_commit_hash
            # for each_hash in cached_data:
            if ev in cached_data[each_hash]:
                for each_var in cached_data[each_hash][ev]:
                    for each_file in cached_data[each_hash][ev][each_var]:
                        for f_type in filtered_files:
                            if re.search(f_type + '$', each_file):
                                f_list = dict()
                                f_list['var_name'] = each_var
                                f_list['hash'] = each_hash
                                f_list['ev'] = ev
                                f_list['file_name'] = each_file
                                f_list['occ'] = len(cached_data[each_hash][ev][each_var][each_file])
                                all_var_list.append(f_list)
                break

        local_cached_data['code_sensing'] = cached_data

        return all_var_list
    return list()


@callback(
    Output('code-view', "children"),
    Input('var-search-table', "derived_virtual_data"),
    Input('var-search-table', "derived_virtual_selected_rows"),
    Input('selected-second-commits-table', "derived_virtual_data"),
    Input('selected-second-commits-table', "derived_virtual_selected_rows"),
    prevent_initial_call=True)
def action_on_selected_vars(rows, derived_virtual_selected_rows, selected_commits_row, derived_selected_commits_list):
    changed_id = [p['prop_id'] for p in callback_context.triggered][0]
    blank_markdown = dcc.Markdown("", id='actual-source-block')
    
    mk_data = None
    global perf_runner
    global local_cached_data

    if 'code_sensing' not in local_cached_data:
        return no_update

    cached_data = local_cached_data['code_sensing']
    if cached_data is None:
        return no_update
    
    
    if len(derived_selected_commits_list) < 2:
        line_padding = 5
        selected_hash = ""
        if derived_virtual_selected_rows is None or len(derived_virtual_selected_rows) == 0 or len(rows) < derived_virtual_selected_rows[0]:
            return blank_markdown
    
        mk_string = "\n\n Commit: " + rows[derived_virtual_selected_rows[0]]['hash'][:7] + "\n\n"
        for i in range(len(derived_virtual_selected_rows)):
            mk_data = rows[derived_virtual_selected_rows[i]]
            selected_hash = mk_data['hash']
            # selected_hash = selected_commits_row[derived_selected_commits_list[0]]["long_hash"]
            if selected_hash in cached_data and mk_data['ev'] in cached_data[selected_hash] and mk_data['var_name'] in cached_data[selected_hash][mk_data['ev']] and mk_data['file_name'] in cached_data[selected_hash][mk_data['ev']][mk_data['var_name']]:
                f_lines = cached_data[selected_hash][mk_data['ev']][mk_data['var_name']][mk_data['file_name']]
            else:
                return dcc.Markdown("##### Selected variable in file does not exist in the selected commit.", id='actual-source-block')
        
            file_url = perf_runner.current_git_directory + '/' + mk_data['file_name']
            
            is_first = True
            with open(file_url) as fp:
                line_index = 1
                for line in fp:
                    for lp in range(line_padding):
                        if line_index-lp in f_lines:
                            if lp == 0:
                                if is_first:
                                    is_first = False
                                else:
                                    mk_string += "```"
                                mk_string += "\n\n[" + mk_data['file_name'] + " Line:" + str(line_index) + "](https://github.com/" + perf_runner.git_user_repo + "/blob/" + selected_hash + "/" + mk_data['file_name'] + "#L" + str(line_index) + ")\n\n``` "
                                mk_string +=  os.path.splitext(mk_data['file_name'])[1] + "\n\n"
                            mk_string += line + '\n'
                    line_index = line_index + 1
            if is_first is False:
                mk_string += "```"
        
        mk_component = dcc.Markdown(mk_string, id='actual-source-block')

        return mk_component
    elif len(derived_selected_commits_list) > 2:
        return dcc.Markdown("##### Please select upto two commits", id='actual-source-block')
    
    if derived_virtual_selected_rows is not None and len(derived_virtual_selected_rows) > 0 and len(rows) >= derived_virtual_selected_rows[0]:
        mk_data = list()
        for i in range(len(derived_virtual_selected_rows)):
            mk_data.append(rows[derived_virtual_selected_rows[i]]['file_name'])

    git_repo = getGitRepo(perf_runner.git_user_repo)
    first_commit_hash = selected_commits_row[derived_selected_commits_list[0]]["long_hash"]
    second_commit_hash = selected_commits_row[derived_selected_commits_list[1]]["long_hash"]
    comp_result = git_repo.compare(first_commit_hash, second_commit_hash)
    mk_string = ""
    if len(comp_result.files) == 0:
        comp_result = git_repo.compare(second_commit_hash, first_commit_hash)
        mk_string += "\n\n Base: " + second_commit_hash[:7] + "\n\n Head: " + first_commit_hash[:7] + "\n"
    else:
        mk_string += "\n\n Base: " + first_commit_hash[:7] + "\n\n Head: " + second_commit_hash[:7] + "\n"
    
    for each_files in comp_result.files:
        if each_files.patch is not None:
            if mk_data is None or each_files.filename in mk_data:
                mk_string += "\n\n[" + each_files.filename + "](https://github.com/" + perf_runner.git_user_repo + "/compare/" + first_commit_hash + ".." + second_commit_hash + ")\n\n"
                mk_string += "```diff\n\n"
                mk_string += each_files.patch
                mk_string += "\n```\n"

    mk_component = dcc.Markdown(mk_string, id='actual-source-block')

    return mk_component

@callback(
    Output('search-var-info', 'children'),
    Input('submit-var-search', 'n_clicks'),
    State('custom-var-search', 'value'),
    prevent_initial_call=True
)
def update_search_var_output_text(n_clicks, search_var):
    global perf_runner
    results = parse_clover_output.get_all_db_data(perf_runner.testname, perf_runner.current_working_directory)
    if results is not None and len(results) > 0:
        pass
    return 'Searched vars: {}'.format(
        search_var
    )


@callback(
    Output('container-button-basic', 'children'),
    Output('submit-var-search','disabled'),
    Input('submit-val', 'n_clicks'),
    State('input-on-submit', 'value'),
    prevent_initial_call=True
)
def update_branch_output_text(n_clicks, value):
    repo_name = value
    global perf_runner
    perf_runner.initGitRepo(repo_name)
    return ['Fetched branches for {}'.format(
        value
    ), False]

@callback(
    Output('branch-multi-option', 'options'),
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

def get_browsable_perf_metric(df):
    if df is None:
        return list()
    column_list = list()
    for column in df:
        if column == 'testname' or column.startswith("git_") or 'min' in column or 'max' in column or 'x_cells' in column or 'y_cells' in column:
            continue
        column_list.append(column)
    return column_list

def find_interesting_perf_metric(df):
    if df is None:
        return list()
    for column in df:
        if column == 'testname' or column.startswith("git_") or 'min' in column or 'max' in column or 'x_cells' in column or 'y_cells' in column:
            continue
        df[column] = pd.to_numeric(df[column].fillna(value=np.nan), errors='coerce')
    another = df.select_dtypes(include='number').std() > 0.5
    return list(another[another].index)

@callback(
    Output('git-graph', 'figure'),
    Output('perf-metric-option', 'options'),
    Output('perf-metric-option', 'value'),
    Input('input-on-submit', 'value'),
    Input('branch-multi-option', 'value'),
    Input('git-graph', 'selectedData'),
    Input('run-perf-commit', 'n_clicks'),
    State('custom-var-search', 'value'),
    Input('filter-file-multi-options', 'value'),
    State('var-search-table', "derived_virtual_data"),
    State('var-search-table', "derived_virtual_selected_rows"),
    Input('perf-metric-option', 'value'),
    prevent_initial_call=True
)
def update_branch_selection_output(repo_name, value, selectedData, n_clicks, search_var, filtered_files, rows, derived_virtual_selected_rows, perf_list):
    changed_id = [p['prop_id'] for p in callback_context.triggered][0]
    global perf_runner
    if 'branch-multi-option' in changed_id or 'perf-metric-option' in changed_id:
        selected_branches = value
        git_nodes = getGitGraph(repo_name, selected_branches)

        results = parse_clover_output.get_all_db_data(perf_runner.testname, perf_runner.current_working_directory)
        sorted_df = None
        if results is not None and len(results) > 0:
            sorted_df = results.sort_values(by=['git_committed_date'], ascending=True)
        pfl = perf_list
        if perf_list is None or len(perf_list) == 0:
            pfl = find_interesting_perf_metric(sorted_df)
        return [generateGitChart(sorted_df, git_nodes,perf_filter=pfl), get_browsable_perf_metric(sorted_df), pfl]

    elif 'run-perf-commit' in changed_id:
        if selectedData is None:
            [no_update, no_update]
        dss = pd.DataFrame(selectedData['points'])
        
        msg_list = [x[0] for x in dss['customdata']]
        hash_list = [x[1] for x in dss['customdata']]

        if 'customdata' not in dss:
            [no_update, no_update]
        
        c_df = pd.DataFrame({
            'date(time)':dss['x'].to_list(),
            'hash':hash_list,
            'message': msg_list
        })
        
        table_df = c_df.dropna().drop_duplicates(subset=['hash']).sort_values(by=['date(time)'], ascending=True)
        global local_cached_data
        if 'code_sensing' not in local_cached_data:
            local_cached_data['code_sensing'] = dict()
        cached_data = local_cached_data['code_sensing']

        for ind in table_df.index:
            candidate_commit_hash = table_df['hash'][ind]
            
            if parse_clover_output.test_artifact_query(perf_runner.testname, perf_runner.current_working_directory, candidate_commit_hash):
                try:
                    result = subprocess.run('cd ' + perf_runner.current_git_directory + ' && git checkout -f ' + candidate_commit_hash, shell=True)
                except subprocess.CalledProcessError as cpe:
                    result = cpe.output
                finally:
                    print("checkout done")
            # perf_runner.git_repo.git.checkout(force=True,hash=candidate_commit_hash)
            else:
                my_env = os.environ.copy()
                my_env['CANDIDATE_COMMIT_HASH'] = candidate_commit_hash
                my_env['SOURCE_BASE_DIRECTORY'] = perf_runner.current_git_directory
                my_env['CANDIDATE_COMMIT_HASH'] = candidate_commit_hash
                # my_env["PATH"] = f"/Users/ssakin/Softwares/anaconda3/envs/cdsi/bin:{my_env['PATH']}"
                try:
                    command = ['source runner_script.sh']
                    result = subprocess.run(command, env=my_env, shell=True)
                except subprocess.CalledProcessError as cpe:
                    result = cpe.output
                finally:
                    print("final done")
                    
                    data = parse_clover_output.parse_clover_output_file(perf_runner.testname, perf_runner.current_git_directory)
                    tau_data = parse_clover_output.parse_tau_output_file(perf_runner.testname, perf_runner.current_git_directory)
                    data.update(tau_data)
                    parse_clover_output.add_output_to_dsi(data, perf_runner.testname, perf_runner.current_working_directory)

            cached_data[candidate_commit_hash] = cached_data.get(candidate_commit_hash, dict())
            for each_var in search_var.split(","):
                ev = each_var.rstrip()
                if ev not in cached_data[candidate_commit_hash]:
                    var_dict = code_sensing.recursive_customized_match([ev], filtered_files, perf_runner.current_git_directory)
                    cached_data[candidate_commit_hash][ev] = cached_data[candidate_commit_hash].get(ev, dict())
                    cached_data[candidate_commit_hash][ev].update(var_dict)


        print('got chart updates')
        local_cached_data['code_sensing'] = cached_data
        results = parse_clover_output.get_all_db_data(perf_runner.testname, perf_runner.current_working_directory)
        if results is not None and len(results) > 0 and value is not None:
            sorted_df = results.sort_values(by=['git_committed_date'], ascending=True)
            selected_branches = value
            git_nodes = getGitGraph(repo_name, selected_branches)
            mk_data = None
            if derived_virtual_selected_rows is not None and len(derived_virtual_selected_rows) > 0 and len(rows) >= derived_virtual_selected_rows[0]:
                mk_data = rows[derived_virtual_selected_rows[0]]
            pfl = perf_list
            if perf_list is None or len(perf_list) == 0:
                pfl = find_interesting_perf_metric(sorted_df)
            return [generateGitChart(sorted_df, git_nodes, mk_data, pfl), get_browsable_perf_metric(sorted_df), pfl]
        return [no_update, no_update, no_update]
    return [no_update, no_update, no_update]


@callback(
    Output('run-perf-commit','disabled'),
    Output('selected-second-commits-table', 'data'),
    Input('git-graph', 'selectedData')
)
def update_git_selection(selection):
    if selection is None:
        return [True, list()]
        
    dss = pd.DataFrame(selection['points'])

    if dss is None or 'customdata' not in dss:
        return [True, no_update]
    
    msg_list = [x[0] for x in dss['customdata']]
    hash_list = [x[1][:7] for x in dss['customdata']]
    long_hash_list = [x[1] for x in dss['customdata']]

    c_df = pd.DataFrame({
        'date(time)':dss['x'].to_list(),
        'hash':hash_list,
        'long_hash':long_hash_list,
        'message': msg_list
    })
    table_df = c_df.dropna().drop_duplicates(subset=['hash']).sort_values(by=['date(time)'], ascending=True)
    c_df_list = list()
    for ea_val in table_df.values.tolist():
        c_dict = {
            'date(time)':ea_val[0],
            'hash':ea_val[1],
            'long_hash':ea_val[2],
            'message': ea_val[3]
        }
        c_df_list.append(c_dict)
    return [False, c_df_list]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # parser.add_argument('--testname', help='the test name')
    # parser.add_argument('--gitdir', help='the git directory')
    # args = parser.parse_args()
    # if args.gitdir is None:
    #     parser.print_help()
    #     sys.exit(0)
    # git_repo = git.Repo(args.gitdir)
    global perf_runner
    perf_runner = PerfRunner()
    
    initAndLoadCachedData(perf_runner.current_working_directory)
    # df = pd.read_csv("clover_random_test.csv")
    # sorted_df = df.sort_values(by=['git_committed_date'], ascending=True)
    # git_nodes = getGitGraph(sorted_df["git_repo_name"][0],[])
    main(None, None)