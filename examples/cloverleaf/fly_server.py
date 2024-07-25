# #!/usr/bin/env python3

import argparse
import sys
from dash import Dash, html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import random
import pandas as pd
# import git
from github import Github
import requests
import datetime


sorted_df = None


def getGitRepo(user_repo):
    repo = Github().get_repo(user_repo)
    return repo

def getGitGraph(user_repo):
    # considering perf_data is already sorted by commit time
    print("Trying to get git graphs")
    # git_graph_max_depth = 8
    # repo = getGitRepo(user_repo)
    # branches_list = list(repo.get_branches())

    # visited_commit_hash = []
    # commit_queue = []
    # nodes_list = []
    # checked_branches_list = ["master", "feature/CMake", "feature/caliper", "fix_gcc", "parse_fix", "development"]
    # for each_branch in branches_list:
    #     if each_branch.name in checked_branches_list:
    #         commit_queue.append({"commit":each_branch.commit,"branch":each_branch.name, "depth":0})
    #         # print(each_branch.commit.commit.committer.date.strftime("%b-%d,%Y(%H:%M)"))

    # while len(commit_queue) > 0:
    #     top_here = commit_queue.pop(0)
    #     c_commit = top_here["commit"]
    #     if c_commit.commit.sha in visited_commit_hash:
    #         continue
    #     if top_here["depth"] > git_graph_max_depth:
    #         break
    #     c_branch = top_here["branch"]
    #     visited_commit_hash.append(c_commit.commit.sha)
        
    #     for each_parent in c_commit.commit.parents:
    #         nodes_list.append({"sha":c_commit.commit.sha, 
    #                        "date":c_commit.commit.committer.date,
    #                        "depth":top_here["depth"],
    #                        "branch":top_here["branch"],
    #         })
    #         child_commit = repo.get_commit(sha=each_parent.sha)
    #         nodes_list.append({"sha":child_commit.commit.sha, 
    #                         "date":child_commit.commit.committer.date,
    #                         "depth":top_here["depth"]+1,
    #                         "branch":None
    #         })
    #         if child_commit.commit.sha not in visited_commit_hash:
    #             commit_queue.append({"commit":child_commit,"branch":None, "depth":top_here["depth"]+1})
    # print(nodes_list)
    # nodes_list = [{'sha': '07d783c332e3ac0fb63a743736d2707838b429c9', 'date': datetime.datetime(2022, 12, 24, 14, 26, 8, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'feature/CMake'}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '08604c63cb7d289f6ad3ebab11c9a3952215e993', 'date': datetime.datetime(2021, 8, 12, 13, 1, 7, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'feature/caliper'}, {'sha': '30dd8a6f2f13a71a576c02501a6c786bc6e2ceff', 'date': datetime.datetime(2021, 8, 9, 12, 10, 38, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '0fdb917bf10d20363dd8b88d762851908643925b', 'date': datetime.datetime(2021, 8, 9, 12, 23, 14, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'master'}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': 'de4876d5a45af5c85100af17188c2fc7d3c831bf', 'date': datetime.datetime(2022, 12, 24, 14, 2, 54, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '0fdb917bf10d20363dd8b88d762851908643925b', 'date': datetime.datetime(2021, 8, 9, 12, 23, 14, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '30dd8a6f2f13a71a576c02501a6c786bc6e2ceff', 'date': datetime.datetime(2021, 8, 9, 12, 10, 38, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '769988b91f43128506868613875e07a040f7453c', 'date': datetime.datetime(2021, 8, 9, 11, 32, 56, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '158e23d08f73d36f71e144851451955b3ae02dff', 'date': datetime.datetime(2021, 8, 2, 18, 40, 45, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': 'de4876d5a45af5c85100af17188c2fc7d3c831bf', 'date': datetime.datetime(2022, 12, 24, 14, 2, 54, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '37c34c42421f605e73f3698bbbbfe7181e82615e', 'date': datetime.datetime(2020, 5, 6, 14, 48, 4, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '769988b91f43128506868613875e07a040f7453c', 'date': datetime.datetime(2021, 8, 9, 11, 32, 56, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '89cc919b28f687a25d30b44ddf547201da930c14', 'date': datetime.datetime(2020, 7, 14, 9, 16, 46, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '158e23d08f73d36f71e144851451955b3ae02dff', 'date': datetime.datetime(2021, 8, 2, 18, 40, 45, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '37c34c42421f605e73f3698bbbbfe7181e82615e', 'date': datetime.datetime(2020, 5, 6, 14, 48, 4, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '9e4093c80cbdb8ac33497569ad0d217c1e8d386c', 'date': datetime.datetime(2020, 5, 6, 14, 44, 38, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': '89cc919b28f687a25d30b44ddf547201da930c14', 'date': datetime.datetime(2020, 7, 14, 9, 16, 46, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '07fcf4d773ba7626e6ea36c7002f7b2cd7c76b2a', 'date': datetime.datetime(2020, 7, 14, 8, 44, 25, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': '9e4093c80cbdb8ac33497569ad0d217c1e8d386c', 'date': datetime.datetime(2020, 5, 6, 14, 44, 38, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '07fcf4d773ba7626e6ea36c7002f7b2cd7c76b2a', 'date': datetime.datetime(2020, 7, 14, 8, 44, 25, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': 'e37e1d7aab99070a65094e784721b4d05fb86444', 'date': datetime.datetime(2020, 7, 14, 8, 13, 22, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '8d0cfdebc707e9372384a68635bf607772bc4ba5', 'date': datetime.datetime(2019, 3, 11, 13, 7, 50, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': 'e37e1d7aab99070a65094e784721b4d05fb86444', 'date': datetime.datetime(2020, 7, 14, 8, 13, 22, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '439c8d846ede012c89f7be451763a32dbaa5eb2c', 'date': datetime.datetime(2020, 7, 13, 20, 16, 34, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}]
    nodes_list = [{'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'development'}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'development'}, {'sha': '57875df8f3dcf4ffd79df41d621e6577dc6c2d16', 'date': datetime.datetime(2016, 5, 5, 14, 48, 27, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '07d783c332e3ac0fb63a743736d2707838b429c9', 'date': datetime.datetime(2022, 12, 24, 14, 26, 8, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'feature/CMake'}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '08604c63cb7d289f6ad3ebab11c9a3952215e993', 'date': datetime.datetime(2021, 8, 12, 13, 1, 7, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'feature/caliper'}, {'sha': '30dd8a6f2f13a71a576c02501a6c786bc6e2ceff', 'date': datetime.datetime(2021, 8, 9, 12, 10, 38, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '821792e5b424d84b6043c5dfa22ae3cc4e67fa0f', 'date': datetime.datetime(2020, 7, 3, 8, 37, 23, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'fix_gcc'}, {'sha': '34869cbce7ba601168e65ea1bbb718e3688d08fa', 'date': datetime.datetime(2020, 7, 1, 17, 58, 18, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '0fdb917bf10d20363dd8b88d762851908643925b', 'date': datetime.datetime(2021, 8, 9, 12, 23, 14, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'master'}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '8d0cfdebc707e9372384a68635bf607772bc4ba5', 'date': datetime.datetime(2019, 3, 11, 13, 7, 50, tzinfo=datetime.timezone.utc), 'depth': 0, 'branch': 'parse_fix'}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '4f23dd3beb6376f0b6d4922336943760664d5b9a', 'date': datetime.datetime(2015, 8, 5, 22, 32, 14, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '57875df8f3dcf4ffd79df41d621e6577dc6c2d16', 'date': datetime.datetime(2016, 5, 5, 14, 48, 27, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': 'a144711c6e803e71b9e8df7b515deb62ccfab2a5', 'date': datetime.datetime(2016, 2, 19, 14, 50, 2, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': 'de4876d5a45af5c85100af17188c2fc7d3c831bf', 'date': datetime.datetime(2022, 12, 24, 14, 2, 54, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '0e66b53c9c876a7f8298ef31666c9b52dbbbfd75', 'date': datetime.datetime(2022, 12, 24, 14, 14, 59, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '0fdb917bf10d20363dd8b88d762851908643925b', 'date': datetime.datetime(2021, 8, 9, 12, 23, 14, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '30dd8a6f2f13a71a576c02501a6c786bc6e2ceff', 'date': datetime.datetime(2021, 8, 9, 12, 10, 38, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '769988b91f43128506868613875e07a040f7453c', 'date': datetime.datetime(2021, 8, 9, 11, 32, 56, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '34869cbce7ba601168e65ea1bbb718e3688d08fa', 'date': datetime.datetime(2020, 7, 1, 17, 58, 18, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '379232461cec34a32bf46857151211cc22b0328a', 'date': datetime.datetime(2020, 7, 1, 17, 16, 4, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 1, 'branch': None}, {'sha': '158e23d08f73d36f71e144851451955b3ae02dff', 'date': datetime.datetime(2021, 8, 2, 18, 40, 45, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '4f23dd3beb6376f0b6d4922336943760664d5b9a', 'date': datetime.datetime(2015, 8, 5, 22, 32, 14, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '3822dab44664b151a857da568e4b0974ac807195', 'date': datetime.datetime(2015, 8, 5, 13, 45, 53, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': 'a144711c6e803e71b9e8df7b515deb62ccfab2a5', 'date': datetime.datetime(2016, 2, 19, 14, 50, 2, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': 'f274ea05a6c8adde7fafcd6ba614dd845a2852df', 'date': datetime.datetime(2016, 2, 11, 17, 42, 46, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': 'de4876d5a45af5c85100af17188c2fc7d3c831bf', 'date': datetime.datetime(2022, 12, 24, 14, 2, 54, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '37c34c42421f605e73f3698bbbbfe7181e82615e', 'date': datetime.datetime(2020, 5, 6, 14, 48, 4, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '769988b91f43128506868613875e07a040f7453c', 'date': datetime.datetime(2021, 8, 9, 11, 32, 56, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '1398cd9cfa7570db9c8eb7a17a2f3e698c90aeb4', 'date': datetime.datetime(2021, 8, 3, 8, 49, 14, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '379232461cec34a32bf46857151211cc22b0328a', 'date': datetime.datetime(2020, 7, 1, 17, 16, 4, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '168f85db7a20c558b09c8c164cf1d958af2e76fe', 'date': datetime.datetime(2020, 7, 1, 16, 47, 48, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '89cc919b28f687a25d30b44ddf547201da930c14', 'date': datetime.datetime(2020, 7, 14, 9, 16, 46, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '158e23d08f73d36f71e144851451955b3ae02dff', 'date': datetime.datetime(2021, 8, 2, 18, 40, 45, tzinfo=datetime.timezone.utc), 'depth': 2, 'branch': None}, {'sha': '2efdde26ab1eef67dfce5a29163e8d513b6dffc4', 'date': datetime.datetime(2020, 8, 20, 14, 20, 55, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '3822dab44664b151a857da568e4b0974ac807195', 'date': datetime.datetime(2015, 8, 5, 13, 45, 53, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '85c60df6dbcdeed36488e22e3d5e1822734c648f', 'date': datetime.datetime(2014, 12, 11, 8, 56, 3, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': 'f274ea05a6c8adde7fafcd6ba614dd845a2852df', 'date': datetime.datetime(2016, 2, 11, 17, 42, 46, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': '37c34c42421f605e73f3698bbbbfe7181e82615e', 'date': datetime.datetime(2020, 5, 6, 14, 48, 4, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '9e4093c80cbdb8ac33497569ad0d217c1e8d386c', 'date': datetime.datetime(2020, 5, 6, 14, 44, 38, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': '168f85db7a20c558b09c8c164cf1d958af2e76fe', 'date': datetime.datetime(2020, 7, 1, 16, 47, 48, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '612c2da46cffe26941e5a06492215bdef2c3f971', 'date': datetime.datetime(2020, 7, 1, 14, 27, 25, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': '89cc919b28f687a25d30b44ddf547201da930c14', 'date': datetime.datetime(2020, 7, 14, 9, 16, 46, tzinfo=datetime.timezone.utc), 'depth': 3, 'branch': None}, {'sha': '07fcf4d773ba7626e6ea36c7002f7b2cd7c76b2a', 'date': datetime.datetime(2020, 7, 14, 8, 44, 25, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': '85c60df6dbcdeed36488e22e3d5e1822734c648f', 'date': datetime.datetime(2014, 12, 11, 8, 56, 3, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': '2bf5369ebdbe09554609a756c8308bb9fcc6e75a', 'date': datetime.datetime(2014, 12, 11, 8, 52, 49, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '9e4093c80cbdb8ac33497569ad0d217c1e8d386c', 'date': datetime.datetime(2020, 5, 6, 14, 44, 38, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '612c2da46cffe26941e5a06492215bdef2c3f971', 'date': datetime.datetime(2020, 7, 1, 14, 27, 25, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': 'b609b672bd0c179b0fa2bdbac3efc46e16ee199a', 'date': datetime.datetime(2020, 6, 24, 13, 49, 17, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '07fcf4d773ba7626e6ea36c7002f7b2cd7c76b2a', 'date': datetime.datetime(2020, 7, 14, 8, 44, 25, tzinfo=datetime.timezone.utc), 'depth': 4, 'branch': None}, {'sha': 'e37e1d7aab99070a65094e784721b4d05fb86444', 'date': datetime.datetime(2020, 7, 14, 8, 13, 22, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '2bf5369ebdbe09554609a756c8308bb9fcc6e75a', 'date': datetime.datetime(2014, 12, 11, 8, 52, 49, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '21a9572ebc14c5a1bb945e991a1a859dc533febe', 'date': datetime.datetime(2014, 12, 8, 9, 53, 11, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': 'c97a402a197d1b7df1b230cf39a59ef505570c50', 'date': datetime.datetime(2016, 5, 23, 10, 28, 22, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': '6cf9b1a810531cf691be3b080c7ecfd73019c9b3', 'date': datetime.datetime(2020, 5, 6, 14, 29, 46, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '8d0cfdebc707e9372384a68635bf607772bc4ba5', 'date': datetime.datetime(2019, 3, 11, 13, 7, 50, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': 'b609b672bd0c179b0fa2bdbac3efc46e16ee199a', 'date': datetime.datetime(2020, 6, 24, 13, 49, 17, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '0ddf495cf21cc59f84e274617522a1383e2c328c', 'date': datetime.datetime(2015, 10, 28, 8, 39, 46, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': 'b609b672bd0c179b0fa2bdbac3efc46e16ee199a', 'date': datetime.datetime(2020, 6, 24, 13, 49, 17, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '8d0cfdebc707e9372384a68635bf607772bc4ba5', 'date': datetime.datetime(2019, 3, 11, 13, 7, 50, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': 'e37e1d7aab99070a65094e784721b4d05fb86444', 'date': datetime.datetime(2020, 7, 14, 8, 13, 22, tzinfo=datetime.timezone.utc), 'depth': 5, 'branch': None}, {'sha': '439c8d846ede012c89f7be451763a32dbaa5eb2c', 'date': datetime.datetime(2020, 7, 13, 20, 16, 34, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': '21a9572ebc14c5a1bb945e991a1a859dc533febe', 'date': datetime.datetime(2014, 12, 8, 9, 53, 11, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': '87c674df4e022f1736faef4de9019470a0a516e1', 'date': datetime.datetime(2014, 12, 8, 9, 6, 2, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': None}, {'sha': '439c8d846ede012c89f7be451763a32dbaa5eb2c', 'date': datetime.datetime(2020, 7, 13, 20, 16, 34, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': 'b5e598dc0f10ca804dce4a748e3c2314545269cd', 'date': datetime.datetime(2020, 7, 3, 8, 48, 29, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': None}, {'sha': '439c8d846ede012c89f7be451763a32dbaa5eb2c', 'date': datetime.datetime(2020, 7, 13, 20, 16, 34, tzinfo=datetime.timezone.utc), 'depth': 6, 'branch': None}, {'sha': '3f889495db94c6fba5a5ec1f9937f49e8b66f94d', 'date': datetime.datetime(2020, 7, 13, 14, 2, 3, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': None}, {'sha': '87c674df4e022f1736faef4de9019470a0a516e1', 'date': datetime.datetime(2014, 12, 8, 9, 6, 2, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': None}, {'sha': '64b6f813b5011e645be359cc67fa13f70340f16b', 'date': datetime.datetime(2014, 12, 8, 9, 5, 35, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': None}, {'sha': '87c674df4e022f1736faef4de9019470a0a516e1', 'date': datetime.datetime(2014, 12, 8, 9, 6, 2, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': None}, {'sha': '9a6c86bccdcae131a3e4ce54ee04607d73ee9f7c', 'date': datetime.datetime(2014, 12, 8, 8, 45, 18, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': None}, {'sha': 'b5e598dc0f10ca804dce4a748e3c2314545269cd', 'date': datetime.datetime(2020, 7, 3, 8, 48, 29, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': None}, {'sha': '1c4d88e0366154665361e18d8adb62646d5c3a59', 'date': datetime.datetime(2020, 7, 3, 8, 36, 42, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': None}, {'sha': 'b5e598dc0f10ca804dce4a748e3c2314545269cd', 'date': datetime.datetime(2020, 7, 3, 8, 48, 29, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': None}, {'sha': '821792e5b424d84b6043c5dfa22ae3cc4e67fa0f', 'date': datetime.datetime(2020, 7, 3, 8, 37, 23, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': None}, {'sha': '3f889495db94c6fba5a5ec1f9937f49e8b66f94d', 'date': datetime.datetime(2020, 7, 13, 14, 2, 3, tzinfo=datetime.timezone.utc), 'depth': 7, 'branch': None}, {'sha': 'b5e598dc0f10ca804dce4a748e3c2314545269cd', 'date': datetime.datetime(2020, 7, 3, 8, 48, 29, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': None}, {'sha': '64b6f813b5011e645be359cc67fa13f70340f16b', 'date': datetime.datetime(2014, 12, 8, 9, 5, 35, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': None}, {'sha': 'b3067689319bb3e30baa3263c5c75edc7ce60f1e', 'date': datetime.datetime(2014, 12, 8, 9, 5, 22, tzinfo=datetime.timezone.utc), 'depth': 9, 'branch': None}, {'sha': '9a6c86bccdcae131a3e4ce54ee04607d73ee9f7c', 'date': datetime.datetime(2014, 12, 8, 8, 45, 18, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': None}, {'sha': '4cb8e6f7dfe04d0920e08840e811001649f44e70', 'date': datetime.datetime(2014, 12, 8, 8, 35, 19, tzinfo=datetime.timezone.utc), 'depth': 9, 'branch': None}, {'sha': '1c4d88e0366154665361e18d8adb62646d5c3a59', 'date': datetime.datetime(2020, 7, 3, 8, 36, 42, tzinfo=datetime.timezone.utc), 'depth': 8, 'branch': None}, {'sha': '5b5913a91c2bd9f2d7064039e1d47f2cf9212afe', 'date': datetime.datetime(2020, 7, 3, 8, 34, 47, tzinfo=datetime.timezone.utc), 'depth': 9, 'branch': None}]
    return nodes_list

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
                            x=0,
                            y=0.7,
                            traceorder='normal'
    ))
    # if vlineDate is not None:
    perFig.add_vline(x=vlineDate, line_dash = 'dash')
    return perFig

def generateGitChart(git_nodes):
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

    def getNodeWidth(sha, depth):
        if sha not in hash_depth:
            branch_depth[depth] = branch_depth.get(depth, 0) + 1
            hash_depth[sha] = branch_depth[depth]
        return hash_depth[sha]

    def make_annotations(font_size=15, font_color='rgb(0,0,0)'):
        annotations = []
        for ind in git_nodes_df.index:
            if git_nodes_df['branch'][ind] is not None:
                annotations.append(
                    dict(
                        text=git_nodes_df['branch'][ind],
                        x=commit_dates[ind],
                        y=getNodeWidth(git_nodes_df['sha'][ind], git_nodes_df['depth'][ind]),
                        xref='x2', yref='y2',
                        font=dict(color=font_color, size=font_size),# textangle=-90,
                        showarrow=True, arrowhead=2, arrowsize=1, arrowwidth=2,
                        align="right", valign="top", xanchor="auto", yanchor="bottom"
                    )
                )
        return annotations
    cd = []
    for ind in git_nodes_df.index:
        if ind % 2 == 1:
            le = getNodeWidth(git_nodes_df['sha'][ind-1], git_nodes_df['depth'][ind-1])
            re = getNodeWidth(git_nodes_df['sha'][ind], git_nodes_df['depth'][ind])
            Xe += [commit_dates[ind-1], commit_dates[ind], None]
            Ye += [le, re, None]
            cd += [git_nodes_df['sha'][ind-1], git_nodes_df['sha'][ind], None]
    
    gitFig.add_trace(go.Scatter(x=Xe,
                                y=Ye,
                                mode='lines+markers',
                                marker=dict(symbol='circle-dot',
                                    size=12,
                                    color='#6175c1',
                                    line=dict(color='rgb(50,50,50)', width=1)
                                    ),
                                customdata=cd,
                                name="col_name"),
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
                         hovermode="x unified")
    gitFig.update_traces(xaxis='x2')
    return gitFig


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
                # html.Div(
                #     dcc.Graph(
                #         figure=generatePerfChart(),
                #         id='perf_graph'
                #     ), style={'width': 790}),
                html.Div(
                    dcc.Graph(
                        figure=generateGitChart(git_nodes),
                        id='git_graph'
                    ), style={'width': 790,'height':1500,'margin-top': 0}),
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
    
    





    app.run_server(debug=True)


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
    git_nodes = getGitGraph(sorted_df["git_repo_name"][0])
    main(git_nodes)