
import argparse
import sys
import numpy as np
import pandas as pd
from github import Github




def getGitRepo(user_repo):
    repo = Github().get_repo(user_repo)
    return repo

def testGitIsssue():
    user_repo = "sayefsakin/flycatcher"
    repo = getGitRepo(user_repo)
    repo.create_issue(title="This is a new issue")
    issue = repo.get_issue(number=1)
    ic = issue.get_comments()
    issue.create_comment("This")
    print(issue.title)

def main():
    testGitIsssue()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    main()