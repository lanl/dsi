import subprocess
import urllib.request, json, sys
from dsi.backends.sqlite import Sqlite, DataType

"""
Create the git_test database
"""
def generateGitTestDB(path_to_db, isVerbose=False):
    store = Sqlite(path_to_db)

    data_type = DataType()
    data_type.name = "filesystem"
    data_type.properties["author"] = Sqlite.STRING
    data_type.properties["date"] = Sqlite.STRING
    data_type.properties["message"] = Sqlite.STRING
    data_type.properties["sha"] = Sqlite.STRING
    store.put_artifact_type(data_type, isVerbose)
    store.close()   



def get_cmd_output(cmd: list) -> str:
    proc = subprocess.run(cmd, capture_output=True, shell=True)
    if proc.stderr != b"":
        raise Exception(proc.stderr)
    return proc.stdout.strip().decode("utf-8")

def git_history_check(username, repo_name):
    # # Get the current git hash
    # sha = get_cmd_output(cmd='git rev-parse HEAD')
    # # Get the root of the git project
    # git_root = get_cmd_output(cmd='git rev-parse --show-toplevel')

    # # String replace git_sha_commit placeholder for Plugin and Backend implementations
    # metadata = '/'.join([git_root, 'dsi/plugins/metadata.py'])
    # filesystem = '/'.join([git_root, 'dsi/backends/filesystem.py'])

    # json_str = urllib.request.urlopen("https://api.github.com/repos/{}/{}/commits"
    #     .format(username, repo_name)).read()
    # commits = json.loads(json_str)
    # for c in commits:
    #     print(c['sha'][0:8],c['commit']['message'].split('\n')[0])

    json_str = urllib.request.urlopen("https://api.github.com/repos/{}/{}/compare/f133b9a...2fd3ca8"
        .format(username, repo_name)).read()
    commits = json.loads(json_str)
    for c in commits["commits"]:
        print(c['sha'][0:8],c['commit']['message'].split('\n')[0])

if __name__ == "__main__":
    print("hello for git history check")
    git_history_check("UK-MAC", "CloverLeaf_MPI")
    # generateGitTestDB('git_test.db', True)
