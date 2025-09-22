import git
from github import Github
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
import pandas as pd

def main():
    df = pd.read_csv("clover_random_test.csv")

    def on_move(event):
        if event.inaxes:
            print(f'data coords {event.xdata} {event.ydata},',
                f'pixel coords {event.x} {event.y}')


    def on_click(event):
        if event.button is MouseButton.LEFT:
            print('disconnecting callback')
            plt.disconnect(binding_id)



    sh_hash = [substring[:7] for substring in df["git_hash"]]
    for col in ["PdV", "Cell Advection", "MPI Halo Exchange", "Self Halo Exchange", "Momentum Advection", "Total"]:
        plt.plot(sh_hash, df[col], label=col, linestyle='dashdot')
    plt.xticks(rotation=90)
    plt.xlabel("Commit (new --> old)")
    plt.ylabel("Time (s)")
    plt.legend()

    binding_id = plt.connect('motion_notify_event', on_move)
    plt.connect('button_press_event', on_click)

    plt.show()


def extraTest():

    current_git_directory = "/tmp/fly_dsi/src"
    git_repo = git.Repo(current_git_directory)

    candidate_commit_hash = "-f `b5e598dc0f10ca804dce4a748e3c2314545269cd`"
    git_repo.git.reset("HEAD")
    git_repo.git.checkout(candidate_commit_hash)
    print("hello")

if __name__ == '__main__':
    # main()
    extraTest()