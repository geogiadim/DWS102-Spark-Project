import pandas as pd
import matplotlib.pyplot as plt


path_2d = "datasets/2d/1k-points"
files = {
    'correlated_data': path_2d + 'correlated_data.txt',
    'uniform_data': path_2d + 'uniform_data.txt',
    'normal_data': path_2d + 'normal_data.txt',
    'anticorrelated_data': path_2d + 'anticorrelated_data.txt',
}

fig, axs = plt.subplots(2, 2, figsize=(12, 12))

for ax, (label, file_path) in zip(axs.flatten(), files.items()):
    df = pd.read_csv(file_path, sep="\t", header=None)
    df.columns = ['X', 'Y']

    ax.plot(df['X'], df['Y'], 'o', markersize=3)
    ax.set_title(label.replace('_', ' ').title())
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.grid(True)

plt.tight_layout()
plt.show()
