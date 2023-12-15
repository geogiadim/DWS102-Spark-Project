import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D


files = {
    'correlated_data': 'datasets/3d_correlated_data.txt',
    'uniform_data': 'datasets/3d_uniform_data.txt',
    'normal_data': 'datasets/3d_normal_data.txt',
    'anticorrelated_data': 'datasets/3d_anticorrelated_data.txt',
}


fig = plt.figure(figsize=(12, 12))

# Generating a 3D subplot for each dataset
for i, (label, file_path) in enumerate(files.items(), start=1):
    ax = fig.add_subplot(2, 2, i, projection='3d')

    # Read data from the file
    df = pd.read_csv(file_path, sep="\t", header=None)
    df.columns = ['X', 'Y', 'Z']

    # 3D Plotting
    ax.scatter(df['X'], df['Y'], df['Z'], marker='o')
    ax.set_title(label.replace('_', ' ').title())
    ax.set_xlabel('X axis')
    ax.set_ylabel('Y axis')
    ax.set_zlabel('Z axis')

plt.tight_layout()
plt.show()
