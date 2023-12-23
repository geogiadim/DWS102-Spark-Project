import numpy as np
import pandas as pd

class DataGenerator:
    def __init__(self, number_of_dimensions, distribution, number_of_points):
        self.number_of_dimensions = number_of_dimensions
        self.distribution = distribution
        self.number_of_points = number_of_points

    def generate_data(self):
        if self.distribution == "uniform":
            data = np.random.uniform(0, 1, (self.number_of_points, self.number_of_dimensions))
        elif self.distribution == "normal":
            data = np.clip(np.random.normal(0.5, 0.15, (self.number_of_points, self.number_of_dimensions)), 0, 1)
        elif self.distribution == "correlated":
            base = np.linspace(0, 1, self.number_of_points)
            data = np.array([base + np.random.normal(0, 0.05, self.number_of_points) for _ in range(self.number_of_dimensions)]).T
            data = np.clip(data, 0, 1)
        elif self.distribution == "anticorrelated":
            base = np.linspace(0, 1, self.number_of_points)
            data = np.array([base if i % 2 == 0 else 1 - base + np.random.normal(0, 0.05, self.number_of_points) for i in range(self.number_of_dimensions)]).T
            data = np.clip(data, 0, 1)
        else:
            raise ValueError("Unsupported distribution type")

        return data

    def export_data(self, filename):
        data = self.generate_data()
        df = pd.DataFrame(data)
        df.to_csv(filename, sep='\t', index=False, header=False)


number_of_points = 1000
# 2d data
path_2d = "datasets/2d/1k-points"
generator = DataGenerator(2, "normal", number_of_points)
generator.export_data(path_2d + 'normal_data.txt')

generator = DataGenerator(2, "correlated", number_of_points)
generator.export_data(path_2d + 'correlated_data.txt')

generator = DataGenerator(2, "anticorrelated", number_of_points)
generator.export_data(path_2d + 'anticorrelated_data.txt')

generator = DataGenerator(2, "uniform", number_of_points)
generator.export_data(path_2d + 'uniform_data.txt')


# 3d data
path_3d = "datasets/3d/1k-points"
generator = DataGenerator(3, "normal", number_of_points)
generator.export_data(path_3d + '3d_normal_data.txt')

generator = DataGenerator(3, "correlated", number_of_points)
generator.export_data(path_3d + '3d_correlated_data.txt')

generator = DataGenerator(3, "anticorrelated", number_of_points)
generator.export_data(path_3d + '3d_anticorrelated_data.txt')

generator = DataGenerator(3, "uniform", number_of_points)
generator.export_data(path_3d + '3d_uniform_data.txt')
