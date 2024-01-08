import numpy as np
import pandas as pd

class DataGenerator:
    def __init__(self, number_of_dimensions, distribution, number_of_points):
        self.number_of_dimensions = number_of_dimensions
        self.distribution = distribution
        self.number_of_points = number_of_points

    def generate_data(self):
        if self.distribution == "uniform":
            data = np.random.uniform(0.05, 1, (self.number_of_points, self.number_of_dimensions))
        elif self.distribution == "normal":
            data = np.clip(np.random.normal(0.5, 0.15, (self.number_of_points, self.number_of_dimensions)), 0.1, 1)
        elif self.distribution == "correlated":
            base = np.linspace(0.05, 1, self.number_of_points)
            data = np.array([base + np.random.normal(0, 0.05, self.number_of_points) for _ in range(self.number_of_dimensions)]).T
            data = np.clip(data, 0.05, 1)
        elif self.distribution == "anticorrelated":
            # only one dimension is anticorrelated to all others
            base = np.linspace(0.05, 1, self.number_of_points)
            data = np.array([base if i == 0 else 1 - base + np.random.normal(0, 0.05, self.number_of_points) for i in range(self.number_of_dimensions)]).T
            data = np.clip(data, 0.05, 1)
        else:
            raise ValueError("Unsupported distribution type")

        return data

    def export_data(self, filename):
        data = self.generate_data()
        df = pd.DataFrame(data)
        df.to_csv(filename, sep='\t', index=False, header=False)

# 2d data
generator = DataGenerator(2, "normal", 1000)
generator.export_data('datasets/normal_data.txt')

generator = DataGenerator(2, "correlated", 1000)
generator.export_data('datasets/correlated_data.txt')

generator = DataGenerator(2, "anticorrelated", 1000)
generator.export_data('datasets/anticorrelated_data.txt')

generator = DataGenerator(2, "uniform", 1000)
generator.export_data('datasets/uniform_data.txt')


# 3d data
generator = DataGenerator(3, "normal", 1000)
generator.export_data('datasets/3d_normal_data.txt')

generator = DataGenerator(3, "correlated", 1000)
generator.export_data('datasets/3d_correlated_data.txt')

generator = DataGenerator(3, "anticorrelated", 1000)
generator.export_data('datasets/3d_anticorrelated_data.txt')

generator = DataGenerator(3, "uniform", 1000)
generator.export_data('datasets/3d_uniform_data.txt')

# 4d data
generator = DataGenerator(4, "normal", 1000)
generator.export_data('datasets/4d_normal_data.txt')

generator = DataGenerator(4, "correlated", 1000)
generator.export_data('datasets/4d_correlated_data.txt')

generator = DataGenerator(4, "anticorrelated", 1000)
generator.export_data('datasets/4d_anticorrelated_data.txt')

generator = DataGenerator(4, "uniform", 1000)
generator.export_data('datasets/4d_uniform_data.txt')

# 4d data big data
generator = DataGenerator(4, "normal", 10000)
generator.export_data('datasets/4d_normal_data_10k.txt')

generator = DataGenerator(4, "correlated", 10000)
generator.export_data('datasets/4d_correlated_data_10k.txt')

generator = DataGenerator(4, "anticorrelated", 10000)
generator.export_data('datasets/4d_anticorrelated_data_10k.txt')

generator = DataGenerator(4, "uniform", 10000)
generator.export_data('datasets/4d_uniform_data_10k.txt')

# 4d data big data
generator = DataGenerator(4, "normal", 50000)
generator.export_data('datasets/4d_normal_data_50k.txt')

generator = DataGenerator(4, "correlated", 50000)
generator.export_data('datasets/4d_correlated_data_50k.txt')

generator = DataGenerator(4, "anticorrelated", 50000)
generator.export_data('datasets/4d_anticorrelated_data_50k.txt')

generator = DataGenerator(4, "uniform", 50000)
generator.export_data('datasets/4d_uniform_data_50k.txt')


# 4d data big data
generator = DataGenerator(4, "normal", 1000000)
generator.export_data('datasets/4d_normal_data_1m.txt')

generator = DataGenerator(4, "correlated", 1000000)
generator.export_data('datasets/4d_correlated_data_1m.txt')

generator = DataGenerator(4, "anticorrelated", 1000000)
generator.export_data('datasets/4d_anticorrelated_data_1m.txt')

generator = DataGenerator(4, "uniform", 1000000)
generator.export_data('datasets/4d_uniform_data_1m.txt')


# 4d data big data
generator = DataGenerator(4, "normal", 1000000)
generator.export_data('datasets/4d_normal_data_1m.txt')

generator = DataGenerator(4, "correlated", 1000000)
generator.export_data('datasets/4d_correlated_data_1m.txt')

generator = DataGenerator(4, "anticorrelated", 1000000)
generator.export_data('datasets/4d_anticorrelated_data_1m.txt')

generator = DataGenerator(4, "uniform", 1000000)
generator.export_data('datasets/4d_uniform_data_1m.txt')