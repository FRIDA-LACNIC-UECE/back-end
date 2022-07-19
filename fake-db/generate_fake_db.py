from faker import Faker
import pandas as pd

def create_dataset(num_of_rows, number_of_columns, faker):
    dataset = []
    for row in range(num_of_rows):
        line = []
        for col in range(number_of_columns):
            name = faker.name()
            line.append(name)
        dataset.append(line)
    return pd.DataFrame(dataset)

Faker.seed(123)
fake = Faker()

dataset = create_dataset(10, 3, fake)

dataset.show()