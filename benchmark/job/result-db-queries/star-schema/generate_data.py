import argparse
import itertools
import random
import string
import csv
from typing import Any

# Configuration parameters
SEED: int = 42
STRING_LENGTH: int = 16
MAX_INT: int = 100
OUTPUT_DIR: str = "./benchmark/job/result-db-queries/star-schema/"

FACT: str = "fact.csv"
DIM1: str = "dim1.csv"
DIM2: str = "dim2.csv"
DIM3: str = "dim3.csv"
DIM4: str = "dim4.csv"

# fact = {
#     'fk1': "INTEGER NOT NULL",
#     'fk2': "INTEGER NOT NULL",
#     'fk3': "INTEGER NOT NULL",
#     'fk4': "INTEGER NOT NULL",
#     'a': "INTEGER NOT NULL",
#     'b': "CHAR() NOT NULL"
# }

# dim1 = {
#     'id': "INTEGER PRIMARY KEY",
#     'a': "INTEGER NOT NULL",
#     'b': "INTEGER NOT NULL"
# }

# def write_postgres_schema(output_file: str):
#     # write fact table
#     fact_str = "CREATE TABLE Fact (\n\t" #)
#     for attr, data_type in fact.items():
#        fact_str += f"{attr} "

def write_to_csv(output_file: str, data: list[tuple]) -> None:
    with open(f'{output_file}', 'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(data)

def random_string(length: int) -> str:
    letters = string.ascii_lowercase
    res = ''.join(random.choice(letters) for _ in range(length))
    return res

def random_integer(max_value: int) -> int:
    return random.randint(0, max_value)

def generate_fact_table(file: str, dim1_size: int, dim2_size: int, dim3_size: int, dim4_size: int) -> None:
    fact_data = []
    for i, t in enumerate(itertools.product(range(dim1_size), range(dim2_size), range(dim3_size), range(dim4_size))):
        fact_data.append((i,) + t + (random_integer(MAX_INT), random_string(STRING_LENGTH)))
    write_to_csv(f'{OUTPUT_DIR}{file}', fact_data)

def generate_dimension_table(file: str, size: int) -> None:
    dim_data = []
    for i in range(size):
        dim_data.append((i, random_integer(MAX_INT), random_string(STRING_LENGTH)))
    write_to_csv(f'{OUTPUT_DIR}{file}', dim_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog = 'Generate Star Schema Data',
                                     description = '''Generate fake data for a star schema.''')

    parser.add_argument("--dimension_size", default=60, type=int)

    args = parser.parse_args()
    config: dict[str, Any] = vars(args)

    dim_size = config['dimension_size']

    # initialize seed for reproducible results
    random.seed(SEED)
    # dimension table sizes
    dim1_size: int = dim_size
    dim2_size: int = dim_size
    dim3_size: int = dim_size
    dim4_size: int = dim_size

    # maximal size if each tuple from a dimension joins with all other tuples of all other dimension tables
    # fact_size = dim1_size * dim2_size * dim3_size * dim4_size
    generate_fact_table(FACT, dim1_size, dim2_size, dim3_size, dim4_size)
    generate_dimension_table(DIM1, dim1_size)
    generate_dimension_table(DIM2, dim2_size)
    generate_dimension_table(DIM3, dim3_size)
    generate_dimension_table(DIM4, dim4_size)
