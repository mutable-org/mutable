import os
import random
import itertools
import string
import csv
import os
from typing import List, Tuple

STRING_LENGTH: int = 100
NUMBER_OF_DIMENSION_TABLES: int = 2
FOREIGN_KEYS_PER_RELATION: int = 2000
NUMBER_OF_ADDITIONAL_INFO = 10
SEED: int = 42
BASE_PATH: str = f"./benchmark/result-db-eval/synthetic/data"
DATABASE_NAME: str = f"synthetic"


def write_to_csv(output_file: str, data: List[Tuple]) -> None:
    with open(f"{output_file}", "w") as csv_file:
        for tup in data:
            csv_file.write(f'{",".join([str(i) for i in tup])}\n')

def generate_random_str() -> str:
    return "".join(random.choice(string.ascii_uppercase) for _ in range(STRING_LENGTH))


def generate_fact_table(output_file: str) -> None:
    with open(f"{output_file}", "w") as csv_file:
        for tup in itertools.product(
                *[[i for i in range(FOREIGN_KEYS_PER_RELATION)] for _ in range(NUMBER_OF_DIMENSION_TABLES)],
        ):
            full_tuple: tuple = tup + (generate_random_str(),)
            csv_file.write(f'{",".join([str(i) for i in full_tuple])}\n')


def generate_dimension_table(output_file: str) -> None:
    tuples: List[Tuple] = []
    counter: int = 0
    for i in range(FOREIGN_KEYS_PER_RELATION):
        tuples.append((i, counter, generate_random_str()))
        counter = (counter + 1) % NUMBER_OF_ADDITIONAL_INFO
    write_to_csv(output_file, tuples)

def generate_additional_info_table(output_file: str) -> None:
    tuples: List[Tuple] = []
    for i in range(NUMBER_OF_ADDITIONAL_INFO):
        tuples.append((i, generate_random_str()))
    write_to_csv(output_file, tuples)


def generate_all_relations() -> None:
    generate_dimension_table(f"{BASE_PATH}/dim.csv")
    generate_fact_table(f"{BASE_PATH}/fact.csv")
    generate_additional_info_table(f"{BASE_PATH}/add.csv")

if __name__ == "__main__":
    if not os.path.isdir(f"{BASE_PATH}"):
        os.mkdir(f"{BASE_PATH}")
    generate_all_relations()
