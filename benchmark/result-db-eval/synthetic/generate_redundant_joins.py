import os
import random
import itertools
import string
import csv
import os
from typing import List, Tuple

MAX_INT: int = 100
STRING_LENGTH: int = 100
SCALE: int = 1
NUMBER_OF_DIMENSION_TABLES: int = 7
FOREIGN_KEYS_PER_RELATION: int = 2000
NUMBER_OF_ADDITIONAL_INFO = 10
ADDITIONAL_KEYS: int = 1000
SEED: int = 42
BASE_PATH: str = f"{os.getcwd()}/benchmark/job/result-db-eval/redundant/data2"
DATABASE_NAME: str = f"redundant"


def write_to_csv(output_file: str, data: List[Tuple]) -> None:
    with open(f"{output_file}", "w") as csv_file:
        for tup in data:
            csv_file.write(f'{",".join([str(i) for i in tup])}\n')


def generate_random_int() -> int:
    return random.randint(1, MAX_INT)


def generate_random_str() -> str:
    return "".join(random.choice(string.ascii_uppercase) for _ in range(STRING_LENGTH))


def generate_fact_table(output_file: str) -> None:
    tuples: List[Tuple] = []
    for tup in itertools.product(
            *[[i for i in range(FOREIGN_KEYS_PER_RELATION)] for _ in range(NUMBER_OF_DIMENSION_TABLES)],
    ):
        tuples.append(tup + (generate_random_str(),))
    write_to_csv(output_file, tuples)


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
    generate_fact_table(f"{BASE_PATH}/add.csv")

def generate_sql_setup_script(output_file: str) -> None:
    script = f"DROP DATABASE IF EXISTS {DATABASE_NAME};\n"
    script += f"CREATE DATABASE {DATABASE_NAME};\n"
    script += rf"\connect {DATABASE_NAME};"
    script += "\n"
    for i in [1, 7]:
        script += f"DROP TABLE IF EXISTS rel_{i};\n"
        script += f"CREATE TABLE rel_{i} (\n"
        script += "id INTEGER NOT NULL,\n"
        script += "fk INTEGER NOT NULL,\n"
        script += f"word CHAR({STRING_LENGTH}) NOT NULL);\n"
        script += f"COPY rel_{i}(id, fk, word) FROM '{BASE_PATH}/dim.csv' DELIMITER ',';\n"

    for i in range(2, 7):
        script += f"DROP TABLE IF EXISTS rel_{i};\n"
        script += f"CREATE TABLE rel_{i} (\n"
        script += "id INTEGER NOT NULL,\n"
        script += f"word CHAR({STRING_LENGTH}) NOT NULL);\n"
        script += f"COPY rel_{i}(id, word) FROM '{BASE_PATH}/add.csv' DELIMITER ',';\n"

    script += f"DROP TABLE IF EXISTS rel_0;\n"
    script += f"CREATE TABLE rel_0 (\n"
    for j in range(2):
        script += f"id_{j} INTEGER NOT NULL,\n"
    script += f"word CHAR({STRING_LENGTH}) NOT NULL);\n"
    attributes = [f"id_{j}" for j in range(2)]
    attributes.append("word")
    schema = ", ".join(attributes)
    script += f"COPY rel_0({schema}) FROM '{BASE_PATH}/fact.csv' DELIMITER ',';\n"
    with open(f"{output_file}", "w") as sql_file:
        sql_file.write(script)


if __name__ == "__main__":
    generate_all_relations()
    generate_sql_setup_script(f"{BASE_PATH}/setup_postgres.sql")
