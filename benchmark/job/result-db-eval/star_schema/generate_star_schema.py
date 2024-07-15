import os
import random
import itertools
import string
import csv
import os

MAX_INT: int = 100
STRING_LENGTH: int = 12
NUMBER_OF_DIMENSION_TABLES: int = 7
FOREIGN_KEYS_PER_RELATION: int = 10
SEED: int = 42
BASE_PATH: str = f"{os.getcwd()}/benchmark/job/result-db-eval/star_schema/data"
DATABASE_NAME: str = f"star_schema_{NUMBER_OF_DIMENSION_TABLES}"


def write_to_csv(output_file: str, data: list[tuple]) -> None:
    with open(f"{output_file}", "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(data)


def generate_random_int() -> int:
    return random.randint(1, MAX_INT)


def generate_random_str() -> str:
    return "".join(random.choice(string.ascii_uppercase) for _ in range(STRING_LENGTH))


def generate_fact_table(output_file: str) -> None:
    tuples: list[tuple] = []
    for tup in itertools.product(
            *[[i for i in range(FOREIGN_KEYS_PER_RELATION)] for _ in range(NUMBER_OF_DIMENSION_TABLES)],
    ):
        tuples.append(tup + (generate_random_str(),))
    write_to_csv(output_file, tuples)

def generate_dimension_table(output_file: str) -> None:
    tuples: list[tuple] = []
    for i in range(FOREIGN_KEYS_PER_RELATION):
        tuples.append((i,))
    write_to_csv(output_file, tuples)


def generate_all_relations() -> None:
    generate_dimension_table(f"{BASE_PATH}/dim.csv")
    generate_fact_table(f"{BASE_PATH}/fact_{NUMBER_OF_DIMENSION_TABLES}.csv")


def generate_sql_setup_script(output_file: str) -> None:
    script = f"DROP DATABASE IF EXISTS {DATABASE_NAME};\n"
    script += f"CREATE DATABASE {DATABASE_NAME};\n"
    script += rf"\connect {DATABASE_NAME};"
    script += "\n"
    for i in range(NUMBER_OF_DIMENSION_TABLES):
        script += f"DROP TABLE IF EXISTS dim_{i};\n"
        script += f"CREATE TABLE dim_{i} (\n"
        script += "id INTEGER NOT NULL\n);\n"
        script += f"COPY dim_{i}(id) FROM '{BASE_PATH}/dim.csv' DELIMITER ',';\n"
    script += f"DROP TABLE IF EXISTS fact;\n"
    script += f"CREATE TABLE fact (\n"
    for i in range(NUMBER_OF_DIMENSION_TABLES):
        script += f"id_{i} INTEGER NOT NULL,\n"
    script += f"word CHAR({STRING_LENGTH}) NOT NULL);\n"
    attributes = [f"id_{i}" for i in range(NUMBER_OF_DIMENSION_TABLES)]
    attributes.append("word")
    schema = ", ".join(attributes)
    script += f"COPY fact({schema}) FROM '{BASE_PATH}/fact.csv' DELIMITER ',';\n"
    with open(f"{output_file}", "w") as sql_file:
        sql_file.write(script)


if __name__ == "__main__":
    generate_all_relations()
    generate_sql_setup_script(f"{BASE_PATH}/setup_postgres_{NUMBER_OF_DIMENSION_TABLES}.sql")