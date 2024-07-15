import os
import random
import itertools
import string
import csv
import os

MAX_INT: int = 100
STRING_LENGTH: int = 1
NUMBER_OF_RELATIONS: int = 20
FOREIGN_KEYS_PER_RELATION: int = 2
SEED: int = 42
BASE_PATH: str = f"{os.getcwd()}/benchmark/job/result-db-eval/large_joins/data"
DATABASE_NAME: str = "large_joins"


def write_to_csv(output_file: str, data: list[tuple]) -> None:
    with open(f"{output_file}", "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(data)


def generate_random_int() -> int:
    return random.randint(1, MAX_INT)


def generate_random_str() -> str:
    return "".join(random.choice(string.ascii_uppercase) for _ in range(STRING_LENGTH))


def generate_relation(output_file: str) -> None:
    tuples: list[tuple] = []
    for tup in itertools.product(
        range(FOREIGN_KEYS_PER_RELATION), range(FOREIGN_KEYS_PER_RELATION), range(FOREIGN_KEYS_PER_RELATION*3)
    ):
        tuples.append(tup + (generate_random_int(),))
    write_to_csv(output_file, tuples)


def generate_all_relations() -> None:
    for i in range(NUMBER_OF_RELATIONS):
        generate_relation(f"{BASE_PATH}/rel_{i}.csv")


def generate_sql_setup_script(output_file: str) -> None:
    script = f"DROP DATABASE IF EXISTS {DATABASE_NAME};\n"
    script += f"CREATE DATABASE {DATABASE_NAME};\n"
    script += rf"\connect {DATABASE_NAME};"
    script += "\n"
    for i in range(NUMBER_OF_RELATIONS):
        script += f"DROP TABLE IF EXISTS rel_{i};\n"
        script += f"CREATE TABLE rel_{i} (\n"
        script += "fk1 INTEGER NOT NULL,\n"
        script += "fk2 INTEGER NOT NULL,\n"
        script += "fk3 INTEGER NOT NULL,\n"
        script += "num INTEGER NOT NULL\n);\n"
    for i in range(NUMBER_OF_RELATIONS):
        script += f"COPY rel_{i}(fk1, fk2, fk3, num) FROM '{BASE_PATH}/rel_{i}.csv' DELIMITER ',';\n"
    with open(f"{output_file}", "w") as sql_file:
        sql_file.write(script)


if __name__ == "__main__":
    generate_all_relations()
    generate_sql_setup_script(f"{BASE_PATH}/setup_postgres.sql")
