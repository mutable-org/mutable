#!/bin/env python3

import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

if __name__ == '__main__':
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_def = TableDefinition(
                table_name='Attributes_f',
                columns=[
                    TableDefinition.Column('a0', SqlType.double(), NOT_NULLABLE),
                    TableDefinition.Column('a1', SqlType.double(), NOT_NULLABLE),
                    TableDefinition.Column('a2', SqlType.double(), NOT_NULLABLE),
                    TableDefinition.Column('a3', SqlType.double(), NOT_NULLABLE),
                    TableDefinition.Column('a4', SqlType.double(), NOT_NULLABLE),
                    TableDefinition.Column('a5', SqlType.double(), NOT_NULLABLE),
                    TableDefinition.Column('a6', SqlType.double(), NOT_NULLABLE),
                    TableDefinition.Column('a7', SqlType.double(), NOT_NULLABLE),
                    TableDefinition.Column('a8', SqlType.double(), NOT_NULLABLE),
                    TableDefinition.Column('a9', SqlType.double(), NOT_NULLABLE),
                ]
            )
            connection.catalog.create_table(table_def)
            num_rows = connection.execute_command(f'COPY {table_def.table_name} FROM \'benchmark/operators/data/Attributes_f.csv\' WITH DELIMITER \',\' CSV HEADER')

            times = list()
            queries = [
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.0',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.1',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.2',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.3',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.4',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.5',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.6',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.7',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.8',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 0.9',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < 1.0',
            ]

            for q in queries:
                begin = time.time_ns()
                res = connection.execute_query(q)
                res.close()
                end = time.time_ns()
                times.append(end - begin)

            for t in times:
                print(t / 1e6) # in milliseconds
