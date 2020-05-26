#!/bin/env python3

import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

if __name__ == '__main__':
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_def = TableDefinition(
                table_name='Attributes_i32',
                columns=[
                    TableDefinition.Column('a0', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a1', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a2', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a3', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a4', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a5', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a6', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a7', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a8', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a9', SqlType.int(), NOT_NULLABLE),
                ]
            )

            times = list()
            queries = [
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < -2147483647 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < -1717986918 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < -1288490188 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < -858993459 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < -429496729 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < 0 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < 429496729 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < 858993459 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < 1288490188 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < 1717986918 ORDER BY a0',
                f'SELECT a0 FROM {table_def.table_name} WHERE a0 < 2147483647 ORDER BY a0',
            ]

            for q in queries:
                if connection.catalog.has_table(table_def.table_name):
                    connection.execute_command(f'DROP TABLE {table_def.table_name}')
                connection.catalog.create_table(table_def)
                num_rows = connection.execute_command(f'COPY {table_def.table_name} FROM \'benchmark/operators/data/Attributes_i32.csv\' WITH DELIMITER \',\' CSV HEADER')
                begin = time.time_ns()
                with connection.execute_query(q) as result:
                    i = 0
                    for row in result:
                        i += 1
                end = time.time_ns()
                times.append(end - begin)

            for t in times:
                print(t / 1e6) # in milliseconds
