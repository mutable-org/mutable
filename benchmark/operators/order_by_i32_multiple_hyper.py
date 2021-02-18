#!/bin/env python3

import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

if __name__ == '__main__':
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_def = TableDefinition(
                table_name='Distinct_i32',
                columns=[
                    TableDefinition.Column('id',      SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('n1',      SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('n10',     SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('n100',    SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('n1000',   SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('n10000',  SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('n100000', SqlType.int(), NOT_NULLABLE),
                ]
            )

            times = list()
            queries = [
                f'SELECT id FROM {table_def.table_name} ORDER BY n100000',
                f'SELECT id FROM {table_def.table_name} ORDER BY n10000, n1000',
                f'SELECT id FROM {table_def.table_name} ORDER BY n10000, n1000, n100',
                f'SELECT id FROM {table_def.table_name} ORDER BY n10000, n1000, n100, n10',
            ]

            for q in queries:
                connection.catalog.create_table(table_def)
                connection.execute_command(f'COPY {table_def.table_name} FROM \'benchmark/operators/data/Distinct_i32.csv\' WITH DELIMITER \',\' CSV HEADER')
                begin = time.time_ns()
                with connection.execute_query(q) as result:
                    for row in result:
                        pass
                end = time.time_ns()
                times.append(end - begin)
                connection.execute_command(f'DROP TABLE {table_def.table_name}')

            for t in times:
                print(t / 1e6) # in milliseconds
