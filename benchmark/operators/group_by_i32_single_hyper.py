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
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < -2147483647 GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < -1717986918 GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < -1288490188 GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < -858993459  GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < -429496729  GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < 0           GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < 429496729   GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < 858993459   GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < 1288490188  GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < 1717986918  GROUP BY n1000) AS T',
                f'SELECT COUNT(*) FROM (SELECT 1 FROM {table_def.table_name} WHERE n1000 < 2147483647  GROUP BY n1000) AS T',
            ]

            for q in queries:
                if connection.catalog.has_table(table_def.table_name):
                    connection.execute_command(f'DROP TABLE {table_def.table_name}')
                connection.catalog.create_table(table_def)
                num_rows = connection.execute_command(f'COPY {table_def.table_name} FROM \'benchmark/operators/data/Distinct_i32.csv\' WITH DELIMITER \',\' CSV HEADER')
                begin = time.time_ns()
                with connection.execute_query(q) as result:
                    i = 0
                    for row in result:
                        i += 1
                end = time.time_ns()
                times.append(end - begin)

            for t in times:
                print(t / 1e6) # in milliseconds
