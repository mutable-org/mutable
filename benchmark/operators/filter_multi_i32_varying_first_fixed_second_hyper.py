#!/bin/env python3

import os
import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

if __name__ == '__main__':
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_def = TableDefinition(
                table_name='Attributes_multi_i32',
                columns=[
                    TableDefinition.Column('id', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a0', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a1', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a2', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a3', SqlType.int(), NOT_NULLABLE),
                ]
            )
            connection.catalog.create_table(table_def)
            num_rows = connection.execute_command(f'COPY {table_def.table_name} FROM \'benchmark/operators/data/Attributes_multi_i32.csv\' WITH DELIMITER \',\' CSV HEADER')

            times = list()
            queries = [
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < -2104533974 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < -1932735282 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < -1717986917 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 < -1288490188 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 <  -858993458 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 <  -429496729 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 <           0 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 <   429496729 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 <   858993458 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 <  1288490188 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 <  1717986917 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 <  1932735282 AND a1 < -2104533974',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE a0 <  2104533974 AND a1 < -2104533974',
            ]

            for q in queries:
                begin = time.time_ns()
                with connection.execute_query(q) as result:
                    i = 0
                    for row in result:
                        i += 1
                end = time.time_ns()
                times.append(end - begin)

            for t in times:
                print(t / 1e6) # in milliseconds
