#!/bin/env python3

import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

if __name__ == '__main__':
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_def = TableDefinition(
                table_name='Attribute_i64',
                columns=[
                    TableDefinition.Column('id',  SqlType.int(),     NOT_NULLABLE),
                    TableDefinition.Column('val', SqlType.big_int(), NOT_NULLABLE),
                ]
            )
            connection.catalog.create_table(table_def)
            num_rows = connection.execute_command(f'COPY {table_def.table_name} FROM \'benchmark/operators/data/Attribute_i64.csv\' WITH DELIMITER \',\' CSV HEADER')

            times = list()
            queries = [
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < -9223372036854775807',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < -7378697629483821056',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < -5534023222112865280',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < -3689348814741910528',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < -1844674407370954752',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val <                    0',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val <  1844674407370954752',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val <  3689348814741909504',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val <  5534023222112866304',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val <  7378697629483821056',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val <  9223372036854775807',
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
