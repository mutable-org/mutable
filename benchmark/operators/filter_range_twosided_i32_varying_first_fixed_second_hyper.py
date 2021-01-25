#!/bin/env python3

import os
import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

if __name__ == '__main__':
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_def = TableDefinition(
                table_name='Attribute_i32',
                columns=[
                    TableDefinition.Column('id',  SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('val', SqlType.int(), NOT_NULLABLE),
                ]
            )
            connection.catalog.create_table(table_def)
            num_rows = connection.execute_command(f'COPY {table_def.table_name} FROM \'benchmark/operators/data/Attribute_i32.csv\' WITH DELIMITER \',\' CSV HEADER')

            times = list()
            queries = [
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE -2104533975 < val AND val < -2061584302',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE -1932735283 < val AND val < -1889785610',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE -1717986918 < val AND val < -1675037245',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE -1288490188 < val AND val < -1245540515',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  -858993459 < val AND val <  -816043786',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  -429496729 < val AND val <  -386547056',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE           0 < val AND val <    42949672',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE   429496729 < val AND val <   472446402',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE   858993459 < val AND val <   901943132',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  1288490188 < val AND val <  1331439861',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  1717986918 < val AND val <  1760936591',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  1932735283 < val AND val <  1975684956',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  2104533975 < val AND val <  2147483647',
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
