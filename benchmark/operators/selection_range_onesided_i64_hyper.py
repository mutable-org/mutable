#!/bin/env python3

import sys
sys.path.insert(0, 'benchmark')

import os
import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

import hyperconf

if __name__ == '__main__':
    hyperconf.init() # prepare for measurements
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_def = TableDefinition(
                table_name='Attribute_i64',
                columns=[
                    TableDefinition.Column('id',  SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('val', SqlType.big_int(), NOT_NULLABLE),
                ]
            )

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

            times = hyperconf.benchmark_execution_times(connection, queries, [
                    (table_def, 'benchmark/operators/data/Attribute_i64.csv', { 'FORMAT': 'csv', 'DELIMITER': "','", 'HEADER': 1 })
            ])

            print('\n'.join(map(lambda t: f'{t:.3f}', times)))
