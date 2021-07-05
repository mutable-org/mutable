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
                table_name='Attributes_multi_i32',
                columns=[
                    TableDefinition.Column('id', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a0', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a1', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a2', SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('a3', SqlType.int(), NOT_NULLABLE),
                ]
            )

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

            times = hyperconf.benchmark_execution_times(connection, queries, [
                    (table_def, 'benchmark/operators/data/Attributes_multi_i32.csv', { 'FORMAT': 'csv', 'DELIMITER': "','", 'HEADER': 1 })
            ])

            print('\n'.join(map(lambda t: f'{t:.3f}', times)))
