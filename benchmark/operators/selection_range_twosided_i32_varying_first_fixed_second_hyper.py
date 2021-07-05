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
                table_name='Attribute_i32',
                columns=[
                    TableDefinition.Column('id',  SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('val', SqlType.double(), NOT_NULLABLE),
                ]
            )

            queries = [
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE -2104533974 < val AND val < -2061584301',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE -1932735282 < val AND val < -1889785609',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE -1717986917 < val AND val < -1675037244',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE -1288490188 < val AND val < -1245540515',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  -858993458 < val AND val <  -816043785',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  -429496729 < val AND val <  -386547056',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE           0 < val AND val <    42949672',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE   429496729 < val AND val <   472446402',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE   858993458 < val AND val <   901943131',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  1288490188 < val AND val <  1331439861',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  1717986917 < val AND val <  1760936590',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  1932735282 < val AND val <  1975684955',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE  2104533974 < val AND val <  2147483647',
            ]

            times = hyperconf.benchmark_execution_times(connection, queries, [
                    (table_def, 'benchmark/operators/data/Attribute_i32.csv', { 'FORMAT': 'csv', 'DELIMITER': "','", 'HEADER': 1 })
            ])

            print('\n'.join(map(lambda t: f'{t:.3f}', times)))
