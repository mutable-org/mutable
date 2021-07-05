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
                table_name='Attribute_d',
                columns=[
                    TableDefinition.Column('id',  SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('val', SqlType.double(), NOT_NULLABLE),
                ]
            )

            queries = [
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.01',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.05',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.10',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.20',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.30',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.40',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.50',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.60',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.70',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.80',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.90',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.95',
                f'SELECT COUNT(*) FROM {table_def.table_name} WHERE val < 0.99',
            ]

            times = hyperconf.benchmark_execution_times(connection, queries, [
                    (table_def, 'benchmark/operators/data/Attribute_d.csv', { 'FORMAT': 'csv', 'DELIMITER': "','", 'HEADER': 1 })
            ])

            print('\n'.join(map(lambda t: f'{t:.3f}', times)))
