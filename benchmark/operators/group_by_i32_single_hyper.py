#!/bin/env python3

import sys
sys.path.insert(0, 'benchmark')

import numpy
import os
import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

import hyperconf

if __name__ == '__main__':
    hyperconf.init() # prepare for measurements
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            columns = [
                TableDefinition.Column('id',      SqlType.int(), NOT_NULLABLE),
                TableDefinition.Column('n1',      SqlType.int(), NOT_NULLABLE),
                TableDefinition.Column('n10',     SqlType.int(), NOT_NULLABLE),
                TableDefinition.Column('n100',    SqlType.int(), NOT_NULLABLE),
                TableDefinition.Column('n1000',   SqlType.int(), NOT_NULLABLE),
                TableDefinition.Column('n10000',  SqlType.int(), NOT_NULLABLE),
                TableDefinition.Column('n100000', SqlType.int(), NOT_NULLABLE),
            ]

            table_tmp = TableDefinition(
                table_name='tmp',
                columns=columns
            )
            hyperconf.load_table(connection, table_tmp, 'benchmark/operators/data/Distinct_i32.csv', FORMAT='csv', DELIMITER="','", HEADER=1)
            num_rows = connection.execute_scalar_query(f'SELECT COUNT(*) FROM {table_tmp.table_name}')

            table_def = TableDefinition(
                table_name='Distinct_i32',
                columns=columns
            )
            connection.catalog.create_table(table_def)

            #  query = f'SELECT COUNT(*) FROM (SELECT DISTINCT n100000 FROM {table_def.table_name}) AS T'
            query = f'SELECT COUNT(DISTINCT n100000) FROM {table_def.table_name} AS T'

            for sf in numpy.linspace(0, 1, num=11):
                connection.execute_command(f'INSERT INTO {table_def.table_name} SELECT * FROM {table_tmp.table_name} LIMIT {int(num_rows * sf)}')
                with connection.execute_query(query) as result:
                    for row in result:
                        pass
                connection.execute_command(f'DELETE FROM {table_def.table_name}')

            matches = hyperconf.filter_results(
                hyperconf.extract_results(),
                { 'k': 'query-end'},
                [ hyperconf.MATCH_SELECT ]
            )
            times = map(lambda m: m['v']['execution-time'] * 1000, matches[1:])
            print('\n'.join(map(lambda t: f'{t:.3f}', times)))
