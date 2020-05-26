#!/bin/env python3

import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

if __name__ == '__main__':
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_R = TableDefinition(
                table_name='R',
                columns=[
                    TableDefinition.Column('id',  SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('fid', SqlType.int(), NOT_NULLABLE),
                ]
            )
            connection.catalog.create_table(table_R)
            connection.execute_command(f'COPY {table_R.table_name} FROM \'benchmark/operators/data/Relation.csv\' WITH DELIMITER \',\' CSV HEADER')

            table_S = TableDefinition(
                table_name='S',
                columns=[
                    TableDefinition.Column('id',  SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('fid', SqlType.int(), NOT_NULLABLE),
                ]
            )
            connection.catalog.create_table(table_S)
            connection.execute_command(f'COPY {table_S.table_name} FROM \'benchmark/operators/data/Relation.csv\' WITH DELIMITER \',\' CSV HEADER')

            times = list()
            queries = [
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <      0 AND {table_S.table_name}.id <      0',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <  10000 AND {table_S.table_name}.id <  10000',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <  20000 AND {table_S.table_name}.id <  20000',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <  30000 AND {table_S.table_name}.id <  30000',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <  40000 AND {table_S.table_name}.id <  40000',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <  50000 AND {table_S.table_name}.id <  50000',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <  60000 AND {table_S.table_name}.id <  60000',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <  70000 AND {table_S.table_name}.id <  70000',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <  80000 AND {table_S.table_name}.id <  80000',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id <  90000 AND {table_S.table_name}.id <  90000',
                f'SELECT COUNT(*) FROM {table_R.table_name}, {table_S.table_name} WHERE {table_R.table_name}.id = {table_S.table_name}.fid AND {table_R.table_name}.id < 100000 AND {table_S.table_name}.id < 100000',
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
