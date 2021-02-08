#!/bin/env python3

import numpy
import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

if __name__ == '__main__':
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
            connection.catalog.create_table(table_tmp)
            connection.execute_command(f'COPY {table_tmp.table_name} FROM \'benchmark/operators/data/Distinct_i32.csv\' WITH DELIMITER \',\' CSV HEADER')
            num_rows = connection.execute_scalar_query(f'SELECT COUNT(*) FROM {table_tmp.table_name}')

            table_def = TableDefinition(
                table_name='Distinct_i32',
                columns=columns
            )

            query = f'SELECT id FROM {table_def.table_name} ORDER BY n100000'
            scale_factors = numpy.linspace(0, 1, num=11)
            times = list()

            for sf in scale_factors:
                connection.catalog.create_table(table_def)
                connection.execute_command(f'INSERT INTO {table_def.table_name} SELECT * FROM {table_tmp.table_name} LIMIT {int(num_rows * sf)}')

                begin = time.time_ns()
                with connection.execute_query(query) as result:
                    for row in result:
                        pass
                end = time.time_ns()
                times.append(end - begin)

                connection.execute_command(f'DELETE FROM {table_def.table_name}')
                connection.execute_command(f'DROP TABLE {table_def.table_name}')

            for t in times:
                print(t / 1e6) # in milliseconds
