#!/bin/env python3

import numpy
import time
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName

if __name__ == '__main__':
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            lineitem = TableDefinition(
                table_name='Lineitem',
                columns=[
                    TableDefinition.Column('l_orderkey',        SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('l_partkey',         SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('l_suppkey',         SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('l_linenumber',      SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('l_quantity',        SqlType.numeric(10,2), NOT_NULLABLE),
                    TableDefinition.Column('l_extendedprice',   SqlType.numeric(10,2), NOT_NULLABLE),
                    TableDefinition.Column('l_discount',        SqlType.numeric(10,2), NOT_NULLABLE),
                    TableDefinition.Column('l_tax',             SqlType.numeric(10,2), NOT_NULLABLE),
                    TableDefinition.Column('l_returnflag',      SqlType.char(1), NOT_NULLABLE),
                    TableDefinition.Column('l_linestatus',      SqlType.char(1), NOT_NULLABLE),
                    TableDefinition.Column('l_shipdate',        SqlType.date(), NOT_NULLABLE),
                    TableDefinition.Column('l_commitdate',      SqlType.date(), NOT_NULLABLE),
                    TableDefinition.Column('l_receiptdate',     SqlType.date(), NOT_NULLABLE),
                    TableDefinition.Column('l_shipinstruct',    SqlType.char(25), NOT_NULLABLE),
                    TableDefinition.Column('l_shipmode',        SqlType.char(10), NOT_NULLABLE),
                    TableDefinition.Column('l_comment',         SqlType.char(44), NOT_NULLABLE),
                ]
            )
            part = TableDefinition(
                table_name='Part',
                columns=[
                    TableDefinition.Column('p_partkey',     SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('p_name',        SqlType.char(55), NOT_NULLABLE),
                    TableDefinition.Column('p_mfgr',        SqlType.char(25), NOT_NULLABLE),
                    TableDefinition.Column('p_brand',       SqlType.char(10), NOT_NULLABLE),
                    TableDefinition.Column('p_type',        SqlType.char(25), NOT_NULLABLE),
                    TableDefinition.Column('p_size',        SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('p_container',   SqlType.char(10), NOT_NULLABLE),
                    TableDefinition.Column('p_retailprice', SqlType.numeric(10,2), NOT_NULLABLE),
                    TableDefinition.Column('p_comment',     SqlType.char(23), NOT_NULLABLE),
                ]
            )

            query = f'''
SELECT
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
        {lineitem.table_name},
        {part.table_name}
WHERE
        l_partkey = p_partkey
        AND l_shipdate >= date '1995-09-01'
        AND l_shipdate < date '1995-10-01';'''

            times = list()
            for i in range(5):
                connection.catalog.create_table(lineitem)
                connection.catalog.create_table(part)
                connection.execute_command(f'COPY {lineitem.table_name} FROM \'benchmark/tpc-h/data/lineitem.tbl\' WITH DELIMITER \'|\' CSV')
                connection.execute_command(f'COPY {part.table_name} FROM \'benchmark/tpc-h/data/part.tbl\' WITH DELIMITER \'|\' CSV')

                begin = time.time_ns()
                with connection.execute_query(query) as result:
                    for row in result:
                        pass
                end = time.time_ns()
                times.append(end - begin)

                connection.execute_command(f'DELETE FROM {lineitem.table_name}')
                connection.execute_command(f'DELETE FROM {part.table_name}')
                connection.execute_command(f'DROP TABLE {lineitem.table_name}')
                connection.execute_command(f'DROP TABLE {part.table_name}')

            for t in times:
                print(t / 1e6) # in milliseconds
