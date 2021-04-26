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
                columns = [
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
            orders = TableDefinition(
                table_name='Orders',
                columns = [
                    TableDefinition.Column('o_orderkey',        SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('o_custkey',         SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('o_orderstatus',     SqlType.char(1), NOT_NULLABLE),
                    TableDefinition.Column('o_totalprice',      SqlType.numeric(10,2), NOT_NULLABLE),
                    TableDefinition.Column('o_orderdate',       SqlType.date(), NOT_NULLABLE),
                    TableDefinition.Column('o_orderpriority',   SqlType.char(15), NOT_NULLABLE),
                    TableDefinition.Column('o_clerk',           SqlType.char(15), NOT_NULLABLE),
                    TableDefinition.Column('o_shippriority',    SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('o_comment',         SqlType.char(80), NOT_NULLABLE),
                ]
            )
            customer = TableDefinition(
                table_name='Customer',
                columns = [
                    TableDefinition.Column('c_custkey',     SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('c_name',        SqlType.char(25), NOT_NULLABLE),
                    TableDefinition.Column('c_address',     SqlType.char(40), NOT_NULLABLE),
                    TableDefinition.Column('c_nationkey',   SqlType.int(), NOT_NULLABLE),
                    TableDefinition.Column('c_phone',       SqlType.char(15), NOT_NULLABLE),
                    TableDefinition.Column('c_acctbal',     SqlType.numeric(10,2), NOT_NULLABLE),
                    TableDefinition.Column('c_mktsegment',  SqlType.char(10), NOT_NULLABLE),
                    TableDefinition.Column('c_comment',     SqlType.char(117), NOT_NULLABLE),
                ]
            )

            query = f'''
SELECT
        l_orderkey,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        o_orderdate,
        o_shippriority
FROM
        {customer.table_name},
        {orders.table_name},
        {lineitem.table_name}
WHERE
        c_mktsegment = 'BUILDING'
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < date '1995-03-15'
        AND l_shipdate > date '1995-03-15'
GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
ORDER BY
        revenue DESC,
        o_orderdate
LIMIT 10;'''

            times = list()
            for i in range(5):
                connection.catalog.create_table(lineitem)
                connection.execute_command(f'COPY {lineitem.table_name} FROM \'benchmark/tpc-h/data/lineitem.tbl\' WITH DELIMITER \'|\' CSV')
                connection.catalog.create_table(orders)
                connection.execute_command(f'COPY {orders.table_name} FROM \'benchmark/tpc-h/data/orders.tbl\' WITH DELIMITER \'|\' CSV')
                connection.catalog.create_table(customer)
                connection.execute_command(f'COPY {customer.table_name} FROM \'benchmark/tpc-h/data/customer.tbl\' WITH DELIMITER \'|\' CSV')

                begin = time.time_ns()
                with connection.execute_query(query) as result:
                    for row in result:
                        pass
                end = time.time_ns()
                times.append(end - begin)

                connection.execute_command(f'DELETE FROM {lineitem.table_name}')
                connection.execute_command(f'DROP TABLE {lineitem.table_name}')
                connection.execute_command(f'DELETE FROM {orders.table_name}')
                connection.execute_command(f'DROP TABLE {orders.table_name}')
                connection.execute_command(f'DELETE FROM {customer.table_name}')
                connection.execute_command(f'DROP TABLE {customer.table_name}')

            for t in times:
                print(t / 1e6) # in milliseconds
