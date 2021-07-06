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
            lineitem = hyperconf.table_defs['Lineitem']
            orders   = hyperconf.table_defs['Orders']
            customer = hyperconf.table_defs['Customer']

            query = f'''\
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
            for _ in range(5):
                times.extend(
                    hyperconf.benchmark_execution_times(connection, [query], [
                        (lineitem, 'benchmark/tpc-h/data/lineitem.tbl', { 'FORMAT': 'csv', 'DELIMITER': "'|'" }),
                        (orders,   'benchmark/tpc-h/data/orders.tbl',   { 'FORMAT': 'csv', 'DELIMITER': "'|'" }),
                        (customer, 'benchmark/tpc-h/data/customer.tbl', { 'FORMAT': 'csv', 'DELIMITER': "'|'" }),
                    ])
                )

            median = hyperconf.median(times)
            print(f'{median:.3f}')
