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

            query = f'''\
SELECT
        SUM(l_extendedprice * l_discount) AS revenue
FROM
        {lineitem.table_name}
WHERE
        l_shipdate >= date '1994-01-01'
        AND l_shipdate < date '1995-01-01'
        AND l_quantity < 24;'''

            times = list()
            for _ in range(3):
                times.extend(
                    hyperconf.benchmark_execution_times(connection, [query], [
                        (lineitem, 'benchmark/tpc-h/data/lineitem.tbl', { 'FORMAT': 'csv', 'DELIMITER': "'|'" }),
                    ])
                )

            median = hyperconf.median(times)
            print(f'{median:.3f}')
