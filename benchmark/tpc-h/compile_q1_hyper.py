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
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        AVG(l_quantity) AS avg_qty,
        AVG(l_extendedprice) AS avg_price,
        AVG(l_discount) AS avg_disc,
        COUNT(*) AS count_order
FROM
        {lineitem.table_name}
WHERE
        l_shipdate <= date '1998-09-02'
GROUP BY
        l_returnflag,
        l_linestatus
ORDER BY
        l_returnflag,
        l_linestatus'''

            times = list()
            for _ in range(3):
                times.extend(
                    hyperconf.benchmark_compilation_times(connection, [query], [
                        (lineitem, 'benchmark/tpc-h/data/lineitem.tbl', { 'FORMAT': 'csv', 'DELIMITER': "'|'" }),
                    ])
                )

            median = hyperconf.median(times)
            print(f'{median:.3f}')
