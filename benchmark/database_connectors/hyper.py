from .connector import *
from . import hyperconf

from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
        TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName
import time
import os
import sys


# Converting table names to lower case is needed because then
# the table name can be directly used in queries instead
# of using 'table_def.table_name'
def table_name_to_lower_case(name: str):
    return name.lower()


class HyPer(Connector):

    def execute(self, n_runs, params: dict):
        measurement_times = dict()

        # Check wether tables contain scale factors
        with_scale_factors = False
        for table in params['data'].values():
            if (table.get('scale_factors')):
                with_scale_factors = True
                break

        hyperconf.init() # prepare for measurements

        for run_id in range(n_runs):
            # If tables contain scale factors, they have to be loaded separately for every case
            if (with_scale_factors and bool(params.get('readonly'))):
                with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                    with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                        # Create tmp tables used for copying
                        for table_name, table in params['data'].items():
                            columns = self.parse_attributes(table['attributes'])
                            table_tmp = TableDefinition(
                                table_name=f"{table_name_to_lower_case(table_name)}_tmp",
                                columns=columns
                            )
                            hyperconf.load_table(connection, table_tmp, table['file'], FORMAT=table['format'], DELIMITER=f"\'{table['delimiter']}\'", HEADER=table['header'])

                            table_def = TableDefinition(
                                table_name=table_name_to_lower_case(table_name),
                                columns=columns
                            )
                            connection.catalog.create_table(table_def)

                        # Execute cases
                        for case, query in params['cases'].items():
                            for table_name, table in params['data'].items():
                                table_name = table_name_to_lower_case(table_name)
                                if table.get('scale_factors'):
                                    sf = table['scale_factors'][case]
                                else:
                                    sf = 1
                                header = int(table.get('header', 0))
                                num_rows = round((table['lines_in_file'] - header) * sf)
                                connection.execute_command(f'INSERT INTO {table_name} SELECT * FROM {table_name}_tmp LIMIT {num_rows};')

                            with connection.execute_query(query) as result:
                                for row in result:
                                    pass
                            for table_name, table in params['data'].items():
                                connection.execute_command(f'DELETE FROM {table_name_to_lower_case(table_name)};')

                        # extract results
                        matches = hyperconf.filter_results(
                            hyperconf.extract_results(),
                            { 'k': 'query-end'},
                            [ hyperconf.MATCH_SELECT ]
                        )
                        times = map(lambda m: m['v']['execution-time'] * 1000, matches)
                        times = list(map(lambda t: f'{t:.3f}', times))
                        times = times[run_id * len(list(params['cases'].keys())) : ]    # get only times of this run, ignore previous runs
                        times = list(zip(params['cases'].keys(), times))
                        for case, time in times:
                            if case not in measurement_times.keys():
                                measurement_times[case] = list()
                            measurement_times[case].append(time)


            else:
                # Otherwise, tables have to be created just once before the measurements
                with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                    with Connection(endpoint=hyper.endpoint, database='benchmark.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                        table_defs = self.get_all_table_defs(params)
                        queries = self.get_cases_queries(params, table_defs)
                        data = self.get_data(params, table_defs)

                        times = hyperconf.benchmark_execution_times(connection, queries.values(), data)
                        times = times[run_id * len(queries.keys()):]    # get only times of this run, ignore previous runs
                        times = list(zip(queries.keys(), list(map(lambda t: float(f'{t:.3f}'), times))))

                        for case, time in times:
                            if case not in measurement_times.keys():
                                measurement_times[case] = list()
                            measurement_times[case].append(time)


        return {'HyPer': measurement_times}



    # returns dict of {table_name: table_def} for each table_def
    def get_all_table_defs(self, params: dict):
        table_defs = dict()
        for table_name, table in params['data'].items():
            table_defs[table_name] = self.get_single_table_def(table_name, table)
            return table_defs


    def get_single_table_def(self, table_name, table: dict):
        # If table exists, return it
        table_def = hyperconf.table_defs.get(table_name)
        if (table_def):
            table_def.table_name = table_name_to_lower_case(table_def.table_name)
            return table_def

        # Otherwise create a new table_def
        columns = self.parse_attributes(table['attributes'])
        return TableDefinition(table_name=table_name_to_lower_case(table_name), columns=columns)


    # Parse attributes of one table
    def parse_attributes(self, attributes: dict):
        columns = []
        for column_name, ty in attributes.items():
            not_null = NOT_NULLABLE if 'NOT NULL' in ty else NULLABLE
            ty = ty.split(' ')
            match (ty[0]):
                case 'INT':
                    typ = SqlType.int()
                case 'CHAR':
                    typ = SqlType.char(ty[1])
                case 'DECIMAL':
                    typ = SqlType.numeric(ty[1], ty[2])
                case 'DATE':
                    typ = SqlType.date()
                case 'DOUBLE':
                    typ = SqlType.double()
                case 'FLOAT':
                    typ = SqlType.double()
                case 'BIGINT':
                    typ = SqlType.big_int()
                case _:
                    raise Exception(f"Unknown type given for '{column_name}'")
            columns.append(TableDefinition.Column(column_name, typ, NOT_NULLABLE))
        return columns


    # Returns cases/queries but with table names converted to lower case matching the table_def
    def get_cases_queries(self, params: dict, table_defs: dict):
        cases_queries = dict()
        for case, query in params['cases'].items():
            for table_name, table_def in table_defs.items():
                query.replace(table_name, f"{table_def.table_name}")
            cases_queries[case] = query

        return cases_queries


    def get_data(self, params: dict, table_defs: dict):
        result = []
        for table_name, table in params['data'].items():
            file = table['file']
            delimiter = table.get('delimiter', ',')
            if delimiter:
                delimiter = delimiter.replace("'", "")
            form = table.get('format')
            header = int(table.get('header', 0))
            par = {
                'delimiter': "'" + delimiter + "'",
                'format': form,
                'header': header
            }
            result.append((table_defs[table_name], file, par))
        return result

