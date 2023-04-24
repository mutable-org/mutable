from .connector import *

import time
import os
import psycopg2
import psycopg2.extensions
from psycopg2.extras import LoggingConnection, LoggingCursor
import logging
import subprocess
import shlex

db_options = {
    'dbname': 'benchmark_tmp',
    'user': 'postgres'
}
tmp_sql_file = 'tmp.sql'


# The connector for PostgreSQL
class PostgreSQL(Connector):

    def __init__(self, args = dict()):
        pass

    # Runs an experiment one time, all parameters are in 'params'
    def execute(self, n_runs, params: dict):
        try:
            self.clean_up()
        except psycopg2.OperationalError as ex:
            raise ConnectorException(str(ex))

        # map that is returned with the measured times
        measurement_times = dict()

        # Check wether tables contain scale factors
        with_scale_factors = False
        for table in params['data'].values():
            if (table.get('scale_factors')):
                with_scale_factors = True
                break

        for _ in range(n_runs):
            try:
                # Set up database
                self.setup()
                connection = psycopg2.connect(**db_options)
                connection.autocommit = True
                cursor = connection.cursor()
                cursor.execute("set jit=off;")
                self.create_tables(cursor, params['data'], with_scale_factors)

                # If tables contain scale factors, they have to be loaded separately for every case
                if (with_scale_factors or not bool(params.get('readonly'))):
                    for case, query_stmt in params['cases'].items():
                        # Create tables from tmp tables with scale factor
                        for table_name, table in params['data'].items():
                            if table.get('scale_factors'):
                                sf = table['scale_factors'][case]
                            else:
                                sf = 1
                            header = int(table.get('header', 0))
                            num_rows = round((table['lines_in_file'] - header) * sf)
                            cursor.execute(f"DELETE FROM {table_name};")     # empty existing table
                            cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_tmp LIMIT {num_rows};")    # copy data with scale factor

                        # Write case/query to a file that will be passed to the command to execute
                        with open(tmp_sql_file, "w") as tmp:
                            tmp.write("\\timing on\n")
                            tmp.write(query_stmt + '\n')
                            tmp.write("\\timing off\n")

                        # Execute query as benchmark and get measurement time
                        command = f"psql -U {db_options['user']} -d {db_options['dbname']} -f {tmp_sql_file} | grep 'Time' | cut -d ' ' -f 2"
                        stream = os.popen(f'{command}')
                        for idx, line in enumerate(stream):
                            time = float(line.replace("\n", "").replace(",", ".")) # in milliseconds
                            if case not in measurement_times.keys():
                                measurement_times[case] = list()
                            measurement_times[case].append(time)

                        stream.close()


                # Otherwise, tables have to be created just once before the measurements (done above)
                else:
                    connection.close()

                    # Write cases/queries to a file that will be passed to the command to execute
                    with open(tmp_sql_file, "w") as tmp:
                        tmp.write("\\timing on\n")
                        for case_query in params['cases'].values():
                            tmp.write(case_query + '\n')
                        tmp.write("\\timing off\n")

                    # Execute query file and collect measurement data
                    command = f"psql -U {db_options['user']} -d {db_options['dbname']} -f {tmp_sql_file} | grep 'Time' | cut -d ' ' -f 2"
                    stream = os.popen(f'{command}')
                    for idx, line in enumerate(stream):
                        time = float(line.replace("\n", "").replace(",", ".")) # in milliseconds
                        case = list(params['cases'].keys())[idx]
                        if case not in measurement_times.keys():
                            measurement_times[case] = list()
                        measurement_times[case].append(time)
                    stream.close()

            finally:
                if(connection):
                    connection.close()
                self.clean_up()

        return {'PostgreSQL': measurement_times}


    # Sets up the database
    def setup(self):
        # Delete existing 'benchmark_tmp' database and create a new empty one
        connection = psycopg2.connect(user=db_options['user'])
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {db_options['dbname']};")
        cursor.execute(f"CREATE DATABASE {db_options['dbname']};")
        connection.close()


    # Deletes the used temporary database and file
    def clean_up(self):
        connection = psycopg2.connect(user=db_options['user'])
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {db_options['dbname']};")
        connection.close()
        if os.path.exists(tmp_sql_file):
            os.remove(tmp_sql_file)


    # Parse attributes of one table, return as string ready for a CREATE TABLE query
    def parse_attributes(self, attributes: dict):
        columns = '('
        for column_name, ty in attributes.items():
            not_null = 'NOT NULL' if 'NOT NULL' in ty else ''
            ty = ty.split(' ')
            match (ty[0]):
                case 'INT':
                    typ = 'INT'
                case 'CHAR':
                    typ = f'CHAR({ty[1]})'
                case 'DECIMAL':
                    typ = f'DECIMAL({ty[1]},{ty[2]})'
                case 'DATE':
                    typ = 'DATE'
                case 'DOUBLE':
                    typ = 'DOUBLE PRECISION'
                case 'FLOAT':
                    typ = 'REAL'
                case 'BIGINT':
                    typ = 'BIGINT'
                case _:
                    raise Exception(f"Unknown type given for '{column_name}'")
            columns += f"{column_name} {typ} {not_null}, "
        columns = columns[:-2] + ')'
        return columns


    # Creates tables in the database and copies contents of given files into them
    # Call with 'with_scale_factors'=False if data should be loaded as a whole
    # Call with 'with_scale_factors'=True if data should be placed in tmp tables
    # and copied for each case with different scale factor
    def create_tables(self, cursor, data: dict, with_scale_factors):
        for table_name, table in data.items():
            columns = self.parse_attributes(table['attributes'])

            delimiter = table.get('delimiter')
            header = table.get('header')
            format = table['format'].upper()

            if with_scale_factors:
                table_name += "_tmp"

            create = f"CREATE TABLE {table_name} {columns};"
            copy = f"COPY {table_name} FROM STDIN"
            if delimiter:
                delim = delimiter.replace("'", "")
                copy += f" WITH DELIMITER \'{delim}\'"
            if format:
                copy += f" {format}"
            if header:
                copy += ' HEADER' if (header==1) else ''

            copy += ";"

            cursor.execute(create)
            with open(f"{os.path.abspath(os.getcwd())}/{table['file']}", 'r') as datafile:
                cursor.copy_expert(sql=copy, file=datafile)

            if with_scale_factors:
                cursor.execute(f"CREATE TABLE {table_name[:-4]} {columns};")     # Create actual table that will be used for experiment
