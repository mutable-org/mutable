import argparse
import csv
import glob
import os
import subprocess
from typing import Any

METHODS: dict[str, str] = {
    "default": "0. Single Table",
    "RM1": "RM1. Dynamic SELECT DISTINCT",
    "RM2": "RM2. Materialized SELECT DISTINCT",
    "RM3": "RM3. Dynamic Subquery",
    "RM4": "RM4. Materialized Subquery"
}

def run_benchmarks(config) -> None:
    measurements: dict[str, list[Any]] = {
        'database': list(),
        'system': list(),
        'query': list(),
        'method': list(),
        'data_transfer': list(),
        'run': list(),
        'num_query_internal': list(),
        'time': list()
    }

    queries: list[str] = config['queries']
    database: str = config['database']
    directory: str = config['directory']

    if (not queries):
        print(f'No queries provided. Benchmark script will not run.')

    for q in queries:
        # execute the queries
        if not os.path.isdir(f'{directory}/{q}'):
            print(f'No {q} directory found. Skipping.')
            continue
        files = sorted(glob.glob(f'{directory}/{q}/*.sql'))
        for f in files: # iterate over all files in rewrite-methods/<query>/*.sql
            for n in range(config['num_runs']):
                basename = os.path.basename(f).rsplit('.')[0]
                query_name = basename.rsplit('_', 1)[0]
                if "no-data-transfer" in query_name: # remove superfluous "no-data-transfer" from query name
                    query_name = query_name.rsplit('_', 1)[0]
                method = basename.rsplit('_', 1)[1]
                data_transfer = not "no-data-transfer" in f

                command = f"psql -U {config['user']} -d {database} -f {f} | grep 'Time: [0-9]*.[0-9]* ms' | cut -d ' ' -f 2"
                # execute command -> returns `CompletedProcess` object
                completed_proc = subprocess.run(command, capture_output=True, text=True, shell=True)
                return_code = completed_proc.returncode
                # TODO: the exit code of a pipeline is the exit code of the last command, i.e. not the psql command
                if return_code: # something went wrong during execution
                    print(f'Failure during execution of `{command}` with return code {return_code}.')
                output = completed_proc.stdout.strip().split('\n') # split output string into individual timings
                if not output:
                    print(f'file: {f} >>> No output!')
                # num_query_internal: the query number inside the file (e.g. the materialized view has multiple queries
                # for computing the mat. view and subsequently computing the individual result sets -> this allows
                # for more flexibility when creating the graphs
                for num_query_internal, time in enumerate(output):
                    measurements['database'].append(database)
                    measurements['system'].append("postgres")
                    measurements['query'].append(query_name)
                    measurements['method'].append(METHODS[method])
                    measurements['data_transfer'].append(data_transfer)
                    measurements['run'].append(n)
                    measurements['num_query_internal'].append(num_query_internal)
                    measurements['time'].append(time)
                print(f'\tFinished run {n} of file {f}')

        # write measurements to csv file
        with open(f'{directory}/rewrite-results.csv', 'w', newline='\n') as csv_file:
            field_names = measurements.keys()
            writer = csv.DictWriter(csv_file, fieldnames=field_names)
            writer.writeheader()

            num_measurements = len(measurements['database'])
            for i in range(num_measurements):
                writer.writerow({'database': measurements['database'][i],
                                 'system': measurements['system'][i],
                                 'query': measurements['query'][i],
                                 'method': measurements['method'][i],
                                 'data_transfer': measurements['data_transfer'][i],
                                 'run': measurements['run'][i],
                                 'num_query_internal': measurements['num_query_internal'][i],
                                 'time': measurements['time'][i]})

        print(f'Finished {q}.')


########################################################################################################################
# Main
########################################################################################################################
if __name__ == '__main__':
    # Command Line Arguments
    parser = argparse.ArgumentParser(prog = 'Rewrite Benchmark',
                                     description = '''Benchmark Framework for the execution of rewritten queries in the
                                     context of SELECT RESULTDB.''')

    parser.add_argument("-u", "--user", default='joris')
    parser.add_argument("-d", "--database", default='imdb')
    parser.add_argument("-n", "--num_runs", default=5, type=int)
    parser.add_argument("-q", "--queries", default=[], nargs='+', type=str)
    parser.add_argument("--directory", default=os.path.dirname(os.path.realpath(__file__)), type=str)

    args = parser.parse_args()
    config: dict[str, Any] = vars(args)

    if "imdb" in config['queries']:
        assert len(config['queries']) == 1, "list of queries may only contain 'imdb' or actual queries"
        config['queries'] = ["q1a", "q2a", "q3b", "q4a", "q5b", "q6a", "q7b", "q8d", "q9b", "q10a", "q13a", "q17a"]
    elif "star" in config['queries']:
        assert len(config['queries']) == 1, "list of queries may only contain 'star' or actual queries"
        config['queries'] = [
                             'star_joins_1',
                             'star_joins_2',
                             'star_joins_3',
                             'star_joins_4',
                             'star_proj_2_dim',
                             'star_proj_3_dim',
                             'star_proj_4_dim',
                             'star_proj_fact_1_dim',
                             'star_proj_fact_2_dim',
                             'star_proj_fact_3_dim',
                             'star_proj_fact_4_dim',
                             'star_sel_10',
                             'star_sel_20',
                             'star_sel_30',
                             'star_sel_40',
                             'star_sel_50',
                             'star_sel_60',
                             'star_sel_70',
                             'star_sel_80',
                             'star_sel_90',
                             'star_sel_100',
                            ]

    if __debug__: print('Configuration:', config)

    # Setup
    run_benchmarks(config)
