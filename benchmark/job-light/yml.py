#!env python3

import os
import glob


QUERIES_DIR = os.path.join(os.getcwd(), 'benchmark', 'job-light', 'queries')
OUTPUT_DIR = os.path.join(os.getcwd(), 'benchmark', 'job-light')
FILENAMES = ['job-light.sql']  #, 'scale.sql', 'synthetic.sql']


def contains_number(string):
    return any(char.isdigit() for char in string)


def num_joins(query):
    predicates = query.split(' WHERE ')[1].split(' AND ')
    return sum([not contains_number(pred) for pred in predicates])


# Generate one benchmark file per benchmark and number of joins
if __name__ == '__main__':
    print(f'Query directory: {QUERIES_DIR} \nQuery filenames: {", ".join(FILENAMES)}')
    for filename in FILENAMES:
        # Benchmark name
        benchmark = filename[0:-4]

        # query counter
        join_counter = dict()

        # Find all query files for query i
        queries = list()
        with open(os.path.join(QUERIES_DIR, filename), 'r') as f:
            queries = sorted(f.read().rstrip('\n').split('\n'))

        # Generate yml files
        for q in queries:

            # update query counter
            n = num_joins(q)
            case = join_counter.get(n, 0) + 1
            join_counter[n] = case

            # Check if new benchmark file is required
            benchmark_file = f'{benchmark}_{n}.yml'
            if case == 1:
                # New file required
                with open(os.path.join(OUTPUT_DIR, benchmark_file), 'w') as f:
                    print(f'Generating {benchmark_file}')
                    # Write description
                    f.write(f'description: {benchmark} queries with {n} join{"s" if n>1 else ""}\n')
                    # Write suite
                    f.write('suite: job-light\n')
                    # Write benchmark
                    f.write(f'benchmark: {benchmark}_{n}\n')
                    # Write readonly
                    f.write('readonly: yes\n')
                    # Write pattern
                    f.write('pattern: \'^Execute query:.*\'\n')
                    # Write cases
                    f.write('cases:\n')
                    # Write case
                    f.write(f'    {case}: {q}\n')
            else:
                with open(os.path.join(OUTPUT_DIR, benchmark_file), 'a') as f:
                    # Write case
                    f.write(f'    {case}: {q}\n')
