#!env python3

import os
import glob

QUERIES_DIR = os.path.join(os.getcwd(), 'benchmark', 'job', 'queries')
OUTPUT_DIR = os.path.join(os.getcwd(), 'benchmark', 'job')

if __name__ == '__main__':
    print(f'Query directory: {QUERIES_DIR} \nQuery filenames: [1-33][a-f].sql')
    for i in range(1, 34):
        # Find all query files for query i
        queries =  sorted(glob.glob(os.path.join(QUERIES_DIR, f'{i}[a-z].sql')))

        # Go to next query if none are found
        if len(queries) == 0:
            continue
        print(f'Generating q{i}.yml')

        # Open output yml file
        with open(os.path.join(OUTPUT_DIR, f'q{i}.yml'), 'w') as f:

            # Write description
            f.write(f'description: Join Order Benchmark Query {i}\n')
            # Write suite
            f.write('suite: job\n')
            # Write benchmark
            f.write(f'benchmark: q{i}\n')
            # Write readonly
            f.write('readonly: yes\n')
            # Write pattern
            f.write('pattern: \'^Execute query:.*\'\n')
            # Write cases
            f.write('cases:\n')
            for q in queries:
                with open(q, 'r') as qf:
                    # Write case
                    query = qf.read().rstrip('\n').replace('\n', f'\n{8*" "}')
                    f.write(f'    {q[-5]}: |\n        {query}\n')
