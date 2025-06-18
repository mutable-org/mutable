import pandas as pd
import numpy as np
import os
import itertools
import random

def generate_tables_with_varied_overlaps(n, max_rows=500):
    """
    Generate n pandas DataFrames with different overlaps between pairs of tables.
    All tables share at least one common ID to ensure complete joins work.
    One pair of tables will have a large intersection producing a join larger than either table.
    """
    # Create a single ID that will appear in all tables
    common_id = np.random.randint(1, 1000000)

    # Track all IDs to avoid unintended overlaps
    used_ids = {common_id}

    # Create base tables with just the common ID
    tables = []
    for i in range(n):
        tables.append(pd.DataFrame({
            'id': [common_id],
            f'col_{i+1}': [np.random.randint(0, 1000)]
        }))

    # Choose one random pair for high cardinality join
    high_card_pair = tuple(sorted(random.sample(range(n), 2)))

    # Create pairwise overlaps with different sizes
    for (i, j) in itertools.combinations(range(n), 2):
        # For our selected high cardinality pair
        if (i, j) == high_card_pair:
            # Generate shared IDs that will appear multiple times
            shared_ids = []
            while len(shared_ids) < 50:  # 50 distinct shared IDs
                new_id = np.random.randint(1, 1000000)
                if new_id not in used_ids:
                    shared_ids.append(new_id)
                    used_ids.add(new_id)

            # Add each shared ID multiple times to both tables
            for id_val in shared_ids:
                # Add 3-6 occurrences in table i
                occurrences_i = np.random.randint(3, 7)
                for _ in range(occurrences_i):
                    tables[i] = pd.concat([tables[i], pd.DataFrame({
                        'id': [id_val],
                        f'col_{i+1}': [np.random.randint(0, 1000)]
                    })], ignore_index=True)

                # Add 3-6 occurrences in table j
                occurrences_j = np.random.randint(3, 7)
                for _ in range(occurrences_j):
                    tables[j] = pd.concat([tables[j], pd.DataFrame({
                        'id': [id_val],
                        f'col_{j+1}': [np.random.randint(0, 1000)]
                    })], ignore_index=True)
        else:
            # For normal pairs, create moderate overlaps
            overlap_size = np.random.randint(5, 31)

            # Generate unique IDs for this overlap
            overlap_ids = []
            while len(overlap_ids) < overlap_size:
                new_id = np.random.randint(1, 1000000)
                if new_id not in used_ids:
                    overlap_ids.append(new_id)
                    used_ids.add(new_id)

            # Add these IDs to both tables
            for id_val in overlap_ids:
                tables[i] = pd.concat([tables[i], pd.DataFrame({
                    'id': [id_val],
                    f'col_{i+1}': [np.random.randint(0, 1000)]
                })], ignore_index=True)

                tables[j] = pd.concat([tables[j], pd.DataFrame({
                    'id': [id_val],
                    f'col_{j+1}': [np.random.randint(0, 1000)]
                })], ignore_index=True)

    # Add unique rows to each table to reach a random size (limited to max_rows)
    for i in range(n):
        target_size = np.random.randint(100, max_rows + 1)
        current_size = len(tables[i])

        if current_size < target_size:
            unique_ids = []
            while len(unique_ids) < (target_size - current_size):
                new_id = np.random.randint(1, 1000000)
                if new_id not in used_ids:
                    unique_ids.append(new_id)
                    used_ids.add(new_id)

            unique_rows = pd.DataFrame({
                'id': unique_ids,
                f'col_{i+1}': np.random.randint(0, 1000, size=len(unique_ids))
            })

            tables[i] = pd.concat([tables[i], unique_rows], ignore_index=True)

        # Shuffle rows
        tables[i] = tables[i].sample(frac=1).reset_index(drop=True)

    return tables, high_card_pair

def save_tables_to_csv(tables, filenames, high_card_pair):
    """
    Save each DataFrame in tables to a CSV file with the corresponding filename.
    """
    if len(tables) != len(filenames):
        raise ValueError("Number of tables and filenames must match.")

    # Ensure directory exists
    os.makedirs(os.path.dirname(filenames[0]), exist_ok=True)

    for df, fname in zip(tables, filenames):
        df.to_csv(fname, index=False)

    # Generate summary of table sizes and join cardinalities
    with open(os.path.join(os.path.dirname(filenames[0]), "join_stats.txt"), "w") as f:
        f.write("Table sizes:\n")
        for i, df in enumerate(tables):
            f.write(f"Table {i+1}: {len(df)} rows\n")

        f.write("\nPairwise join cardinalities:\n")
        for i in range(len(tables)):
            for j in range(i+1, len(tables)):
                join_result = tables[i].merge(tables[j], on='id')
                is_high_card = (i, j) == high_card_pair
                note = " (HIGH CARDINALITY PAIR)" if is_high_card else ""
                f.write(f"Table {i+1} JOIN Table {j+1}: {len(join_result)} rows{note}\n")

        f.write("\nMulti-table join cardinalities:\n")
        # For 3-way joins
        f.write("\n3-way joins:\n")
        for combo in itertools.combinations(range(len(tables)), 3):
            # Different join orders for the same 3 tables
            orders = [
                ((combo[0], combo[1]), combo[2]),
                ((combo[0], combo[2]), combo[1]),
                ((combo[1], combo[2]), combo[0])
            ]

            for first_join, third_table in orders:
                # First join
                intermediate = tables[first_join[0]].merge(tables[first_join[1]], on='id')
                # Join with third table
                final = intermediate.merge(tables[third_table], on='id')

                f.write(f"(Table {first_join[0]+1} JOIN Table {first_join[1]+1}) JOIN Table {third_table+1}: "
                       f"[Intermediate: {len(intermediate)} rows] → Final: {len(final)} rows\n")

        # For 4-way joins (if we have 4 or more tables)
        if len(tables) >= 4:
            f.write("\n4-way joins (selected orders):\n")
            for combo in itertools.combinations(range(len(tables)), 4):
                # Sample a few join orders for 4 tables
                # ((t1 JOIN t2) JOIN t3) JOIN t4
                intermediate1 = tables[combo[0]].merge(tables[combo[1]], on='id')
                intermediate2 = intermediate1.merge(tables[combo[2]], on='id')
                final1 = intermediate2.merge(tables[combo[3]], on='id')

                # (t1 JOIN t2) JOIN (t3 JOIN t4)
                left = tables[combo[0]].merge(tables[combo[1]], on='id')
                right = tables[combo[2]].merge(tables[combo[3]], on='id')
                final2 = left.merge(right, on='id')

                f.write(f"((Table {combo[0]+1} JOIN Table {combo[1]+1}) JOIN Table {combo[2]+1}) JOIN Table {combo[3]+1}: "
                       f"[Intermediate1: {len(intermediate1)} rows, Intermediate2: {len(intermediate2)} rows] → Final: {len(final1)} rows\n")

                f.write(f"(Table {combo[0]+1} JOIN Table {combo[1]+1}) JOIN (Table {combo[2]+1} JOIN Table {combo[3]+1}): "
                       f"[Left: {len(left)} rows, Right: {len(right)} rows] → Final: {len(final2)} rows\n")

if __name__ == "__main__":
    tables, high_card_pair = generate_tables_with_varied_overlaps(4)
    benchmark_path = "benchmark/small/data"
    filenames = [f"{benchmark_path}/table{i+1}.csv" for i in range(len(tables))]
    save_tables_to_csv(tables, filenames, high_card_pair)

    # Print info about the high cardinality join
    i, j = high_card_pair
    join_result = tables[i].merge(tables[j], on='id')
    print(f"High cardinality join: Table {i+1} JOIN Table {j+1}")
    print(f"Table {i+1} size: {len(tables[i])} rows")
    print(f"Table {j+1} size: {len(tables[j])} rows")
    print(f"Join result size: {len(join_result)} rows")

    if len(join_result) > max(len(tables[i]), len(tables[j])):
        print("SUCCESS: Join result is larger than either input table")
    else:
        print("NOTE: Join result is not larger than both input tables, you may want to run the script again")

    print("\nAll join statistics saved to join_stats.txt in the data directory")