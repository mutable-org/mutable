import pandas as pd
import numpy as np

def generate_joinable_tables(n, max_rows=1000, min_overlap=10):
    """
    Generate n pandas DataFrames with 2 columns: 'id' and 'col_i'.
    All DataFrames are joinable on 'id', and inner-joining all yields at least min_overlap rows.
    Each DataFrame has a different size, up to max_rows.
    """
    # Create a base set of ids that will be shared across all tables (for guaranteed overlap)
    shared_ids = np.random.choice(range(1, max_rows * 2), size=min_overlap, replace=False)
    tables = []
    used_ids = set(shared_ids)
    for i in range(n):
        # Each table has a random size between min_overlap and max_rows
        size = np.random.randint(min_overlap, max_rows + 1)
        # Add unique ids to fill up to the desired size
        available_ids = [x for x in range(1, max_rows * 2) if x not in used_ids]
        num_extra = size - min_overlap
        if num_extra > len(available_ids):
            raise ValueError(f"Not enough unique ids left to sample {num_extra} extra ids without replacement. "
                             f"Try increasing max_rows or decreasing n/min_overlap.")
        extra_ids = np.random.choice(
            available_ids,
            size=num_extra,
            replace=False
        )
        ids = np.concatenate([shared_ids, extra_ids])
        np.random.shuffle(ids)
        used_ids.update(extra_ids)
        df = pd.DataFrame({
            'id': ids,
            f'col_{i+1}': np.random.randint(0, 1000, size=size)
        })
        tables.append(df)
    return tables

# Example usage:
def save_tables_to_csv(tables, filenames):
    """
    Save each DataFrame in tables to a CSV file with the corresponding filename.
    """
    if len(tables) != len(filenames):
        raise ValueError("Number of tables and filenames must match.")
    for df, fname in zip(tables, filenames):
        df.to_csv(fname, index=False)

if __name__ == "__main__":
    tables = generate_joinable_tables(4)
    benchmark_path = "benchmark/small/data"
    filenames = [f"{benchmark_path}/table1.csv",
                f"{benchmark_path}/table2.csv",
                f"{benchmark_path}/table3.csv",
                f"{benchmark_path}/table4.csv"]
    save_tables_to_csv(tables, filenames)
    # Inner join all tables on 'id'
    result = tables[0]
    for t in tables[1:]:
        result = result.merge(t, on='id')
    print(result)