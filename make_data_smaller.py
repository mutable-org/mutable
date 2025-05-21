from pathlib import Path
import csv

def save_first_n_rows(csv_path, n):
    csv_path = Path(csv_path)
    output_path = csv_path.with_name(f"{csv_path.stem}_first_{n}{csv_path.suffix}")

    with csv_path.open('r', newline='', encoding='latin1') as infile, \
         output_path.open('w', newline='', encoding='latin1') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for i, row in enumerate(reader):
            if i >= n:
                break
            writer.writerow(row)
    return output_path

def delete_first_n_files(directory):
    directory = Path(directory)
    for file in directory.glob("*first_*"):
        if file.is_file():
            file.unlink()


if __name__ == "__main__":

    delete_first_n_files("benchmark/job-light/data")


    relevant_files = [
        "benchmark/job-light/data/cast_info.csv",
        "benchmark/job-light/data/title.csv"
    ]
    for file in relevant_files:
        save_first_n_rows(file, 1_000)

