# How to Run Experiments

This file presents all required information about generating/obtaining the data for the experiments, running them, 
and visualize their results with jupyter. 

## Installation
Our implementation was done in `mutable`, for which we offer two methods on
how to install it and its preliminaries: A native, local installation and `Docker`. 

### Local Installation
For the local installation, please follow the instruction found [here](doc/setup.md) to build `mutable`. Note that you need to
build mutable with its WebAssembly-Backend.
You also need to perform the following steps:
1. Set up a virtual python environment. Make sure to have Python3.10 installed!
```
pipenv sync --python 3.10
```
2. (Optional) Setup a local [PostgreSQL](https://www.postgresql.org/) installation. PostgreSQL will be required in order to generate
cardinality estimations later on. As cardinality estimations are already contained in the repository, this step is optional. 
### Docker
In order to use docker, please follow the following instructions. We always assume you to be in the root directory
of the repository.

1. Install [docker](https://www.docker.com/get-started/) and make sure your docker daemon is running.
2. Create two folders used for volumes by the docker containers.
```
mkdir postgres paper_benchmarks
```
2. Build/download the required images. This can take up to an hour.
```
docker compose build
```
3. Start the mutable container.
```
docker compose up mutable -d
```
4. (Optional) Start the postgres container. This is only required if you want to recreate the injected cardinality files.
```
docker compose up postgres -d
```
4. Get an interactive shell in the `mutable` container. All following commands need to be executed from within that shell.
```
docker compose exec -it mutable /bin/bash 
```
Note that the `postgres_user` in docker will always be `postgres`.

## Switch to virtual python environment
For all following commands, we assume you to be in the virtual python environment, i.e., you have executed
```
pipenv shell
```
and run commands from within that shell.
## Generate/download data and recreate cardinalities
We first need to generate/download the required data. For that you need to:
1. Download the IMDb data.
    ```
    python benchmark/get_data.py job
    ```
2. Create the synthetic dataset.
    ```
    python benchmark/result-db-eval/synthetic/generate_synthetic_joins.py
    ```
3. (Optional) Load the IMDb/synthetic data into postgres. This is only required if you want to recreate the injected cardinalities.
    ```
    ./benchmark/result-db-eval/job/setup_postgres.sh <postgres_user>
    ```
    ```
    ./benchmark/result-db-eval/synthetic/setup_postgres.sh <postgres_user>
    ```
4. (Optional) Recreate the injected cardinality files used in the experiments. This may take some time. On our machine, this took an hour. Note that this step is optional, as the cardinality files are already contained in the repository.
    ```
    python benchmark/result-db-eval/create_injected_cardinalities.py --user <postgres_user>
    ```

## Run experiments
In order to execute all experiments, perform the following steps:
1. Run the enumeration time experiments.
    ```
    ./benchmark/result-db-eval/run_evaluation_time_experiments.sh
    ```
2. Run the synthetic data experiments.
    ```
    ./benchmark/result-db-eval/run_synthetic_experiments.sh
    ```
3. Run the JOB experiments.
    ```
    python benchmark/Benchmark.py --output benchmark/result-db-eval/job/recreated_results/job.csv benchmark/result-db-eval/job/
    ```

## Visualize results
In order to visualize the results with jupyter, and generate PDFs with the results, you need to run the following commands:
1. Go into the folder with the jupyter notebook.
    ```
    cd benchmark/result-db-eval/
    ```
2. Start the jupyter server. 
   * If you are using the local installation.
       ```
       jupyter notebook
       ```
       This should open jupyter in your browser.
   * If you are using Docker.
       ```
       jupyter notebook --ip=0.0.0.0 --no-browser --allow-root
       ```
        Copy the link at the bottom containing the IP `127.0.0.1` and paste it into your browser.
3. Open `Visualization.ipynb` and configure which results you want to see. You can decide whether you want to create visualizations for the existing results, 
   or the results that were recreated in your own run of the experiments. In case you want to see the existing results, set the parameter
   `use_existing_results` to `True`, in case you want to see the recreated results, set it to `False`. The parameter can be found
    at the bottom of the first jupyter cell.
   
   
