#!env python3

import os
import urllib.request
import zlib
import tarfile
import argparse

BASE_URL = 'https://bigdata.uni-saarland.de/mutable/'
BASE_DIR = os.path.join(os.getcwd(), 'benchmark')

benchmarks = ['job', 'job-light']

def compute_checksum(filename):
    with open(filename, 'rb') as f:
        return hex(zlib.crc32(f.read()) & 0xffffffff)

def verify_checksum():
    if not os.path.isfile(CRC_FILE):
        print('ERROR: Checksum file not downloaded.')
        exit(1)
    print('Verifying checksum')
    with open(CRC_FILE, 'rb') as f:
        expected = f.read().decode('UTF-8').strip()
        actual = compute_checksum(DATA_FILE)
        if actual != expected:
            return False
        return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download benchmark data.')
    parser.add_argument('benchmark', help=f'the benchmark, currently supported {", ".join(benchmarks)}')
    args = parser.parse_args()
    benchmark = args.benchmark

    # Check if benchmark is supported
    if benchmark not in benchmarks:
        print('ERROR: Benchmark not supported.')
        exit(1)

    # Define file locations and urls
    DATA_DIR  = os.path.join(BASE_DIR, benchmark, 'data')
    CRC_URL   = BASE_URL + benchmark + '.crc32'
    CRC_FILE  = os.path.join(DATA_DIR, f'{benchmark}.crc32')
    DATA_URL  = BASE_URL + benchmark + '.tgz'
    DATA_FILE = os.path.join(DATA_DIR, f'{benchmark}.tgz')

    # Create data directory
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)

    # Download checksum
    print('Downloading checksum file')
    urllib.request.urlretrieve(CRC_URL, CRC_FILE)

    # Download data
    if not os.path.isfile(DATA_FILE):
        print(f'Downloading data to {DATA_DIR}')
        urllib.request.urlretrieve(DATA_URL, DATA_FILE)
        if not verify_checksum():
            print('WARNING: Checksum verification failed after download.')
    elif not verify_checksum():
        print(f'Checksum verification failed. Re-downloading data to {DATA_DIR}')
        urllib.request.urlretrieve(DATA_URL, DATA_FILE)
        if not verify_checksum():
            print('WARNING: Checksum verification failed after download.')

    # Extract data
    print(f'Extracting data to {DATA_DIR}')
    tar = tarfile.open(DATA_FILE, 'r:gz')
    tar.extractall(path=DATA_DIR)
    tar.close()
