#!env python3

import os
import urllib.request
import zlib
import tarfile

DATA_URL = 'https://bigdata.uni-saarland.de/mutable/imdb.tgz'
CRC_URL = 'https://bigdata.uni-saarland.de/mutable/imdb.crc32'

DATA_DIR = os.path.join(os.getcwd(), 'benchmark', 'job', 'data')
DATA_FILE = os.path.join(DATA_DIR, 'imdb.tgz')
CRC_FILE = os.path.join(DATA_DIR, 'imdb.crc32')

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
