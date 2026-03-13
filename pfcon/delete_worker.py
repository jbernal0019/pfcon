"""
Standalone delete worker script that runs inside a container to perform
asynchronous deletion of job data from the storebase.

Usage:
    python -m pfcon.delete_worker /share/outgoing

Inside the container:
    /share/outgoing  -> storebase key directory (read-write)

The script reads delete parameters from /share/outgoing/delete_params.json,
removes the incoming/ and outgoing/ subdirectories and any leftover param
files, and exits with 0 on success or non-zero on failure.
"""

import json
import os
import sys
import shutil
import logging


logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def do_delete(key_dir):
    """Delete job data from the storebase key directory."""
    params_file = os.path.join(key_dir, 'delete_params.json')

    with open(params_file) as f:
        params = json.load(f)

    job_id = params['jid']

    logger.info(f'Starting data deletion for job {job_id}')

    for subdir in ('incoming', 'outgoing'):
        path = os.path.join(key_dir, subdir)
        if os.path.isdir(path):
            shutil.rmtree(path)
            logger.info(f'Removed {subdir}/ for job {job_id}')

    # Remove leftover param files
    for fname in ('job_params.json', 'job_params.json.consumed',
                   'upload_params.json', 'delete_params.json'):
        fpath = os.path.join(key_dir, fname)
        if os.path.isfile(fpath):
            os.remove(fpath)

    logger.info(f'Data deletion completed for job {job_id}')


def main():
    if len(sys.argv) != 2:
        print(f'Usage: {sys.argv[0]} <key_dir>', file=sys.stderr)
        sys.exit(1)

    key_dir = sys.argv[1]
    do_delete(key_dir)


if __name__ == '__main__':
    main()
