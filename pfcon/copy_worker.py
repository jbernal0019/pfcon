"""
Standalone copy worker script that runs inside a container to perform the
file fetch operation asynchronously for fslink and swift storage modes.

Usage:
    pixi run python -m pfcon.copy_worker /share/outgoing      (dev image)
    python -m pfcon.copy_worker /share/outgoing                (prod image)
    /entrypoint.sh python -m pfcon.copy_worker /share/outgoing (k8s)

Inside the container:
    /share/incoming  -> shared filesystem root (read-only, fslink only)
    /share/outgoing  -> storebase key directory (read-write)

The script reads job parameters from /share/outgoing/job_params.json,
fetches input files into /share/outgoing/incoming/ (from the shared
filesystem for fslink or from Swift object storage for swift), processes
chrislink files, and exits with 0 on success or non-zero on failure.
"""

import json
import os
import sys
import logging

from pfcon.storage.fslink_storage import FSLinkStorage
from pfcon.storage.swift_storage import SwiftStorage


logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) != 2:
        print(f'Usage: {sys.argv[0]} <key_dir>', file=sys.stderr)
        sys.exit(1)

    key_dir = sys.argv[1]  # /share/outgoing
    params_file = os.path.join(key_dir, 'job_params.json')

    with open(params_file) as f:
        params = json.load(f)

    job_id = params['jid']
    input_dirs = params['input_dirs']
    storage_env = params['storage_env']
    job_output_path = params['output_dir'].strip('/')

    incoming_dir = os.path.join(key_dir, 'incoming')
    os.makedirs(incoming_dir, exist_ok=True)

    logger.info(f'Starting file fetch ({storage_env}) for job {job_id}')

    if storage_env == 'fslink':
        # /share/incoming is the shared FS root inside the copy container
        config = {'STOREBASE_MOUNT': '/share/incoming'}
        storage = FSLinkStorage(config)
        d_info = storage.store_data(job_id, incoming_dir, input_dirs,
                                    job_output_path=job_output_path)

    elif storage_env == 'swift':
        # Swift credentials are passed as environment variables
        config = {
            'SWIFT_CONTAINER_NAME': os.environ['SWIFT_CONTAINER_NAME'],
            'SWIFT_CONNECTION_PARAMS': {
                'user': os.environ['SWIFT_USERNAME'],
                'key': os.environ['SWIFT_KEY'],
                'authurl': os.environ['SWIFT_AUTH_URL'],
            }
        }
        storage = SwiftStorage(config)
        d_info = storage.store_data(job_id, incoming_dir, input_dirs,
                                    job_output_path=job_output_path)

        # Create the outgoing dir for the main job (swift writes output here)
        outgoing_dir = os.path.join(key_dir, 'outgoing')
        os.makedirs(outgoing_dir, exist_ok=True)
    else:
        logger.error(f'Unsupported storage_env: {storage_env}')
        sys.exit(1)

    logger.info(f'File fetch completed for job {job_id}: {d_info}')


if __name__ == '__main__':
    main()
