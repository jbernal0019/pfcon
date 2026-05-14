"""
Handle filesystem-based storage. This is used when pfcon is in-network and configured
to directly access the data from a ChRIS shared filesystem. It only assumes that the
output (read-write) directory in the shared storage is directly mounted into the plugin
container. Unlike the 'filesystem' storage this supports ChRIS links.
"""

import logging
import datetime
import os
import shutil
import json
import io


from .base_storage import BaseStorage


logger = logging.getLogger(__name__)


class FSLinkStorage(BaseStorage):

    def __init__(self, config):

        super().__init__(config)

        self.fs_mount_base_dir = config.get('STOREBASE_MOUNT')

    def store_data(self, job_id, job_incoming_dir, data, **kwargs):
        """
        Copy all the files from the filesystem tree under each input folder (storage
        prefix) in the specified data list into the specified job incoming directory.
        """
        self.job_id = job_id
        self.job_output_path = kwargs['job_output_path']

        all_file_paths = set()

        for storage_path in data:
            storage_path = storage_path.strip('/')
            file_paths = set()
            visited_paths = set()

            self._find_all_file_paths(storage_path, file_paths, visited_paths)

            for f_path in file_paths:
                if f_path not in all_file_paths:  # copy a given file only once
                    fs_file_path = os.path.join(self.fs_mount_base_dir, f_path)

                    rel_file_path = f_path.replace(storage_path, '', 1).lstrip('/')
                    local_file_path = os.path.join(job_incoming_dir, rel_file_path)

                    try:
                        shutil.copy(fs_file_path, local_file_path)
                    except FileNotFoundError:
                        os.makedirs(os.path.dirname(local_file_path))
                        shutil.copy(fs_file_path, local_file_path)

                    all_file_paths.add(f_path)

        nfiles = len(all_file_paths)
        logger.info(f'{nfiles} files fetched from the filesystem for job {job_id}')

        nlinks = self.process_chrislink_files(job_incoming_dir)
        nfiles -= nlinks

        return {
            'jid': job_id,
            'nfiles': nfiles,
            'timestamp': f'{datetime.datetime.now()}',
            'path': job_incoming_dir
        }

    def get_data(self, job_id, job_outgoing_dir, **kwargs):
        """
        List the output files' relative paths from the folder specified by
        the job_output_path keyword argument which in turn is relative to the filesystem
        base directory (assumed to be the storebase mount directory).
        Then create a job json object containing the job_output_path prefix and the
        list of relative file paths.

        Any .chrislink files found in the outgoing directory are removed: ChRIS links
        may only be authored by upstream CUBE, never by plugin output.
        """
        fs_rel_file_paths = []
        job_output_path = kwargs['job_output_path']
        abs_path = os.path.join(self.fs_mount_base_dir, job_output_path)

        for root, dirs, files in os.walk(abs_path):
            rel_path = os.path.relpath(root, abs_path)
            if rel_path == '.':
                rel_path = ''

            for filename in files:
                local_file_path = os.path.join(root, filename)

                if filename.endswith('.chrislink'):
                    logger.warning(f'Removing unauthorized .chrislink file '
                                   f'{local_file_path} from job {job_id} output; '
                                   f'plugins are not allowed to create ChRIS links')
                    try:
                        os.remove(local_file_path)
                    except OSError as e:
                        logger.error(f'Failed to remove unauthorized .chrislink file '
                                     f'{local_file_path} for job {job_id}, '
                                     f'detail: {str(e)}')
                    continue

                if not os.path.islink(local_file_path):
                    fs_rel_file_paths.append(os.path.join(rel_path, filename))

        data = {'job_output_path': job_output_path,
                'rel_file_paths': fs_rel_file_paths}
        return io.BytesIO(json.dumps(data).encode())

    def delete_data(self, job_dir):
        """
        Delete job data from the local storage.
        """
        shutil.rmtree(job_dir)

    def _find_all_file_paths(self, storage_path, file_paths, visited_paths):
        """
        Find all file paths under the passed storage path (prefix) by
        recursively following ChRIS links. The resulting set of file paths is given
        by the file_paths set argument.
        """
        if not any(storage_path.startswith(p) for p in visited_paths):  # avoid infinite loops
            visited_paths.add(storage_path)
            job_id = self.job_id
            job_output_path = self.job_output_path
            fs_abs_path = os.path.join(self.fs_mount_base_dir, storage_path)

            l_ls = []
            if os.path.isfile(fs_abs_path):
                l_ls.append(fs_abs_path)
            else:
                for root, dirs, files in os.walk(fs_abs_path):
                    for filename in files:
                        l_ls.append(os.path.join(root, filename))

            for abs_file_path in l_ls:
                if abs_file_path.endswith('.chrislink'):
                    try:
                        with open(abs_file_path, 'rb') as f:
                            linked_path =  f.read().decode().strip()
                    except Exception as e:
                        logger.error(f'Failed to read file {abs_file_path} for '
                                     f'job {job_id}, detail: {str(e)}')
                        raise

                    if f'{job_output_path}/'.startswith(linked_path.rstrip('/') + '/'):
                        # link files are not allowed to point to the job output dir or
                        # any of its ancestors
                        logger.error(f'Found invalid input path {linked_path} for job '
                                     f'{job_id} pointing to an ancestor of the job '
                                     f'output dir: {job_output_path}')
                        raise ValueError(f'Invalid input path: {linked_path}')

                    self._find_all_file_paths(linked_path, file_paths,
                                              visited_paths)  # recursive call
                file_paths.add(abs_file_path.replace(self.fs_mount_base_dir, '',
                                                  1).lstrip('/'))
