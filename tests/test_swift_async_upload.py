"""
Tests for the async upload functionality with Swift storage.

These tests verify that:
- JobFile.get returns metadata immediately without uploading to Swift
- An upload container is scheduled to perform the actual upload
- Job.get includes upload_status for finished swift jobs
- Job.delete removes copy, main, and upload containers
- Idempotent JobFile.get calls do not schedule duplicate upload containers
- The full end-to-end flow (POST → poll → file → upload complete) works

All tests require a running Docker daemon and Swift service.
"""

import logging
from pathlib import Path
import shutil
import os
import io
import time
import json
from unittest import TestCase

from flask import url_for

from pfcon.app import create_app
from pfcon.compute.container_user import ContainerUser
from pfcon.compute.abstractmgr import ManagerException, JobStatus
from pfcon.resources import get_compute_mgr
from pfcon.storage.swiftmanager import SwiftManager


class SwiftAsyncUploadTests(TestCase):
    """
    Base class for async upload tests with Swift storage.
    """
    def setUp(self):
        logging.disable(logging.WARNING)

        self.app = create_app({'PFCON_INNETWORK': True,
                               'STORAGE_ENV': 'swift',
                               'SWIFT_CONTAINER_NAME': 'users',
                               'SWIFT_CONNECTION_PARAMS': {
                                   'user': 'chris:chris1234',
                                   'key': 'testing',
                                   'authurl': 'http://swift_service:8080/auth/v1.0'}
                               })
        self.client = self.app.test_client()

        with self.app.test_request_context():
            url = url_for('api.auth')
            creds = {
                'pfcon_user': self.app.config.get('PFCON_USER'),
                'pfcon_password': self.app.config.get('PFCON_PASSWORD')
            }
            response = self.client.post(url, data=json.dumps(creds),
                                        content_type='application/json')
            self.headers = {'Authorization': 'Bearer ' + response.json['token']}

            self.swift_manager = SwiftManager(
                self.app.config.get('SWIFT_CONTAINER_NAME'),
                self.app.config.get('SWIFT_CONNECTION_PARAMS'))

            self.swift_input_path = 'foo/feed/input'
            self.swift_output_path = 'foo/feed/output'

            with io.StringIO('Test file') as f:
                self.swift_manager.upload_obj(
                    self.swift_input_path + '/test.txt',
                    f.read(), content_type='text/plain')

            self.storebase_mount = self.app.config.get('STOREBASE_MOUNT')
            self.storebase = self.app.config.get('STOREBASE')
            self.container_env = self.app.config.get('CONTAINER_ENV')
            self.user = ContainerUser.parse(
                self.app.config.get('CONTAINER_USER'))

            self.job_dir = ''

    def tearDown(self):
        if os.path.isdir(self.job_dir):
            shutil.rmtree(self.job_dir)

        # delete files from swift storage
        for prefix in (self.swift_input_path, self.swift_output_path):
            l_ls = self.swift_manager.ls(prefix)
            for obj_path in l_ls:
                self.swift_manager.delete_obj(obj_path)

        logging.disable(logging.NOTSET)

    def _remove_container(self, name):
        """Remove a container by name, ignoring if it doesn't exist."""
        with self.app.test_request_context():
            compute_mgr = get_compute_mgr(self.container_env)
            try:
                job = compute_mgr.get_job(name)
                compute_mgr.remove_job(job)
            except ManagerException:
                pass

    def _get_container_status(self, name):
        """Get a container's JobStatus, or None if not found."""
        with self.app.test_request_context():
            compute_mgr = get_compute_mgr(self.container_env)
            try:
                job = compute_mgr.get_job(name)
                info = compute_mgr.get_job_info(job)
                return info.status
            except ManagerException:
                return None

    def _submit_job(self, job_id):
        """Submit a job via POST and return the response."""
        with self.app.test_request_context():
            url = url_for('api.joblist')

        data = {
            'jid': job_id,
            'entrypoint': ['python3', '/usr/local/bin/simplefsapp'],
            'args': ['--dir', '/share/incoming'],
            'auid': 'cube',
            'number_of_workers': '1',
            'cpu_limit': '1000',
            'memory_limit': '200',
            'gpu_limit': '0',
            'image': 'fnndsc/pl-simplefsapp',
            'type': 'fs',
            'input_dirs': [self.swift_input_path],
            'output_dir': self.swift_output_path
        }
        return self.client.post(url, data=data, headers=self.headers)

    def _poll_until_finished(self, job_id, timeout=90):
        """Poll Job.get until the main job finishes. Returns the last response."""
        with self.app.test_request_context():
            job_url = url_for('api.job', job_id=job_id)

        for _ in range(timeout // 3):
            time.sleep(3)
            response = self.client.get(job_url, headers=self.headers)
            status = response.json['compute']['status']
            if status in ('finishedSuccessfully', 'finishedWithError'):
                return response

        return response

    def _cleanup_job(self, job_id):
        """Delete a job via the API and also force-remove any leftover containers."""
        with self.app.test_request_context():
            job_url = url_for('api.job', job_id=job_id)
        self.client.delete(job_url, headers=self.headers)

        # Force-remove any containers that the DELETE might have missed
        for suffix in ('-copy', '', '-upload'):
            self._remove_container(job_id + suffix)


class TestJobFileAsyncUpload(SwiftAsyncUploadTests):
    """
    Test that JobFile.get returns metadata immediately and schedules
    an upload container.
    """

    def test_get_returns_metadata_without_uploading(self):
        """
        JobFile.get with job_output_path should return JSON metadata
        immediately and schedule an upload container. The files should
        NOT yet be in Swift at the time of the response.
        """
        job_id = 'chris-jid-upload-1'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        # Create local output files (simulating plugin output)
        outgoing = os.path.join(self.job_dir, 'outgoing')
        test_file_dir = os.path.join(outgoing, 'results')
        Path(test_file_dir).mkdir(parents=True, exist_ok=True)

        with open(os.path.join(test_file_dir, 'output.txt'), 'w') as f:
            f.write('output data')

        try:
            with self.app.test_request_context():
                url = url_for('api.jobfile', job_id=job_id)

            response = self.client.get(
                url,
                query_string={'job_output_path': self.swift_output_path},
                headers=self.headers)

            self.assertEqual(response.status_code, 200)

            content = json.loads(response.data.decode())
            self.assertEqual(content['job_output_path'], self.swift_output_path)
            self.assertIn('results/output.txt', content['rel_file_paths'])

            # Verify the upload container was scheduled
            status = self._get_container_status(job_id + '-upload')
            self.assertIsNotNone(status,
                                 'Upload container should have been scheduled')
        finally:
            self._remove_container(job_id + '-upload')

    def test_get_schedules_upload_container(self):
        """
        Verify that the upload container eventually completes and files
        appear in Swift.
        """
        job_id = 'chris-jid-upload-2'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        outgoing = os.path.join(self.job_dir, 'outgoing')
        Path(outgoing).mkdir(parents=True, exist_ok=True)

        with open(os.path.join(outgoing, 'result.txt'), 'w') as f:
            f.write('upload test data')

        try:
            with self.app.test_request_context():
                url = url_for('api.jobfile', job_id=job_id)

            self.client.get(
                url,
                query_string={'job_output_path': self.swift_output_path},
                headers=self.headers)

            # Wait for the upload container to finish
            for _ in range(20):
                time.sleep(3)
                status = self._get_container_status(job_id + '-upload')
                if status in (JobStatus.finishedSuccessfully,
                              JobStatus.finishedWithError):
                    break

            self.assertEqual(status, JobStatus.finishedSuccessfully,
                             'Upload container should finish successfully')

            # Verify file was uploaded to Swift
            swift_files = self.swift_manager.ls(self.swift_output_path)
            swift_file_paths = list(swift_files)
            self.assertIn(self.swift_output_path + '/result.txt',
                          swift_file_paths)
        finally:
            self._remove_container(job_id + '-upload')

    def test_get_idempotent_no_duplicate_upload(self):
        """
        Calling JobFile.get multiple times should not schedule duplicate
        upload containers.
        """
        job_id = 'chris-jid-upload-3'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        outgoing = os.path.join(self.job_dir, 'outgoing')
        Path(outgoing).mkdir(parents=True, exist_ok=True)

        with open(os.path.join(outgoing, 'file.txt'), 'w') as f:
            f.write('idempotency test')

        try:
            with self.app.test_request_context():
                url = url_for('api.jobfile', job_id=job_id)

            # First call
            response1 = self.client.get(
                url,
                query_string={'job_output_path': self.swift_output_path},
                headers=self.headers)
            self.assertEqual(response1.status_code, 200)

            # Second call (should not fail or create a duplicate)
            response2 = self.client.get(
                url,
                query_string={'job_output_path': self.swift_output_path},
                headers=self.headers)
            self.assertEqual(response2.status_code, 200)

            # Both responses should have the same metadata
            content1 = json.loads(response1.data.decode())
            content2 = json.loads(response2.data.decode())
            self.assertEqual(content1, content2)
        finally:
            self._remove_container(job_id + '-upload')

    def test_get_without_query_params_returns_zip(self):
        """
        JobFile.get without job_output_path should fall back to returning
        a zip file (not scheduling an upload container).
        """
        job_id = 'chris-jid-upload-4'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        outgoing = os.path.join(self.job_dir, 'outgoing')
        Path(outgoing).mkdir(parents=True, exist_ok=True)

        with open(os.path.join(outgoing, 'test.txt'), 'w') as f:
            f.write('zip fallback test')

        with self.app.test_request_context():
            url = url_for('api.jobfile', job_id=job_id)

        response = self.client.get(url, headers=self.headers)
        self.assertEqual(response.status_code, 200)

        # Should not have scheduled an upload container
        status = self._get_container_status(job_id + '-upload')
        self.assertIsNone(status,
                          'Upload container should NOT be scheduled without '
                          'job_output_path')

    def test_upload_params_written_to_disk(self):
        """
        Verify that upload_params.json is written to the key directory
        when JobFile.get is called with job_output_path.
        """
        job_id = 'chris-jid-upload-5'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        outgoing = os.path.join(self.job_dir, 'outgoing')
        Path(outgoing).mkdir(parents=True, exist_ok=True)

        with open(os.path.join(outgoing, 'data.txt'), 'w') as f:
            f.write('params test')

        try:
            with self.app.test_request_context():
                url = url_for('api.jobfile', job_id=job_id)

            self.client.get(
                url,
                query_string={'job_output_path': self.swift_output_path},
                headers=self.headers)

            params_file = os.path.join(self.job_dir, 'upload_params.json')
            self.assertTrue(os.path.isfile(params_file),
                            'upload_params.json should exist')

            with open(params_file) as f:
                params = json.load(f)

            self.assertEqual(params['jid'], job_id)
            self.assertEqual(params['job_output_path'],
                             self.swift_output_path)
        finally:
            self._remove_container(job_id + '-upload')


class TestJobUploadStatus(SwiftAsyncUploadTests):
    """
    Test that Job.get includes upload_status for finished swift jobs.
    """

    def test_upload_status_not_started(self):
        """
        Job.get should report upload_status='notStarted' when the main
        job has finished but JobFile.get hasn't been called yet.
        """
        job_id = 'chris-jid-upst-1'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        incoming = os.path.join(self.job_dir, 'incoming')
        input_dir = os.path.relpath(incoming, self.storebase_mount)
        Path(incoming).mkdir(parents=True, exist_ok=True)

        outgoing = os.path.join(self.job_dir, 'outgoing')
        output_dir = os.path.relpath(outgoing, self.storebase_mount)
        Path(outgoing).mkdir(parents=True, exist_ok=True)

        with open(os.path.join(incoming, 'test.txt'), 'w') as f:
            f.write('job input test file')

        try:
            with self.app.test_request_context():
                job_url = url_for('api.job', job_id=job_id)

                mounts_dict = {
                    'inputdir_source': os.path.join(self.storebase, input_dir),
                    'inputdir_target': '/share/incoming',
                    'outputdir_source': os.path.join(self.storebase, output_dir),
                    'outputdir_target': '/share/outgoing'
                }
                resources_dict = {'number_of_workers': 1, 'cpu_limit': 1000,
                                  'memory_limit': 200, 'gpu_limit': 0}

                compute_mgr = get_compute_mgr(self.container_env)
                compute_mgr.schedule_job(
                    'fnndsc/pl-simplefsapp',
                    ['python3', '/usr/local/bin/simplefsapp',
                     '--dir', '/share/incoming', '/share/outgoing'],
                    job_id, resources_dict, [],
                    self.user.get_uid(), self.user.get_gid(), mounts_dict)

            # Wait for main job to finish
            for _ in range(20):
                time.sleep(3)
                response = self.client.get(job_url, headers=self.headers)
                if response.json['compute']['status'] == 'finishedSuccessfully':
                    break

            self.assertEqual(response.json['compute']['status'],
                             'finishedSuccessfully')
            self.assertIn('upload_status', response.json['compute'])
            self.assertEqual(response.json['compute']['upload_status'],
                             'notStarted')
        finally:
            self._remove_container(job_id)

    def test_upload_status_uploading_and_complete(self):
        """
        After JobFile.get triggers the upload, Job.get should eventually
        transition upload_status from 'uploading' to 'uploadComplete'.
        """
        job_id = 'chris-jid-upst-2'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        incoming = os.path.join(self.job_dir, 'incoming')
        input_dir = os.path.relpath(incoming, self.storebase_mount)
        Path(incoming).mkdir(parents=True, exist_ok=True)

        outgoing = os.path.join(self.job_dir, 'outgoing')
        output_dir = os.path.relpath(outgoing, self.storebase_mount)
        Path(outgoing).mkdir(parents=True, exist_ok=True)

        with open(os.path.join(incoming, 'test.txt'), 'w') as f:
            f.write('job input test file')

        try:
            with self.app.test_request_context():
                job_url = url_for('api.job', job_id=job_id)
                jobfile_url = url_for('api.jobfile', job_id=job_id)

                mounts_dict = {
                    'inputdir_source': os.path.join(self.storebase, input_dir),
                    'inputdir_target': '/share/incoming',
                    'outputdir_source': os.path.join(self.storebase, output_dir),
                    'outputdir_target': '/share/outgoing'
                }
                resources_dict = {'number_of_workers': 1, 'cpu_limit': 1000,
                                  'memory_limit': 200, 'gpu_limit': 0}

                compute_mgr = get_compute_mgr(self.container_env)
                compute_mgr.schedule_job(
                    'fnndsc/pl-simplefsapp',
                    ['python3', '/usr/local/bin/simplefsapp',
                     '--dir', '/share/incoming', '/share/outgoing'],
                    job_id, resources_dict, [],
                    self.user.get_uid(), self.user.get_gid(), mounts_dict)

            # Wait for main job to finish
            for _ in range(20):
                time.sleep(3)
                response = self.client.get(job_url, headers=self.headers)
                if response.json['compute']['status'] == 'finishedSuccessfully':
                    break

            self.assertEqual(response.json['compute']['status'],
                             'finishedSuccessfully')

            # Trigger the upload via JobFile.get
            self.client.get(
                jobfile_url,
                query_string={'job_output_path': self.swift_output_path},
                headers=self.headers)

            # Poll Job.get until upload_status is 'uploadComplete'
            upload_status = None
            for _ in range(20):
                time.sleep(3)
                response = self.client.get(job_url, headers=self.headers)
                upload_status = response.json['compute'].get('upload_status')
                if upload_status == 'uploadComplete':
                    break

            self.assertEqual(upload_status, 'uploadComplete')
        finally:
            self._remove_container(job_id + '-upload')
            self._remove_container(job_id)


class TestJobDeleteWithUpload(SwiftAsyncUploadTests):
    """
    Test that Job.delete removes copy, main, and upload containers.
    """

    def test_delete_removes_all_three_containers(self):
        """
        After a full job lifecycle (POST → poll → file), Job.delete
        should remove the copy, main, and upload containers.
        """
        job_id = 'chris-jid-del-1'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        # Submit the job (schedules copy container)
        response = self._submit_job(job_id)
        self.assertEqual(response.status_code, 201)

        # Poll until main job finishes
        response = self._poll_until_finished(job_id)
        self.assertEqual(response.json['compute']['status'],
                         'finishedSuccessfully')

        # Trigger upload
        with self.app.test_request_context():
            jobfile_url = url_for('api.jobfile', job_id=job_id)

        self.client.get(
            jobfile_url,
            query_string={'job_output_path': self.swift_output_path},
            headers=self.headers)

        # Wait briefly for upload container to be scheduled
        time.sleep(3)

        # Verify all three containers exist
        self.assertIsNotNone(self._get_container_status(job_id + '-copy'),
                             'Copy container should exist')
        self.assertIsNotNone(self._get_container_status(job_id),
                             'Main container should exist')
        self.assertIsNotNone(self._get_container_status(job_id + '-upload'),
                             'Upload container should exist')

        # Delete the job
        with self.app.test_request_context():
            job_url = url_for('api.job', job_id=job_id)
        response = self.client.delete(job_url, headers=self.headers)
        self.assertEqual(response.status_code, 204)

        # Verify all containers are gone
        self.assertIsNone(self._get_container_status(job_id + '-copy'),
                          'Copy container should be removed')
        self.assertIsNone(self._get_container_status(job_id),
                          'Main container should be removed')
        self.assertIsNone(self._get_container_status(job_id + '-upload'),
                          'Upload container should be removed')

    def test_delete_during_upload(self):
        """
        Job.delete should succeed even if the upload container is still
        running.
        """
        job_id = 'chris-jid-del-2'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        # Submit and wait for main job
        response = self._submit_job(job_id)
        self.assertEqual(response.status_code, 201)

        response = self._poll_until_finished(job_id)
        self.assertEqual(response.json['compute']['status'],
                         'finishedSuccessfully')

        # Trigger upload
        with self.app.test_request_context():
            jobfile_url = url_for('api.jobfile', job_id=job_id)
        self.client.get(
            jobfile_url,
            query_string={'job_output_path': self.swift_output_path},
            headers=self.headers)

        # Delete immediately (upload may still be running)
        with self.app.test_request_context():
            job_url = url_for('api.job', job_id=job_id)
        response = self.client.delete(job_url, headers=self.headers)
        self.assertEqual(response.status_code, 204)

        # Force-cleanup any leftovers
        for suffix in ('-copy', '', '-upload'):
            self._remove_container(job_id + suffix)


class TestEndToEndAsyncUpload(SwiftAsyncUploadTests):
    """
    Full end-to-end test of the async upload flow.
    """

    def test_full_lifecycle(self):
        """
        POST → poll copy → poll main → JobFile.get (metadata) →
        poll upload_status → verify files in Swift → DELETE
        """
        job_id = 'chris-jid-e2e-1'
        self.job_dir = os.path.join(self.storebase_mount, 'key-' + job_id)

        try:
            # 1. Submit job
            response = self._submit_job(job_id)
            self.assertEqual(response.status_code, 201)
            self.assertEqual(response.json['compute']['status'], 'beforeCreate')
            self.assertEqual(response.json['compute']['message'],
                             'copyNotStarted')

            # 2. Poll until main job finishes
            response = self._poll_until_finished(job_id)
            self.assertEqual(response.json['compute']['status'],
                             'finishedSuccessfully')

            # 3. Verify upload_status is 'notStarted' before triggering upload
            self.assertEqual(
                response.json['compute'].get('upload_status'), 'notStarted')

            # 4. Get job file (triggers async upload, returns metadata)
            with self.app.test_request_context():
                jobfile_url = url_for('api.jobfile', job_id=job_id)

            file_response = self.client.get(
                jobfile_url,
                query_string={'job_output_path': self.swift_output_path},
                headers=self.headers)

            self.assertEqual(file_response.status_code, 200)
            content = json.loads(file_response.data.decode())
            self.assertEqual(content['job_output_path'],
                             self.swift_output_path)
            self.assertIsInstance(content['rel_file_paths'], list)
            self.assertGreater(len(content['rel_file_paths']), 0)

            # 5. Poll Job.get until upload completes
            with self.app.test_request_context():
                job_url = url_for('api.job', job_id=job_id)

            upload_status = None
            for _ in range(20):
                time.sleep(3)
                response = self.client.get(job_url, headers=self.headers)
                upload_status = response.json['compute'].get('upload_status')
                if upload_status == 'uploadComplete':
                    break

            self.assertEqual(upload_status, 'uploadComplete')

            # 6. Verify files actually made it to Swift
            swift_files = list(self.swift_manager.ls(self.swift_output_path))
            self.assertGreater(len(swift_files), 0,
                               'Files should be uploaded to Swift')

            # 7. Delete the job (cleans up all containers)
            response = self.client.delete(job_url, headers=self.headers)
            self.assertEqual(response.status_code, 204)

            # 8. Verify all containers are removed
            for suffix in ('-copy', '', '-upload'):
                self.assertIsNone(
                    self._get_container_status(job_id + suffix),
                    f'Container {job_id}{suffix} should be removed')

        except Exception:
            # Ensure cleanup on failure
            self._cleanup_job(job_id)
            raise
