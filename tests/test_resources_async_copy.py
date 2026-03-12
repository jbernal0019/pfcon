"""
Unit tests for the async copy feature.

Tests the copy-phase state machine (Job.get) and async scheduling
(JobList.post) with a mocked DockerManager — no running Docker daemon
required.

The async copy flow is:
  POST  → schedules a "<job_id>-copy" container; saves job_params.json;
           returns immediately with data={}, status='notCreated'
  GET   → while copy not found: returns status='notCreated',
           message='copyNotStarted'
        → while copy running:  returns status='notCreated', message='copying'
        → when copy succeeds:  schedules the main plugin container, removes
           job_params.json, returns main-job status
        → when copy fails:     returns status='undefined', message='copyFailed'
  DELETE→ removes copy, upload, and main containers (if they exist)
"""

import json
import logging
import os
import shutil
import tempfile
from unittest import TestCase
from unittest import mock

from flask import url_for

from pfcon.app import create_app
from pfcon.compute.abstractmgr import JobInfo, JobStatus, ManagerException


def _make_job_info(status, message='', image='fnndsc/pfcon:test', cmd='',
                   timestamp=''):
    return JobInfo(
        name='test-job',
        image=image,
        cmd=cmd,
        timestamp=timestamp,
        message=message,
        status=status,
    )


class TestAsyncCopyStateMachine(TestCase):
    """
    Tests for the async copy phase state machine using a mocked DockerManager.

    setUp creates a temporary directory that serves as both STOREBASE and
    STOREBASE_MOUNT.  Environment variables are patched so that Config.__init__
    picks up COMPUTE_VOLUME_TYPE=host (avoiding Docker auto-detection) and the
    app is created in dev mode with fslink storage and a fake PFCON_COPY_IMAGE.
    """

    def setUp(self):
        logging.disable(logging.WARNING)
        self.tmpdir = tempfile.mkdtemp()

        # Patch env vars read by Config.__init__ before the app is created.
        # COMPUTE_VOLUME_TYPE=host avoids the docker_local_volume auto-detection
        # that would call get_storebase_from_docker().
        env_patch = {
            'APPLICATION_MODE': 'dev',
            'COMPUTE_VOLUME_TYPE': 'host',
            'STOREBASE': self.tmpdir,
            'STOREBASE_MOUNT': self.tmpdir,
        }
        with mock.patch.dict(os.environ, env_patch):
            # config_dict overrides applied after Config.__init__ runs.
            # PFCON_INNETWORK / STORAGE_ENV / PFCON_COPY_IMAGE are not read
            # by Config.__init__ in a way that would cause side-effects for
            # fslink (no external service calls), so they can be set here.
            self.app = create_app({
                'PFCON_INNETWORK': True,
                'STORAGE_ENV': 'fslink',
                'PFCON_COPY_IMAGE': 'fnndsc/pfcon:test',
            })

        self.client = self.app.test_client()

        with self.app.test_request_context():
            url = url_for('api.auth')
            creds = {
                'pfcon_user': self.app.config.get('PFCON_USER'),
                'pfcon_password': self.app.config.get('PFCON_PASSWORD'),
            }
            r = self.client.post(url, data=json.dumps(creds),
                                 content_type='application/json')
            self.headers = {'Authorization': 'Bearer ' + r.json['token']}
            self.joblist_url = url_for('api.joblist')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        logging.disable(logging.NOTSET)

    def _job_url(self, job_id):
        with self.app.test_request_context():
            return url_for('api.job', job_id=job_id)

    def _write_params_file(self, job_id, storage_env='fslink', extra=None):
        """
        Create the directory structure and job_params.json that would exist
        after a real POST with async copy (simulates the saved state).
        Returns (key_dir, params_file_path).
        """
        key_dir = os.path.join(self.tmpdir, 'key-' + job_id)
        os.makedirs(os.path.join(key_dir, 'incoming'), exist_ok=True)
        params = {
            'jid': job_id,
            'storage_env': storage_env,
            'args': ['--dir', '/share/incoming'],
            'args_path_flags': [],
            'auid': 'cube',
            'number_of_workers': 1,
            'cpu_limit': 1000,
            'memory_limit': 200,
            'gpu_limit': 0,
            'image': 'fnndsc/pl-simplefsapp',
            'entrypoint': ['python3', '/usr/local/bin/simplefsapp'],
            'type': 'fs',
            'env': [],
            'input_dirs': ['home/foo/feed/input'],
            'output_dir': 'home/foo/feed/output',
        }
        if extra:
            params.update(extra)
        params_file = os.path.join(key_dir, 'job_params.json')
        with open(params_file, 'w') as f:
            json.dump(params, f)
        return key_dir, params_file

    # -----------------------------------------------------------------------
    # POST tests
    # -----------------------------------------------------------------------

    def test_post_schedules_copy_job_and_returns_empty_data(self):
        """POST returns data={} and schedules a '<job_id>-copy' container."""
        job_id = 'async-post-1'
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
            'input_dirs': ['home/foo/feed/input'],
            'output_dir': 'home/foo/feed/output',
        }
        copy_job_info = _make_job_info(JobStatus.notStarted,
                                       cmd='python -m pfcon.copy_worker /share/outgoing')

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.schedule_job.return_value = 'mock_copy_job'
            mock_mgr.get_job_info.return_value = copy_job_info

            response = self.client.post(self.joblist_url, data=data,
                                        headers=self.headers)

        self.assertEqual(response.status_code, 201)
        # Async copy: no file count on POST
        self.assertEqual(response.json['data'], {})
        self.assertIn('compute', response.json)
        self.assertEqual(response.json['compute']['status'], 'notCreated')

        # job_params.json must have been written with correct contents
        params_file = os.path.join(self.tmpdir, 'key-' + job_id, 'job_params.json')
        self.assertTrue(os.path.isfile(params_file))
        with open(params_file) as f:
            saved = json.load(f)
        self.assertEqual(saved['jid'], job_id)
        self.assertEqual(saved['storage_env'], 'fslink')
        self.assertEqual(saved['image'], 'fnndsc/pl-simplefsapp')

        # schedule_job must have been called with '<job_id>-copy' as the name
        mock_mgr.schedule_job.assert_called_once()
        scheduled_name = mock_mgr.schedule_job.call_args[0][2]
        self.assertEqual(scheduled_name, job_id + '-copy')

    def test_post_missing_copy_image_returns_500(self):
        """POST with no PFCON_COPY_IMAGE configured returns HTTP 500."""
        job_id = 'async-post-err'
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
            'input_dirs': ['home/foo/feed/input'],
            'output_dir': 'home/foo/feed/output',
        }
        original = self.app.config.get('PFCON_COPY_IMAGE')
        self.app.config['PFCON_COPY_IMAGE'] = None
        try:
            with mock.patch('pfcon.resources.DockerManager'):
                response = self.client.post(self.joblist_url, data=data,
                                            headers=self.headers)
            self.assertEqual(response.status_code, 500)
        finally:
            self.app.config['PFCON_COPY_IMAGE'] = original

    # -----------------------------------------------------------------------
    # GET (copy phase) tests
    # -----------------------------------------------------------------------

    def test_get_copy_container_not_found_returns_before_create(self):
        """
        GET while copy container is not yet visible returns
        status='notCreated', message='copyNotStarted'.
        """
        job_id = 'async-get-1'
        self._write_params_file(job_id)
        url = self._job_url(job_id)

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.side_effect = ManagerException('not found',
                                                             status_code=404)
            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        self.assertEqual(compute['status'], 'notCreated')
        self.assertEqual(compute['message'], 'copyNotStarted')
        self.assertEqual(compute['jid'], job_id)

    def test_get_copy_container_running_returns_before_create(self):
        """
        GET while copy container status is 'started' returns
        status='notCreated', message='copying'.
        """
        job_id = 'async-get-2'
        self._write_params_file(job_id)
        url = self._job_url(job_id)

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.return_value = 'mock_copy_job'
            mock_mgr.get_job_info.return_value = _make_job_info(JobStatus.started)

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        self.assertEqual(compute['status'], 'notCreated')
        self.assertEqual(compute['message'], 'copying')

    def test_get_copy_container_notStarted_returns_before_create(self):
        """
        GET while copy container status is 'notStarted' returns
        status='notCreated', message='copying'.
        """
        job_id = 'async-get-3'
        self._write_params_file(job_id)
        url = self._job_url(job_id)

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.return_value = 'mock_copy_job'
            mock_mgr.get_job_info.return_value = _make_job_info(JobStatus.notStarted)

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        self.assertEqual(compute['status'], 'notCreated')
        self.assertEqual(compute['message'], 'copying')

    def test_get_copy_container_failed_returns_undefined(self):
        """
        GET after copy container exits with error returns
        status='undefined', message='copyFailed' and includes the copy logs.
        """
        job_id = 'async-get-4'
        self._write_params_file(job_id)
        url = self._job_url(job_id)

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.return_value = 'mock_copy_job'
            mock_mgr.get_job_info.return_value = _make_job_info(
                JobStatus.finishedWithError, message='OOMKilled')
            mock_mgr.get_job_logs.return_value = 'Error: container killed (OOM)'

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        self.assertEqual(compute['status'], 'undefined')
        self.assertEqual(compute['message'], 'copyFailed')
        self.assertEqual(compute['logs'], 'Error: container killed (OOM)')

    def test_get_copy_succeeded_schedules_main_job_and_removes_params(self):
        """
        GET after copy container finishes successfully schedules the main
        plugin container, removes job_params.json, and returns the main-job
        status.
        """
        job_id = 'async-get-5'
        key_dir, params_file = self._write_params_file(job_id)
        os.makedirs(os.path.join(key_dir, 'outgoing'), exist_ok=True)
        url = self._job_url(job_id)

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.return_value = 'mock_copy_job'
            # First call → copy job info (success); second call → main job info
            mock_mgr.get_job_info.side_effect = [
                _make_job_info(JobStatus.finishedSuccessfully),
                _make_job_info(JobStatus.notStarted,
                               image='fnndsc/pl-simplefsapp'),
            ]
            mock_mgr.schedule_job.return_value = 'mock_main_job'

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        # Returns main-job status, not 'fetchingFiles'
        self.assertEqual(compute['status'], 'notStarted')
        self.assertEqual(compute['jid'], job_id)

        # job_params.json must have been consumed and removed
        self.assertFalse(os.path.isfile(params_file))
        self.assertFalse(os.path.isfile(params_file + '.consumed'))

        # The main job must have been scheduled under the original job_id
        mock_mgr.schedule_job.assert_called_once()
        scheduled_name = mock_mgr.schedule_job.call_args[0][2]
        self.assertEqual(scheduled_name, job_id)

    def test_get_copy_succeeded_main_job_image_matches_request(self):
        """
        The main job is scheduled with the image from the original POST request,
        not with the copy-container image.
        """
        job_id = 'async-get-6'
        key_dir, _ = self._write_params_file(
            job_id, extra={'image': 'fnndsc/pl-simpledsapp'})
        os.makedirs(os.path.join(key_dir, 'outgoing'), exist_ok=True)
        url = self._job_url(job_id)

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.return_value = 'mock_copy_job'
            mock_mgr.get_job_info.side_effect = [
                _make_job_info(JobStatus.finishedSuccessfully),
                _make_job_info(JobStatus.notStarted, image='fnndsc/pl-simpledsapp'),
            ]
            mock_mgr.schedule_job.return_value = 'mock_main_job'

            self.client.get(url, headers=self.headers)

        scheduled_image = mock_mgr.schedule_job.call_args[0][0]
        self.assertEqual(scheduled_image, 'fnndsc/pl-simpledsapp')

    def test_get_after_copy_phase_checks_main_job_directly(self):
        """
        GET when job_params.json does not exist (copy phase already complete)
        queries the main job directly, not the copy container.
        """
        job_id = 'async-get-7'
        url = self._job_url(job_id)
        # No params_file: simulates state after successful copy phase

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.return_value = 'mock_main_job'
            mock_mgr.get_job_info.return_value = _make_job_info(
                JobStatus.finishedSuccessfully,
                image='fnndsc/pl-simplefsapp',
                message='finished')
            mock_mgr.get_job_logs.return_value = 'all done'

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        self.assertEqual(compute['status'], 'finishedSuccessfully')
        self.assertEqual(compute['logs'], 'all done')
        # get_job was called with the main job name (no '-copy' suffix)
        mock_mgr.get_job.assert_called_once_with(job_id)

    def test_get_concurrent_race_condition_params_already_consumed(self):
        """
        If two concurrent GET requests both see the copy as finished, only one
        should schedule the main job.  The second sees FileNotFoundError on
        os.rename and falls back to reading the main-job status.
        """
        job_id = 'async-get-8'
        key_dir, params_file = self._write_params_file(job_id)
        os.makedirs(os.path.join(key_dir, 'outgoing'), exist_ok=True)
        url = self._job_url(job_id)

        # Simulate the race: os.rename raises FileNotFoundError (params
        # already consumed by a concurrent request), then the main job is
        # already running.
        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager, \
             mock.patch('pfcon.resources.os.rename',
                        side_effect=FileNotFoundError):
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.return_value = 'mock_main_job'
            mock_mgr.get_job_info.side_effect = [
                _make_job_info(JobStatus.finishedSuccessfully),  # copy job
                _make_job_info(JobStatus.started,                # main job
                               image='fnndsc/pl-simplefsapp'),
            ]
            mock_mgr.get_job_logs.return_value = ''

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        # Returns main-job status, not error
        self.assertEqual(compute['status'], 'started')
        # schedule_job must NOT have been called (race loser doesn't schedule)
        mock_mgr.schedule_job.assert_not_called()

    # -----------------------------------------------------------------------
    # DELETE tests
    # -----------------------------------------------------------------------

    def test_delete_during_copy_phase_returns_204(self):
        """
        DELETE while the main job has not yet been scheduled (still in copy
        phase) returns 204.  The copy container is removed; the missing main
        job is tolerated.
        """
        job_id = 'async-del-1'
        key_dir, _ = self._write_params_file(job_id)
        url = self._job_url(job_id)

        mock_copy_job = mock.MagicMock()

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.side_effect = [
                mock_copy_job,  # get_job(job_id + '-copy') → found
                ManagerException('not found', status_code=404),  # get_job(job_id + '-upload') → missing
                ManagerException('not found', status_code=404),  # get_job(job_id) → missing
            ]

            response = self.client.delete(url, headers=self.headers)

        self.assertEqual(response.status_code, 204)
        # Copy container must be removed
        mock_mgr.remove_job.assert_called_once_with(mock_copy_job)

    def test_delete_removes_copy_and_main_job(self):
        """
        DELETE when both copy and main containers exist removes both.
        """
        job_id = 'async-del-2'
        url = self._job_url(job_id)

        mock_copy_job = mock.MagicMock()
        mock_main_job = mock.MagicMock()

        mock_upload_job = mock.MagicMock()

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.side_effect = [
                mock_copy_job,   # get_job(job_id + '-copy')
                mock_upload_job, # get_job(job_id + '-upload')
                mock_main_job,   # get_job(job_id)
            ]

            response = self.client.delete(url, headers=self.headers)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(mock_mgr.remove_job.call_count, 3)
        mock_mgr.remove_job.assert_any_call(mock_copy_job)
        mock_mgr.remove_job.assert_any_call(mock_upload_job)
        mock_mgr.remove_job.assert_any_call(mock_main_job)

    def test_delete_when_copy_container_missing_removes_main_job(self):
        """
        DELETE when copy container is already gone (e.g. already cleaned up)
        still removes the main job.
        """
        job_id = 'async-del-3'
        url = self._job_url(job_id)

        mock_main_job = mock.MagicMock()

        with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
            mock_mgr = MockDockerManager.return_value
            mock_mgr.get_job.side_effect = [
                ManagerException('not found', status_code=404),  # copy job missing
                ManagerException('not found', status_code=404),  # upload job missing
                mock_main_job,                                   # main job present
            ]

            response = self.client.delete(url, headers=self.headers)

        self.assertEqual(response.status_code, 204)
        mock_mgr.remove_job.assert_called_once_with(mock_main_job)

    # -----------------------------------------------------------------------
    # Swift storage variant (params-file level check)
    # -----------------------------------------------------------------------

    def test_get_copy_succeeded_swift_uses_key_dir_as_output(self):
        """
        For swift storage the output_dir in mounts must be key-<job_id>/outgoing
        (not the ChRIS path stored in params), because the main job writes to
        the local storebase, not back to Swift.
        """
        job_id = 'async-swift-1'
        key_dir, params_file = self._write_params_file(
            job_id, storage_env='swift',
            extra={'output_dir': 'foo/feed/output'})
        os.makedirs(os.path.join(key_dir, 'outgoing'), exist_ok=True)
        url = self._job_url(job_id)

        # Override app config so resources.py sees swift storage
        original_storage = self.app.config.get('STORAGE_ENV')
        self.app.config['STORAGE_ENV'] = 'swift'
        # Provide minimal Swift config to avoid AttributeError in delete handler
        self.app.config.setdefault('SWIFT_CONNECTION_PARAMS', {
            'user': 'u', 'key': 'k', 'authurl': 'http://swift:8080/auth/v1.0'})
        self.app.config.setdefault('SWIFT_CONTAINER_NAME', 'users')

        try:
            with mock.patch('pfcon.resources.DockerManager') as MockDockerManager:
                mock_mgr = MockDockerManager.return_value
                mock_mgr.get_job.return_value = 'mock_copy_job'
                mock_mgr.get_job_info.side_effect = [
                    _make_job_info(JobStatus.finishedSuccessfully),
                    _make_job_info(JobStatus.notStarted,
                                   image='fnndsc/pl-simplefsapp'),
                ]
                mock_mgr.schedule_job.return_value = 'mock_main_job'

                response = self.client.get(url, headers=self.headers)
        finally:
            self.app.config['STORAGE_ENV'] = original_storage

        self.assertEqual(response.status_code, 200)
        # For swift: outputdir_source must be key-<job_id>/outgoing (or its
        # host-path equivalent), NOT 'foo/feed/output'.
        schedule_kwargs = mock_mgr.schedule_job.call_args[0]
        mounts_dict = schedule_kwargs[7]   # 8th positional arg
        expected_output_subpath = 'key-' + job_id + '/outgoing'
        self.assertIn(expected_output_subpath,
                      mounts_dict['outputdir_source'])
