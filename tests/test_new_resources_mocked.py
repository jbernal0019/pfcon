"""
Unit tests for the new client-managed job resources (CopyJob, PluginJob,
UploadJob, DeleteJob) with a mocked DockerManager — no running Docker
daemon required.

Tests cover:
- CopyJobList POST: schedules copy container, saves job_params.json
- CopyJob GET: returns copy container status
- CopyJob DELETE: removes copy container
- PluginJobList POST: schedules plugin container directly (no copy)
- PluginJob GET/DELETE: returns/removes plugin container
- UploadJobList POST: schedules upload container, idempotency checks
- UploadJob GET/DELETE: returns/removes upload container
- DeleteJobList POST: schedules delete container
- DeleteJob GET/DELETE: returns/removes delete container
- No-op behavior for storage modes that don't need copy/upload
"""

import json
import logging
import os
import shutil
import tempfile
from unittest import TestCase, mock

from flask import url_for

from pfcon.app import create_app
from pfcon.compute.abstractmgr import JobInfo, JobStatus, ManagerException


def _make_job_info(status, message='', image='test-image', cmd='',
                   timestamp=''):
    return JobInfo(
        name='test-job',
        image=image,
        cmd=cmd,
        timestamp=timestamp,
        message=message,
        status=status,
    )


class NewResourcesTestBase(TestCase):
    """Base class for all new resource tests with mocked compute."""

    def setUp(self):
        logging.disable(logging.WARNING)
        self.tmpdir = tempfile.mkdtemp()

        env_patch = {
            'APPLICATION_MODE': 'dev',
            'COMPUTE_VOLUME_TYPE': 'host',
            'STOREBASE': self.tmpdir,
            'STOREBASE_MOUNT': self.tmpdir,
        }
        with mock.patch.dict(os.environ, env_patch):
            self.app = create_app({
                'PFCON_INNETWORK': True,
                'STORAGE_ENV': 'fslink',
                'PFCON_OP_IMAGE': 'ghcr.io/fnndsc/pfconopjob:test',
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

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        logging.disable(logging.NOTSET)


# ---------------------------------------------------------------------------
# CopyJobList / CopyJob tests
# ---------------------------------------------------------------------------

class TestCopyJobList(NewResourcesTestBase):

    def test_get_returns_server_info(self):
        with self.app.test_request_context():
            url = url_for('api.copyjoblist')
        response = self.client.get(url, headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertIn('server_version', response.json)
        self.assertEqual(response.json['storage_env'], 'fslink')

    def test_post_schedules_copy_container(self):
        job_id = 'copy-test-1'
        data = {
            'jid': job_id,
            'input_dirs': ['home/foo/feed/input'],
            'output_dir': 'home/foo/feed/output',
        }

        copy_info = _make_job_info(JobStatus.notStarted)

        with self.app.test_request_context():
            url = url_for('api.copyjoblist')

        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.schedule_job.return_value = 'mock_copy_job'
            mgr.get_job_info.return_value = copy_info

            response = self.client.post(url, data=data,
                                        headers=self.headers)

        self.assertEqual(response.status_code, 201)
        self.assertIn('compute', response.json)

        # job_params.json must have been written
        params_file = os.path.join(self.tmpdir, 'key-' + job_id,
                                   'job_params.json')
        self.assertTrue(os.path.isfile(params_file))
        with open(params_file) as f:
            saved = json.load(f)
        self.assertEqual(saved['jid'], job_id)
        self.assertEqual(saved['storage_env'], 'fslink')

        # Schedule was called with '-copy' suffix
        scheduled_name = mgr.schedule_job.call_args[0][2]
        self.assertEqual(scheduled_name, job_id + '-copy')

    def test_post_noop_for_zipfile_storage(self):
        """Copy is a no-op for non-fslink/swift storage."""
        original = self.app.config['PFCON_INNETWORK']
        self.app.config['PFCON_INNETWORK'] = False
        self.app.config['STORAGE_ENV'] = 'zipfile'
        try:
            with self.app.test_request_context():
                url = url_for('api.copyjoblist')
            data = {
                'jid': 'copy-noop-1',
                'input_dirs': ['foo'],
                'output_dir': 'bar',
            }
            response = self.client.post(url, data=data,
                                        headers=self.headers)
            self.assertEqual(response.status_code, 201)
            self.assertEqual(response.json['compute']['status'],
                             'finishedSuccessfully')
            self.assertEqual(response.json['compute']['message'],
                             'copySkipped')
        finally:
            self.app.config['PFCON_INNETWORK'] = original
            self.app.config['STORAGE_ENV'] = 'fslink'


class TestCopyJob(NewResourcesTestBase):

    def test_get_returns_copy_status(self):
        job_id = 'copy-get-1'
        with self.app.test_request_context():
            url = url_for('api.copyjob', job_id=job_id)

        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.get_job.return_value = 'mock_copy_job'
            mgr.get_job_info.return_value = _make_job_info(
                JobStatus.started, image='pfconopjob')
            mgr.get_job_logs.return_value = ''

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        self.assertEqual(compute['jid'], job_id)
        self.assertEqual(compute['status'], 'started')
        # get_job was called with '-copy' suffix
        mgr.get_job.assert_called_once_with(job_id + '-copy')

    def test_delete_removes_copy_container(self):
        job_id = 'copy-del-1'
        with self.app.test_request_context():
            url = url_for('api.copyjob', job_id=job_id)

        mock_job = mock.MagicMock()
        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.get_job.return_value = mock_job

            response = self.client.delete(url, headers=self.headers)

        self.assertEqual(response.status_code, 204)
        mgr.get_job.assert_called_once_with(job_id + '-copy')
        mgr.remove_job.assert_called_once_with(mock_job)

    def test_delete_tolerates_missing_container(self):
        job_id = 'copy-del-2'
        with self.app.test_request_context():
            url = url_for('api.copyjob', job_id=job_id)

        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.get_job.side_effect = ManagerException('not found',
                                                        status_code=404)

            response = self.client.delete(url, headers=self.headers)

        self.assertEqual(response.status_code, 204)
        mgr.remove_job.assert_not_called()


# ---------------------------------------------------------------------------
# PluginJobList / PluginJob tests
# ---------------------------------------------------------------------------

class TestPluginJobList(NewResourcesTestBase):

    def test_post_schedules_plugin_directly_for_fslink(self):
        """For fslink in-network, PluginJobList.post schedules the plugin
        container directly (no copy phase)."""
        job_id = 'plugin-test-1'

        # Create the incoming dir (as if copy already ran)
        incoming = os.path.join(self.tmpdir, 'key-' + job_id, 'incoming')
        os.makedirs(incoming, exist_ok=True)
        with open(os.path.join(incoming, 'test.txt'), 'w') as f:
            f.write('test')

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

        plugin_info = _make_job_info(JobStatus.notStarted,
                                     image='fnndsc/pl-simplefsapp')

        with self.app.test_request_context():
            url = url_for('api.pluginjoblist')

        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.schedule_job.return_value = 'mock_plugin_job'
            mgr.get_job_info.return_value = plugin_info

            response = self.client.post(url, data=data,
                                        headers=self.headers)

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json['data'], {})
        self.assertIn('compute', response.json)

        # Main job scheduled directly (no '-copy' suffix)
        scheduled_name = mgr.schedule_job.call_args[0][2]
        self.assertEqual(scheduled_name, job_id)


class TestPluginJob(NewResourcesTestBase):

    def test_get_returns_plugin_status(self):
        job_id = 'plugin-get-1'
        with self.app.test_request_context():
            url = url_for('api.pluginjob', job_id=job_id)

        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.get_job.return_value = 'mock_job'
            mgr.get_job_info.return_value = _make_job_info(
                JobStatus.finishedSuccessfully,
                image='fnndsc/pl-simplefsapp')
            mgr.get_job_logs.return_value = 'done'

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        self.assertEqual(compute['status'], 'finishedSuccessfully')
        self.assertEqual(compute['logs'], 'done')
        # Called with the job_id directly (no suffix)
        mgr.get_job.assert_called_once_with(job_id)

    def test_delete_removes_plugin_container(self):
        job_id = 'plugin-del-1'
        with self.app.test_request_context():
            url = url_for('api.pluginjob', job_id=job_id)

        mock_job = mock.MagicMock()
        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.get_job.return_value = mock_job

            response = self.client.delete(url, headers=self.headers)

        self.assertEqual(response.status_code, 204)
        mgr.get_job.assert_called_once_with(job_id)
        mgr.remove_job.assert_called_once_with(mock_job)


class TestPluginJobFile(NewResourcesTestBase):

    def test_get_returns_metadata_for_fslink(self):
        job_id = 'pluginfile-1'

        # Create output files
        output_path = os.path.join(self.tmpdir, 'home/foo/feed/output')
        os.makedirs(os.path.join(output_path, 'results'), exist_ok=True)
        with open(os.path.join(output_path, 'results', 'out.txt'), 'w') as f:
            f.write('output')

        with self.app.test_request_context():
            url = url_for('api.pluginjobfile', job_id=job_id)

        response = self.client.get(
            url,
            query_string={'job_output_path': 'home/foo/feed/output'},
            headers=self.headers)

        self.assertEqual(response.status_code, 200)
        content = json.loads(response.data.decode())
        self.assertEqual(content['job_output_path'], 'home/foo/feed/output')
        self.assertIn('results/out.txt', content['rel_file_paths'])

    def test_get_without_query_param_returns_400(self):
        job_id = 'pluginfile-2'
        with self.app.test_request_context():
            url = url_for('api.pluginjobfile', job_id=job_id)

        response = self.client.get(url, headers=self.headers)
        self.assertEqual(response.status_code, 400)

    def test_get_does_not_schedule_upload(self):
        """PluginJobFile.get must NOT schedule an upload container
        (unlike the legacy JobFile)."""
        job_id = 'pluginfile-noupload'

        output_path = os.path.join(self.tmpdir, 'home/foo/feed/output')
        os.makedirs(output_path, exist_ok=True)
        with open(os.path.join(output_path, 'test.txt'), 'w') as f:
            f.write('data')

        with self.app.test_request_context():
            url = url_for('api.pluginjobfile', job_id=job_id)

        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value

            response = self.client.get(
                url,
                query_string={'job_output_path': 'home/foo/feed/output'},
                headers=self.headers)

        self.assertEqual(response.status_code, 200)
        # schedule_job must not have been called
        mgr.schedule_job.assert_not_called()


# ---------------------------------------------------------------------------
# UploadJobList / UploadJob tests
# ---------------------------------------------------------------------------

class TestUploadJobList(NewResourcesTestBase):

    def test_post_noop_for_fslink(self):
        """Upload is a no-op for fslink storage."""
        with self.app.test_request_context():
            url = url_for('api.uploadjoblist')
        data = {
            'jid': 'upload-noop-1',
            'job_output_path': 'foo/output',
        }
        response = self.client.post(url, data=data, headers=self.headers)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json['compute']['status'],
                         'finishedSuccessfully')
        self.assertEqual(response.json['compute']['message'],
                         'uploadSkipped')

    def test_post_schedules_upload_for_swift(self):
        """Upload container is scheduled for swift storage."""
        self.app.config['STORAGE_ENV'] = 'swift'
        self.app.config.setdefault('SWIFT_CONNECTION_PARAMS', {
            'user': 'u', 'key': 'k', 'authurl': 'http://swift:8080/auth/v1.0'
        })
        self.app.config.setdefault('SWIFT_CONTAINER_NAME', 'users')

        job_id = 'upload-swift-1'
        key_dir = os.path.join(self.tmpdir, 'key-' + job_id)
        os.makedirs(key_dir, exist_ok=True)

        try:
            with self.app.test_request_context():
                url = url_for('api.uploadjoblist')

            data = {
                'jid': job_id,
                'job_output_path': 'foo/output',
            }

            upload_info = _make_job_info(JobStatus.notStarted)

            with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
                mgr = MockMgr.return_value
                # First call: get_job raises (no existing upload)
                mgr.get_job.side_effect = [
                    ManagerException('not found', status_code=404),
                ]
                mgr.schedule_job.return_value = 'mock_upload_job'
                mgr.get_job_info.return_value = upload_info

                with mock.patch(
                        'pfcon.resources.connect_to_pfcon_networks'):
                    response = self.client.post(url, data=data,
                                                headers=self.headers)

            self.assertEqual(response.status_code, 201)
            scheduled_name = mgr.schedule_job.call_args[0][2]
            self.assertEqual(scheduled_name, job_id + '-upload')

            # upload_params.json should exist
            params_file = os.path.join(key_dir, 'upload_params.json')
            self.assertTrue(os.path.isfile(params_file))
        finally:
            self.app.config['STORAGE_ENV'] = 'fslink'

    def test_post_idempotent_existing_upload(self):
        """If upload container already exists and is running, return
        its status without scheduling a new one."""
        self.app.config['STORAGE_ENV'] = 'swift'
        self.app.config.setdefault('SWIFT_CONNECTION_PARAMS', {
            'user': 'u', 'key': 'k', 'authurl': 'http://swift:8080/auth/v1.0'
        })
        self.app.config.setdefault('SWIFT_CONTAINER_NAME', 'users')

        job_id = 'upload-idemp-1'

        try:
            with self.app.test_request_context():
                url = url_for('api.uploadjoblist')

            data = {
                'jid': job_id,
                'job_output_path': 'foo/output',
            }

            with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
                mgr = MockMgr.return_value
                # Existing upload container found and running
                mgr.get_job.return_value = 'existing_upload'
                mgr.get_job_info.return_value = _make_job_info(
                    JobStatus.started, image='pfconopjob')
                mgr.get_job_logs.return_value = ''

                response = self.client.post(url, data=data,
                                            headers=self.headers)

            self.assertEqual(response.status_code, 201)
            self.assertEqual(response.json['compute']['status'], 'started')
            # schedule_job NOT called (idempotent)
            mgr.schedule_job.assert_not_called()
        finally:
            self.app.config['STORAGE_ENV'] = 'fslink'

    def test_post_reschedules_failed_upload(self):
        """If previous upload failed, remove it and re-schedule."""
        self.app.config['STORAGE_ENV'] = 'swift'
        self.app.config.setdefault('SWIFT_CONNECTION_PARAMS', {
            'user': 'u', 'key': 'k', 'authurl': 'http://swift:8080/auth/v1.0'
        })
        self.app.config.setdefault('SWIFT_CONTAINER_NAME', 'users')

        job_id = 'upload-resched-1'
        key_dir = os.path.join(self.tmpdir, 'key-' + job_id)
        os.makedirs(key_dir, exist_ok=True)

        try:
            with self.app.test_request_context():
                url = url_for('api.uploadjoblist')

            data = {
                'jid': job_id,
                'job_output_path': 'foo/output',
            }

            failed_info = _make_job_info(JobStatus.finishedWithError)
            new_info = _make_job_info(JobStatus.notStarted)

            with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
                mgr = MockMgr.return_value
                # First get_job: existing failed upload
                # After remove, schedule_job succeeds
                mock_failed = mock.MagicMock()
                mgr.get_job.side_effect = [mock_failed]
                mgr.get_job_info.side_effect = [failed_info, new_info]
                mgr.schedule_job.return_value = 'new_upload_job'

                with mock.patch(
                        'pfcon.resources.connect_to_pfcon_networks'):
                    response = self.client.post(url, data=data,
                                                headers=self.headers)

            self.assertEqual(response.status_code, 201)
            # Old upload was removed
            mgr.remove_job.assert_called_once_with(mock_failed)
            # New upload was scheduled
            mgr.schedule_job.assert_called_once()
        finally:
            self.app.config['STORAGE_ENV'] = 'fslink'


class TestUploadJob(NewResourcesTestBase):

    def test_get_returns_upload_status(self):
        job_id = 'upload-get-1'
        with self.app.test_request_context():
            url = url_for('api.uploadjob', job_id=job_id)

        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.get_job.return_value = 'mock_upload'
            mgr.get_job_info.return_value = _make_job_info(
                JobStatus.finishedSuccessfully)
            mgr.get_job_logs.return_value = 'upload done'

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        self.assertEqual(compute['jid'], job_id)
        self.assertEqual(compute['status'], 'finishedSuccessfully')
        mgr.get_job.assert_called_once_with(job_id + '-upload')

    def test_delete_removes_upload_container(self):
        job_id = 'upload-del-1'
        with self.app.test_request_context():
            url = url_for('api.uploadjob', job_id=job_id)

        mock_job = mock.MagicMock()
        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.get_job.return_value = mock_job

            response = self.client.delete(url, headers=self.headers)

        self.assertEqual(response.status_code, 204)
        mgr.get_job.assert_called_once_with(job_id + '-upload')
        mgr.remove_job.assert_called_once_with(mock_job)


# ---------------------------------------------------------------------------
# DeleteJobList / DeleteJob tests
# ---------------------------------------------------------------------------

class TestDeleteJobList(NewResourcesTestBase):

    def test_post_schedules_delete_container(self):
        job_id = 'del-test-1'
        key_dir = os.path.join(self.tmpdir, 'key-' + job_id)
        os.makedirs(os.path.join(key_dir, 'incoming'), exist_ok=True)

        with self.app.test_request_context():
            url = url_for('api.deletejoblist')

        data = {'jid': job_id}
        del_info = _make_job_info(JobStatus.notStarted)

        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.schedule_job.return_value = 'mock_del_job'
            mgr.get_job_info.return_value = del_info

            response = self.client.post(url, data=data,
                                        headers=self.headers)

        self.assertEqual(response.status_code, 201)
        self.assertIn('compute', response.json)

        # delete_params.json must exist
        params_file = os.path.join(key_dir, 'delete_params.json')
        self.assertTrue(os.path.isfile(params_file))
        with open(params_file) as f:
            saved = json.load(f)
        self.assertEqual(saved['jid'], job_id)

        # Scheduled with '-delete' suffix
        scheduled_name = mgr.schedule_job.call_args[0][2]
        self.assertEqual(scheduled_name, job_id + '-delete')

    def test_post_noop_when_no_key_dir(self):
        """If the key directory doesn't exist, return success immediately."""
        job_id = 'del-noop-1'
        with self.app.test_request_context():
            url = url_for('api.deletejoblist')

        data = {'jid': job_id}
        response = self.client.post(url, data=data, headers=self.headers)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json['compute']['status'],
                         'finishedSuccessfully')
        self.assertEqual(response.json['compute']['message'],
                         'deleteSkipped')


class TestDeleteJob(NewResourcesTestBase):

    def test_get_returns_delete_status(self):
        job_id = 'del-get-1'
        with self.app.test_request_context():
            url = url_for('api.deletejob', job_id=job_id)

        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.get_job.return_value = 'mock_del'
            mgr.get_job_info.return_value = _make_job_info(
                JobStatus.finishedSuccessfully)
            mgr.get_job_logs.return_value = 'deleted'

            response = self.client.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        compute = response.json['compute']
        self.assertEqual(compute['jid'], job_id)
        self.assertEqual(compute['status'], 'finishedSuccessfully')
        mgr.get_job.assert_called_once_with(job_id + '-delete')

    def test_delete_removes_delete_container(self):
        job_id = 'del-del-1'
        with self.app.test_request_context():
            url = url_for('api.deletejob', job_id=job_id)

        mock_job = mock.MagicMock()
        with mock.patch('pfcon.base_resources.DockerManager') as MockMgr:
            mgr = MockMgr.return_value
            mgr.get_job.return_value = mock_job

            response = self.client.delete(url, headers=self.headers)

        self.assertEqual(response.status_code, 204)
        mgr.get_job.assert_called_once_with(job_id + '-delete')
        mgr.remove_job.assert_called_once_with(mock_job)
