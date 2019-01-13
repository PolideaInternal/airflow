# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import json
import unittest
from copy import deepcopy

from airflow import AirflowException
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook, \
    TIME_TO_SLEEP_IN_SECONDS, GcpTransferOperationStatus
from tests.contrib.utils.base_gcp_mock import \
    mock_base_gcp_hook_no_default_project_id, \
    mock_base_gcp_hook_default_project_id

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = 'project-id'
BODY = {
    'description': 'AAA',
    'project_id': PROJECT_ID
}

TRANSFER_JOB_NAME = "transfer-job"
TRANSFER_OPERATION_NAME = "transfer-operation"

TRANSFER_JOB = {"name": TRANSFER_JOB_NAME}
TRANSFER_OPERATION = {"name": TRANSFER_OPERATION_NAME}

TRANSFER_JOB_FILTER = {
    'project_id': 'project-id',
    'job_names': [TRANSFER_JOB_NAME]
}
TRANSFER_OPERATION_FILTER = {
    'project_id': 'project-id',
    'job_names': [TRANSFER_JOB_NAME]
}
UPDATE_TRANSFER_JOB_BODY = {
    "transferJob": {
        'description': 'description-1'
    },
    'project_id': PROJECT_ID,
    "update_transfer_job_field_mask": 'description'
}


class TestGCPTransferServiceHookWithPassedProjectId(unittest.TestCase):
    def setUp(self):
        with mock.patch('airflow.contrib.hooks.'
                        'gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_no_default_project_id):
            self.gct_hook = GCPTransferServiceHook(gcp_conn_id='test')

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_create_transfer_job(self, get_conn):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        res = self.gct_hook.create_transfer_job(
            body=BODY
        )
        self.assertEqual(res, TRANSFER_JOB)
        create_method.assert_called_once_with(body=BODY)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_get_transfer_job(self, get_conn):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        res = self.gct_hook.get_transfer_job(
            job_name=TRANSFER_JOB_NAME,
            project_id=PROJECT_ID
        )
        self.assertIsNotNone(res)
        self.assertEqual(TRANSFER_JOB_NAME, res['name'])
        get_method.assert_called_once_with(
            jobName=TRANSFER_JOB_NAME,
            projectId=PROJECT_ID
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_list_transfer_job(self, get_conn):
        list_method = get_conn.return_value.transferJobs.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"transferJobs": [TRANSFER_JOB]}

        list_next = get_conn.return_value.transferJobs.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_job(
            filter=TRANSFER_JOB_FILTER
        )
        self.assertIsNotNone(res)
        self.assertEqual(res, [TRANSFER_JOB])
        list_method.assert_called_once_with(
            filter=json.dumps(TRANSFER_JOB_FILTER)
        )
        list_execute_method.assert_called_once_with(num_retries=5)
        list_next.assert_called_once()

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_update_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        res = self.gct_hook.update_transfer_job(
            job_name=TRANSFER_JOB_NAME,
            body=UPDATE_TRANSFER_JOB_BODY,
        )
        self.assertIsNotNone(res)
        update_method.assert_called_once_with(
            jobName=TRANSFER_JOB_NAME,
            body=UPDATE_TRANSFER_JOB_BODY,
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_delete_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute

        self.gct_hook.delete_transfer_job(
            job_name=TRANSFER_JOB_NAME,
            project_id=PROJECT_ID
        )

        update_method.assert_called_once_with(
            jobName=TRANSFER_JOB_NAME,
            body={
                'project_id': PROJECT_ID,
                'transfer_job': {'status': 'DELETED'},
                'update_transfer_job_field_mask': 'status'
            },
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_cancel_transfer_operation(self, get_conn):
        cancel_method = get_conn.return_value.transferOperations.\
            return_value.cancel
        execute_method = cancel_method.return_value.execute

        self.gct_hook.cancel_transfer_operation(
            operation_name=TRANSFER_OPERATION_NAME,
        )
        cancel_method.assert_called_once_with(
            name=TRANSFER_OPERATION_NAME
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_resume_transfer_operation(self, get_conn):
        resume_method = get_conn.return_value.transferOperations. \
            return_value.resume
        execute_method = resume_method.return_value.execute

        self.gct_hook.resume_transfer_operation(
            operation_name=TRANSFER_OPERATION_NAME,
        )
        resume_method.assert_called_once_with(
            name=TRANSFER_OPERATION_NAME
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_pause_transfer_operation(self, get_conn):
        pause_method = get_conn.return_value.transferOperations. \
            return_value.cancel
        execute_method = pause_method.return_value.execute

        self.gct_hook.cancel_transfer_operation(
            operation_name=TRANSFER_OPERATION_NAME,
        )
        pause_method.assert_called_once_with(
            name=TRANSFER_OPERATION_NAME
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_get_transfer_operation(self, get_conn):
        get_method = get_conn.return_value.transferOperations. \
            return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TRANSFER_OPERATION
        res = self.gct_hook.get_transfer_operation(
            operation_name=TRANSFER_OPERATION_NAME,
        )
        self.assertEqual(res, TRANSFER_OPERATION)
        get_method.assert_called_once_with(
            name=TRANSFER_OPERATION_NAME
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_list_transfer_operation(self, get_conn):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"operations": [TRANSFER_OPERATION]}

        list_next = get_conn.return_value.transferOperations.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_operations(
            filter=TRANSFER_OPERATION_FILTER
        )
        self.assertIsNotNone(res)
        self.assertEqual(res, [TRANSFER_OPERATION])
        list_method.assert_called_once_with(
            filter=json.dumps(TRANSFER_OPERATION_FILTER),
            name='transferOperations'
        )
        list_execute_method.assert_called_once_with(num_retries=5)
        list_next.assert_called_once()

    @mock.patch('time.sleep')
    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_wait_for_transfer_job(self, get_conn, mock_sleep):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.side_effect = [
            {'operations': [
                {'metadata': {
                    'status': GcpTransferOperationStatus.IN_PROGRESS
                }}
            ]},
            {'operations': [
                {'metadata': {
                    'status': GcpTransferOperationStatus.SUCCESS
                }}
            ]},
        ]
        get_conn.return_value.transferOperations\
            .return_value.list_next\
            .return_value = None

        self.gct_hook.wait_for_transfer_job({
            'projectId': 'test-project',
            'name': 'transferJobs/test-job',
        })
        self.assertTrue(list_method.called)
        list_args, list_kwargs = list_method.call_args_list[0]
        self.assertEqual(list_kwargs.get('name'), 'transferOperations')
        self.assertEqual(
            json.loads(list_kwargs.get('filter')),
            {
                'project_id': 'test-project',
                'job_names': ['transferJobs/test-job']
            },
        )
        mock_sleep.assert_called_once_with(TIME_TO_SLEEP_IN_SECONDS)

    @mock.patch('time.sleep')
    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_wait_for_transfer_job_failed(self, get_conn, mock_sleep):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = \
            {
                'operations': [{
                    'name': TRANSFER_OPERATION_NAME,
                    'metadata': {
                        'status': GcpTransferOperationStatus.FAILED
                    }
                }]
            }

        get_conn.return_value.transferOperations \
            .return_value.list_next \
            .return_value = None

        with self.assertRaises(AirflowException):
            self.gct_hook.wait_for_transfer_job({
                'projectId': 'test-project',
                'name': 'transferJobs/test-job',
            })
            self.assertTrue(list_method.called)

    @mock.patch('time.sleep')
    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_wait_for_transfer_job_expect_failed(self, get_conn, mock_sleep):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {
            'operations': [{
                'name': TRANSFER_OPERATION_NAME,
                'metadata': {
                    'status': GcpTransferOperationStatus.FAILED}}
            ]
        }

        get_conn.return_value.transferOperations \
            .return_value.list_next \
            .return_value = None

        self.gct_hook.wait_for_transfer_job(
            job={
                'projectId': 'test-project',
                'name': 'transferJobs/test-job',
            },
            expected_status=GcpTransferOperationStatus.FAILED
        )
        self.assertTrue(True)


class TestGCPTransferServiceHookWithProjectIdFromConnection(unittest.TestCase):
    def setUp(self):
        with mock.patch('airflow.contrib.hooks.'
                        'gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_default_project_id):
            self.gct_hook = GCPTransferServiceHook(gcp_conn_id='test')

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_create_transfer_job(self, get_conn):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = deepcopy(TRANSFER_JOB)
        res = self.gct_hook.create_transfer_job(
            body=self._without_project_id(BODY)
        )
        self.assertEqual(res, TRANSFER_JOB)
        create_method.assert_called_once_with(body=self._with_project_id(BODY, 'example-project'))
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_get_transfer_job(self, get_conn):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        res = self.gct_hook.get_transfer_job(
            job_name=TRANSFER_JOB_NAME
        )
        self.assertIsNotNone(res)
        self.assertEqual(TRANSFER_JOB_NAME, res['name'])
        get_method.assert_called_once_with(
            jobName=TRANSFER_JOB_NAME,
            projectId='example-project'
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_list_transfer_job(self, get_conn):
        list_method = get_conn.return_value.transferJobs.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"transferJobs": [TRANSFER_JOB]}

        list_next = get_conn.return_value.transferJobs.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_job(
            filter=self._without_project_id(TRANSFER_JOB_FILTER)
        )
        self.assertIsNotNone(res)
        self.assertEqual(res, [TRANSFER_JOB])
        list_method.assert_called_once_with(
            filter=json.dumps(self._with_project_id(TRANSFER_JOB_FILTER, 'example-project'))
        )
        list_execute_method.assert_called_once_with(num_retries=5)
        list_next.assert_called_once()

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_update_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        res = self.gct_hook.update_transfer_job(
            job_name=TRANSFER_JOB_NAME,
            body=self._without_project_id(UPDATE_TRANSFER_JOB_BODY),
        )
        self.assertIsNotNone(res)
        update_method.assert_called_once_with(
            jobName=TRANSFER_JOB_NAME,
            body=self._with_project_id(UPDATE_TRANSFER_JOB_BODY,
                                       'example-project'),
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_delete_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute

        self.gct_hook.delete_transfer_job(
            job_name=TRANSFER_JOB_NAME
        )

        update_method.assert_called_once_with(
            jobName=TRANSFER_JOB_NAME,
            body={
                'project_id': 'example-project',
                'transfer_job': {'status': 'DELETED'},
                'update_transfer_job_field_mask': 'status'
            },
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_list_transfer_operation(self, get_conn):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"operations": [TRANSFER_OPERATION]}

        list_next = get_conn.return_value.transferOperations.return_value.list_next
        list_next.return_value = None

        res = self.gct_hook.list_transfer_operations(
            filter=self._without_project_id(TRANSFER_OPERATION_FILTER)
        )
        self.assertIsNotNone(res)
        self.assertEqual(res, [TRANSFER_OPERATION])
        list_method.assert_called_once_with(
            filter=json.dumps(self._with_project_id(
                TRANSFER_OPERATION_FILTER, 'example-project')),
            name='transferOperations'
        )
        list_execute_method.assert_called_once_with(num_retries=5)
        list_next.assert_called_once()

    @staticmethod
    def _without_project_id(body):
        body = deepcopy(body)
        del body['project_id']
        return body

    @staticmethod
    def _with_project_id(body, project_id):
        body = deepcopy(body)
        del body['project_id']
        body['project_id'] = project_id
        return body


class TestGCPTransferServiceHookWithoutProjectId(unittest.TestCase):
    def setUp(self):
        with mock.patch('airflow.contrib.hooks.'
                        'gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_no_default_project_id):
            self.gct_hook = GCPTransferServiceHook(gcp_conn_id='test')

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_create_transfer_job(self, get_conn):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = deepcopy(TRANSFER_JOB)
        with self.assertRaises(AirflowException) as e:
            self.gct_hook.create_transfer_job(
                body=self._without_project_id(BODY)
            )

        self.assertEqual(
            str(e.exception),
            'The project id must be passed either as body project_id parameter '
            'or as project_id extra in GCP connection definition. Both are '
            'not set!'
        )

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_get_transfer_job(self, get_conn):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        with self.assertRaises(AirflowException) as e:
            self.gct_hook.get_transfer_job(
                job_name=TRANSFER_JOB_NAME
            )
        self.assertEqual(
            str(e.exception),
            'The project id must be passed either as keyword project_id '
            'parameter or as project_id extra in GCP connection definition. '
            'Both are not set!'
        )

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_list_transfer_job(self, get_conn):
        list_method = get_conn.return_value.transferJobs.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"transferJobs": [TRANSFER_JOB]}

        list_next = get_conn.return_value.transferJobs.return_value.list_next
        list_next.return_value = None

        with self.assertRaises(AirflowException) as e:
            self.gct_hook.list_transfer_job(
                filter=self._without_project_id(TRANSFER_JOB_FILTER)
            )

        self.assertEqual(
            str(e.exception),
            'The project id must be passed either as filter project_id '
            'parameter or as project_id extra in GCP connection definition. Both are '
            'not set!'
        )

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_update_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        with self.assertRaises(AirflowException) as e:
            self.gct_hook.update_transfer_job(
                job_name=TRANSFER_JOB_NAME,
                body=self._without_project_id(UPDATE_TRANSFER_JOB_BODY),
            )

        self.assertEqual(
            str(e.exception),
            'The project id must be passed either as body project_id parameter '
            'or as project_id extra in GCP connection definition. '
            'Both are not set!'
        )

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_delete_transfer_job(self, get_conn):
        with self.assertRaises(AirflowException) as e:
            self.gct_hook.delete_transfer_job(
                job_name=TRANSFER_JOB_NAME
            )

        self.assertEqual(
            str(e.exception),
            'The project id must be passed either as keyword project_id '
            'parameter or as project_id extra in GCP connection definition. Both are '
            'not set!'
        )

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.'
                'GCPTransferServiceHook.get_conn')
    def test_list_transfer_operation(self, get_conn):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.return_value = {"operations": [TRANSFER_OPERATION]}

        list_next = get_conn.return_value.transferOperations.return_value.list_next
        list_next.return_value = None

        with self.assertRaises(AirflowException) as e:
            self.gct_hook.list_transfer_operations(
                filter=self._without_project_id(TRANSFER_OPERATION_FILTER)
            )

        self.assertEqual(
            str(e.exception),
            'The project id must be passed either as filter project_id '
            'parameter or as project_id extra in GCP connection definition. Both are '
            'not set!'
        )

    @staticmethod
    def _without_project_id(body):
        body = deepcopy(body)
        del body['project_id']
        return body
