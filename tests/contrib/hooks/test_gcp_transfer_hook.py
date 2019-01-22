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

from airflow import AirflowException
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook, \
    GcpTransferJobsStatus, TIME_TO_SLEEP_IN_SECONDS, GcpTransferOperationStatus
from tests.contrib.utils.base_gcp_mock import \
    mock_base_gcp_hook_no_default_project_id

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

BODY = {
    'description': 'AAA'
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


class TestGCPTransferServiceHook(unittest.TestCase):
    def setUp(self):
        with mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_no_default_project_id):
            self.gct_hook = GCPTransferServiceHook(gcp_conn_id='test')

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
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

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
    def test_get_transfer_job(self, get_conn):
        get_method = get_conn.return_value.transferJobs.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        res = self.gct_hook.get_transfer_job(
            job_name=TRANSFER_JOB_NAME
        )
        self.assertIsNotNone(res)
        self.assertEquals(TRANSFER_JOB_NAME, res['name'])
        get_method.assert_called_once_with(jobName=TRANSFER_JOB_NAME)
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
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


    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
    def test_update_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        res = self.gct_hook.update_transfer_job(
            job_name=TRANSFER_JOB_NAME,
            body=TRANSFER_JOB,
            field_mask='name'
        )
        self.assertIsNotNone(res)
        update_method.assert_called_once_with(
            jobName=TRANSFER_JOB_NAME,
            body=TRANSFER_JOB,
            updateFieldMask='name'
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
    def test_delete_transfer_job(self, get_conn):
        update_method = get_conn.return_value.transferJobs.return_value.patch
        execute_method = update_method.return_value.execute

        self.gct_hook.delete_transfer_job(
            job_name=TRANSFER_JOB_NAME,
        )

        update_method.assert_called_once_with(
            jobName=TRANSFER_JOB_NAME,
            body={
                'status': GcpTransferJobsStatus.DELETED
            },
            updateFieldMask='status'
        )
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
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

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
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

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
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
    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
    def test_wait_for_transfer_job(self, get_conn, mock_sleep):
        list_method = get_conn.return_value.transferOperations.return_value.list
        list_execute_method = list_method.return_value.execute
        list_execute_method.side_effect = [
            {'operations': [{'metadata': {
                 'status': GcpTransferOperationStatus.IN_PROGRESS}}
            ]},
            {'operations': [{'metadata': {
                'status': GcpTransferOperationStatus.SUCCESS}}
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
    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
    def test_wait_for_transfer_job_failed(self, get_conn, mock_sleep):
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

        with self.assertRaises(AirflowException):
            self.gct_hook.wait_for_transfer_job({
                'projectId': 'test-project',
                'name': 'transferJobs/test-job',
            })
            self.assertTrue(list_method.called)

    @mock.patch('time.sleep')
    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
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
