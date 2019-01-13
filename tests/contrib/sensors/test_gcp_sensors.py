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
import unittest

from parameterized import parameterized

from airflow.contrib.hooks.gcp_transfer_hook import GcpTransferOperationStatus
from airflow.contrib.sensors.gcp_transfer_sensor import \
    GcpStorageTransferOperationWaitForJobStatusSensor

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestGcpStorageTransferOperationWaitForJobStatusSensor(unittest.TestCase):
    @mock.patch(
        'airflow.contrib.sensors.gcp_transfer_sensor.GCPTransferServiceHook')
    def test_wait_for_status_success(self, mock_tool):
        operations = [
            {'metadata': {'status': GcpTransferOperationStatus.SUCCESS}}
        ]
        mock_tool.return_value.list_transfer_operations\
            .return_value = operations
        mock_tool.check_operations_result.return_value = True

        op = GcpStorageTransferOperationWaitForJobStatusSensor(
            task_id='task-id',
            operation_name='operation-name',
            job_name='job-name',
            project_id='project-id',
            expected_status=GcpTransferOperationStatus.SUCCESS
        )
        result = op.poke({})

        mock_tool.return_value.list_transfer_operations.assert_called_with(
            filter={
                'project_id': 'project-id',
                'job_names': ['job-name'],
            }
        )
        mock_tool.check_operations_result.assert_called_with(
            operations=operations,
            expected_status=GcpTransferOperationStatus.SUCCESS
        )
        self.assertTrue(result)

    @mock.patch(
        'airflow.contrib.sensors.gcp_transfer_sensor.GCPTransferServiceHook')
    def test_wait_for_status_success_default_expected_status(self, mock_tool):
        op = GcpStorageTransferOperationWaitForJobStatusSensor(
            task_id='task-id',
            operation_name='operation-name',
            job_name='job-name',
            project_id='project-id',
        )
        result = op.poke({})

        mock_tool.check_operations_result.assert_called_with(
            operations=mock.ANY,
            expected_status=GcpTransferOperationStatus.SUCCESS
        )
        self.assertTrue(result)

