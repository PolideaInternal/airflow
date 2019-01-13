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
import datetime
import unittest

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook
from airflow.contrib.hooks.gcp_transfer_hook import TIME_TO_SLEEP_IN_SECONDS
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

TRANSFER_JOB = {"name": "transfer-job"}


class TestGCPTransferServiceHook(unittest.TestCase):
    def setUp(self):
        with mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_no_default_project_id):
            self.gct_hook_no_project_id = GCPTransferServiceHook(gcp_conn_id='test')

    @mock.patch('airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook.get_conn')
    def test_create_transfer_job(self, get_conn):
        create_method = get_conn.return_value.transferJobs.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = TRANSFER_JOB
        res = self.gct_hook_no_project_id.create_transfer_job(
            body=BODY
        )
        self.assertEqual(res, TRANSFER_JOB)
        create_method.assert_called_once_with(body=BODY)
        execute_method.assert_called_once_with(num_retries=5)
