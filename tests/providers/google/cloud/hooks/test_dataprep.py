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
from unittest import TestCase, mock

from mock import patch
from requests import HTTPError

from airflow.providers.google.cloud.hooks import dataprep


class TestGoogleDataprepHook(TestCase):

    def setUp(self):
        self.hook = dataprep.GoogleDataprepHook()

    @patch('airflow.providers.google.cloud.hooks.dataprep.requests.get')
    def test_should_be_called_once(self, get_request_mock):
        self.hook.get_jobs_for_job_group(666)
        get_request_mock.assert_called_once()

    @patch('airflow.providers.google.cloud.hooks.dataprep.requests.get', side_effect=HTTPError)
    def test_raise_http_error(self, get_request_mock):
        with mock.patch('airflow.providers.google.cloud.hooks.dataprep.GoogleDataprepHook'
                        '.get_jobs_for_job_group.retry.sleep'):
            with self.assertRaises(HTTPError):
                self.hook.get_jobs_for_job_group(666)
                get_request_mock.assert_called_once()
