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
from tenacity import RetryError

from airflow.providers.google.cloud.hooks import dataprep

JOB_ID = 1234567


class TestGoogleDataprepHook(TestCase):
    def setUp(self):
        self.hook = dataprep.GoogleDataprepHook()

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.get")
    def test_mock_should_be_called_once(self, get_request_mock):
        self.hook.get_jobs_for_job_group(JOB_ID)
        get_request_mock.assert_called_once()

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_raise_http_error_(self, get_request_mock):
        self.hook.get_jobs_for_job_group(JOB_ID)
        self.assertEqual(get_request_mock.call_count, 2)

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[
            mock.MagicMock(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
        ],
    )
    def test_should_not_retry_after_success(self, get_request_mock):
        self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
        self.hook.get_jobs_for_job_group(JOB_ID)
        self.assertEqual(get_request_mock.call_count, 1)

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_should_retry_after_error(self, get_request_mock):
        self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
        self.hook.get_jobs_for_job_group(JOB_ID)
        self.assertEqual(get_request_mock.call_count, 5)

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError()],
    )
    def test_raise_stop_iteration_error_after_first_call(self, get_request_mock):  # pylint: disable=unused-argument
        with self.assertRaises(RetryError) as cm:
            self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
            self.hook.get_jobs_for_job_group(JOB_ID)
        self.assertIsInstance(cm.exception.last_attempt.exception(), StopIteration)

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_raise_http_error_after_five_calls(self, get_request_mock):
        with self.assertRaises(RetryError) as cm:
            self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
            self.hook.get_jobs_for_job_group(JOB_ID)
        self.assertIsInstance(cm.exception.last_attempt.exception(), HTTPError)
        self.assertEqual(get_request_mock.call_count, 5)
