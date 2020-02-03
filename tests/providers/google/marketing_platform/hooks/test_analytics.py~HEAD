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
from unittest import mock

from airflow.providers.google.marketing_platform.hooks.analytics import GoogleAnalyticsHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

API_VERSION = "v3"
GCP_CONN_ID = "google_cloud_default"


class TestGoogleAnalyticsHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.base.CloudBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleAnalyticsHook(API_VERSION, GCP_CONN_ID)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook._authorize"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.analytics.build"
    )
    def test_gen_conn(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "analytics",
            API_VERSION,
            http=mock_authorize.return_value,
            cache_discovery=False,
        )
        self.assertEqual(mock_build.return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_list_accounts(self, get_conn_mock):
        num_retries = 5
        self.hook.list_accounts()

        get_conn_mock.return_value. \
            management.return_value. \
            accounts.return_value. \
            list.return_value. \
            execute.assert_called_once_with(num_retries=num_retries)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_response_object_type_from_list_accounts(self, get_conn_mock):  # pylint: disable=unused-argument
        response_type = self.hook.list_accounts()
        assert isinstance(response_type, list), f"{response_type} is not a list type"
