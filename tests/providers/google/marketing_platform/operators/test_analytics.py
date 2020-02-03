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

from airflow.providers.google.marketing_platform.operators.analytics import (
    GoogleAnalyticsGetAdsLinksOperator, GoogleAnalyticsListAccountsOperator,
)

API_VERSION = "api_version"
GCP_CONN_ID = "google_cloud_default"


class TestGoogleAnalyticsListAccountsOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    def test_execute(self, hook_mock):
        start_index = 1
        max_results = 1
        op = GoogleAnalyticsListAccountsOperator(
            start_index=start_index,
            max_results=max_results,
            api_version=API_VERSION,
            gcp_connection_id=GCP_CONN_ID,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once()
        hook_mock.return_value.list_accounts.assert_called_once()


class TestGoogleAnalyticsGetAdsLinksOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    def test_execute(self, hook_mock):
        account_id = "123456"
        web_property_id = "UA-123456-1"
        web_property_adwords_link_id = "AABBCCDDEEFFGG"
        op = GoogleAnalyticsGetAdsLinksOperator(
            account_id=account_id,
            web_property_id=web_property_id,
            web_property_adwords_link_id=web_property_adwords_link_id,
            api_version=API_VERSION,
            gcp_connection_id=GCP_CONN_ID,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once()
        hook_mock.return_value.get_adwords_links.assert_called_once()
