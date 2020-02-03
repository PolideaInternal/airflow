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
"""
This module contains Google Analytics 360 operators.
"""

from airflow.models import BaseOperator
from airflow.providers.google.marketing_platform.hooks.analytics import GoogleAnalyticsHook
from airflow.utils.decorators import apply_defaults


class GoogleAnalyticsListAccountsOperator(BaseOperator):
    """
    Lists all accounts to which the user has access.

    .. seealso::
        Check official API docs:
        https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/accounts/list

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsListAccountsOperator`

    :param max_results: The maximum number of accounts to include in this response.
    :type: max_results: int
    :param: start_index: An index of the first account to retrieve
        Use this parameter as a pagination mechanism along with the max-results parameter.
    :type: start_index: int
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    """

    template_fields = ("api_version", "gcp_connection_id",)

    @apply_defaults
    def __init__(self,
                 max_results: int,
                 start_index: int,
                 api_version: str = "v3",
                 gcp_connection_id: str = "google cloud default",
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.max_results = max_results
        self.start_index = start_index
        self.api_version = api_version
        self.gcp_connection_id = gcp_connection_id

    def execute(self, context):
        hook = GoogleAnalyticsHook(api_version=self.api_version,
                                   gcp_connection_id=self.gcp_connection_id)
        result = hook.list_accounts()
        return result
        # TODO: add params to hook max_results=self.max_results, start_index=self.start_index
