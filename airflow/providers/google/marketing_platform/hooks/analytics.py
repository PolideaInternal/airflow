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
from googleapiclient.discovery import Resource, build

from airflow.providers.google.cloud.hooks.base import CloudBaseHook


class GoogleAnalyticsHook(CloudBaseHook):
    """
    Hook for Google Analytics 360.
    """

    def __init__(
        self,
        api_version: str = "v3",
        gcp_connection_id: str = "google cloud default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_version = api_version
        self.gcp_connection_is = gcp_connection_id
        self._conn = None

    def get_conn(self) -> Resource:
        """
        Retrieves connection to Google Analytics 360.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "analytics",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def list_accounts(self, ) -> list:
        """
        Lists accounts list from Google Analytics 360.

        # :param max_result: The maximum number of accounts to include in this response.
        # :type: max_result: int
        # :param start_index: An index of the first account to retrieve
        # Use this parameter as a pagination mechanism along with the max-results parameter.
        # :type: start_index: int
        """
        # TODO: max_result: int, start_index: int -- waiting for G response if this params are supported

        self.log.info("Retrieving accounts list")

        # pylint: disable=no-member
        response = (
            self.get_conn()
            .management()
            .accounts()
            .list()
            .execute(num_retries=self.num_retries)
        )
        return self._list_emails_from_response(response)

    @staticmethod
    def _list_emails_from_response(response) -> list:
        """
        :param response
        :type response: list
        """
        result = [value for key, value in response.items() if key == "username"]
        return result
