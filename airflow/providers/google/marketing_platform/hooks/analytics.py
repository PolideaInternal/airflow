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
        gcp_connection_id: str = "google_cloud_default",
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

    def list_accounts(self):
        """
        Lists accounts list from Google Analytics 360.

        # :param max_result
        # :type integer
        # :param start_index
        # :type integer
        """

        self.log.info("Retrieving accounts list.")

        response = (
            self.get_conn()
            .management()
            .accounts()
            .list()
            .execute(num_retries=self.num_retries)
        )  # pylint: disable=no-member
        return self._list_emails_from_response(response)

    @staticmethod
    def _list_emails_from_response(response):
        result = [value for key, value in response.items() if key == "username"]
        return result

    def get_adwords_links(
        self, account_id: str, web_property_id: str, web_property_ad_words_link_id: str
    ) -> dict:
        """
        Returns a web property-Google Ads link to which the user has access.

        # :param account_id: ID of the account which the given web property belongs to.
        # :type string
        # :param web_property_id: Web property-Google Ads link ID.
        # :type string
        # :param web_property_ad_words_link_id: Web property ID to retrieve the Google Ads link for.
        # :type string
        """

        # pylint: disable=no-member
        self.log.info("Retrieving ad words links")
        ad_words_link = (
            self.get_conn()
            .management()
            .webPropertyAdWordsLinks()
            .get(
                accountId=account_id,
                webPropertyId=web_property_id,
                webPropertyAdWordsLinkId=web_property_ad_words_link_id,
            )
            .execute(num_retries=self.num_retries)
        )
        return ad_words_link
