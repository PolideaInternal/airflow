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
import os

from airflow.hooks.base_hook import BaseHook


class DataprepBaseHook(BaseHook):
    """
    A base hook for Dataprep cloud service to enable programmatic control over its objects.
    The Cloud Dataprep Platform supports a range of REST API endpoints across its objects.
    Most of the endpoints accept JSON as input and return JSON responses.
    This means that you must usually add the following headers to your request:
    Content-type: application/json
    Accept: application/json

    Authentication.
    API access tokens can be acquired and applied to your requests to obscure sensitive
    Personally Identifiable Information (PII) and are compliant with common privacy and security standards.
    These tokens last for a preconfigured time period and can be renewed as needed.
    You can create and delete access tokens through the Settings area of the application.
    With each request, you submit the token as part of the Authorization header.
    All references you can find here:
    https://cloud.google.com/dataprep/docs/references

    :param dataprep_conn_id: The connection ID to use when fetching connection info.
    :type dataprep_conn_id: str
    """

    token = os.environ["DATAPREP_TOKEN"]
    URL = "https://api.clouddataprep.com/v4/jobGroups/"

    def __init__(self, dataprep_conn_id: str = "dataprep_conn_id") -> None:
        super().__init__()
        self.dataprep_conn_id = dataprep_conn_id
        # self.access_token = self.get_connection(self.dataprep_conn_id).password
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        self.url = self.URL
