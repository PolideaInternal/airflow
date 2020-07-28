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
This module contains Google Dataprep hook.
"""
import os

import requests
from tenacity import retry, stop_after_attempt

TOKEN = os.environ["DATAPREP_TOKEN"]
URL = "https://api.clouddataprep.com/v4/jobGroups/"


class GoogleDataprepHook:
    """
    Hook for connection with Dataprep API.

    """

    def __init__(self) -> None:
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {TOKEN}",
        }
        self.url = URL

    @retry(stop=stop_after_attempt(5))
    def get_jobs_for_job_group(self, job_id: int):
        """
        Get information about the batch jobs within a Cloud Dataprep job.

        :param job_id The ID of the job that will be fetched.
        :type job_id: int
        """

        url: str = f"{self.url}{job_id}/jobs"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
