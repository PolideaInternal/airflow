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

import time
import requests
from googleapiclient.discovery import build

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

NUM_RETRIES = 5


class GCFHook(GoogleCloudBaseHook):
    """Hook for Google Cloud Functions APIs."""
    _conn = None

    def __init__(self,
                 api_version,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GCFHook, self).__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """Returns a Google Cloud Functions service object."""
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('cloudfunctions', self.api_version,
                               http=http_authorized, cache_discovery=False)
        return self._conn

    def list_functions(self, location):
        list_response = self.get_conn().projects().locations().functions().list(
            parent=location).execute(num_retries=NUM_RETRIES)
        return list_response.get("functions", [])

    def create_new_function(self, location, body):
        response = self.get_conn().projects().locations().functions().create(
            location=location,
            body=body
        ).execute(num_retries=NUM_RETRIES)
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(operation_name)

    def update_function(self, name, body, update_mask):
        response = self.get_conn().projects().locations().functions().patch(
            updateMask=",".join(update_mask),
            name=name,
            body=body
        ).execute(num_retries=NUM_RETRIES)
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(operation_name)

    def upload_function_zip(self, parent, zip_path):
        response = self.get_conn().projects().locations().functions().generateUploadUrl(
            parent=parent
        ).execute(num_retries=NUM_RETRIES)
        upload_url = response.get('uploadUrl')
        with open(zip_path, 'rb') as fp:
            requests.put(
                url=upload_url,
                data=fp.read(),
                # Those two headers needs to be specified according to:
                # https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/generateUploadUrl
                # nopep8
                headers={
                    'Content-type': 'application/zip',
                    'x-goog-content-length-range': '0,104857600',
                }
            )
        return upload_url

    def delete_function(self, name):
        response = self.get_conn().projects().locations().functions().delete(
            name=name).execute(num_retries=NUM_RETRIES)
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(operation_name)

    def _wait_for_operation_to_complete(self, operation_name):
        service = self.get_conn()
        while True:
            operation_response = service.operations().get(
                name=operation_name,
            ).execute(num_retries=NUM_RETRIES)
            if operation_response.get("done"):
                return operation_response["response"]
            time.sleep(1)
