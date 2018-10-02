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
from googleapiclient.discovery import build

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

# Number of retries - used by googleapiclient method calls to perform retries
# For requests that are "retriable"
NUM_RETRIES = 5

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 1


class CloudSqlOperationStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    UNKNOWN = "UNKNOWN"


# noinspection PyAbstractClass
class CloudSqlHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud SQL APIs.
    """
    _conn = None

    def __init__(self,
                 api_version,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(CloudSqlHook, self).__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """
        Retrieves connection to Cloud SQL.

        :return: Google Cloud SQL services object.
        :rtype: dict
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('sqladmin', self.api_version,
                               http=http_authorized, cache_discovery=False)
        return self._conn

    def get_instance(self, project_id, instance_id):
        """
        Retrieves a resource containing information about a Cloud SQL instance.

        :param project_id: Project ID of the project that contains the instance.
        :type project_id: str
        :param instance_id: Database instance ID. This does not include the project ID.
        :type instance_id: str
        :return: A Cloud SQL instance resource.
        :rtype: dict
        """
        return self.get_conn().instances().get(
            project=project_id,
            instance=instance_id
        ).execute(num_retries=NUM_RETRIES)

    def insert_instance(self, project_id, body):
        """
        Inserts a Cloud SQL instance defined by project_id and body.

        :param project_id: Project ID of the project where the instance will be created.
        :type project_id: str
        :param body: Body required by the Cloud SQL insert API, as described in
            https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/insert#request-body
        :type body: dict
        :return: True if the operation succeeded, raises an error otherwise
        :rtype: bool
        """
        response = self.get_conn().instances().insert(
            project=project_id,
            body=body
        ).execute(num_retries=NUM_RETRIES)
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(project_id, operation_name)

    def patch_instance(self, project_id, body, instance_id):
        """
        Partially updates a Cloud SQL instance defined by project_id, body and
        instance_id.

        :param project_id: Project ID of the project where the instance exists.
        :type project_id: str
        :param body: Body required by the Cloud SQL patch API, as described in
            https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/patch#request-body
        :type body: dict
        :param instance_id: Name of the Cloud SQL instance. This does not include the
            project ID.
        :type instance_id: str
        :return: True if the operation succeeded, raises an error otherwise
        :rtype: bool
        """
        response = self.get_conn().instances().patch(
            project=project_id,
            instance=instance_id,
            body=body
        ).execute(num_retries=NUM_RETRIES)
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(project_id, operation_name)

    def delete_instance(self, project_id, instance_id):
        """
        Deletes a Cloud SQL instance defined by project_id and instance_id.

        :param project_id: Project ID of the project where the instance exists.
        :type project_id: str
        :param instance_id: Name of the Cloud SQL instance. This does not include the
            project ID.
        :type instance_id: str
        :return: True if the operation succeeded, raises an error otherwise
        :rtype: bool
        """
        response = self.get_conn().instances().delete(
            project=project_id,
            instance=instance_id,
        ).execute(num_retries=NUM_RETRIES)
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(project_id, operation_name)

    def _wait_for_operation_to_complete(self, project_id, operation_name):
        """
        Waits for the named operation to complete - checks status of the
        asynchronous call.

        :param operation_name: name of the operation
        :type operation_name: str
        :return: response returned by the operation
        :rtype: dict
        """
        service = self.get_conn()
        while True:
            operation_response = service.operations().get(
                project=project_id,
                operation=operation_name,
            ).execute(num_retries=NUM_RETRIES)
            if operation_response.get("status") == CloudSqlOperationStatus.DONE:
                error = operation_response.get("error")
                if error:
                    # Extracting the errors list as string and trimming square braces
                    error_msg = str(error.get("errors"))[1:-1]
                    raise AirflowException(error_msg)
                # No meaningful info to return from the response in case of success
                return True
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)
