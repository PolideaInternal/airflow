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

import json
import time
from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

# Magic constant:
# See:
# https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations/list
TRANSFER_OPERATIONS = 'transferOperations'
# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 10


class GcpTransferJobsStatus:
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    DELETED = "DELETED"


class GcpTransferOperationStatus:
    IN_PROGRESS = "IN_PROGRESS"
    PAUSED = "PAUSED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class GcpTransferSource:
    GCS = "GCS"
    AWS_S3 = "AWS_S3"
    HTTP = "HTTP"


NEGATIVE_STATUS = {
    GcpTransferOperationStatus.FAILED,
    GcpTransferOperationStatus.ABORTED
}

# Number of retries - used by googleapiclient method calls to perform retries
# For requests that are "retriable"
NUM_RETRIES = 5


# noinspection PyAbstractClass
class GCPTransferServiceHook(GoogleCloudBaseHook):
    """
    Hook for GCP Storage Transfer Service.
    """
    _conn = None

    def __init__(self,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GCPTransferServiceHook, self).__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """
        Retrieves connection to Google Storage Transfer service.

        :return: Google Storage Transfer service object
        :rtype: dict
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('storagetransfer', self.api_version,
                               http=http_authorized, cache_discovery=False)
        return self._conn

    def create_transfer_job(self, body):
        return self.get_conn()\
            .transferJobs()\
            .create(body=body)\
            .execute(num_retries=NUM_RETRIES)

    def get_transfer_job(self, job_name):
        return self.get_conn()\
            .transferJobs()\
            .list(jobName=job_name)\
            .execute(num_retries=NUM_RETRIES)

    def list_transfer_job(self, filter):
        return self.get_conn()\
            .transferJobs()\
            .list(filter=filter)\
            .execute(num_retries=NUM_RETRIES)

    def update_transfer_job(self, job_name, body):
        return self.get_conn()\
            .transferJobs()\
            .update(jobName=job_name, body=body)\
            .execute(num_retries=NUM_RETRIES)

    def delete_transfer_job(self, job_name):
        # This is a soft delete state. After a transfer job is set to this
        # state, the job and all the transfer executions are subject to garbage
        # collection. Transfer jobs become eligible for garbage collection
        # 30 days after their status is set to DELETED.
        return self.get_conn().transferJob().update(jobName=job_name, body={
            'status': GcpTransferJobsStatus.DELETED
        }).execute(num_retries=NUM_RETRIES)

    def cancel_transfer_operation(self, operation_name):
        return self.get_conn()\
            .transferOperations()\
            .cancel(name=operation_name)\
            .execute(num_retries=NUM_RETRIES)

    def delete_transfer_operation(self, operation_name):
        return self.get_conn()\
            .transferOperations()\
            .delete(name=operation_name)\
            .execute(num_retries=NUM_RETRIES)

    def get_transfer_operation(self, operation_name):
        return self.get_conn()\
            .transferOperations()\
            .list(name=operation_name)\
            .execute(num_retries=NUM_RETRIES)

    def list_transfer_operations(self, filter):
        conn = self.get_conn()

        operations = []

        request = conn.transferOperations().list(
            name=TRANSFER_OPERATIONS,
            filter=json.dumps(filter)
        )

        while request is not None:
            response = request.execute(num_retries=NUM_RETRIES)
            operations.extend(response['operations'])

            request = conn.transferOperations().list_next(
                previous_request=request,
                previous_response=response)

        return operations

    def pause_transfer_operation(self, operation_name):
        self.get_conn()\
            .transferOperations()\
            .pause(name=operation_name)\
            .execute(num_retries=NUM_RETRIES)

    def resume_transfer_operation(self, operation_name):
        self.get_conn()\
            .transferOperations()\
            .resume(name=operation_name)\
            .execute(num_retries=NUM_RETRIES)

    def wait_for_transfer_job(self,
                              job,
                              expected_status=GcpTransferOperationStatus.SUCCESS):
        while True:
            operations = self.list_transfer_operations(filter={
                'project_id': job['projectId'],
                'job_names': [job['name']]
            })

            if GCPTransferServiceHook.check_operations_result(operations,
                                                              expected_status):
                return True
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)

    @staticmethod
    def check_operations_result(operations, expected_status):
        if len(operations) == 0:
            return False

        for operation in operations:
            status = operation['metadata']['status']

            if expected_status not in NEGATIVE_STATUS \
               and status in NEGATIVE_STATUS:
                name = operation['name']
                # TODO: Better error message
                raise AirflowException('Operation {} {}'.format(
                    name, status))

            if status != expected_status:
                return False

        return True
