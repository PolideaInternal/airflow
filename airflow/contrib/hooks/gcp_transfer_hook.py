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
import datetime
from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

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
        return self.get_conn().transferJobs().create(body=body).execute()

    def get_transfer_job(self, job_name):
        return self.get_conn().transferJobs().list(jobName=job_name).execute()

    def list_transfer_job(self, filter):
        return self.get_conn().transferJobs().list(filter=filter).execute()

    def update_transfer_job(self, job_name, body):
        self.get_conn().transferJobs().update(jobName=job_name, body=body).execute()

    def delete_transfer_job(self, job_name):
        # This is a soft delete state. After a transfer job is set to this
        # state, the job and all the transfer executions are subject to garbage
        # collection. Transfer jobs become eligible for garbage collection
        # 30 days after their status is set to DELETED.
        return self.get_conn().transferJob().update(jobName=job_name, body={
            'status': GcpTransferJobsStatus.DELETED
        }).execute()

    def cancel_transfer_operation(self, operation_name):
        return self.get_conn()\
            .transferOperations()\
            .cancel(name=operation_name)\
            .execute()

    def delete_transfer_operation(self, operation_name):
        return self.get_conn()\
            .transferOperations()\
            .delete(name=operation_name)\
            .execute()

    def get_transfer_operation(self,  operation_name):
        return self.get_conn()\
            .transferOperations()\
            .list(name=operation_name)\
            .execute()

    def list_transfer_operations(self, operation_name):
        conn = self.get_conn()

        operations = []

        request = conn.transferOperations().list(name=operation_name)
        while request is not None:
            response = request.execute()
            operations.extend(response['operations'])

            request = conn.transferOperations().list_next(
                previous_request=request,
                previous_response=response)

        return operations

    def pause_transfer_operation(self,  operation_name):
        self.get_conn()\
            .transferOperations()\
            .pause(name=operation_name)\
            .execute()

    def resume_transfer_operation(self,  operation_name):
        self.get_conn()\
            .transferOperations()\
            .resume(name=operation_name)\
            .execute()

    def wait_for_transfer_job(self, job):
        while True:
            result = self.get_conn()\
                .transferOperations()\
                .list(
                    name='transferOperations',
                    filter=json.dumps({
                        'project_id': job['projectId'],
                        'job_names': [job['name']],
                    })
                ).execute()
            if GCPTransferServiceHook._check_operations_result(result):
                return True
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)

    @staticmethod
    def _check_operations_result(result):
        operations = result.get('operations', [])
        if len(operations) == 0:
            return False
        for operation in operations:
            status = operation['metadata']['status']
            if status in {
                GcpTransferOperationStatus.FAILED,
                GcpTransferOperationStatus.ABORTED
            }:
                name = operation['name']
                # TODO: Better error message
                raise AirflowException('Operation {} {}'.format(
                    name, status))
            if status != GcpTransferOperationStatus.SUCCESS:
                return False
        return True
