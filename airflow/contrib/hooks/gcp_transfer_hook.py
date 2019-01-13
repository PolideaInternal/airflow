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
from copy import deepcopy

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
    Hook for Google Storage Transfer Service.
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

    @GoogleCloudBaseHook.convert_http_exception
    def create_transfer_job(self, body):
        """
        Creates a transfer job that runs periodically.

        :param body: A request body, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
        :type body: dict
        :return: If successful, TransferJob.
        :rtype: dict
        """
        body = self._append_project_id(body, 'body project_id parameter')
        return self.get_conn()\
            .transferJobs()\
            .create(body=body)\
            .execute(num_retries=NUM_RETRIES)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    @GoogleCloudBaseHook.convert_http_exception
    def get_transfer_job(self, job_name, project_id=None):
        """
        Gets the latest state of a long-running operation in Google Storage
        Transfer Service.

        :param job_name: Name of the job to be fetched
        :type job_name: str
        :param project_id: The ID of the  GCP project.
        :type project_id: str
        :return: Transfer Job
        :rtype: dict
        """
        return self.get_conn()\
            .transferJobs()\
            .get(jobName=job_name, projectId=project_id)\
            .execute(num_retries=NUM_RETRIES)

    def list_transfer_job(self, filter):
        """
        Lists long-running operations in Google Storage Transfer
        Service that match the specified filter.

        :param filter: A request filter, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
        :type filter: dict
        :return: List of Transfer Jobs
        :rtype: list[dict]
        """
        conn = self.get_conn()
        filter = self._append_project_id(filter, 'filter project_id parameter')
        request = conn.transferJobs().list(
            filter=json.dumps(filter)
        )
        jobs = []

        while request is not None:
            response = request.execute(num_retries=NUM_RETRIES)
            jobs.extend(response['transferJobs'])

            request = conn.transferJobs().list_next(
                previous_request=request,
                previous_response=response)

        return jobs

    @GoogleCloudBaseHook.convert_http_exception
    def update_transfer_job(self, job_name, body):
        """
        Updates a transfer job that runs periodically.

        :param job_name: Name of the job to be updated
        :type job_name: str
        :param body: A request body, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
        :type body: dict
        :return: If successful, TransferJob.
        :rtype: dict
        """
        body = self._append_project_id(body, 'body project_id parameter')
        return self.get_conn()\
            .transferJobs()\
            .patch(
                jobName=job_name,
                body=body)\
            .execute(num_retries=NUM_RETRIES)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    @GoogleCloudBaseHook.convert_http_exception
    def delete_transfer_job(self, job_name, project_id):
        """
        Deletes a transfer job. This is a soft delete. After a transfer job is
        deleted, the job and all the transfer executions are subject to garbage
        collection. Transfer jobs become eligible for garbage collection
        30 days after soft delete.

        :param job_name: Name of the job to be deleted
        :type job_name: str
        :param project_id: The ID of the GCP project.
        :type project_id: str
        :rtype: None
        """

        return self.get_conn().transferJobs().patch(
            jobName=job_name,
            body={
                'project_id': project_id,
                'transfer_job': {
                    'status': GcpTransferJobsStatus.DELETED
                },
                'update_transfer_job_field_mask': 'status'
            },
        ).execute(num_retries=NUM_RETRIES)

    @GoogleCloudBaseHook.convert_http_exception
    def cancel_transfer_operation(self, operation_name):
        """
        Cancels an transfer operation in Google Storage Transfer Service.

        :param operation_name: Name of the transfer operation.
        :type operation_name: str
        :rtype: None
        """
        self.get_conn()\
            .transferOperations()\
            .cancel(name=operation_name)\
            .execute(num_retries=NUM_RETRIES)

    @GoogleCloudBaseHook.convert_http_exception
    def get_transfer_operation(self, operation_name):
        """
        Gets an transfer operation in Google Storage Transfer Service.

        :param operation_name: Name of the transfer operation.
        :type operation_name: str
        :return: transfer operation
        :rtype: dict
        """
        return self.get_conn()\
            .transferOperations()\
            .get(name=operation_name)\
            .execute(num_retries=NUM_RETRIES)

    @GoogleCloudBaseHook.convert_http_exception
    def list_transfer_operations(self, filter):
        """
        Gets an transfer operation in Google Storage Transfer Service.

        :param filter: A request filter, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
            With one additional improvement:

            * project_id is optional if you have a project id defined
              in the connection
              See: :ref:`connection-type-GCP`

        :type filter: dict
        :return: transfer operation
        :rtype: list[dict]
        """
        conn = self.get_conn()

        filter = self._append_project_id(filter, 'filter project_id parameter')

        operations = []

        request = conn.transferOperations().list(
            name=TRANSFER_OPERATIONS,
            filter=json.dumps(filter)
        )

        while request is not None:
            response = request.execute(num_retries=NUM_RETRIES)
            if 'operations' in response:
                operations.extend(response['operations'])

            request = conn.transferOperations().list_next(
                previous_request=request,
                previous_response=response)

        return operations

    @GoogleCloudBaseHook.convert_http_exception
    def pause_transfer_operation(self, operation_name):
        """
        Pauses an transfer operation in Google Storage Transfer Service.

        :param operation_name: Name of the transfer operation.
        :type operation_name: str
        :rtype: None
        """
        self.get_conn()\
            .transferOperations()\
            .pause(name=operation_name)\
            .execute(num_retries=NUM_RETRIES)

    @GoogleCloudBaseHook.convert_http_exception
    def resume_transfer_operation(self, operation_name):
        """
        Resumes an transfer operation in Google Storage Transfer Service.

        :param operation_name: Name of the transfer operation.
        :type operation_name: str
        :rtype: None
        """
        self.get_conn()\
            .transferOperations()\
            .resume(name=operation_name)\
            .execute(num_retries=NUM_RETRIES)

    @GoogleCloudBaseHook.convert_http_exception
    def wait_for_transfer_job(self,
                              job,
                              expected_status=[GcpTransferOperationStatus.SUCCESS]):
        """
        Waits until the job reaches the expected state.

        :param job: Transfer job
        :type job: dict
        :param expected_status: State that is expected
            See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
        :type expected_status: string
        :rtype: None
        """
        while True:
            operations = self.list_transfer_operations(filter={
                'project_id': job['projectId'],
                'job_names': [job['name']]
            })

            if GCPTransferServiceHook.check_operations_statuses(operations, expected_status):
                return
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)

    def _append_project_id(self, body, alternative):
        body = deepcopy(body)
        body['project_id'] = body.get('project_id', self.project_id)
        if not body['project_id']:
            raise AirflowException("The project id must be passed either as "
                                   "%s or as project_id extra in GCP "
                                   "connection definition. Both are not set!"
                                   % alternative)
        return body

    @staticmethod
    def check_operations_statuses(operations, expected_statuses):
        """
        Checks whether the operation list has an operation with the
        expected status, then returns true
        If it encounters operations in FAILED or ABORTED state
        throw :class:`airflow.exceptions.AirflowException`.

        :param operations: List of transfer operations to check.
        :type operations: list[dict]
        :param expected_statuses: statusUl that is expected
            See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
        :type expected_statuses: array[str]
        :return: If there is an operation with the expected state
            in the operation list, returns true,
        :raises: airflow.exceptions.AirflowException If it encounters operations
            with a state in the list,
        :rtype: bool
        """
        if len(operations) == 0:
            return False

        for operation in operations:
            status = operation['metadata']['status']

            if status in NEGATIVE_STATUS \
               and status not in expected_statuses:
                raise AirflowException('An unexpected operation status was '
                                       'encountered. Expected: %s, Found: %s',
                                       expected_statuses, status)

            if status not in expected_statuses and status is not GcpTransferOperationStatus.SUCCESS:
                return False

        return True
