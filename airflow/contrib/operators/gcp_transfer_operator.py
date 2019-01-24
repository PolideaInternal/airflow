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
#
from copy import deepcopy
from datetime import date, time

from airflow import AirflowException
try:
    from airflow.contrib.hooks.aws_hook import AwsHook
except ImportError:
    AwsHook = None
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class TransferJobPreprocessor:

    def __init__(self, body, aws_conn_id='aws_default'):
        self.body = body
        self.aws_conn_id = aws_conn_id

    def _verify_data_source(self):
        if 'transferSpec' not in self.body:
            return

        is_gcs = 'gcsDataSource' in self.body['transferSpec']
        is_aws_s3 = 'awsS3DataSource' in self.body['transferSpec']
        is_http = 'httpDataSource' in self.body['transferSpec']

        # if not is_gcs and not is_aws_s3 and not is_http:
        #     raise AirflowException("You must choose data source")
        if not is_gcs ^ is_aws_s3 ^ is_http:
            raise AirflowException("You must choose only one data source")

    def _inject_aws_credentials(self):
        if 'transferSpec' not in self.body or \
           'awsS3DataSource' not in self.body['transferSpec']:
            return

        aws_hook = AwsHook(self.aws_conn_id)
        aws_credentials = aws_hook.get_credentials()
        aws_access_key_id = aws_credentials.access_key
        aws_secret_access_key = aws_credentials.secret_key
        self.body['transferSpec']['awsS3DataSource']["awsAccessKey"] = {
            "accessKeyId": aws_access_key_id,
            "secretAccessKey": aws_secret_access_key
        }

    def _reformat_date(self, field_key):
        if field_key not in self.body['schedule']:
            return
        if isinstance(self.body['schedule'][field_key], date):
            self.body['schedule'][field_key] = self.\
                _convert_date(self.body['schedule'][field_key])

    def _reformat_time(self, field_key):
        if field_key not in self.body['schedule']:
            return
        if isinstance(self.body['schedule'][field_key], time):
            self.body['schedule'][field_key] = self.\
                _convert_time(self.body['schedule'][field_key])

    def _restrict_aws_credentials(self):
        if 'transferSpec' in self.body and \
           'awsS3DataSource' in self.body['transferSpec'] and \
           'awsAccessKey' in self.body['transferSpec']['awsS3DataSource']:
            raise AirflowException("Credentials in body is not allowed. "
                                   "To store credentials, use connections.")

    def _reformat_schedule(self):
        if 'schedule' not in self.body:
            return
        self._reformat_date('scheduleStartDate')
        self._reformat_date('scheduleEndDate')
        self._reformat_time('startTimeOfDay')

    def process_body(self):
        self._restrict_aws_credentials()
        self._verify_data_source()
        self._inject_aws_credentials()
        self._reformat_schedule()

    @staticmethod
    def _convert_date(field_date):
        return {
            'day': field_date.day,
            'month': field_date.month,
            'year': field_date.year
        }

    @staticmethod
    def _convert_time(time):
        return {
            "hours": time.hour,
            "minutes": time.minute,
            "seconds": time.second,
        }


class GcpTransferServiceJobsCreateOperator(BaseOperator):

    """
    Creates a transfer job that runs periodically.

    :param body: The request body, as described in
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/create#request-body
        With two additional improvements:
        * dates can be given in the form datetime.date
        * credentials to AWS  AWS should be stored in connection and
          indicated by the aws_conn_id parameter
    :type body: dict
    :param gcp_conn_id: The connection ID used to retrieve credentials to
        Amazon Web Service.
    :type gcp_conn_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    # :param validate_body: Whether the body should be validated.
    #     Defaults to True.
    # :type validate_body: bool
    """
    @apply_defaults
    def __init__(self,
                 body,
                 aws_conn_id='aws_default',
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):
        super(GcpTransferServiceJobsCreateOperator, self)\
            .__init__(*args, **kwargs)
        self.body = deepcopy(body)
        self.api_version = api_version
        self._hook = GCPTransferServiceHook(
            api_version=api_version,
            gcp_conn_id=gcp_conn_id,
        )
        self._preprocessor = TransferJobPreprocessor(
            body=self.body,
            aws_conn_id=aws_conn_id,
        )
        self._validate_inputs()

    def _validate_inputs(self):
        self._preprocessor.process_body()

    def execute(self, context):
        self._validate_inputs()
        return self._hook.create_transfer_job(
            body=self.body
        )


class GcpTransferServiceJobsUpdateOperator(BaseOperator):
    """
    Updates a transfer job that runs periodically.
    :param job_name: Name of the job to be updated
    :type job_name: str
    :param body: The request body, as described in
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
        With two additional improvements:
        * dates can be given in the form datetime.date
        * credentials to AWS  AWS should be stored in connection and
          indicated by the aws_conn_id parameter
    :type body: dict
    :param gcp_conn_id: The connection ID used to retrieve credentials to
        Amazon Web Service.
    :type gcp_conn_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    # :param validate_body: Whether the body should be validated.
    #     Defaults to True.
    # :type validate_body: bool
    """
    # [START gcp_transfer_job_update_template_fields]
    template_fields = ['job_name']
    # [END gcp_transfer_job_update_template_fields]

    @apply_defaults
    def __init__(self,
                 job_name,
                 body,
                 aws_conn_id='aws_default',
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.job_name = job_name
        self.body = body
        self._hook = GCPTransferServiceHook(
            api_version=api_version,
            gcp_conn_id=gcp_conn_id,
        )
        self._preprocessor = TransferJobPreprocessor(
            body=body['transfer_job'],
            aws_conn_id=aws_conn_id,
        )
        self._validate_inputs()

    def _validate_inputs(self):
        self._preprocessor.process_body()

    def execute(self, context):
        return self._hook.update_transfer_job(
            job_name=self.job_name,
            body=self.body,
        )


class GcpTransferServiceJobsDeleteOperator(BaseOperator):
    """
    Delete a transfer job.
    :param job_name: The name of the operation resource to be cancelled.
    :type job_name: str
    :type gcp_conn_id: str
    :param project_id: Optional, Google Cloud Platform Project ID.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str

    """
    # [START gcp_transfer_job_update_template_fields]
    template_fields = ['job_name']
    # [END gcp_transfer_job_update_template_fields]

    @apply_defaults
    def __init__(self,
                 job_name,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 project_id=None,
                 *args,
                 **kwargs):
        super(GcpTransferServiceJobsDeleteOperator, self)\
            .__init__(*args, **kwargs)
        self.job_name = job_name
        self.api_version = api_version
        self.project_id = project_id
        self._hook = GCPTransferServiceHook(
            api_version=api_version,
            gcp_conn_id=gcp_conn_id,
        )
        self._validate_inputs()

    def _validate_inputs(self):
        pass

    def execute(self, context):
        self._validate_inputs()
        return self._hook.delete_transfer_job(
            job_name=self.job_name,
            project_id=self.project_id
        )


class GcpTransferServiceOperationsGetOperator(BaseOperator):
    """
    Gets the latest state of a long-running operation in Google Storage Transfer
    Service.

    :param operation_name: Name of the transfer operation. Required.
    :type operation_name: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :param gcp_conn_id: The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_get_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_get_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        return hook.get_transfer_operation(
            operation_name=self.operation_name
        )


class GcpTransferServiceOperationsListOperator(BaseOperator):
    """
    Lists long-running operations in Google Storage Transfer
    Service that match the specified filter.

    :param filter: A list of query parameters specified as dict in the form of
        {"project_id" : "my_project_id", "job_names" : ["jobid1", "jobid2",...],
        "operation_names" : ["opid1", "opid2",...],
        "transfer_statuses":["status1", "status2",...]}. Since job_names,
        operation_names, and transfer_statuses support multiple values, they
        must be specified with array notation. job_names, operation_names, and
        transfer_statuses are optional.
    :type filter: dict
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :param gcp_conn_id: The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_get_template_fields]
    template_fields = ['filter']
    # [END gcp_transfer_operation_get_template_fields]

    def __init__(self,
                 filter,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.filter = filter
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        pass

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        return hook.list_transfer_operations(
            filter=self.filter
        )


class GcpTransferServiceOperationsPauseOperator(BaseOperator):
    """
    Pauses an transfer operation in Google Storage Transfer Service.

    :param operation_name: Name of the transfer operation.
    :type operation_name: str
    :param api_version:  API version used (e.g. v1).
    :type api_version: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_pause_template_fields]
    template_fields = ['operation_name']
    # [END gcp_transfer_operation_pause_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )

        return hook.pause_transfer_operation(
            operation_name=self.operation_name
        )


class GcpTransferServiceOperationsResumeOperator(BaseOperator):
    """
    Resumes an transfer operation in Google Storage Transfer Service.

    :param operation_name: Name of the transfer operation.
    :type operation_name: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :param gcp_conn_id: The connection ID used to connect to Google
    Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_resume_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_resume_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):

        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )

        return hook.resume_transfer_operation(
            operation_name=self.operation_name
        )


class GcpTransferServiceOperationsCancelOperator(BaseOperator):
    """
    Cancels an transfer operation in Google Storage Transfer Service.

    :param operation_name: Name of the transfer operation.
    :type operation_name: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :param gcp_conn_id: The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_cancel_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version',)
    # [END gcp_transfer_operation_cancel_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        return hook.cancel_transfer_operation(
            operation_name=self.operation_name
        )
