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
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook, \
    GcpTransferJobsStatus
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

try:
    from airflow.contrib.hooks.aws_hook import AwsHook
except ImportError:
    AwsHook = None


class TransferJobPreprocessor:

    def __init__(self, body, aws_conn_id='aws_default'):
        self.body = body
        self.aws_conn_id = aws_conn_id

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

    def _reformat_schedule(self):
        if 'schedule' not in self.body:
            return
        self._reformat_date('scheduleStartDate')
        self._reformat_date('scheduleEndDate')
        self._reformat_time('startTimeOfDay')

    def process_body(self):
        self._inject_aws_credentials()
        self._reformat_schedule()
        return self.body

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


class TransferJobValidator:

    def __init__(self, body):
        self.body = body

    def _verify_data_source(self):
        if 'transferSpec' not in self.body:
            return

        is_gcs = 'gcsDataSource' in self.body['transferSpec']
        is_aws_s3 = 'awsS3DataSource' in self.body['transferSpec']
        is_http = 'httpDataSource' in self.body['transferSpec']

        if not is_gcs ^ is_aws_s3 ^ is_http:
            raise AirflowException("You must choose only one data source")

    def _restrict_aws_credentials(self):
        if 'transferSpec' in self.body and \
           'awsS3DataSource' in self.body['transferSpec'] and \
           'awsAccessKey' in self.body['transferSpec']['awsS3DataSource']:
            raise AirflowException("Credentials in body is not allowed. "
                                   "To store credentials, use connections.")

    def _restrict_empty_body(self):
        if not self.body:
            raise AirflowException("The required parameter 'body' "
                                   "is empty or None")

    def validate_body(self):
        self._restrict_empty_body()
        self._restrict_aws_credentials()
        self._verify_data_source()


class GcpTransferServiceJobCreateOperator(BaseOperator):
    """
    Creates a transfer job that runs periodically.

    .. warning::

        This operator is NOT idempotent. If you run it many times, many transfer
        job will be created on the Google Cloud Platform.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceJobCreateOperator`

    :param body: The request body, as described in
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/create#request-body
        With three additional improvements:

        * dates can be given in the form datetime.date
        * times can be given in the form datetime.time
        * credentials to AWS should be stored in connection and
          selected by the aws_conn_id parameter

    :type body: dict
    :param aws_conn_id: The connection ID used to retrieve credentials to
        Amazon Web Service.
    :type aws_conn_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :return: Google Storage Transfer service object
    :rtype: dict
    """
    # [START gcp_transfer_job_create_template_fields]
    template_fields = ('body', 'gcp_conn_id', 'aws_conn_id', )
    # [END gcp_transfer_job_create_template_fields]

    @apply_defaults
    def __init__(self,
                 body,
                 aws_conn_id='aws_default',
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):
        super(GcpTransferServiceJobCreateOperator, self)\
            .__init__(*args, **kwargs)
        self.body = deepcopy(body)
        self.aws_conn_id = aws_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        TransferJobValidator(
            body=self.body,
        ).validate_body()

    def execute(self, context):
        TransferJobPreprocessor(
            body=self.body,
            aws_conn_id=self.aws_conn_id,
        ).process_body()
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        return hook.create_transfer_job(
            body=self.body
        )


class GcpTransferServiceJobUpdateOperator(BaseOperator):
    """
    Updates a transfer job that runs periodically.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceJobUpdateOperator`

    :param job_name: Name of the job to be updated
    :type job_name: str
    :param body: The request body, as described in
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
        With three additional improvements:

        * dates can be given in the form datetime.date
        * times can be given in the form datetime.time
        * credentials to Amazon Web Service should be stored in connection and
          indicated by the aws_conn_id parameter

    :type body: dict
    :param aws_conn_id: The connection ID used to retrieve credentials to
        Amazon Web Service.
    :type aws_conn_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :return: Google Storage Transfer service object
    :rtype: dict
    """
    # [START gcp_transfer_job_update_template_fields]
    template_fields = ('job_name', 'body', 'gcp_conn_id', 'aws_conn_id', )
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
        super(GcpTransferServiceJobUpdateOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.body = body
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.aws_conn_id = aws_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        TransferJobValidator(
            body=self.body,
        ).validate_body()
        if not self.job_name:
            raise AirflowException("The required parameter 'job_name' "
                                   "is empty or None")

    def execute(self, context):
        TransferJobPreprocessor(
            body=self.body,
            aws_conn_id=self.aws_conn_id,
        ).process_body()
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        return hook.update_transfer_job(
            job_name=self.job_name,
            body=self.body,
        )


class GcpTransferServiceJobDeleteOperator(BaseOperator):
    """
    Delete a transfer job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceJobDeleteOperator`

    :param job_name: The name of the operation resource to be cancelled.
    :type job_name: str Name of the transfer job. Required.
    :type gcp_conn_id: str
    :param project_id: Optional, Google Cloud Platform Project ID.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    # [START gcp_transfer_job_delete_template_fields]
    template_fields = ('job_name', 'project_id', 'gcp_conn_id', )
    # [END gcp_transfer_job_delete_template_fields]

    @apply_defaults
    def __init__(self,
                 job_name,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 project_id=None,
                 *args,
                 **kwargs):
        super(GcpTransferServiceJobDeleteOperator, self)\
            .__init__(*args, **kwargs)
        self.job_name = job_name
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.job_name:
            raise AirflowException("The required parameter 'job_name' "
                                   "is empty or None")

    def execute(self, context):
        self._validate_inputs()
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        hook.delete_transfer_job(
            job_name=self.job_name,
            project_id=self.project_id
        )


class GcpTransferServiceOperationGetOperator(BaseOperator):
    """
    Gets the latest state of a long-running operation in Google Storage Transfer
    Service.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationGetOperator`

    :param operation_name: Name of the transfer operation. Required.
    :type operation_name: str
    :param gcp_conn_id: The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :return: Google Storage Transfer service object
    :rtype: dict
    """
    # [START gcp_transfer_operation_get_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', )
    # [END gcp_transfer_operation_get_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):
        super(GcpTransferServiceOperationGetOperator, self).__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
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
        operation = hook.get_transfer_operation(
            operation_name=self.operation_name,
        )
        self.log.info(operation)
        return operation


class GcpTransferServiceOperationsListOperator(BaseOperator):
    """
    Lists long-running operations in Google Storage Transfer
    Service that match the specified filter.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationsListOperator`

    :param filter: A request filter, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
    :type filter: dict
    :param gcp_conn_id: The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :return: List of Transfer Jobs
    :rtype: dict[]
    """
    # [START gcp_transfer_operations_list_template_fields]
    template_fields = ('filter', 'gcp_conn_id', )
    # [END gcp_transfer_operations_list_template_fields]

    def __init__(self,
                 filter,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):
        super(GcpTransferServiceOperationsListOperator, self).__init__(*args, **kwargs)
        self.filter = filter
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.filter:
            raise AirflowException("The required parameter 'filter' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        operations_list = hook.list_transfer_operations(
            filter=self.filter
        )
        self.log.info(operations_list)
        return operations_list


class GcpTransferServiceOperationPauseOperator(BaseOperator):
    """
    Pauses an transfer operation in Google Storage Transfer Service.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationPauseOperator`

    :param operation_name: Name of the transfer operation.
    :type operation_name: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param api_version:  API version used (e.g. v1).
    :type api_version: str
    :return: Google Storage Transfer service object
    :rtype: dict
    """
    # [START gcp_transfer_operation_pause_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', )
    # [END gcp_transfer_operation_pause_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):
        super(GcpTransferServiceOperationPauseOperator, self).__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
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
        hook.pause_transfer_operation(
            operation_name=self.operation_name
        )


class GcpTransferServiceOperationResumeOperator(BaseOperator):
    """
    Resumes an transfer operation in Google Storage Transfer Service.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationResumeOperator`

    :param operation_name: Name of the transfer operation.
    :type operation_name: str
    :param gcp_conn_id: The connection ID used to connect to Google
        Cloud Platform.
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_resume_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_resume_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):

        self.operation_name = operation_name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        super(GcpTransferServiceOperationResumeOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )

        hook.resume_transfer_operation(
            operation_name=self.operation_name
        )


class GcpTransferServiceOperationCancelOperator(BaseOperator):
    """
    Cancels an transfer operation in Google Storage Transfer Service.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationCancelOperator`

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
        super(GcpTransferServiceOperationCancelOperator, self).__init__(*args, **kwargs)
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
        hook.cancel_transfer_operation(
            operation_name=self.operation_name
        )


class S3ToGoogleCloudStorageTransferOperator(BaseOperator):
    """
    Synchronizes an S3 bucket with a Google Cloud Storage bucket using the
    GCP Storage Transfer Service.

    .. warning::

        This operator is NOT idempotent. If you run it many times, many transfer
        job will be created on the Google Cloud Platform.

    **Example**:

    .. code-block:: python

       s3_to_gcs_transfer_op = S3ToGoogleCloudStorageTransferOperator(
            task_id='s3_to_gcs_transfer_example',
            s3_bucket='my-s3-bucket',
            project_id='my-gcp-project',
            gcs_bucket='my-gcs-bucket',
            dag=my_dag)

    :param s3_bucket: The S3 bucket where to find the objects. (templated)
    :type s3_bucket: str
    :param gcs_bucket: The destination Google Cloud Storage bucket
        where you want to store the files. (templated)
    :type gcs_bucket: str
    :param project_id: Optional ID of the Google Cloud Platform Console project that
        owns the job
    :type project_id: str
    :param aws_conn_id: The source S3 connection
    :type aws_conn_id: str
    :param gcp_conn_id: The destination connection ID to use
        when connecting to Google Cloud Storage.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param description: Optional transfer service job description
    :type description: str
    :param schedule: Optional transfer service schedule;
        If not set, run transfer job once as soon as the operator runs
        The format is described
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs.
        With two additional improvements:

        * dates they can be passed as :class:`datetime.date`
        * times they can be passed as :class:`datetime.time`

    :type schedule: dict
    :param object_conditions: Optional transfer service object conditions; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec
    :type object_conditions: dict
    :param transfer_options: Optional transfer service transfer options; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec
    :type transfer_options: dict
    :param wait: Wait for transfer to finish
    :type wait: bool
    """

    template_fields = ('gcp_conn_id', 's3_bucket', 'gcs_bucket', 'description',
                       'object_conditions')
    ui_color = '#e09411'

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 gcs_bucket,
                 project_id=None,
                 aws_conn_id='aws_default',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 description=None,
                 schedule=None,
                 object_conditions=None,
                 transfer_options=None,
                 wait=True,
                 *args,
                 **kwargs):

        super(S3ToGoogleCloudStorageTransferOperator, self).__init__(
            *args,
            **kwargs)
        self.s3_bucket = s3_bucket
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self.aws_conn_id = aws_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.description = description
        self.schedule = schedule
        self.object_conditions = object_conditions
        self.transfer_options = transfer_options
        self.wait = wait

    def execute(self, context):
        transfer_hook = GCPTransferServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)
        body = {
            "description": self.description,
            "status": GcpTransferJobsStatus.ENABLED,
            "transferSpec": {
                'awsS3DataSource': {
                    'bucketName': self.s3_bucket,
                },
                'gcsDataSink': {
                    'bucketName': self.gcs_bucket,
                },
            }
        }

        if self.project_id is not None:
            body["project_id"] = self.project_id

        if self.schedule is not None:
            body['schedule'] = self.schedule

        if self.object_conditions is not None:
            body['transferSpec']['object_conditions'] = self.object_conditions

        if self.transfer_options is not None:
            body['transferSpec']['transferOptions'] = self.transfer_options

        TransferJobPreprocessor(
            body=body,
            aws_conn_id=self.aws_conn_id
        ).process_body()

        job = transfer_hook.create_transfer_job(
            body=body
        )

        if self.wait:
            transfer_hook.wait_for_transfer_job(job)


class GoogleCloudStorageToGoogleCloudStorageTransferOperator(BaseOperator):
    """
    Copies objects from a bucket to another using the GCP Storage Transfer
    Service.

    .. warning::

        This operator is NOT idempotent. If you run it many times, many transfer
        job will be created on the Google Cloud Platform.

    **Example**:

    .. code-block:: python

       gcs_to_gcs_transfer_op = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id='gcs_to_gcs_transfer_example',
            source_bucket='my-source-bucket',
            destination_bucket='my-destination-bucket',
            project_id='my-gcp-project',
            dag=my_dag)

    :param source_bucket: The source Google cloud storage bucket where the
         object is. (templated)
    :type source_bucket: str
    :param destination_bucket: The destination Google cloud storage bucket
        where the object should be. (templated)
    :type destination_bucket: str
    :param project_id: The ID of the Google Cloud Platform Console project that
        owns the job
    :type project_id: str
    :param gcp_conn_id: Optional connection ID to use when connecting to Google Cloud
        Storage.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param description: Optional transfer service job description
    :type description: str
    :param schedule: Optional transfer service schedule;
        If not set, run transfer job once as soon as the operator runs
        See:
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs.
        With two additional improvements:

        * dates they can be passed as :class:`datetime.date`
        * times they can be passed as :class:`datetime.time`

    :type schedule: dict
    :param object_conditions: Optional transfer service object conditions; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#ObjectConditions
    :type object_conditions: dict
    :param transfer_options: Optional transfer service transfer options; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#TransferOptions
    :type transfer_options: dict
    :param wait: Wait for transfer to finish; defaults to `True`
    :type wait: bool
    """

    template_fields = ('gcp_conn_id', 'source_bucket', 'destination_bucket',
                       'description', 'object_conditions')
    ui_color = '#e09411'

    @apply_defaults
    def __init__(self,
                 source_bucket,
                 destination_bucket,
                 project_id=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 description=None,
                 schedule=None,
                 object_conditions=None,
                 transfer_options=None,
                 wait=True,
                 *args,
                 **kwargs):

        super(GoogleCloudStorageToGoogleCloudStorageTransferOperator, self).__init__(
            *args,
            **kwargs)
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.description = description
        self.schedule = schedule
        self.object_conditions = object_conditions
        self.transfer_options = transfer_options
        self.wait = wait

    def execute(self, context):
        transfer_hook = GCPTransferServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)

        body = {
            "description": self.description,
            "status": GcpTransferJobsStatus.ENABLED,
            "transferSpec": {
                'gcsDataSource': {
                    'bucketName': self.source_bucket,
                },
                'gcsDataSink': {
                    'bucketName': self.destination_bucket,
                },
            }
        }

        for attr_name in ['project_id', 'schedule']:
            attr_value = getattr(self, attr_name)
            if attr_value is not None:
                body[attr_name] = attr_value

        for attr_name in ['object_conditions', 'transfer_options']:
            attr_value = getattr(self, attr_name)
            if attr_value is not None:
                body['transferSpec'][attr_name] = attr_value

        TransferJobPreprocessor(
            body=body
        ).process_body()

        job = transfer_hook.create_transfer_job(
            body=body
        )

        if self.wait:
            transfer_hook.wait_for_transfer_job(job)
