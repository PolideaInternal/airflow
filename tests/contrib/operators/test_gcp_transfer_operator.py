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
import itertools
import unittest
from copy import deepcopy
from datetime import date, time

from parameterized import parameterized
from botocore.credentials import Credentials

from airflow import AirflowException, configuration
from airflow.contrib.operators.gcp_transfer_operator import (
    GcpTransferServiceOperationCancelOperator,
    GcpTransferServiceOperationResumeOperator,
    GcpTransferServiceOperationsListOperator,
    TransferJobValidator,
    TransferJobPreprocessor,
    GcpTransferServiceJobCreateOperator,
    GcpTransferServiceJobUpdateOperator,
    GcpTransferServiceOperationGetOperator,
    GcpTransferServiceOperationPauseOperator,
    S3ToGoogleCloudStorageTransferOperator,
    GoogleCloudStorageToGoogleCloudStorageTransferOperator)
from airflow.models import TaskInstance, DAG
from airflow.utils import timezone


try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None
try:
    import boto3
except ImportError:
    boto3 = None

GCP_PROJECT_ID = 'project-id'
TASK_ID = 'task-id'

JOB_NAME = "job-name"
OPERATION_NAME = "operation-name"
AWS_BUCKET_NAME = "aws-bucket-name"
GCS_BUCKET_NAME = "gcp-bucket-name"
DESCRIPTION = "description"

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

FILTER = {'jobNames': [JOB_NAME]}

AWS_ACCESS_KEY_ID = "test-key-1"
AWS_ACCESS_SECRET = "test-secret-1"
AWS_ACCESS_KEY = {'accessKeyId': AWS_ACCESS_KEY_ID,
                  'secretAccessKey': AWS_ACCESS_SECRET}

NATIVE_DATE = date(2018, 10, 15)
DICT_DATE = {
    'day': 15,
    'month': 10,
    'year': 2018
}
NATIVE_TIME = time(hour=11, minute=42, second=43)
DICT_TIME = {
    'hours': 11,
    'minutes': 42,
    'seconds': 43,
}
SCHEDULE_NATIVE = {
    'scheduleStartDate': NATIVE_DATE,
    'scheduleEndDate': NATIVE_DATE,
    'startTimeOfDay': NATIVE_TIME,
}

SCHEDULE_DICT = {
    'scheduleStartDate': {'day': 15, 'month': 10, 'year': 2018},
    'scheduleEndDate': {'day': 15, 'month': 10, 'year': 2018},
    'startTimeOfDay': {
        'hours': 11, 'minutes': 42, 'seconds': 43}
}

SOURCE_AWS = {"awsS3DataSource": {"bucketName": AWS_BUCKET_NAME}}
SOURCE_GCS = {"gcsDataSource": {"bucketName": GCS_BUCKET_NAME}}
SOURCE_HTTP = {"httpDataSource": {"list_url": "http://example.com"}}

VALID_TRANSFER_JOB_BASE = {
    "name": JOB_NAME,
    'description': DESCRIPTION,
    'status': 'ENABLED',
    'schedule': SCHEDULE_DICT,
    'transferSpec': {
        'gcsDataSink': {'bucketName': GCS_BUCKET_NAME}
    }
}
VALID_TRANSFER_JOB_GCS = deepcopy(VALID_TRANSFER_JOB_BASE)
VALID_TRANSFER_JOB_GCS['transferSpec'].update(deepcopy(SOURCE_GCS))
VALID_TRANSFER_JOB_AWS = deepcopy(VALID_TRANSFER_JOB_BASE)
VALID_TRANSFER_JOB_AWS['transferSpec'].update(deepcopy(SOURCE_AWS))

VALID_TRANSFER_JOB_GCS = {
    "name": JOB_NAME,
    'description': DESCRIPTION,
    'status': 'ENABLED',
    'schedule': SCHEDULE_NATIVE,
    'transferSpec': {
        "gcsDataSource": {"bucketName": GCS_BUCKET_NAME},
        'gcsDataSink': {'bucketName': GCS_BUCKET_NAME}
    }
}

VALID_TRANSFER_JOB_RAW = {
    'description': DESCRIPTION,
    'status': 'ENABLED',
    'schedule': SCHEDULE_DICT,
    'transferSpec': {
        'gcsDataSink': {'bucketName': GCS_BUCKET_NAME}
    }
}

VALID_TRANSFER_JOB_GCS_RAW = deepcopy(VALID_TRANSFER_JOB_RAW)
VALID_TRANSFER_JOB_GCS_RAW['transferSpec'].update(SOURCE_GCS)
VALID_TRANSFER_JOB_AWS_RAW = deepcopy(VALID_TRANSFER_JOB_RAW)
VALID_TRANSFER_JOB_AWS_RAW['transferSpec'].update(deepcopy(SOURCE_AWS))
VALID_TRANSFER_JOB_AWS_RAW['transferSpec']['awsS3DataSource']['awsAccessKey'] = AWS_ACCESS_KEY


VALID_OPERATION = {
    "name": "operation-name"
}


class TransferJobValidatorTest(unittest.TestCase):

    def test_should_raise_exception_when_encounters_aws_credentials(self):
        body = {
            "transferSpec": {
                "awsS3DataSource": {
                    "awsAccessKey": AWS_ACCESS_KEY
                },
            }
        }
        with self.assertRaises(AirflowException) as cm:
            TransferJobValidator(
                body=body
            ).validate_body()
        err = cm.exception
        self.assertIn("Credentials in body is not allowed. "
                      "To store credentials, use connections.", str(err))

    def test_should_raise_exception_when_body_empty(self):
        body = None
        with self.assertRaises(AirflowException) as cm:
            TransferJobValidator(
                body=body
            ).validate_body()
        err = cm.exception
        self.assertIn("The required parameter 'body' is empty or None",
                      str(err))

    @parameterized.expand([
        (dict(itertools.chain(SOURCE_AWS.items(), SOURCE_GCS.items())),),
        (dict(itertools.chain(SOURCE_AWS.items(), SOURCE_HTTP.items())),),
        (dict(itertools.chain(SOURCE_GCS.items(), SOURCE_HTTP.items())),),
    ])
    def test_verify_data_source(self, transferSpec):
        body = {
            'transferSpec': transferSpec
        }

        with self.assertRaises(AirflowException) as cm:
            TransferJobValidator(
                body=body
            ).validate_body()
        err = cm.exception
        self.assertIn("You must choose only one data source", str(err))


class TransferJobPreprocessorTest(unittest.TestCase):

    def test_should_do_nothing_on_empty(self):
        body = {}
        TransferJobPreprocessor(
            body=body
        ).process_body()
        self.assertEqual(body, {})

    @unittest.skipIf(boto3 is None,
                     "Skipping test because boto3 is not available")
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_should_inject_aws_credentials(self, mock_hook):
        mock_hook.return_value.get_credentials.return_value = \
            Credentials(AWS_ACCESS_KEY_ID, AWS_ACCESS_SECRET, None)

        body = {
            'transferSpec': deepcopy(SOURCE_AWS)
        }
        body = TransferJobPreprocessor(
            body=body
        ).process_body()
        self.assertEqual(body['transferSpec']
                         ['awsS3DataSource']['awsAccessKey'], AWS_ACCESS_KEY)

    @parameterized.expand([
        ('scheduleStartDate',),
        ('scheduleEndDate',),
    ])
    def test_should_format_date_from_python_to_dict(self, field_attr):
        body = {
            'schedule': {
                field_attr: NATIVE_DATE
            },
        }
        TransferJobPreprocessor(
            body=body
        ).process_body()
        self.assertEqual(body['schedule'][field_attr], DICT_DATE)

    def test_should_format_time_from_python_to_dict(self):
        body = {
            'schedule': {
                'startTimeOfDay': NATIVE_TIME
            },
        }
        TransferJobPreprocessor(
            body=body
        ).process_body()
        self.assertEqual(body['schedule']['startTimeOfDay'], DICT_TIME)


class GcpStorageTransferJobCreateOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.'
                'gcp_transfer_operator.GCPTransferServiceHook')
    def test_job_create_gcs(self, mock_hook):
        mock_hook.return_value \
            .create_transfer_job.return_value = VALID_TRANSFER_JOB_GCS_RAW
        body = deepcopy(VALID_TRANSFER_JOB_GCS)
        del(body['name'])
        op = GcpTransferServiceJobCreateOperator(
            body=body,
            task_id=TASK_ID
        )
        op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

        mock_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_GCS_RAW
        )

    @mock.patch('airflow.contrib.operators.'
                'gcp_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_job_create_aws(self, aws_hook, mock_hook):
        aws_hook.return_value.get_credentials.return_value = \
            Credentials(AWS_ACCESS_KEY_ID, AWS_ACCESS_SECRET, None)
        body = deepcopy(VALID_TRANSFER_JOB_AWS)
        del(body['name'])
        op = GcpTransferServiceJobCreateOperator(
            body=body,
            task_id=TASK_ID
        )
        op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

        mock_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_AWS_RAW
        )

    @mock.patch('airflow.contrib.operators.'
                'gcp_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_job_create_multiple(self, gcp_hook, aws_hook):
        aws_hook.return_value.get_credentials.return_value = \
            Credentials(AWS_ACCESS_KEY_ID, AWS_ACCESS_SECRET, None)
        gcp_hook.return_value \
            .create_transfer_job.return_value = VALID_TRANSFER_JOB_GCS
        body = deepcopy(VALID_TRANSFER_JOB_AWS)

        op = GcpTransferServiceJobCreateOperator(
            body=body,
            task_id=TASK_ID
        )
        op.execute(None)
        op = GcpTransferServiceJobCreateOperator(
            body=body,
            task_id=TASK_ID
        )
        op.execute(None)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceJobCreateOperator(
            body={"description": "{{ dag.dag_id }}"},
            gcp_conn_id='{{ dag.dag_id }}',
            aws_conn_id='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'body')['description'])


class GcpStorageTransferJobUpdateOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.'
                'gcp_transfer_operator.GCPTransferServiceHook')
    def test_job_update(self, mock_hook):
        mock_hook.return_value.update_transfer_job.return_value = VALID_TRANSFER_JOB_GCS
        body = {
            'transferJob': {
                'description': 'example-name'
            },
            'updateTransferJobFieldMask': DESCRIPTION,
        }

        op = GcpTransferServiceJobUpdateOperator(
            job_name=JOB_NAME,
            body=body,
            task_id=TASK_ID
        )
        result = op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.update_transfer_job.assert_called_once_with(
            job_name=JOB_NAME,
            body=body
        )
        self.assertEqual(result, VALID_TRANSFER_JOB_GCS)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceJobUpdateOperator(
            job_name='{{ dag.dag_id }}',
            body={'transferJob': {"name": "{{ dag.dag_id }}"}},
            task_id='task-id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'body')['transferJob']['name'])
        self.assertEqual(dag_id, getattr(op, 'job_name'))


class GpcStorageTransferOperationsGetOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.'
                'gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_get(self, mock_hook):
        mock_hook.return_value\
            .get_transfer_operation.return_value = VALID_OPERATION
        op = GcpTransferServiceOperationGetOperator(
            operation_name=OPERATION_NAME,
            task_id=TASK_ID
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.get_transfer_operation.assert_called_once_with(
            operation_name=OPERATION_NAME,
        )
        self.assertEqual(result, VALID_OPERATION)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_get_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationGetOperator(
            operation_name='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))

    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_get_should_throw_ex_when_operation_name_none(self,
                                                                    mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationGetOperator(
                operation_name="",
                task_id=TASK_ID
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' "
                      "is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationListOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_list(self, mock_hook):
        mock_hook.return_value.list_transfer_operations.return_value = [
            VALID_TRANSFER_JOB_GCS
        ]
        op = GcpTransferServiceOperationsListOperator(
            filter=FILTER,
            task_id=TASK_ID
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.list_transfer_operations.assert_called_once_with(
            filter=FILTER
        )
        self.assertEqual(result, [VALID_TRANSFER_JOB_GCS])

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationsListOperator(
            filter={"job_names": ['{{ dag.dag_id }}']},
            gcp_conn_id='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'filter')['job_names'][0])
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))


class GcpStorageTransferOperationsPauseOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause(self, mock_hook):
        op = GcpTransferServiceOperationPauseOperator(
            operation_name=OPERATION_NAME,
            task_id='task-id'
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.pause_transfer_operation.assert_called_once_with(
            operation_name=OPERATION_NAME
        )

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationPauseOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))

    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause_should_throw_ex_when_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationPauseOperator(
                operation_name="",
                task_id='task-id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' "
                      "is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationsResumeOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operator'
                's.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume(self, mock_hook):
        op = GcpTransferServiceOperationResumeOperator(
            operation_name=OPERATION_NAME,
            task_id=TASK_ID
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value\
            .resume_transfer_operation.assert_called_once_with(
                operation_name=OPERATION_NAME
            )
        self.assertIsNone(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationResumeOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume_should_throw_ex_when_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationResumeOperator(
                operation_name="",
                task_id=TASK_ID,
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' "
                      "is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationsCancelOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel(self, mock_hook):
        op = GcpTransferServiceOperationCancelOperator(
            operation_name=OPERATION_NAME,
            task_id=TASK_ID
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value\
            .cancel_transfer_operation.assert_called_once_with(
                operation_name=OPERATION_NAME
            )
        self.assertIsNone(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationCancelOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel_should_throw_ex_when_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationCancelOperator(
                operation_name="",
                task_id=TASK_ID,
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class S3ToGoogleCloudStorageTransferOperatorTest(unittest.TestCase):
    def test_constructor(self):
        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        self.assertEqual(operator.task_id, TASK_ID)
        self.assertEqual(operator.s3_bucket, AWS_BUCKET_NAME)
        self.assertEqual(operator.gcs_bucket, GCS_BUCKET_NAME)
        self.assertEqual(operator.project_id, GCP_PROJECT_ID)
        self.assertEqual(operator.description, DESCRIPTION)
        self.assertEqual(operator.schedule, SCHEDULE_DICT)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = S3ToGoogleCloudStorageTransferOperator(
            s3_bucket='{{ dag.dag_id }}',
            gcs_bucket='{{ dag.dag_id }}',
            description='{{ dag.dag_id }}',
            object_conditions={'exclude_prefixes': ['{{ dag.dag_id }}']},
            gcp_conn_id='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 's3_bucket'))
        self.assertEqual(dag_id, getattr(op, 'gcs_bucket'))
        self.assertEqual(dag_id, getattr(op, 'description'))
        self.assertEqual(dag_id, getattr(op, 'object_conditions')['exclude_prefixes'][0])
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_execute(self, mock_aws_hook, mock_transfer_hook):
        mock_aws_hook.return_value.get_credentials.return_value = \
            Credentials(AWS_ACCESS_KEY_ID, AWS_ACCESS_SECRET, None)

        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_AWS_RAW
        )

        self.assertTrue(mock_transfer_hook.return_value.wait_for_transfer_job.called)

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_execute_skip_wait(self, mock_aws_hook, mock_transfer_hook):
        mock_aws_hook.return_value.get_credentials.return_value = \
            Credentials(AWS_ACCESS_KEY_ID, AWS_ACCESS_SECRET, None)

        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=AWS_BUCKET_NAME,
            gcs_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
            wait=False,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_AWS_RAW
        )

        self.assertFalse(mock_transfer_hook.return_value.wait_for_transfer_job.called)


class GoogleCloudStorageToGoogleCloudStorageTransferOperatorTest(unittest.TestCase):
    def test_constructor(self):
        operator = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        self.assertEqual(operator.task_id, TASK_ID)
        self.assertEqual(operator.source_bucket, GCS_BUCKET_NAME)
        self.assertEqual(operator.destination_bucket, GCS_BUCKET_NAME)
        self.assertEqual(operator.project_id, GCP_PROJECT_ID)
        self.assertEqual(operator.description, DESCRIPTION)
        self.assertEqual(operator.schedule, SCHEDULE_DICT)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all
    # fields
    @mock.patch('airflow.contrib.operators'
                '.gcp_transfer_operator.GCPTransferServiceHook')
    def test_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            source_bucket='{{ dag.dag_id }}',
            destination_bucket='{{ dag.dag_id }}',
            description='{{ dag.dag_id }}',
            object_conditions={'exclude_prefixes': ['{{ dag.dag_id }}']},
            gcp_conn_id='{{ dag.dag_id }}',
            task_id=TASK_ID,
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'source_bucket'))
        self.assertEqual(dag_id, getattr(op, 'destination_bucket'))
        self.assertEqual(dag_id, getattr(op, 'description'))
        self.assertEqual(dag_id, getattr(op, 'object_conditions')['exclude_prefixes'][0])
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_execute(self, mock_transfer_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            schedule=SCHEDULE_DICT,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_GCS_RAW
        )
        self.assertTrue(mock_transfer_hook.return_value.wait_for_transfer_job.called)

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_execute_skip_wait(self, mock_transfer_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            source_bucket=GCS_BUCKET_NAME,
            destination_bucket=GCS_BUCKET_NAME,
            description=DESCRIPTION,
            wait=False,
            schedule=SCHEDULE_DICT,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            body=VALID_TRANSFER_JOB_GCS_RAW
        )
        self.assertFalse(mock_transfer_hook.return_value.wait_for_transfer_job.called)
