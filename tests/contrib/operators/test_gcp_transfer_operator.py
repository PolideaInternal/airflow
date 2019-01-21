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
from botocore.credentials import ReadOnlyCredentials

from airflow import AirflowException, configuration
from airflow.contrib.operators.gcp_transfer_operator import GcpTransferServiceOperationsCancelOperator, \
    GcpTransferServiceOperationsResumeOperator, GcpTransferServiceOperationsListOperator, TransferJobPreprocessor, \
    GcpTransferServiceJobCreateOperator, GcpTransferServiceJobUpdateOperator, GcpTransferServiceOperationsGetOperator, \
    GcpTransferServiceOperationsPauseOperator
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

GCP_PROJECT_ID = 'project-id'

JOB_NAME = "job-name"
OPERATION_NAME = "operation-name"
AWS_BUCKET_NAME = "aws-bucket-name"
GCS_BUCKET_NAME = "gcp-bucket-name"

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

FILTER = {}

AWS_ACCESS_KEY = "test-key-1"
AWS_ACCESS_SECRET = "test-secret-1"

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

SOURCE_AWS = {"awsS3DataSource": {"bucketName": AWS_BUCKET_NAME}}
SOURCE_GCS = {"gcsDataSource": {"bucketName": AWS_BUCKET_NAME}}
SOURCE_HTTP = {"httpDataSource": {"list_url": "http://example.com"}}

VALID_TRANSFER_JOB = {
    "name": "jib-name",
    'description': "Description",
    'status': 'ENABLED',
    'schedule': {
        'scheduleStartDate': NATIVE_DATE,
        'scheduleEndDate': NATIVE_DATE,
        'startTimeOfDay': NATIVE_TIME,
    },
    'transferSpec': {
        "gcsDataSource": {"bucketName": GCS_BUCKET_NAME},
        'gcsDataSink': {'bucketName': GCS_BUCKET_NAME}
    }
}

VALID_OPERATION = {
    "name" : "operation-name"
}


class TransferJobPreprocessorTest(unittest.TestCase):

    def test_should_do_nothing_on_empty(self):
        body = {}
        TransferJobPreprocessor(
            body=body
        ).process_body()
        self.assertEqual(body, {})

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.AwsHook')
    def test_should_inject_aws_credentials(self, mock_hook):
        mock_hook.return_value.get_credentials.return_value = ReadOnlyCredentials(AWS_ACCESS_KEY, AWS_ACCESS_SECRET, None)

        body = {
            'transferSpec': SOURCE_AWS
        }
        TransferJobPreprocessor(
            body=body
        ).process_body()
        self.assertEqual(body['transferSpec']['awsS3DataSource']['awsAccessKey']['accessKeyId'], AWS_ACCESS_KEY)
        self.assertEqual(body['transferSpec']['awsS3DataSource']['awsAccessKey']['secretAccessKey'], AWS_ACCESS_SECRET)

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
            TransferJobPreprocessor(
                body=body
            ).process_body()
        err = cm.exception
        self.assertIn("You must choose only one data source", str(err))


class GcpStorageTransferJobCreateOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_job_get(self, mock_hook):
        mock_hook.return_value.create_transfer_job.return_value = VALID_TRANSFER_JOB
        body = deepcopy(VALID_TRANSFER_JOB)
        del(body['name'])

        op = GcpTransferServiceJobCreateOperator(
            body=body,
            task_id='task-id'
        )
        result = op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.create_transfer_job.assert_called_once_with(
            body=body
        )
        self.assertEqual(result, VALID_TRANSFER_JOB)


class GcpStorageTransferJobUpdateOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_job_update(self, mock_hook):
        mock_hook.return_value.update_transfer_job.return_value = VALID_TRANSFER_JOB
        body = {
            'transfer_job': {
                'description': 'example-name'
            },
            'update_transfer_job_field_mask': 'description',
        }

        op = GcpTransferServiceJobUpdateOperator(
            job_name=JOB_NAME,
            body=body,
            task_id='task-id'
        )
        result = op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.update_transfer_job.assert_called_once_with(
            job_name=JOB_NAME,
            body=body
        )
        self.assertEqual(result, VALID_TRANSFER_JOB)


class GpcStorageTransferOperationsGetOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_get(self, mock_hook):
        mock_hook.return_value.get_transfer_operation.return_value = VALID_OPERATION
        op = GcpTransferServiceOperationsGetOperator(
            operation_name=OPERATION_NAME,
            task_id='task-id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.get_transfer_operation.assert_called_once_with(
            operation_name=OPERATION_NAME,
        )
        self.assertEqual(result, VALID_OPERATION)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_get_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationsGetOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_get_should_throw_ex_when_operation_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationsGetOperator(
                operation_name="",
                task_id='task-id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationListOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_list(self, mock_hook):
        mock_hook.return_value.list_transfer_operations.return_value = [VALID_TRANSFER_JOB]
        op = GcpTransferServiceOperationsListOperator(
            filter=FILTER,
            task_id='task-id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.list_transfer_operations.assert_called_once_with(
            filter=FILTER
        )
        self.assertEqual(result, [VALID_TRANSFER_JOB])


class GcpStorageTransferOperationsPauseOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause(self, mock_hook):
        op = GcpTransferServiceOperationsPauseOperator(
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
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationsPauseOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_pause_should_throw_ex_when_operation_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationsPauseOperator(
                operation_name="",
                task_id='task-id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationsResumeOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume(self, mock_hook):
        op = GcpTransferServiceOperationsResumeOperator(
            operation_name=OPERATION_NAME,
            task_id='task-id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.resume_transfer_operation.assert_called_once_with(
            operation_name=OPERATION_NAME
        )
        self.assertTrue(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationsResumeOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_resume_should_throw_ex_when_operation_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationsResumeOperator(
                operation_name="",
                task_id='task-id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()


class GcpStorageTransferOperationsCancelOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel(self, mock_hook):
        op = GcpTransferServiceOperationsCancelOperator(
            operation_name=OPERATION_NAME,
            task_id='task-id'
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.cancel_transfer_operation.assert_called_once_with(
            operation_name=OPERATION_NAME
        )
        self.assertTrue(result)

    # Setting all of the operator's input parameters as templated dag_ids
    # (could be anything else) just to test if the templating works for all fields
    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel_with_templates(self, _):
        dag_id = 'test_dag_id'
        configuration.load_test_config()
        args = {
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(dag_id, default_args=args)
        op = GcpTransferServiceOperationsCancelOperator(
            operation_name='{{ dag.dag_id }}',
            gcp_conn_id='{{ dag.dag_id }}',
            api_version='{{ dag.dag_id }}',
            task_id='task-id',
            dag=self.dag
        )
        ti = TaskInstance(op, DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(dag_id, getattr(op, 'operation_name'))
        self.assertEqual(dag_id, getattr(op, 'gcp_conn_id'))
        self.assertEqual(dag_id, getattr(op, 'api_version'))

    @mock.patch('airflow.contrib.operators.gcp_transfer_operator.GCPTransferServiceHook')
    def test_operation_cancel_should_throw_ex_when_operation_name_none(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            op = GcpTransferServiceOperationsCancelOperator(
                operation_name="",
                task_id='task-id'
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The required parameter 'operation_name' is empty or None", str(err))
        mock_hook.assert_not_called()

