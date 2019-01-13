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

"""
Example Airflow DAG that displays interactions with Google Cloud Functions.
It creates a transfer job and then update it.

This DAG relies on the following OS environment variables

* GCP_PROJECT_ID - Google Cloud Project to use for the Google Cloud Transfer Service.
* GCP_TRANSFER_SOURCE_AWS_BUCKET - Amazon Web Services Storage bucket from which files are copied.
* GCP_TRANSFER_FIRST_TARGET_BUCKET - Google Cloud Storage bucket bucket to which files are copied from AWS.
  It is also a source bucket in next step
* GCP_TRANSFER_SECOND_TARGET_BUCKET - Google Cloud Storage bucket bucket to which files are copied


"""
import os
from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.hooks.gcp_transfer_hook import GcpTransferOperationStatus
from airflow.contrib.operators.gcp_transfer_operator import (
    GcpTransferServiceJobCreateOperator,
    GcpTransferServiceJobDeleteOperator,
    GcpTransferServiceJobUpdateOperator,
    GcpTransferServiceOperationsListOperator,
    GcpTransferServiceOperationGetOperator,
    GcpTransferServiceOperationPauseOperator,
    GcpTransferServiceOperationResumeOperator,
    GcpTransferServiceOperationCancelOperator)
from airflow.contrib.sensors.gcp_transfer_sensor import GCPTransferServiceWaitForJobStatusSensor
from airflow.utils.dates import days_ago

# [START howto_operator_gct_common_variables]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_DESCRIPTION = os.environ.get('GCP_DESCRIPTION', 'description')
GCP_TRANSFER_TARGET_BUCKET = os.environ.get('GCP_TRANSFER_TARGET_BUCKET')
WAIT_FOR_OPERATION_POKE_INTERVAL = os.environ.get('WAIT_FOR_OPERATION_POKE_INTERVAL', 5)

GCP_TRANSFER_SOURCE_AWS_BUCKET = os.environ.get('GCP_TRANSFER_SOURCE_AWS_BUCKET')
GCP_TRANSFER_FIRST_TARGET_BUCKET = os.environ.get('GCP_TRANSFER_FIRST_TARGET_BUCKET',
                                                  'gcp-transfer-first-target')
GCP_TRANSFER_SECOND_TARGET_BUCKET = os.environ.get('GCP_TRANSFER_SECOND_TARGET_BUCKET',
                                                   'gcp-transfer-second-target')
# [END howto_operator_gct_common_variables]

# [START howto_operator_gct_create_job_body_aws]
create_body_aws = {
    "description": GCP_DESCRIPTION,
    "status": "ENABLED",
    "projectId": GCP_PROJECT_ID,
    "schedule": {
        "scheduleStartDate": datetime(2015, 1, 1).date(),
        "scheduleEndDate": datetime(2020, 1, 1).date(),
        "startTimeOfDay": datetime.utcnow() + timedelta(minutes=2)
    },
    "transferSpec": {
        'awsS3DataSource': {
            'bucketName': GCP_TRANSFER_SOURCE_AWS_BUCKET
        },
        "gcsDataSink": {
            "bucketName": GCP_TRANSFER_FIRST_TARGET_BUCKET
        },
        "transferOptions": {
            "overwriteObjectsAlreadyExistingInSink": True
        },
    }
}
# [END howto_operator_gct_create_job_body_aws]

# [START howto_operator_gct_create_job_body_gcp]
create_body_gcs = {
    "description": GCP_DESCRIPTION,
    "status": "ENABLED",
    "projectId": GCP_PROJECT_ID,
    "schedule": {
        "scheduleStartDate": datetime(2015, 1, 1).date(),
        "scheduleEndDate": datetime(2020, 1, 1).date(),
        "startTimeOfDay": datetime.utcnow() + timedelta(minutes=2)
    },
    "transferSpec": {
        'gcsDataSource': {
            'bucketName': GCP_TRANSFER_FIRST_TARGET_BUCKET
        },
        "gcsDataSink": {
            "bucketName": GCP_TRANSFER_SECOND_TARGET_BUCKET
        },
        "transferOptions": {
            "overwriteObjectsAlreadyExistingInSink": True
        },
    }
}
# [END howto_operator_gct_create_job_body_gcp]

# [START howto_operator_gct_update_job_body]
update_body = {
    "projectId": GCP_PROJECT_ID,
    "transferJob": {
        "description": "%s_updated" % GCP_DESCRIPTION
    },
    "updateTransferJobFieldMask": "description"
}
# [END howto_operator_gct_update_job_body]

list_filter_dict = {
    "projectId": GCP_PROJECT_ID,
    "jobNames": []
}

# [START howto_operator_gct_default_args]
default_args = {
    'start_date': days_ago(1)
}
# [END howto_operator_gct_default_args]

with models.DAG(
    'example_gcp_transfer',
    default_args=default_args,
    schedule_interval=None  # Override to match your needs
) as dag:

    def next_dep(task, prev):
        prev >> task
        return task

    # [START howto_operator_gct_create_job]
    create_transfer_job_from_aws = GcpTransferServiceJobCreateOperator(
        task_id="create_transfer_job_from_aws",
        body=create_body_aws
    )
    # [END howto_operator_gct_create_job]

    prev_task = create_transfer_job_from_aws

    # [START howto_operator_gct_wat_operation]
    wait_for_operation_to_start = GCPTransferServiceWaitForJobStatusSensor(
        task_id="wait_for_operation_to_start",
        job_name="{{task_instance.xcom_pull('create_transfer_from_aws_job', key='return_value')['name']}}",
        project_id=GCP_PROJECT_ID,
        expected_statuses=[GcpTransferOperationStatus.IN_PROGRESS],
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL
    )
    # [END howto_operator_gct_wat_operation]

    prev_task = next_dep(wait_for_operation_to_start, prev_task)

    # [START howto_operator_gct_pause_operation]
    pause_operation = GcpTransferServiceOperationPauseOperator(
        task_id="pause_operation",
        operation_name="{{task_instance.xcom_pull("
                       "'wait_for_operation_to_start', key='sensed_operations')[0]['name']}}"
    )
    # [END howto_operator_gct_pause_operation]

    prev_task = next_dep(pause_operation, prev_task)

    # [START howto_operator_gct_update_job]
    update_job = GcpTransferServiceJobUpdateOperator(
        task_id="update_job",
        job_name="{{task_instance.xcom_pull('create_transfer_from_aws_job', key='return_value')['name']}}",
        body=update_body
    )
    # [END howto_operator_gct_update_job]

    prev_task = next_dep(update_job, prev_task)

    # [START howto_operator_gct_list_operations]
    list_operations = GcpTransferServiceOperationsListOperator(
        task_id="list_operations",
        filter={
            "project_id": GCP_PROJECT_ID,
            "job_names": ["{{task_instance.xcom_pull("
                          "'create_transfer_from_aws_job',"
                          " key='return_value')['name']}}"]
        }
    )
    # [END howto_operator_gct_list_operations]

    prev_task = next_dep(list_operations, prev_task)

    # [START howto_operator_gct_get_operation]
    get_operation = GcpTransferServiceOperationGetOperator(
        task_id="get_operation",
        operation_name="{{task_instance.xcom_pull('list_operations', key='return_value')[0]['name']}}"
    )
    # [END howto_operator_gct_get_operation]

    prev_task = next_dep(get_operation, prev_task)

    # [START howto_operator_gct_resume_operation]
    resume_operation = GcpTransferServiceOperationResumeOperator(
        task_id="resume_operation",
        operation_name="{{task_instance.xcom_pull('get_operation', key='return_value')['name']}}"
    )
    # [END howto_operator_gct_resume_operation]

    prev_task = next_dep(resume_operation, prev_task)

    # [START howto_operator_gct_wat_operation]
    wait_for_operation_to_end = GCPTransferServiceWaitForJobStatusSensor(
        task_id="wait_for_operation_to_end",
        job_name="{{task_instance.xcom_pull('create_transfer_from_aws_job', key='return_value')['name']}}",
        project_id=GCP_PROJECT_ID,
        expected_statuses=[GcpTransferOperationStatus.SUCCESS],
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL
    )
    # [END howto_operator_gct_wat_operation]

    prev_task = next_dep(wait_for_operation_to_end, prev_task)

    now = datetime.utcnow()
    job_time = now + timedelta(minutes=2)

    create_body_gcs['schedule']['startTimeOfDay'] = datetime.utcnow() + timedelta(minutes=2)

    create_transfer_from_gcp_job = GcpTransferServiceJobCreateOperator(
        task_id="create_transfer_from_gcp_job",
        body=create_body_gcs
    )

    prev_task = next_dep(create_transfer_from_gcp_job, prev_task)

    wait_for_second_operation_to_start = GCPTransferServiceWaitForJobStatusSensor(
        task_id="wait_for_second_operation_to_start",
        job_name="{{task_instance.xcom_pull('create_transfer_from_gcp_job', key='return_value')['name']}}",
        project_id=GCP_PROJECT_ID,
        expected_statuses=[GcpTransferOperationStatus.IN_PROGRESS],
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL
    )

    prev_task = next_dep(wait_for_second_operation_to_start, prev_task)

    # [START howto_operator_gct_cancel_operation]
    cancel_operation = GcpTransferServiceOperationCancelOperator(
        task_id="cancel_operation",
        operation_name="{{task_instance.xcom_pull("
                       "'wait_for_second_operation_to_start', key='sensed_operations')[0]['name']}}"
    )
    # [END howto_operator_gct_cancel_operation]

    prev_task = next_dep(cancel_operation, prev_task)

    # [START howto_operator_gct_delete_job]
    delete_transfer_from_aws_job = GcpTransferServiceJobDeleteOperator(
        task_id="delete_transfer_from_aws_job",
        job_name="{{task_instance.xcom_pull('create_transfer_from_aws_job', key='return_value')['name']}}",
        project_id=GCP_PROJECT_ID
    )
    # [END howto_operator_gct_delete_job]

    prev_task = next_dep(delete_transfer_from_aws_job, prev_task)

    delete_transfer_from_gcp_job = GcpTransferServiceJobDeleteOperator(
        task_id="delete_transfer_from_gcp_job",
        job_name="{{task_instance.xcom_pull('create_transfer_from_gcp_job', key='return_value')['name']}}",
        project_id=GCP_PROJECT_ID
    )

    prev_task = next_dep(delete_transfer_from_gcp_job, prev_task)
