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
https://airflow.apache.org/concepts.html#variables
* GCP_PROJECT_ID - Google Cloud Project to use for the Google Cloud Transfer Service.
* GCP_SOURCE_BUCKET - Google Cloud Storage bucket from which files are copied.
* GCP_TARGET_BUCKET - Google Cloud Storage bucket bucket to which files are copied

"""
import datetime
import json
import os

import airflow
from airflow import models
from airflow.contrib.hooks.gcp_transfer_hook import GcpTransferOperationStatus
from airflow.contrib.operators.gcp_transfer_operator import GcpTransferServiceJobsCreateOperator, \
    GcpTransferServiceJobsDeleteOperator, GcpTransferServiceJobsUpdateOperator, \
    GcpTransferServiceOperationsListOperator, GcpTransferServiceOperationsPauseOperator, \
    GcpTransferServiceOperationsGetOperator, GcpTransferServiceOperationsResumeOperator, \
    GcpTransferServiceOperationsCancelOperator
from airflow.contrib.sensors.gcp_transfer_sensor import GcpStorageTransferOperationWaitForJobStatusSensor
from airflow.utils import dates

# [START howto_operator_gcf_common_variables]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_DESCRIPTION = os.environ.get('GCP_DESCRIPTION', 'description')
GCP_TRANSFER_TARGET_BUCKET = os.environ.get('GCP_TRANSFER_TARGET_BUCKET')
# [END howto_operator_gcf_common_variables]

# [START howto_operator_gcf_deploy_variables]
GCP_TRANSFER_SOURCE_AWS_BUCKET = os.environ.get('GCP_TRANSFER_SOURCE_AWS_BUCKET')
GCP_TRANSFER_SOURCE_GCP_BUCKET = os.environ.get('GCP_TRANSFER_SOURCE_GCP_BUCKET')
GCP_TRANSFER_SOURCE_HTTP_URL = os.environ.get('GCP_TRANSFER_SOURCE_HTTP_URL', '')
# [END howto_operator_gcf_deploy_variables]

now = datetime.datetime.now()
job_time = now + datetime.timedelta(minutes=7)

# [START howto_operator_gcf_deploy_body]
create_body = {
    "description": GCP_DESCRIPTION,
    "status": "ENABLED",
    "projectId": GCP_PROJECT_ID,
    "schedule": {
        "scheduleStartDate": {
            "day": 1,
            "month": 1,
            "year": 2015
        },
        "scheduleEndDate": {
            "day": 1,
            "month": 1,
            "year": 2020
        },
        "startTimeOfDay": {
            "hours": job_time.hour,
            "minutes": job_time.minute
        }
    },
    "transferSpec": {
        "gcsDataSink": {
            "bucketName": GCP_TRANSFER_TARGET_BUCKET
        },
    }
}

update_body = {
    "project_id": GCP_PROJECT_ID,
    "transfer_job": {
        "description": "%s_updated" % GCP_DESCRIPTION
    },
    "update_transfer_job_field_mask": "description"
}

list_filter_dict = {
    "project_id": GCP_PROJECT_ID,
    "job_names": []
}

# [END howto_operator_gcf_deploy_body]

# [START howto_operator_gct_default_args]
default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}
# [END howto_operator_gct_default_args]

# [START howto_operator_gct_source_variants]
if GCT_SOURCE_AWS_BUCKET:
    body['aws_s3_data_source'] = {
        'bucketName': GCT_SOURCE_AWS_BUCKET
    }
elif GCT_SOURCE_GCP_BUCKET:
    body['aws_s3_data_source'] = {
        'bucketName': GCT_SOURCE_GCP_BUCKET
    }
elif GCT_SOURCE_HTTP_URL:
    body['http_data_source'] = {
        'list_url': GCT_SOURCE_HTTP_URL
    }
else:
    raise Exception("Please provide one of data_source. You must set one of "
                    "the environment variable: GCP_TRANSFER_SOURCE_AWS_BUCKET, "
                    "GCP_TRANSFER_SOURCE_GCP_BUCKET, GCP_TRANSFER_SOURCE_HTTP_URL")
# [END howto_operator_gct_source_variants]


with models.DAG(
    'example_gcp_transfer',
    default_args=default_args,
    schedule_interval=None  # Override to match your needs
) as dag:

    def next_dep(task, prev):
        prev >> task
        return task

    # [START howto_operator_gct_create]
    create_job = GcpTransferServiceJobsCreateOperator(
        task_id="create_job",
        body=create_body
    )
    # [END howto_operator_gct_update]
    prev_task = create_job

    # [START howto_operator_gct_update]
    update_job = GcpTransferServiceJobsUpdateOperator(
        task_id="update_job",
        job_name="{{task_instance.xcom_pull('create_job', key='return_value')['name']}}",
        body=update_body
    )

    wait_for_operation_to_start = GcpStorageTransferOperationWaitForJobStatusSensor(
        task_id="wait_for_operation_to_start",
        operation_name=None,
        job_name="{{task_instance.xcom_pull('create_job', key='return_value')['name']}}",
        project_id=GCP_PROJECT_ID,
        expected_status=GcpTransferOperationStatus.IN_PROGRESS,
    )

    list_operations = GcpTransferServiceOperationsListOperator(
        task_id="list_operations",
        filter={
            "project_id": GCP_PROJECT_ID,
            "job_names": ["{{task_instance.xcom_pull('create_job', key='return_value')['name']}}]"]
        }
    )

    pause_operation = GcpTransferServiceOperationsPauseOperator(
        task_id="pause_operation",
        operation_name=""
    )

    get_operation = GcpTransferServiceOperationsGetOperator(
        task_id="get_operation",
        operation_name=""
    )

    resume_operation = GcpTransferServiceOperationsResumeOperator(
        task_id="resume_operation",
        operation_name=""
    )

    cancel_operation = GcpTransferServiceOperationsCancelOperator(
        task_id="cancel_operation",
        operation_name=""
    )

    # # [START howto_operator_gct_delete]
    delete_job = GcpTransferServiceJobsDeleteOperator(
        task_id="delete_job",
        job_name="{{task_instance.xcom_pull('create_job', key='return_value')['name']}}",
        project_id=GCP_PROJECT_ID
    )
    # [END howto_operator_gcf_delete]
