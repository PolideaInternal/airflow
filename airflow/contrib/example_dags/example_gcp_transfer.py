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

import os

from airflow import models
from airflow.contrib.operators.gcp_function_operator \
    import GcfFunctionDeployOperator, GcfFunctionDeleteOperator
from airflow.contrib.operators.gcp_transfer_operator import GcpTransferServiceJobCreateOperator, \
    GcpTransferServiceJobUpdateOperator, GcpTransferServiceJobDeleteOperator
from airflow.utils import dates

# [START howto_operator_gcf_common_variables]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_DESCRIPTION = os.environ.get('GCP_DESCRIPTION', 'description')
GCT_TARGET_GCP_BUCKET = os.environ.get('GCT_TARGET_AWS_BUCKET', '')
# [END howto_operator_gcf_common_variables]

# [START howto_operator_gcf_deploy_variables]
GCT_SOURCE_AWS_BUCKET = os.environ.get('GCT_SOURCE_AWS_BUCKET', '')
GCT_SOURCE_GCP_BUCKET = os.environ.get('GCT_SOURCE_GCP_BUCKET', '')
GCT_SOURCE_HTTP_URL = os.environ.get('GCT_SOURCE_HTTP_URL', '')
# [END howto_operator_gcf_deploy_variables]

# [START howto_operator_gcf_deploy_body]
body = {
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
            "hours": 1,
            "minutes": 1
        }
    },
    "transferSpec": {
        "gcsDataSink": {
            "bucketName": GCT_TARGET_GCP_BUCKET
        },
    }
}
# [END howto_operator_gcf_deploy_body]

# [START howto_operator_gct_default_args]
default_args = {
    'start_date': dates.days_ago(1)
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
                    "the environment variable: GCT_SOURCE_AWS_BUCKET, "
                    "GCT_SOURCE_GCP_BUCKET, GCT_SOURCE_HTTP_URL")
# [END howto_operator_gct_source_variants]


with models.DAG(
    'example_gcp_transfer',
    default_args=default_args,
    schedule_interval=None  # Override to match your needs
) as dag:
    # [START howto_operator_gct_create]
    create_job = GcpTransferServiceJobCreateOperator(
        task_id="gct_create_task",
        body=body,
    )
    # [END howto_operator_gct_update]
    # # [START howto_operator_gct_update]
    # update_job = GpcStorageTransferJobUpdateOperator(
    #     task_id="gct_update_task",
    #     body=body
    # )
    # # [END howto_operator_gct_delete]
    # [START howto_operator_gct_delete]
    delete_job = GcpTransferServiceJobDeleteOperator(
        task_id="gct_delete_task",
        body=body
    )
    # [END howto_operator_gcf_delete]
    # create_job >> update_job >> delete_job
    create_job >> delete_job
