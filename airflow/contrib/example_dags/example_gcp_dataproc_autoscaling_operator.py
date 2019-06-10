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
Example Airflow DAG that creates autoscaling policy in Google Dataproc.

"""

import os
import airflow
from airflow import models

from airflow.contrib.operators.dataproc_operator import (
    DataprocAutoscalingPolicyCreateOperator,
    DataprocAutoscalingPolicyDeleteOperator,
    # DataprocClusterCreateOperator,
    # DataprocClusterDeleteOperator
)

default_args = {'start_date': airflow.utils.dates.days_ago(1)}


CLUSTER_NAME = os.environ.get('GCP_DATAPROC_CLUSTER_NAME', 'example-project')
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'an-id')
REGION = os.environ.get('GCP_LOCATION', 'europe-west1')
POLICY_ID = 'supertest-policy3'
WORKERS_NUM = 2


with models.DAG(
    dag_id='example_auto_scaling_policy',
    default_args=default_args,
    schedule_interval=None
) as dag:
    create_policy = DataprocAutoscalingPolicyCreateOperator(
        task_id='create_policy',
        project_id=PROJECT_ID,
        policy_id=POLICY_ID,
        region=REGION,
        primary_max=WORKERS_NUM,
        secondary_max=WORKERS_NUM,
        graceful_decommission_timeout='1h',
        scale_up_factor=0.1,
        scale_down_factor=0.1
    )

    # create_cluster = DataprocClusterCreateOperator(
    #     task_id="create_cluster",
    #     cluster_name=CLUSTER_NAME,
    #     project_id=PROJECT_ID,
    #     region=REGION,
    #     num_workers=WORKERS_NUM,
    # )
    #
    # delete_cluster = DataprocClusterDeleteOperator(
    #     task_id="delete_cluster",
    #     project_id=PROJECT_ID,
    #     cluster_name=CLUSTER_NAME,
    #     region=REGION
    # )

    delete_policy = DataprocAutoscalingPolicyDeleteOperator(
        task_id="delete_policy",
        project_id=PROJECT_ID,
        policy_id=POLICY_ID,
        region=REGION,
    )

    create_policy >> delete_policy
