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
It creates a function, invokes and then deletes it.

This DAG relies on the following Airflow variables
https://airflow.apache.org/concepts.html#variables
* PROJECT_ID - Google Cloud Project to use for the Cloud Function.
* REGION - Google Cloud Functions region where the function should be
  created.
* ENTRYPOINT - Name of the executable function in the source code.
* and one of the below:
    - SOURCE_ARCHIVE_URL - Path to the zipped source in Google Cloud Storage
    or
    (
        - SOURCE_UPLOAD_URL - Generated upload URL for the zipped source
        and
        - ZIP_PATH - Local path to the zipped source archive
    )
    or
    - SOURCE_REPOSITORY - The URL pointing to the hosted repository where the function is
    defined in a supported Cloud Source Repository URL format
    https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#SourceRepository
"""

import datetime

import airflow
from airflow import models
from airflow.contrib.operators.gcf_function_delete_operator import GCFFunctionDeleteOperator  # nopep8
from airflow.contrib.operators.gcf_function_deploy_operator import GCFFunctionDeployOperator  # nopep8
from airflow.contrib.operators.gcf_function_invoke_operator import GCFFunctionInvokeOperator  # nopep8

PROJECT_ID = models.Variable.get('PROJECT_ID', '')
REGION = models.Variable.get('REGION', '')
SOURCE_ARCHIVE_URL = models.Variable.get('SOURCE_ARCHIVE_URL', '')
SOURCE_UPLOAD_URL = models.Variable.get('SOURCE_UPLOAD_URL', '')
SOURCE_REPOSITORY = models.Variable.get('SOURCE_REPOSITORY', '')
ZIP_PATH = models.Variable.get('ZIP_PATH', '')
ENTRYPOINT = models.Variable.get('ENTRYPOINT', '')
FUNCTION_NAME = 'projects/{}/locations/{}/functions/{}'.format(PROJECT_ID, REGION,
                                                               ENTRYPOINT)
RUNTIME = 'nodejs6'
body = {
    "name": FUNCTION_NAME,
    "entryPoint": ENTRYPOINT,
    "runtime": RUNTIME,
    "httpsTrigger": {}
}

# [START howto_operator_gcf_invoke_args]
default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'project_id': PROJECT_ID,
    'region': REGION
}
# [END howto_operator_gcf_invoke_args]

if SOURCE_ARCHIVE_URL:
    body['sourceArchiveUrl'] = SOURCE_ARCHIVE_URL
elif SOURCE_REPOSITORY:
    body['sourceRepository'] = {
        'url': SOURCE_REPOSITORY
    }
elif ZIP_PATH:
    body['sourceUploadUrl'] = SOURCE_UPLOAD_URL
    default_args['zip_path'] = ZIP_PATH

with models.DAG(
    'example_gcf_function_deploy_invoke_delete',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    deploy_task = GCFFunctionDeployOperator(
        body=body,
        task_id="gcf_deploy_task"
    )

    # [START howto_operator_gcf_invoke]
    invoke_task = GCFFunctionInvokeOperator(
        function_name=ENTRYPOINT,
        task_id="gcf_invoke_task"
    )
    # [END howto_operator_gcf_invoke]

    delete_task = GCFFunctionDeleteOperator(
        name=FUNCTION_NAME,
        task_id="gcf_delete_task"
    )

    deploy_task >> invoke_task >> delete_task
