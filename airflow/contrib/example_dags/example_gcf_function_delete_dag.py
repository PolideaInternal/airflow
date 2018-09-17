
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
Example Airflow DAG that deletes a Google Cloud Function.
This DAG relies on the following Airflow variables
https://airflow.apache.org/concepts.html#variables
* PROJECT_ID - Google Cloud Project where the Cloud Function exists.
* REGION - Google Cloud Functions region where the function exists.
* ENTRYPOINT - Name of the executable function in the source code.
"""

import datetime

import airflow
from airflow import models
from airflow.contrib.operators.gcf_function_delete_operator \
    import GCFFunctionDeleteOperator

PROJECT_ID = models.Variable.get('PROJECT_ID', '')
REGION = models.Variable.get('REGION', '')
ENTRYPOINT = models.Variable.get('ENTRYPOINT', '')
# A fully-qualified name of the function to delete
FUNCTION_NAME = 'projects/{}/locations/{}/functions/{}'.format(PROJECT_ID, REGION,
                                                               ENTRYPOINT)

# [START howto_operator_gcf_delete_args]
default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}
# [END howto_operator_gcf_delete_args]

# [START howto_operator_gcf_delete]
with models.DAG(
    'example_gcf_function_delete_dag',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:

    t1 = GCFFunctionDeleteOperator(
        task_id="gcf_delete_task",
        name=FUNCTION_NAME
    )
