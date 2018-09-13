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

from airflow import AirflowException
from airflow.contrib.hooks.gcf_hook import GCFHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

FUNCTION_NAME_PATTERN = '^projects/[^/]+/locations/[^/]+/functions/[^/]+$'


class GCFFunctionInvokeOperator(BaseOperator):
    """
    Invoke a function with specified name from Google Cloud Functions.

    :param project_id: The Google Developers Console [project ID or project number]
    :type project_id: str
    :param region: The Google Cloud Platform region where the function is located
    :type region: string
    :param function_name: The name of the Cloud Function (short name, e.g. 'function-1')
    :type function_name: string
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(self,
                 project_id,
                 region,
                 function_name,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args, **kwargs):
        self._validate_inputs(project_id, region, function_name)
        self.function_url = self._create_function_url(region, project_id, function_name)
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        super(GCFFunctionInvokeOperator, self).__init__(*args, **kwargs)

    @staticmethod
    def _validate_inputs(project_id, region, function_name):
        if not project_id:
            raise AirflowException("The required parameter 'project_id' is missing")
        if not region:
            raise AirflowException("The required parameter 'region' is missing")
        if not function_name:
            raise AirflowException("The required parameter 'function_name' is missing")

    @staticmethod
    def _create_function_url(region, project_id, entrypoint):
        return 'https://{}-{}.cloudfunctions.net/{}'.format(region, project_id,
                                                            entrypoint)

    def execute(self, context):
        hook = GCFHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)
        resp = hook.invoke_function(url=self.function_url)
        if resp.status_code == 403:
            raise AirflowException(
                'Service account does not have permission to access the '
                'IAP-protected application.')
        elif resp.status_code != 200:
            raise AirflowException(
                'Bad response from application: {!r} / {!r} / {!r}'.format(
                    resp.status_code, resp.headers, resp.text))
        else:
            return resp.text
