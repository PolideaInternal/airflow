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

import re

from googleapiclient.errors import HttpError

from airflow.contrib.hooks.gcf_hook import GCFHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

FUNCTION_NAME_PATTERN = '^projects/[^/]+/locations/[^/]+/functions/[^/]+$'


class GCFFunctionDeleteOperator(BaseOperator):
    """
    Delete a function with specified name from Google Cloud Functions.

    :param name: A fully-qualified function name, matching
        the pattern: `^projects/[^/]+/locations/[^/]+/functions/[^/]+$`
    :type name: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(self,
                 name,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args, **kwargs):
        self.name = name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        self.hook = GCFHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)
        super(GCFFunctionDeleteOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.name:
            raise AttributeError('Empty parameter: name')
        else:
            pattern = re.compile(FUNCTION_NAME_PATTERN)
            if not pattern.match(self.name):
                raise AttributeError(
                    'Parameter name must match pattern: {}'.format(FUNCTION_NAME_PATTERN))

    def execute(self, context):
        try:
            return self.hook.delete_function(self.name)
        except HttpError as e:
            status = e.resp.status
            if status == 404:
                self.log.warning('The function does not exist in this project')
            else:
                self.log.error('An error occurred. Exiting.')
                raise e
