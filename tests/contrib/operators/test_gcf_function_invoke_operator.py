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

import unittest

from parameterized import parameterized
from requests import Response

from airflow import AirflowException
from airflow.contrib.operators.gcf_function_invoke_operator import \
    GCFFunctionInvokeOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = 'project-id'
REGION = 'region'
FUNCTION_NAME = 'helloWorld'


def _prepare_test_missing_params():
    return [
        ('', REGION, FUNCTION_NAME, "The required parameter 'project_id' is missing"),
        (PROJECT_ID, '', FUNCTION_NAME, "The required parameter 'region' is missing"),
        (PROJECT_ID, REGION, '', "The required parameter 'function_name' is missing")
    ]


def _response(status, content):
    response = Response()
    encoding = 'utf8'
    response.__setattr__('_content', content.encode(encoding))
    response.__setattr__('status_code', status)
    response.__setattr__('encoding', encoding)
    return response


class GCFFunctionInvokeTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcf_function_invoke_operator.GCFHook')
    def test_execute(self, mock_hook):
        mock_hook.return_value.invoke_function.return_value = _response(200, 'OK')
        op = GCFFunctionInvokeOperator(
            project_id=PROJECT_ID,
            region=REGION,
            function_name=FUNCTION_NAME,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once()
        mock_hook.return_value.invoke_function.assert_called_once()
        self.assertIn('OK', str(result))

    @parameterized.expand(_prepare_test_missing_params())
    def test_missing_params(self, project_id, region, function_name, msg):
        with self.assertRaises(AirflowException) as cm:
            GCFFunctionInvokeOperator(
                project_id=project_id,
                region=region,
                function_name=function_name,
                task_id="id"
            )
        err = cm.exception
        self.assertTrue(err)
        self.assertIn(msg, str(err))

    @mock.patch('airflow.contrib.operators.gcf_function_invoke_operator.GCFHook')
    def test_gcf_error(self, mock_hook):
        content = 'Function with this name does not exist in this project'
        mock_hook.return_value.invoke_function.return_value = _response(404, content)
        with self.assertRaises(AirflowException) as cm:
            op = GCFFunctionInvokeOperator(
                project_id=PROJECT_ID,
                region=REGION,
                function_name=FUNCTION_NAME,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        mock_hook.assert_called_once()
        mock_hook.return_value.invoke_function.assert_called_once()
        self.assertTrue(err)
        self.assertIn(content, str(err))
