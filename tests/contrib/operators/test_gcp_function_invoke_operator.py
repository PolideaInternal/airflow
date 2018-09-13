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
from airflow.contrib.operators.gcp_function_operator import GcfFunctionInvokeOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = 'project-id'
LOCATION = 'location'
FUNCTION_NAME = 'helloWorld'


def _prepare_test_missing_params():
    return [
        ('', LOCATION, FUNCTION_NAME, "The required parameter 'project_id' is missing"),
        (PROJECT_ID, '', FUNCTION_NAME, "The required parameter 'location' is missing"),
        (PROJECT_ID, LOCATION, '', "The required parameter 'function_name' is missing")
    ]


def _response(status, content):
    response = Response()
    encoding = 'utf8'
    response.__setattr__('_content', content.encode(encoding))
    response.__setattr__('status_code', status)
    response.__setattr__('encoding', encoding)
    return response


class GcfFunctionInvokeTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcp_function_operator.GcfHook')
    def test_execute(self, mock_hook):
        mock_hook.return_value.invoke_function.return_value = _response(200, 'OK')
        op = GcfFunctionInvokeOperator(
            project_id=PROJECT_ID,
            location=LOCATION,
            function_name=FUNCTION_NAME,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once()
        mock_hook.return_value.invoke_function.assert_called_once()
        self.assertIn('OK', str(result))

    @parameterized.expand(_prepare_test_missing_params())
    def test_missing_params(self, project_id, location, function_name, msg):
        with self.assertRaises(AirflowException) as cm:
            GcfFunctionInvokeOperator(
                project_id=project_id,
                location=location,
                function_name=function_name,
                task_id="id"
            )
        err = cm.exception
        self.assertTrue(err)
        self.assertIn(msg, str(err))

    @mock.patch('airflow.contrib.operators.gcp_function_operator.GcfHook')
    def test_gcf_error(self, mock_hook):
        content = 'Function with this name does not exist in this project'
        mock_hook.return_value.invoke_function.return_value = _response(404, content)
        with self.assertRaises(AirflowException) as cm:
            op = GcfFunctionInvokeOperator(
                project_id=PROJECT_ID,
                location=LOCATION,
                function_name=FUNCTION_NAME,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        mock_hook.assert_called_once()
        mock_hook.return_value.invoke_function.assert_called_once()
        self.assertTrue(err)
        self.assertIn(content, str(err))
