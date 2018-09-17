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

from googleapiclient.errors import HttpError
from airflow.contrib.operators.gcf_function_delete_operator import \
    GCFFunctionDeleteOperator, FUNCTION_NAME_PATTERN

try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class GCFFunctionDeleteTest(unittest.TestCase):
    _FUNCTION_NAME = 'projects/project_name/locations/project_location/functions' \
                     '/function_name'
    _DELETE_FUNCTION_EXPECTED = {
        '@type': 'type.googleapis.com/google.cloud.functions.v1.CloudFunction',
        'name': _FUNCTION_NAME,
        'sourceArchiveUrl': 'gs://functions/hello.zip',
        'httpsTrigger': {
            'url': 'https://project_location-project_name.cloudfunctions.net'
                   '/function_name'},
        'status': 'ACTIVE', 'entryPoint': 'entry_point', 'timeout': '60s',
        'availableMemoryMb': 256,
        'serviceAccountEmail': 'project_name@appspot.gserviceaccount.com',
        'updateTime': '2018-08-23T00:00:00Z',
        'versionId': '1', 'runtime': 'nodejs6'}

    @mock.patch('airflow.contrib.operators.gcf_function_delete_operator.GCFHook')
    def test_delete_execute(self, mock_hook):
        mock_hook.return_value.delete_function.return_value = \
            self._DELETE_FUNCTION_EXPECTED
        op = GCFFunctionDeleteOperator(
            name=self._FUNCTION_NAME,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.delete_function.assert_called_once_with(
            'projects/project_name/locations/project_location/functions/function_name'
        )
        self.assertEqual(result['name'], self._FUNCTION_NAME)

    @mock.patch('airflow.contrib.operators.gcf_function_delete_operator.GCFHook')
    def test_correct_name(self, mock_hook):
        op = GCFFunctionDeleteOperator(
            name="projects/project_name/locations/project_location/functions"
                 "/function_name",
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

    @mock.patch('airflow.contrib.operators.gcf_function_delete_operator.GCFHook')
    def test_invalid_name(self, mock_hook):
        with self.assertRaises(AttributeError) as cm:
            op = GCFFunctionDeleteOperator(
                name="invalid_name",
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertEqual(str(err), 'Parameter name must match pattern: {}'.format(
            FUNCTION_NAME_PATTERN))
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcf_function_delete_operator.GCFHook')
    def test_empty_name(self, mock_hook):
        mock_hook.return_value.delete_function.return_value = \
            self._DELETE_FUNCTION_EXPECTED
        with self.assertRaises(AttributeError) as cm:
            GCFFunctionDeleteOperator(
                name="",
                task_id="id"
            )
        err = cm.exception
        self.assertEqual(str(err), 'Empty parameter: name')
        mock_hook.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcf_function_delete_operator.GCFHook')
    def test_gcf_error_silenced_when_function_doesnt_exist(self, mock_hook):
        op = GCFFunctionDeleteOperator(
            name=self._FUNCTION_NAME,
            task_id="id"
        )
        resp = type('', (object,), {"status": 404})()
        mock_hook.return_value.delete_function.side_effect = mock.Mock(
            side_effect=HttpError(resp=resp, content=b'not found'))
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.delete_function.assert_called_once_with(
            'projects/project_name/locations/project_location/functions/function_name'
        )

    @mock.patch('airflow.contrib.operators.gcf_function_delete_operator.GCFHook')
    def test_non_404_gcf_error_bubbled_up(self, mock_hook):
        op = GCFFunctionDeleteOperator(
            name=self._FUNCTION_NAME,
            task_id="id"
        )
        resp = type('', (object,), {"status": 500})()
        mock_hook.return_value.delete_function.side_effect = mock.Mock(
            side_effect=HttpError(resp=resp, content=b'error'))

        with self.assertRaises(HttpError):
            op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.delete_function.assert_called_once_with(
            'projects/project_name/locations/project_location/functions/function_name'
        )
