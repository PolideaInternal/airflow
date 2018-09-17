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

from airflow.contrib.operators.gcf_function_deploy_operator import \
    GCFFunctionDeployOperator
from airflow import AirflowException

from copy import deepcopy

try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = 'project-id'
REGION = 'region'
SOURCE_ARCHIVE_URL = 'gs://folder/file.zip'
ENTRYPOINT = 'helloWorld'
FUNCTION_NAME = 'projects/{}/locations/{}/functions/{}'.format(PROJECT_ID, REGION,
                                                               ENTRYPOINT)
RUNTIME = 'nodejs6'
VALID_RUNTIMES = ['nodejs6', 'nodejs8', 'python37']
VALID_BODY = {
    "name": FUNCTION_NAME,
    "entryPoint": ENTRYPOINT,
    "runtime": RUNTIME,
    "httpsTrigger": {},
    "sourceArchiveUrl": SOURCE_ARCHIVE_URL
}


def _prepare_test_bodies():
    body_no_name = deepcopy(VALID_BODY)
    body_no_name.pop('name', None)
    body_empty_entry_point = deepcopy(VALID_BODY)
    body_empty_entry_point['entryPoint'] = ''
    body_empty_runtime = deepcopy(VALID_BODY)
    body_empty_runtime['runtime'] = ''
    body_values = [
        ({}, "The required parameter 'body' is missing"),
        (body_no_name, "The required field 'name' is missing"),
        (body_empty_entry_point,
         "The field 'entryPoint' value '' does not match regexp"),
        (body_empty_runtime, "The field 'runtime' value '' does not match regexp"),
    ]
    return body_values


class GCFFunctionDeployTest(unittest.TestCase):
    @parameterized.expand(_prepare_test_bodies())
    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_body_empty_or_missing_fields(self, body, message, mock_hook):
        mock_hook.return_value.upload_function_zip.return_value = 'https://uploadUrl'
        with self.assertRaises(AirflowException) as cm:
            op = GCFFunctionDeployOperator(
                project_id="test_project_id",
                region="test_region",
                body=body,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(message, str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_deploy_execute(self, mock_hook):
        mock_hook.return_value.list_functions.return_value = []
        mock_hook.return_value.create_new_function.return_value = True
        op = GCFFunctionDeployOperator(
            project_id=PROJECT_ID,
            region=REGION,
            body=VALID_BODY,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.list_functions.assert_called_once_with(
            'projects/project-id/locations/region'
        )
        mock_hook.return_value.create_new_function.assert_called_once_with(
            'projects/project-id/locations/region',
            VALID_BODY
        )

    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_update_function_if_exists(self, mock_hook):
        mock_hook.return_value.list_functions.return_value = \
            [{'name': VALID_BODY['name']}]
        mock_hook.return_value.update_function.return_value = True
        op = GCFFunctionDeployOperator(
            project_id=PROJECT_ID,
            region=REGION,
            body=VALID_BODY,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.list_functions.assert_called_once_with(
            'projects/project-id/locations/region'
        )
        mock_hook.return_value.update_function.assert_called_once_with(
            'projects/project-id/locations/region/functions/helloWorld',
            VALID_BODY, VALID_BODY.keys())
        mock_hook.return_value.create_new_function.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_empty_project_id(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            GCFFunctionDeployOperator(
                project_id="",
                region="test_region",
                body=None,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter 'project_id' is missing", str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_empty_region(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            GCFFunctionDeployOperator(
                project_id="test_project_id",
                region="",
                body=None,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter 'region' is missing", str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_empty_body(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            GCFFunctionDeployOperator(
                project_id="test_project_id",
                region="test_region",
                body=None,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter 'body' is missing", str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

    @parameterized.expand([
        (runtime,) for runtime in VALID_RUNTIMES
    ])
    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_correct_runtime_field(self, runtime, mock_hook):
        mock_hook.return_value.list_functions.return_value = []
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body['runtime'] = runtime
        op = GCFFunctionDeployOperator(
            project_id="test_project_id",
            region="test_region",
            body=body,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        (network,) for network in [
            "network-01",
            "n-0-2-3-4",
            "projects/PROJECT/global/networks/network-01"
            "projects/PRÓJECT/global/networks/netwórk-01"
        ]
    ])
    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_valid_network_field(self, network, mock_hook):
        mock_hook.return_value.list_functions.return_value = []
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body['network'] = network
        op = GCFFunctionDeployOperator(
            project_id="test_project_id",
            region="test_region",
            body=body,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        (labels,) for labels in [
            {},
            {"label": 'value-01'},
            {"label_324234_a_b_c": 'value-01_93'},
        ]
    ])
    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_valid_labels_field(self, labels, mock_hook):
        mock_hook.return_value.list_functions.return_value = []
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body['labels'] = labels
        op = GCFFunctionDeployOperator(
            project_id="test_project_id",
            region="test_region",
            body=body,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_body_validation_simple(self, mock_hook):
        mock_hook.return_value.list_functions.return_value = []
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body['name'] = ''
        with self.assertRaises(AirflowException) as cm:
            op = GCFFunctionDeployOperator(
                project_id="test_project_id",
                region="test_region",
                body=body,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The field 'name' value '' does not match regexp",
                      str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        ('name', '',
         "The field 'name' value '' does not match regexp"),
        ('description', '', "The field 'description' value '' does not match regexp"),
        ('entryPoint', '', "The field 'entryPoint' value '' does not match regexp"),
        ('entryPoint', ' ', "The field 'entryPoint' value ' ' does not match regexp"),
        ('runtime', 'invalid_runtime',
         "The field 'runtime' value 'invalid_runtime' does not match regexp"),
        ('timeout', 'aaaaa', "The field 'timeout' value 'aaaaa' does not match regexp"),
        ('timeout', '0', "The field 'timeout' value '0' does not match regexp"),
        ('timeout', '0.00000000000000000s',
         "The field 'timeout' value '0.00000000000000000s' does not match regexp"),
        ('availableMemoryMb', '0',
         "Error while validating custom field 'availableMemoryMb':"
         " The available memory has to be greater than 0"),
        ('availableMemoryMb', '-1',
         "Error while validating custom field 'availableMemoryMb':"
         " The available memory has to be greater than 0"),
        ('availableMemoryMb', 'ss',
         "Error while validating custom field 'availableMemoryMb':"
         " invalid literal for int() with base 10: 'ss'"),
        ('labels', {':': 'a'},
         "Error while validating custom field 'labels': "
         "The key ':' does not match regexp"),
        ('labels', {'a': ':'}, "Error while validating custom field 'labels': "
                               "The value ':' of key 'a' does not match regexp"),
        ('labels', {'very_very_very_very_very_very_very_long_label_key_name': ':'},
         "Error while validating custom field 'labels': The value ':' of key "
         "'very_very_very_very_very_very_very_long_label_key_name' "
         "does not match regexp"),
        ('labels', {'a': 'very_very_very_very_very_very_'
                         'very_very_very_long_label_value_name'},
         "Error while validating custom field 'labels': The value "
         "'very_very_very_very_very_very_very_very_very_long_label_value_name' of key "
         "'a' does not match regexp"),
        ('labels', 'a', "Error while validating custom field "
                        "'labels': The field should be dictionary!"),
        ('environmentVariables', {'': 'a'},
         "Error while validating custom field 'environmentVariables': "
         "The key '' does not match regexp"),
        ('network', '', "The field 'network' value '' does not match regexp"),
        ('maxInstances', '0', "Error while validating custom field 'maxInstances':"
                              " The max instances parameter has to be greater than 0"),
        ('maxInstances', '-1', "Error while validating custom field 'maxInstances':"
                               " The max instances parameter has to be greater than 0"),
        ('maxInstances', 'ss', "Error while validating custom field 'maxInstances':"
                               " invalid literal for int() with base 10: 'ss'"),
    ])
    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_invalid_field_values(self, key, value, message, mock_hook):
        mock_hook.return_value.list_functions.return_value = []
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body[key] = value
        with self.assertRaises(AirflowException) as cm:
            op = GCFFunctionDeployOperator(
                project_id="test_project_id",
                region="test_region",
                body=body,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(message, str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        ({'sourceArchiveUrl': ''},
         "The field 'source_code.sourceArchiveUrl' value '' does not match regexp"),
        ({'sourceArchiveUrl': '', 'zip_path': '/path/to/file'},
         "Only one of 'sourceArchiveUrl' in body or 'zip_path' argument possible."),
        ({'sourceArchiveUrl': 'gs://url', 'zip_path': '/path/to/file'},
         "Only one of 'sourceArchiveUrl' in body or 'zip_path' argument possible."),
        ({'sourceArchiveUrl': '', 'sourceUploadUrl': ''},
         "If parameter 'sourceUploadUrl' is empty in 'body' "
         "then argument 'zip_path' needed."),
        ({'sourceArchiveUrl': 'gs://adasda', 'sourceRepository': ''},
         "The field 'source_code.sourceRepository' should be a dictionary"),
        ({'sourceUploadUrl': '', 'sourceRepository': ''},
         "If parameter 'sourceUploadUrl' is empty in 'body' "
         "then argument 'zip_path' needed."),
        ({'sourceArchiveUrl': '', 'sourceUploadUrl': '', 'sourceRepository': ''},
         "If parameter 'sourceUploadUrl' is empty in 'body' "
         "then argument 'zip_path' needed."),
        ({'sourceArchiveUrl': 'gs://url', 'sourceUploadUrl': 'https://url'},
         "The field 'sourceUploadUrl' and 'sourceArchiveUrl' "
         "are part of 'source_code' union so they are "
         "mutually exclusive. They should not be both present in the same dictionary!"),
        ({'sourceUploadUrl': 'https://url', 'zip_path': '/path/to/file'},
         "Only one of 'sourceUploadUrl' in body "
         "or 'zip_path' argument possible. Found both."),
        ({'sourceUploadUrl': ''}, "If parameter 'sourceUploadUrl' is empty "
                                  "in 'body' then argument 'zip_path' needed."),
        ({'sourceRepository': ''}, "The field 'source_code.sourceRepository' "
                                   "should be a dictionary"),
        ({'sourceRepository': {}}, "The required field "
                                   "'source_code.sourceRepository.url' is missing"),
        ({'sourceRepository': {'url': ''}},
         "The field 'source_code.sourceRepository.url' value '' does not match regexp"),
    ]
    )
    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_invalid_source_code_union_field(self, source_code, message, mock_hook):
        mock_hook.return_value.upload_function_zip.return_value = 'https://uploadUrl'
        body = deepcopy(VALID_BODY)
        body.pop('sourceUploadUrl', None)
        body.pop('sourceArchiveUrl', None)
        zip_path = source_code.pop('zip_path', None)
        body.update(source_code)
        with self.assertRaises(AirflowException) as cm:
            op = GCFFunctionDeployOperator(
                project_id="test_project_id",
                region="test_region",
                body=body,
                task_id="id",
                zip_path=zip_path
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(message, str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        ({'sourceArchiveUrl': 'gs://url'},),
        ({'zip_path': '/path/to/file', 'sourceUploadUrl': None},),
        ({'sourceUploadUrl':
         'https://source.developers.google.com/projects/a/repos/b/revisions/c/paths/d'},),
        ({'sourceRepository':
         {'url': 'https://source.developers.google.com/projects/a/'
          'repos/b/revisions/c/paths/d'}},),
    ])
    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_valid_source_code_union_field(self, source_code, mock_hook):
        mock_hook.return_value.upload_function_zip.return_value = 'https://uploadUrl'
        mock_hook.return_value.list_functions.return_value = []
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body.pop('sourceUploadUrl', None)
        body.pop('sourceArchiveUrl', None)
        body.pop('sourceRepository', None)
        body.pop('sourceRepositoryUrl', None)
        zip_path = source_code.pop('zip_path', None)
        body.update(source_code)
        op = GCFFunctionDeployOperator(
            project_id="test_project_id",
            region="test_region",
            body=body,
            task_id="id",
            zip_path=zip_path
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        if zip_path:
            mock_hook.return_value.upload_function_zip.assert_called_once_with(
                parent='projects/test_project_id/locations/test_region',
                zip_path='/path/to/file'
            )
        mock_hook.return_value.list_functions.assert_called_once_with(
            'projects/test_project_id/locations/test_region'
        )
        mock_hook.return_value.create_new_function.assert_called_once_with(
            'projects/test_project_id/locations/test_region',
            body
        )
        mock_hook.reset_mock()

    @parameterized.expand([
        ({'eventTrigger': {}},
         "The required field 'trigger.eventTrigger.eventType' is missing"),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b'}},
         "The required field 'trigger.eventTrigger.resource' is missing"),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b', 'resource': ''}},
         "The field 'trigger.eventTrigger.resource' value '' does not match regexp"),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res',
                           'service': ''}},
         "The field 'trigger.eventTrigger.service' value '' does not match regexp"),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res',
                           'service': 'service_name',
                           'failurePolicy': {'retry': ''}}},
         "The field 'trigger.eventTrigger.failurePolicy.retry' "
         "should be a dictionary and is ''")
    ]
    )
    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_invalid_trigger_union_field(self, trigger, message, mock_hook):
        mock_hook.return_value.upload_function_zip.return_value = 'https://uploadUrl'
        body = deepcopy(VALID_BODY)
        body.pop('httpsTrigger', None)
        body.pop('eventTrigger', None)
        body.update(trigger)
        with self.assertRaises(AirflowException) as cm:
            op = GCFFunctionDeployOperator(
                project_id="test_project_id",
                region="test_region",
                body=body,
                task_id="id",
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(message, str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        ({'httpsTrigger': {}},),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res'}},),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res',
                           'service': 'service_name'}},),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/ą.b',
                           'resource': 'reś',
                           'service': 'service_namę'}},),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res',
                           'service': 'service_name',
                           'failurePolicy': {'retry': {}}}},)
    ])
    @mock.patch('airflow.contrib.operators.gcf_function_deploy_operator.GCFHook')
    def test_valid_trigger_union_field(self, trigger, mock_hook):
        mock_hook.return_value.upload_function_zip.return_value = 'https://uploadUrl'
        mock_hook.return_value.list_functions.return_value = []
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body.pop('httpsTrigger', None)
        body.pop('eventTrigger', None)
        body.update(trigger)
        op = GCFFunctionDeployOperator(
            project_id="test_project_id",
            region="test_region",
            body=body,
            task_id="id",
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.list_functions.assert_called_once_with(
            'projects/test_project_id/locations/test_region'
        )
        mock_hook.return_value.create_new_function.assert_called_once_with(
            'projects/test_project_id/locations/test_region',
            body
        )
        mock_hook.reset_mock()
