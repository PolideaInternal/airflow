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

from airflow import AirflowException
from airflow.contrib.operators.gcf_function_field_validator import FieldValidator, \
    FieldValidationException
from airflow.models import BaseOperator
from airflow.contrib.hooks.gcf_hook import GCFHook
from airflow.utils.decorators import apply_defaults

SOURCE_ARCHIVE_URL = 'sourceArchiveUrl'
SOURCE_UPLOAD_URL = 'sourceUploadUrl'
SOURCE_REPOSITORY = 'sourceRepository'
ZIP_PATH = 'zip_path'


def _validate_available_memory_in_mb(value):
    if int(value) <= 0:
        raise FieldValidationException("The available memory has to be greater than 0")


def _validate_max_instances(value):
    if int(value) <= 0:
        raise FieldValidationException(
            "The max instances parameter has to be greater than 0")


LABEL_KEY_REGEXP = r'^[\w_-]{0,62}$'
LABEL_VALUE_REGEXP = r'^[\w_-]{0,63}$'


def _validate_labels_dict(the_dictionary):
    # type: (dict) -> None
    if not isinstance(the_dictionary, dict):
        raise FieldValidationException("The field should be dictionary!")
    for key in the_dictionary.keys():
        if not re.match(LABEL_KEY_REGEXP, key):
            raise FieldValidationException("The key '{}' does not match regexp '{}'".
                                           format(key, LABEL_KEY_REGEXP))
        value = the_dictionary[key]
        if not re.match(LABEL_VALUE_REGEXP, value):
            raise FieldValidationException(
                "The value '{}' of key '{}' does not match regexp '{}'".
                format(value, key, LABEL_VALUE_REGEXP))


ENV_VARS_KEY_REGEXP = r'^[^\x00=]+$'
ENV_VARS_VALUE_REGEXP = r'^[^\\x00]*$'


def _validate_env_vars_dict(the_dictionary):
    # type: (dict) -> None
    if not isinstance(the_dictionary, dict):
        raise FieldValidationException("The field should be dictionary!")
    for key in the_dictionary.keys():
        if key is None:
            raise FieldValidationException("The key '{}' cannot be None".format(key))
        if not re.match(ENV_VARS_KEY_REGEXP, key):
            raise FieldValidationException("The key '{}' does not match regexp '{}'".
                                           format(key, ENV_VARS_KEY_REGEXP))
        value = the_dictionary[key]
        if not re.match(ENV_VARS_VALUE_REGEXP, value):
            raise FieldValidationException(
                "The value '{}' of key '{}' does not match regexp '{}'".
                format(value, key, ENV_VARS_VALUE_REGEXP))


CLOUD_FUNCTIONS_VALIDATION = [
    dict(name="name", regexp=r'^[^\s]+$'),
    dict(name="description", regexp="^.+$", optional=True),
    dict(name="entryPoint", optional=True, regexp=r'^\w+$'),
    dict(name="runtime", regexp=r'^nodejs6$|^nodejs8$|^python37$', optional=True),
    dict(name="timeout", regexp=r'^\d+(\.\d{1,9})?s$', optional=True),
    dict(name="availableMemoryMb", custom_validation=_validate_available_memory_in_mb,
         optional=True),
    dict(name="labels", custom_validation=_validate_labels_dict, optional=True),
    dict(name="environmentVariables", custom_validation=_validate_env_vars_dict,
         optional=True),
    dict(name="network",
         regexp=r'^[^\s]+$',
         optional=True),
    dict(name="maxInstances", optional=True, custom_validation=_validate_max_instances),

    dict(name="source_code", type="union", fields=[
        dict(name="sourceArchiveUrl", regexp=r'^gs://[^\s]+$'),
        dict(name="sourceRepositoryUrl", regexp=r'^gs://[^\s]+$', api='v1beta2'),
        dict(name="sourceRepository", type="dict", fields=[
            dict(name="url",
                 regexp=r'^[^\s]+$')
        ]),
        dict(name="sourceUploadUrl", regexp=r'^https://[^\s]+$')
    ]),

    dict(name="trigger", type="union", fields=[
        dict(name="httpsTrigger", type="dict", fields=[
            # This dict should be empty at input (url is added at output)
        ]),
        dict(name="eventTrigger", type="dict", fields=[
            dict(name="eventType", regexp=r'^[^\s]+$'),
            dict(name="resource", regexp=r'^[^/]+$'),
            dict(name="service", regexp=r'^[^\s]+$', optional=True),
            dict(name="failurePolicy", type="dict", optional=True, fields=[
                dict(name="retry", type="dict", optional=True)
            ])
        ])
    ]),
]


class GCFFunctionDeployOperator(BaseOperator):
    """
    Create a function in Google Cloud Functions.

    :param project_id: Project ID that the operator works on
    :type project_id: str
    :param region: Region where the operator operates on
    :type region: str
    :param body: Body of the cloud function definition. The body must be CloudFunction
        dictionary as described in:
   https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions
        (note that different API versions require different
        variants of the CloudFunction dictionary)
    :type body: dict or google.cloud.functions.v1.CloudFunction
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: Version of the API used (for example v1).
    :type api_version: str
    :param zip_path: Path to zip file containing source code of the function. I
    f it is set, then sourceUploadUrl should not be specified in the body (or it should
    be empty), then the zip file will be uploaded using upload URL generated
    via generateUploadUrl from cloud functions API
    :type zip_path: str
    """

    @apply_defaults
    def __init__(self,
                 project_id,
                 region,
                 body,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 zip_path=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.region = region
        self.location = 'projects/{}/locations/{}'.format(self.project_id, self.region)
        self.body = body
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.zip_path = zip_path
        self.source_code_validation = SourceCodeValidation(body, zip_path)
        self._field_validator = FieldValidator(CLOUD_FUNCTIONS_VALIDATION,
                                               api=api_version)
        self._hook = GCFHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)
        self._validate_inputs()
        super(GCFFunctionDeployOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is missing")
        if not self.region:
            raise AirflowException("The required parameter 'region' is missing")
        if not self.body:
            raise AirflowException("The required parameter 'body' is missing")
        self.source_code_validation.validate()

    def _validate_all_body_fields(self):
        self._field_validator.validate(self.body)

    def _create_new_function(self):
        self._hook.create_new_function(self.location, self.body)

    def _update_function(self):
        self._hook.update_function(self.body['name'], self.body, self.body.keys())

    def _check_if_function_exists(self):
        return self.body['name'] in map(
            lambda x: x["name"], self._hook.list_functions(self.location)
        )

    def _upload_source_code(self):
        return self._hook.upload_function_zip(parent=self.location,
                                              zip_path=self.zip_path)

    def execute(self, context):
        if self.source_code_validation.should_upload_function():
            self.body[SOURCE_UPLOAD_URL] = self._upload_source_code()
        self._validate_all_body_fields()
        if not self._check_if_function_exists():
            self._create_new_function()
        else:
            self._update_function()


class SourceCodeValidation:
    """
    Responsible for validating if we have enough information about the source code of
    the GCF function. The source code to be used can be specified using different options:
    * non-empty sourceUploadUrl in case the code was just uploaded using
    "generateUploadUrl" mechanism
    * empty sourceUploadUrl and path to zip file (then generateUploadUrl will
    be used to upload the zip file and sourceUploadUrl will be set appropriately
    * archiveUrl that points to valid Google Storage link (starting with gs://)
    where the zip file is uploaded.

    """
    upload_function = None

    def __init__(self, body, zip_path):
        self.body = body
        self.zip_path = zip_path

    @staticmethod
    def _is_present_and_empty(dictionary, field):
        return field in dictionary and not dictionary[field]

    def _is_upload_url_and_no_zip_path(self):
        if self._is_present_and_empty(self.body, SOURCE_UPLOAD_URL):
            if not self.zip_path:
                raise AirflowException(
                    "If parameter '{}' is empty in 'body' then argument '{}' needed.".
                    format(SOURCE_UPLOAD_URL, ZIP_PATH))

    def _is_upload_url_and_zip_path(self):
        if SOURCE_UPLOAD_URL in self.body and self.zip_path:
            if not self.body[SOURCE_UPLOAD_URL]:
                self.upload_function = True
            else:
                raise AirflowException("Only one of '{}' in body or '{}' argument "
                                       "possible. Found both."
                                       .format(SOURCE_UPLOAD_URL, ZIP_PATH))

    def _is_archive_url_and_zip_path(self):
        if SOURCE_ARCHIVE_URL in self.body and self.zip_path:
            raise AirflowException("Only one of '{}' in body or '{}' argument "
                                   "possible. Found both."
                                   .format(SOURCE_ARCHIVE_URL, ZIP_PATH))

    def should_upload_function(self):
        if self.upload_function is None:
            raise AirflowException('validate() method has to be invoked first')
        return self.upload_function

    def validate(self):
        self._is_archive_url_and_zip_path()
        self._is_upload_url_and_zip_path()
        self._is_upload_url_and_no_zip_path()
        if self.upload_function is None:
            self.upload_function = False
