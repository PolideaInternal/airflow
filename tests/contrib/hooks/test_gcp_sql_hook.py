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

from airflow.contrib.hooks.gcp_sql_hook import CloudSqlHook
from airflow.exceptions import AirflowException

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


def _prepare_mocked_hook():
    hook = CloudSqlHook(None, None)
    # Simulating HttpError in get_conn
    hook.get_conn = mock.Mock(
        side_effect=HttpError(resp={'status': '400'},
                              content='Error content'.encode('utf-8'))
    )
    return hook


class TestGcpSqlHook(unittest.TestCase):
    def test_getting_instance(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, "__init__", lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.get_instance(None, None)
            err = cm.exception
            self.assertIn("Getting instance ", str(err))

    def test_creating_instance(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, "__init__", lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.create_instance(None, None)
            err = cm.exception
            self.assertIn("Creating instance ", str(err))

    def test_patching_instance(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, "__init__", lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.patch_instance(None, None, None)
            err = cm.exception
            self.assertIn("Patching instance ", str(err))

    def test_deleting_instance(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, "__init__", lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.delete_instance(None, None)
            err = cm.exception
            self.assertIn("Deleting instance ", str(err))

    def test_getting_database(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, "__init__", lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.get_database(None, None, None)
            err = cm.exception
            self.assertIn("Getting database ", str(err))

    def test_creating_database(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, "__init__", lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.create_database(None, None, None)
            err = cm.exception
            self.assertIn("Creating database ", str(err))

    def test_patching_database(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, "__init__", lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.patch_database(None, None, None, None)
            err = cm.exception
            self.assertIn("Patching database ", str(err))

    def test_deleting_database(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, "__init__", lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.delete_database(None, None, None)
            err = cm.exception
            self.assertIn("Deleting database ", str(err))

    def test_instance_import_ex(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, "__init__", lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.import_instance(None, None, None)
            err = cm.exception
            self.assertIn("Importing instance ", str(err))

    def test_instance_export_ex(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(CloudSqlHook, '__init__', lambda x, y, z: None):
            hook = _prepare_mocked_hook()
            with self.assertRaises(AirflowException) as cm:
                hook.export_instance(None, None, None)
            err = cm.exception
            self.assertIn("Exporting instance ", str(err))
