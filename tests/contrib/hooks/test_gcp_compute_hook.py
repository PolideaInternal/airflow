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

from airflow.contrib.hooks.gcp_compute_hook import GceHook
from airflow.exceptions import AirflowException

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


def _prepare_hook_with_mocked_http_error():
    hook = GceHook(None, None)
    # Simulating HttpError in get_conn
    hook.get_conn = mock.Mock(
        side_effect=HttpError(resp={'status': '400'},
                              content='Error content'.encode('utf-8'))
    )
    return hook


class TestGcpComputeHook(unittest.TestCase):
    def test_starting_instance(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(GceHook, "__init__", lambda x, y, z: None):
            hook = _prepare_hook_with_mocked_http_error()
            with self.assertRaises(AirflowException) as cm:
                hook.start_instance(None, None, None)
            err = cm.exception
            self.assertIn("Starting instance", str(err))

    def test_stopping_instance(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(GceHook, "__init__", lambda x, y, z: None):
            hook = _prepare_hook_with_mocked_http_error()
            with self.assertRaises(AirflowException) as cm:
                hook.stop_instance(None, None, None)
            err = cm.exception
            self.assertIn("Stopping instance", str(err))

    def test_setting_machine_type(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(GceHook, "__init__", lambda x, y, z: None):
            hook = _prepare_hook_with_mocked_http_error()
            with self.assertRaises(AirflowException) as cm:
                hook.set_machine_type(None, None, None, None)
            err = cm.exception
            self.assertIn("Setting machine type", str(err))

    def test_getting_instance_template(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(GceHook, "__init__", lambda x, y, z: None):
            hook = _prepare_hook_with_mocked_http_error()
            with self.assertRaises(AirflowException) as cm:
                hook.get_instance_template(None, None)
            err = cm.exception
            self.assertIn("Getting instance template", str(err))

    def test_inserting_instance_template(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(GceHook, "__init__", lambda x, y, z: None):
            hook = _prepare_hook_with_mocked_http_error()
            with self.assertRaises(AirflowException) as cm:
                hook.insert_instance_template(None, None, None)
            err = cm.exception
            self.assertIn("Inserting instance template", str(err))

    def test_getting_instance_group_manager(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(GceHook, "__init__", lambda x, y, z: None):
            hook = _prepare_hook_with_mocked_http_error()
            with self.assertRaises(AirflowException) as cm:
                hook.get_instance_group_manager(None, None, None)
            err = cm.exception
            self.assertIn("Getting instance group manager", str(err))

    def test_patching_instance_group_manager(self):
        # Mocking __init__ with an empty anonymous function
        with mock.patch.object(GceHook, "__init__", lambda x, y, z: None):
            hook = _prepare_hook_with_mocked_http_error()
            with self.assertRaises(AirflowException) as cm:
                hook.patch_instance_group_manager(None, None, None, None, None)
            err = cm.exception
            self.assertIn("Patching instance group manager", str(err))
