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


import pytest

from tests.gcp.operators.test_compute_system_helper import GCPComputeTestHelper
from tests.gcp.utils.gcp_authenticator import GCP_COMPUTE_KEY
from tests.test_utils.gcp_system_helpers import GCP_DAG_FOLDER, GcpSystemTest, provide_gcp_context

test_helper = GCPComputeTestHelper()


@pytest.fixture
def helper1():
    test_helper.delete_instance()
    test_helper.create_instance()
    yield
    test_helper.delete_instance()


@pytest.fixture
def helper2():
    test_helper.delete_instance_group_and_template(silent=True)
    test_helper.create_instance_group_and_template()
    yield
    test_helper.delete_instance_group_and_template()


@GcpSystemTest.skip(GCP_COMPUTE_KEY)
@pytest.mark.usefixtures("helper1")
def test_run_example_dag_compute():
    with provide_gcp_context(GCP_COMPUTE_KEY):
        GcpSystemTest.run_dag("example_gcp_compute", GCP_DAG_FOLDER)


@GcpSystemTest.skip(GCP_COMPUTE_KEY)
@pytest.mark.usefixtures("helper2")
def test_run_example_dag_compute_igm():
    with provide_gcp_context(GCP_COMPUTE_KEY):
        GcpSystemTest.run_dag("example_gcp_compute_igm", GCP_DAG_FOLDER)
