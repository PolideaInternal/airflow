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

from airflow.gcp.example_dags.example_bigtable import CBT_INSTANCE_ID, GCP_PROJECT_ID
from tests.gcp.utils.gcp_authenticator import GCP_BIGTABLE_KEY
from tests.test_utils.gcp_system_helpers import GCP_DAG_FOLDER, GcpSystemTest, provide_gcp_context

command = GcpSystemTest.commands_registry()


@command
def delete_instance():
    GcpSystemTest.execute_with_ctx(
        cmd=[
            "gcloud",
            "bigtable",
            "--project",
            GCP_PROJECT_ID,
            "--quiet",
            "--verbosity=none",
            "instances",
            "delete",
            CBT_INSTANCE_ID,
        ],
        key=GCP_BIGTABLE_KEY,
    )


@pytest.fixture
def helper():
    yield
    delete_instance()


@command
@GcpSystemTest.skip(GCP_BIGTABLE_KEY)
@pytest.mark.usefixtures("helper")
def test_run_example_dag_gcs_bigtable():
    with provide_gcp_context(GCP_BIGTABLE_KEY):
        GcpSystemTest.run_dag("example_gcp_bigtable_operators", GCP_DAG_FOLDER)
