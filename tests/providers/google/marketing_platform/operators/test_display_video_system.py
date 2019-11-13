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

from airflow.providers.google.marketing_platform.example_dags.example_display_video import BUCKET
from tests.gcp.utils.gcp_authenticator import GCP_DISPLAY_VIDEO_KEY
from tests.test_utils.gcp_system_helpers import GcpSystemTest, provide_gcp_context

# Requires the following scope:
SCOPES = ["https://www.googleapis.com/auth/doubleclickbidmanager"]

command = GcpSystemTest.commands_registry()


@command
def create_bucket():
    GcpSystemTest.create_gcs_bucket(BUCKET)


@command
def delete_bucket():
    GcpSystemTest.delete_gcs_bucket(BUCKET)


@pytest.fixture
def helper():
    create_bucket()
    yield
    delete_bucket()


@command
@GcpSystemTest.skip(GCP_DISPLAY_VIDEO_KEY)
@pytest.mark.usefixtures("helper")
def test_run_example_dag():
    with provide_gcp_context(GCP_DISPLAY_VIDEO_KEY, scopes=SCOPES):
        GcpSystemTest.run_dag(
            "example_display_video",
            "airflow/providers/google/marketing_platform/example_dags",
        )
