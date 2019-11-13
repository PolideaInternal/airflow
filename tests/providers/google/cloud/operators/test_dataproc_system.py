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
import os
from tempfile import TemporaryDirectory

from tests.gcp.utils.gcp_authenticator import GCP_DATAPROC_KEY
from tests.test_utils.gcp_system_helpers import (
    CLOUD_DAG_FOLDER, GcpResourceHelper, provide_gcp_context, skip_gcp_system,
)
from tests.test_utils.system_tests_class import SystemTest

BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-tests")
PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
PYSPARK_URI = "gs://{}/{}".format(BUCKET, PYSPARK_MAIN)


class DataprocSystemHelper(GcpResourceHelper):
    cmd_list = ["create_bucket", "delete_bucket", "upload_file"]

    def create_bucket(self):
        self.create_gcs_bucket(BUCKET)

    def delete_bucket(self):
        self.delete_gcs_bucket(BUCKET)

    def upload_file(self):
        with TemporaryDirectory(prefix="airflow-gcp") as tmp_dir:
            # 1. Create required files
            quickstart_path = os.path.join(tmp_dir, PYSPARK_MAIN)
            with open(quickstart_path, "w") as file:
                file.writelines(
                    [
                        "#!/usr/bin/python\n",
                        "import pyspark\n",
                        "sc = pyspark.SparkContext()\n",
                        "rdd = sc.parallelize(['Hello,', 'world!'])\n",
                        "words = sorted(rdd.collect())\n",
                        "print(words)\n",
                    ]
                )
                file.flush()
            os.chmod(quickstart_path, 555)
            self.upload_to_gcs(PYSPARK_URI, quickstart_path)


@skip_gcp_system(GCP_DATAPROC_KEY, require_local_executor=True)
class DataprocExampleDagsTest(SystemTest, DataprocSystemHelper):
    def setUp(self):
        super().setUp()
        self.create_bucket()
        self.upload_file()

    def tearDown(self):
        self.delete_bucket()
        super().tearDown()

    @provide_gcp_context(GCP_DATAPROC_KEY)
    def test_run_example_dag(self):
        self.run_dag(dag_id="example_gcp_dataproc", dag_folder=CLOUD_DAG_FOLDER)


if __name__ == '__main__':
    DataprocExampleDagsTest.cli()
