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
import stat
import unittest
try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

from parameterized import parameterized

from airflow import models, AirflowException, example_dags, DAG


class ExampleDagsTestCase(unittest.TestCase):

    @parameterized.expand([
        # ("subdags/main_dag.py", "parent_dag", ),
        # ("subdags/subdag.py", "parent_dag.child_dag", ),
        # ("example_bash_operator.py", 'example_bash_operator', ),
        # ("example_branch_operator.py", 'example_branch_operator', ),
        # ('example_branch_dop_operator_3.py', 'example_branch_dop_operator_v3', ),
        # ("example_branch_operator.py", 'example_branch_operator', ),
        # ('example_default_args.py', 'example_default_args'),
        # ('example_documentation.py', 'example_documentation', ),
        # ('example_dynamic_dag.py', 'foo_1', ),
        # ('example_dynamic_dag.py', 'foo_10', ),
        # ('example_http_operator.py', 'example_http_operator', ),
        # ('example_latest_only.py', 'example_latest_only', ),
        # ('example_latest_only_with_trigger.py', 'example_latest_only_with_trigger', ),
        # ('example_lineage.py', 'example_lineage', ),
        # ('example_passing_params_via_test_command.py', 'example_passing_params_via_test_command', ),
        # ('example_python_operator.py','example_python_operator', ),
        # ('example_short_circuit_operator.py','example_short_circuit_operator', ),
        # ('example_skip_dag.py', 'example_skip_dag', ),
        # ('example_subdag_operator.py', 'example_subdag_operator', ),
        ('example_trigger_controller_dag.py', 'example_trigger_controller', ),
        # ('example_trigger_target_dag.py', 'example_trigger_target_dag', ),
        # ("example_xcom.py", 'example_xcom', ),
        # ('test_utils.py', 'test_utils', ),
        # ('tutorial.py', 'tutorial', )
    ], name_func=lambda func, num, p: "{}_{}".format(func.__name__, p.args[0]))
    def test_run_dag(self, filename, dag_id):
        dag_folder = example_dags.__path__[0]
        dag_bag = models.DagBag(dag_folder=os.path.join(dag_folder, filename), include_examples=False)
        dag = dag_bag.get_dag(dag_id)
        if dag is None:
            raise AirflowException(
                "The Dag {dag_id} could not be found. It's either an import problem or the dag was not "
                "symlinked to the DAGs folder. The content of the {dag_folder} folder is {dags_list}".format(
                    dag_id=dag_id,
                    dag_folder=dag_folder,
                    dags_list=os.listdir(dag_folder)
                )
            )
        dag.clear(reset_dag_runs=True)
        dag.run(ignore_first_depends_on_past=True, verbose=True)


class ExampleBashOperatorTestCase(unittest.TestCase):
    SHELL_SCRIPT_PATH = "/tmp/test.sh"

    @classmethod
    def setUpClass(cls):
        with open(cls.SHELL_SCRIPT_PATH, "w") as file:
            file.write("#!/usr/bin/env bash\n")
            file.write("export | grep EXECUTION_DATE")
        os.chmod(cls.SHELL_SCRIPT_PATH, stat.S_IXGRP | stat.S_IXOTH | stat.S_IEXEC)

    # @classmethod
    # def tearDownClass(cls):
    #     if os.path.exists(cls.SHELL_SCRIPT_PATH):
    #         os.remove(cls.SHELL_SCRIPT_PATH)

    def test_run_dag(self):
        filename = "example_bash_operator.py"
        dag_id = 'example_bash_operator'
        dag_folder = example_dags.__path__[0]
        dag_bag = models.DagBag(dag_folder=os.path.join(dag_folder, filename), include_examples=False)
        dag = dag_bag.get_dag(dag_id)
        if dag is None:
            raise AirflowException(
                "The Dag {dag_id} could not be found. It's either an import problem or the dag was not "
                "symlinked to the DAGs folder. The content of the {dag_folder} folder is {dags_list}".format(
                    dag_id=dag_id, dag_folder=dag_folder, dags_list=os.listdir(dag_folder)
                )
            )
        dag.clear(reset_dag_runs=True)
        dag.run(ignore_first_depends_on_past=True, verbose=True)


class ExampleDynamicDagTestCase(unittest.TestCase):

    def test_example(self):
        with mock.patch.dict('sys.modules'):
            import airflow.example_dags.example_dynamic_dag
            for i in range(10):
                dag = getattr(airflow.example_dags.example_dynamic_dag, 'foo_{}'.format(i))
                self.assertIsInstance(dag, DAG)
