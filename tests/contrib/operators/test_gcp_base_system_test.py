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
import json
import os
import subprocess
import unittest
from glob import glob

from airflow.utils import db as db_utils
from airflow import models, settings, configuration, AirflowException
from airflow.utils.timezone import datetime
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor
from tests.contrib.utils.run_once_decorator import run_once

AIRFLOW_MAIN_FOLDER = os.path.realpath(os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir, os.pardir, os.pardir))

AIRFLOW_BREEZE_FOLDER = os.path.realpath(os.path.join(AIRFLOW_MAIN_FOLDER,
                                                      os.pardir, os.pardir))
ENV_FILE_RETRIEVER = os.path.join(AIRFLOW_BREEZE_FOLDER,
                                  "get_system_test_environment_variables.py")


# Retrieve environment variables from airflow breeze if it is used
class RetrieveVariables:
    @staticmethod
    @run_once
    def retrieve_variables():
        if os.path.isfile(ENV_FILE_RETRIEVER):
            if os.environ.get('AIRFLOW__CORE__UNIT_TEST_MODE') != 'True':
                raise Exception("Make sure to set AIRFLOW__CORE__UNIT_TEST_MODE "
                                "environment variable to 'True' "
                                "BEFORE running your test!")
            variables = subprocess.check_output([ENV_FILE_RETRIEVER]).decode("utf-8")
            print("Applying variables retrieved")
            for line in variables.split("\n"):
                try:
                    variable, key = line.split("=")
                except ValueError:
                    continue
                print("{}={}".format(variable, key))
                os.environ[variable] = key


RetrieveVariables.retrieve_variables()

DEFAULT_DATE = datetime(2015, 1, 1)

KEYPATH_EXTRA = 'extra__google_cloud_platform__key_path'
KEYFILE_DICT_EXTRA = 'extra__google_cloud_platform__keyfile_dict'
SCOPE_EXTRA = 'extra__google_cloud_platform__scope'
PROJECT_EXTRA = 'extra__google_cloud_platform__project'

CONTRIB_OPERATORS_EXAMPLES_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "contrib", "example_dags")

OPERATORS_EXAMPLES_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "example_dags")

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME',
                              os.path.join(os.path.expanduser('~'), 'airflow'))
UNIT_TEST_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "tests", "dags")

DAG_FOLDER = os.path.join(AIRFLOW_HOME, "dags")

GCP_COMPUTE_KEY = 'gcp_compute.json'
GCP_FUNCTION_KEY = 'gcp_function.json'
GCP_CLOUDSQL_KEY = 'gcp_cloudsql.json'
GCP_BIGTABLE_KEY = 'gcp_bigtable.json'
GCP_SPANNER_KEY = 'gcp_spanner.json'
GCP_GCS_KEY = 'gcp_gcs.json'

SKIP_TEST_WARNING = """
The test is only run when the test is run in with GCP-system-tests enabled
Airflow Breeze environment. You can enable it in one of two ways:

* Set AIRFLOW_BREEZE_CONFIG_DIR environment variable to point to the airflow-breeze-config
  directory
* Make sure you run the tests in Airflow Breeze environment workspace where
  airflow-breeze-config directory is checked out next to the airflow-breeze-config

""".format(__file__)

original_account = None


class BaseGcpSystemTestCase(unittest.TestCase, LoggingCommandExecutor):
    def __init__(self,
                 method_name,
                 dag_id,
                 gcp_key,
                 dag_name=None,
                 example_dags_folder=CONTRIB_OPERATORS_EXAMPLES_DAG_FOLDER,
                 project_extra=None):
        super(BaseGcpSystemTestCase, self).__init__(method_name)
        self.dag_id = dag_id
        self.dag_name = self.dag_id + '.py' if not dag_name else dag_name
        self.gcp_key = gcp_key
        self.example_dags_folder = example_dags_folder
        self.project_extra = project_extra
        self.full_key_path = None
        self.project_id = self.get_project_id()

    @staticmethod
    def get_project_id():
        return os.environ.get('GCP_PROJECT_ID')

    @staticmethod
    def _get_key_path(key_name):
        """
        Returns key path - if AIRFLOW_BREEZE_CONFIG_DIR points to absolute
            directory, it tries to find the key in this directory. Otherwise it assumes
            that Airflow is run in the Airflow Breeze - checked out directory so
            it tries to find the key folder in the workspace's airflow-breeze-config
            directory.
        :param key_name: name of the key file to find.
        :return: path of the key file or None if the key is not found
        :rtype: str
        """
        if "AIRFLOW_BREEZE_CONFIG_DIR" in os.environ:
            airflow_breeze_config_dir = os.environ["AIRFLOW_BREEZE_CONFIG_DIR"]
        else:
            airflow_breeze_config_dir = os.path.join(AIRFLOW_MAIN_FOLDER,
                                                     os.pardir,
                                                     "airflow-breeze-config")
        if not os.path.isdir(airflow_breeze_config_dir):
            print("The {} is not a directory".format(airflow_breeze_config_dir))
            return None
        key_dir = os.path.join(airflow_breeze_config_dir, "keys")
        if not os.path.isdir(key_dir):
            print("The {} is not a directory".format(key_dir))
            return None
        key_path = os.path.join(key_dir, key_name)
        if not os.path.isfile(key_path):
            print("The {} is missing".format(key_path))
            return None
        return key_path

    def gcp_authenticate(self):
        """
        Authenticate with service account specified.
        """
        self.full_key_path = self._get_key_path(self.gcp_key)

        if not os.path.isfile(self.full_key_path):
            raise Exception("The key {} could not be found. Please copy it to the "
                            "{} path.".format(self.gcp_key, self.full_key_path))
        self.log.info("Setting the GCP key to {}".format(self.full_key_path))
        # Checking if we can authenticate using service account credentials provided
        retcode = subprocess.call(['gcloud', 'auth', 'activate-service-account',
                                   '--key-file={}'.format(self.full_key_path),
                                   '--project={}'.format(self.project_id)])
        if retcode != 0:
            raise AirflowException("The gcloud auth method was not successful!")
        self.update_connection_with_key_path()

    def gcp_revoke_authentication(self):
        """
        Change default authentication to non existing one.
        Tests should be run without default authentication
         because the authentication from Connection table should be used.
        """
        current_account = subprocess.check_output(
            ['gcloud', 'config', 'get-value', 'account',
             '--project={}'.format(self.project_id)]).decode('utf-8')
        self.log.info("Revoking authentication for account: {}".format(current_account))
        subprocess.call(['gcloud', 'config', 'set', 'account', 'none',
                         '--project={}'.format(self.project_id)])

    def gcp_store_authentication(self):
        """
        Store authentication as it was originally so it can be restored and revoke
        authentication.
        Tests should be run without default authentication
         because the authentication from Connection table should be used.
        """
        global original_account
        if not original_account:
            original_account = subprocess.check_output(
                ['gcloud', 'config', 'get-value', 'account',
                 '--project={}'.format(self.project_id)]).decode('utf-8')
            self.log.info("Storing account: to restore it later {}".format(
                original_account))
        self.log.info("Setting GCP account to none")
        subprocess.call(['gcloud', 'config', 'set', 'account', 'none',
                         '--project={}'.format(self.project_id)])

    def gcp_restore_authentication(self):
        """
        Restore authentication to the original one one.
        Tests should be run without default authentication
         because the authentication from Connection table should be used.
        """
        if original_account:
            self.log.info("Restoring original account stored: {}".
                          format(original_account))
            subprocess.call(['gcloud', 'config', 'set', 'account', original_account,
                             '--project={}'.format(self.project_id)])
        else:
            self.log.info("Not restoring the original GCP account ad it is not set")

    def update_connection_with_key_path(self):
        session = settings.Session()
        try:
            conn = session.query(models.Connection).filter(
                models.Connection.conn_id == 'google_cloud_default')[0]
            extras = conn.extra_dejson
            extras[KEYPATH_EXTRA] = self.full_key_path
            if extras.get(KEYFILE_DICT_EXTRA):
                del extras[KEYFILE_DICT_EXTRA]
            extras[SCOPE_EXTRA] = 'https://www.googleapis.com/auth/cloud-platform'
            extras[PROJECT_EXTRA] = self.project_extra
            conn.extra = json.dumps(extras)
            session.commit()
        except BaseException as ex:
            self.log.info('Airflow DB Session error:' + str(ex))
            session.rollback()
            raise
        finally:
            session.close()

    def update_connection_with_dictionary(self):
        session = settings.Session()
        try:
            conn = session.query(models.Connection).filter(
                models.Connection.conn_id == 'google_cloud_default')[0]
            extras = conn.extra_dejson
            with open(self.full_key_path, "r") as path_file:
                content = json.load(path_file)
            extras[KEYFILE_DICT_EXTRA] = json.dumps(content)
            if extras.get(KEYPATH_EXTRA):
                del extras[KEYPATH_EXTRA]
            extras[SCOPE_EXTRA] = 'https://www.googleapis.com/auth/cloud-platform'
            extras[PROJECT_EXTRA] = self.project_extra
            conn.extra = json.dumps(extras)
            session.commit()
        except BaseException as ex:
            self.log.info('Airflow DB Session error:' + str(ex))
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def _get_dag_folder():
        return UNIT_TEST_DAG_FOLDER

    @staticmethod
    def _get_files_to_link(path):
        """
        Returns all file names (note - file names not paths)
        that have the same base name as the .py dag file (for example dag_name.sql etc.)
        :param path: path to the dag file.
        :return: list of files matching the base name
        """
        prefix, ext = os.path.splitext(path)
        assert ext == '.py', "Dag name should be a .py file and is {} file".format(ext)
        files_to_link = []
        for file in glob(prefix + ".*"):
            files_to_link.append(os.path.basename(file))
        return files_to_link

    def _symlink_dag_and_associated_files(self, remove=False):
        target_folder = self._get_dag_folder()
        source_path = os.path.join(self.example_dags_folder, self.dag_name)
        for file_name in self._get_files_to_link(source_path):
            source_path = os.path.join(self.example_dags_folder, file_name)
            target_path = os.path.join(target_folder, file_name)
            if remove:
                try:
                    self.log.info("Remove symlink: {} -> {} ".format(
                        target_path, source_path))
                    os.remove(target_path)
                except OSError:
                    pass
            else:
                if not os.path.exists(target_path):
                    self.log.info("Symlink: {} -> {} ".format(target_path, source_path))
                    os.symlink(source_path, target_path)

    def _run_dag(self):
        dag_bag = models.DagBag(dag_folder=self._get_dag_folder(),
                                include_examples=False)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = dag_bag.get_dag(self.dag_id)
        dag.clear(reset_dag_runs=True)
        dag.run(ignore_first_depends_on_past=True, verbose=True)

    @staticmethod
    def _get_variables_dir():
        return None

    def setUp(self):
        if not os.environ.get('AIRFLOW__CORE__UNIT_TEST_MODE'):
            raise AirflowException(""""
The AIRFLOW__CORE__UNIT_TEST_MODE environment variable must be set to  non-empty value 
BEFORE you run the test so that Airflow engine is setup properly 
and uses unittest.db. Make sure it is set in the scripts executing the 
test or in your test configuration in IDE""")
        configuration.conf.load_test_config()
        self.gcp_store_authentication()
        db_utils.initdb(settings.RBAC)
        db_utils.resetdb(settings.RBAC)
        self.gcp_authenticate()
        # We checked that authentication works - but then we revoke it to make
        # sure we are not relying on the authentication
        self.gcp_revoke_authentication()
        self._symlink_dag_and_associated_files()

    def tearDown(self):
        self.gcp_restore_authentication()
        if not os.environ.get('SKIP_UNLINKING_EXAMPLES'):
            self._symlink_dag_and_associated_files(remove=True)

    @staticmethod
    def skip_check(key_name):
        key_path = BaseGcpSystemTestCase._get_key_path(key_name)
        return key_path is None
