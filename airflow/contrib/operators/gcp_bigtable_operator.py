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

import google.api_core.exceptions

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.hooks.gcp_bigtable_hook import BigTableHook
from airflow.utils.decorators import apply_defaults
from google.cloud.bigtable_admin_v2 import enums
from google.cloud.bigtable.table import ClusterState


class BigTableValidationMixin(object):
    """
    Common class for BigTable operators for validating required fields.
    """

    REQUIRED_ATTRIBUTES = []

    def _validate_inputs(self):
        for attr_name in self.REQUIRED_ATTRIBUTES:
            if not getattr(self, attr_name):
                raise AirflowException('Empty parameter: {}'.format(attr_name))


class BigTableInstanceCreateOperator(BaseOperator, BigTableValidationMixin):
    """
    Creates the BigTable Instance.
    If Instance with given ID already exists, operator will succeed.

    For more details about Instance creation have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.create # noqa: E501

    :type project_id: str
    :param project_id: The ID of the GCP Project.
    :type instance_id: str
    :param instance_id: The ID for the new Instance.
    :type main_cluster_id: str
    :param main_cluster_id: The ID for main Cluster for the new Instance.
    :type main_cluster_zone: str
    :param main_cluster_zone: The zone for main Cluster
        See https://cloud.google.com/bigtable/docs/locations for more details.
    :type replica_cluster_id: str
    :param replica_cluster_id: (optional) The ID for replica Cluster for the new Instance.
    :type replica_cluster_zone: str
    :param replica_cluster_zone: (optional)  The zone for replica Cluster.
    :type instance_type: IntEnum
    :param instance_type: (optional) The type of the Instance.
    :type instance_display_name: str
    :param instance_display_name: (optional) Human-readable name of the Instance. Defaults to ``instance_id``. # noqa: E501
    :type instance_labels: dict
    :param instance_labels: (optional) Dictionary of labels to associate with the Instance.
    :type cluster_nodes: int
    :param cluster_nodes: (optional) Number of nodes for Cluster.
    :type cluster_storage_type: IntEnum
    :param cluster_storage_type: (optional) The type of storage.
    :type timeout: int
    :param timeout: (optional) timeout (in seconds) for Instance creation.
                    If None is not specified, Operator will wait indefinitely.
    """

    REQUIRED_ATTRIBUTES = ('project_id', 'instance_id', 'main_cluster_id', 'main_cluster_zone')
    template_fields = ['project_id', 'instance_id', 'main_cluster_id', 'main_cluster_zone']

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 main_cluster_id,
                 main_cluster_zone,
                 replica_cluster_id=None,
                 replica_cluster_zone=None,
                 instance_display_name=None,
                 instance_type=None,
                 instance_labels=None,
                 cluster_nodes=None,
                 cluster_storage_type=None,
                 timeout=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.main_cluster_id = main_cluster_id
        self.main_cluster_zone = main_cluster_zone
        self.replica_cluster_id = replica_cluster_id
        self.replica_cluster_zone = replica_cluster_zone
        self.instance_display_name = instance_display_name
        self.instance_type = instance_type
        self.instance_labels = instance_labels
        self.cluster_nodes = cluster_nodes
        self.cluster_storage_type = cluster_storage_type
        self.timeout = timeout
        self._validate_inputs()
        self.hook = BigTableHook()
        super(BigTableInstanceCreateOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        instance = self.hook.get_instance(self.project_id, self.instance_id)
        if instance:
            # Based on Instance.__eq__ instance with the same ID and client is considered as equal.
            self.log.info(
                "The Instance '%s' already exists in this project. Consider it as created",
                self.instance_id
            )
            return
        try:
            self.hook.create_instance(
                self.project_id,
                self.instance_id,
                self.main_cluster_id,
                self.main_cluster_zone,
                self.replica_cluster_id,
                self.replica_cluster_zone,
                self.instance_display_name,
                self.instance_type,
                self.instance_labels,
                self.cluster_nodes,
                self.cluster_storage_type,
                self.timeout,
            )
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error('An error occurred. Exiting.')
            raise e


class BigTableInstanceDeleteOperator(BaseOperator, BigTableValidationMixin):
    """
    Deletes the BigTable Instance, including its Clusters and all related Tables.

    For more details about deleting Instance have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.delete # noqa: E501

    :type project_id: str
    :param project_id: The ID of the GCP Project.
    :type instance_id: str
    :param instance_id: The ID of the Instance to delete.
    """
    REQUIRED_ATTRIBUTES = ('project_id', 'instance_id')
    template_fields = ['project_id', 'instance_id']

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self._validate_inputs()
        self.hook = BigTableHook()
        super(BigTableInstanceDeleteOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        try:
            self.hook.delete_instance(self.project_id, self.instance_id)
        except google.api_core.exceptions.NotFound:
            self.log.info(
                "The Instance '%s' does not exist in project '%s'. Consider it as deleted",
                self.instance_id, self.project_id
            )
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error('An error occurred. Exiting.')
            raise e


class BigTableTableCreateOperator(BaseOperator, BigTableValidationMixin):
    """
    Creates the Table in BigTable Instance.

    For more details about creating Table have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.create

    :type project_id: str
    :param project_id: The ID of the GCP Project.
    :type instance_id: str
    :param instance_id: The ID of the Instance that will hold the new Table.
    :type table_id: str
    :param table_id: The ID of the Table to be created.
    """
    REQUIRED_ATTRIBUTES = ('project_id', 'instance_id', 'table_id')
    template_fields = ['project_id', 'instance_id', 'table_id']

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 table_id,
                 initial_split_keys=None,
                 column_families=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.initial_split_keys = initial_split_keys or list()
        self.column_families = column_families or dict()
        self._validate_inputs()
        self.hook = BigTableHook()
        self.instance = None
        super(BigTableTableCreateOperator, self).__init__(*args, **kwargs)

    def _compare_column_families(self):
        table_column_families = self.hook.get_column_families_for_table(self.instance, self.table_id)
        if set(table_column_families.keys()) != set(self.column_families.keys()):
            self.log.error("Table '%s' has different set of Column Families", self.table_id)
            self.log.error("Expected: %s", self.column_families.keys())
            self.log.error("Actual: %s", table_column_families.keys())
            return False

        for key in table_column_families.keys():
            # there is difference in structure between local Column Families and remote ones
            # the local ones are kept in `gc_rule` property of remote ones.
            if table_column_families[key].gc_rule != self.column_families[key]:
                self.log.error("Column Family '%s' differs for Table '%s'.", key, self.table_id)
                return False
        return True

    def execute(self, context):
        self.instance = self.hook.get_instance(self.project_id, self.instance_id)
        if not self.instance:
            raise AirflowException("Dependency: Instance '{}' does not exist in project '{}'.".format(
                self.instance_id, self.project_id))
        try:
            self.hook.create_table(
                self.instance,
                self.table_id,
                self.initial_split_keys,
                self.column_families
            )
        except google.api_core.exceptions.AlreadyExists:
            if not self._compare_column_families():
                raise AirflowException(
                    "Table '{}' already exists with different Column Families.".format(self.table_id))
            self.log.info("The Table '{}' already exists. Consider it as created", self.table_id)


class BigTableTableDeleteOperator(BaseOperator, BigTableValidationMixin):
    """
    Deletes the BigTable Table.

    For more details about deleting Table have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.delete # noqa: E501

    :type project_id: str
    :param project_id: The ID of the GCP Project.
    :type instance_id: str
    :param instance_id: The ID of the Instance.
    :type table_id: str
    :param table_id: The ID of the Table to be deleted.
    """
    REQUIRED_ATTRIBUTES = ('project_id', 'instance_id', 'table_id')
    template_fields = ['project_id', 'instance_id', 'table_id']

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 table_id,
                 app_profile_id=None,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.app_profile_id = app_profile_id
        self._validate_inputs()
        self.hook = BigTableHook()
        super(BigTableTableDeleteOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        instance = self.hook.get_instance(self.project_id, self.instance_id)
        if not instance:
            raise AirflowException("Dependency: Instance '{}' does not exist.".format(self.instance_id))

        try:
            self.hook.delete_table(
                self.project_id,
                self.instance_id,
                self.table_id,
            )
        except google.api_core.exceptions.NotFound:
            # It's OK if table doesn't exists.
            self.log.info("The Table '%s' no longer exists. Consider it as deleted", self.table_id)
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error('An error occurred. Exiting.')
            raise e


class BigTableClusterUpdateOperator(BaseOperator, BigTableValidationMixin):
    """
    Updates a BigTable Cluster.

    For more details about updating Cluster have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/cluster.html#google.cloud.bigtable.cluster.Cluster.update # noqa: E501

    :type project_id: str
    :param project_id: The ID of the GCP Project.
    :type instance_id: str
    :param instance_id: The ID of the Instance.
    :type cluster_id: str
    :param cluster_id: The ID of the Cluster to update.
    :type nodes: int
    :param nodes: Desired number of nodes for the Cluster.
    """
    REQUIRED_ATTRIBUTES = ('project_id', 'instance_id', 'cluster_id', 'nodes')
    template_fields = ['project_id', 'instance_id', 'cluster_id', 'nodes']

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 cluster_id,
                 nodes,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.cluster_id = cluster_id
        self.nodes = nodes
        self._validate_inputs()
        self.hook = BigTableHook()
        super(BigTableClusterUpdateOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        instance = self.hook.get_instance(self.project_id, self.instance_id)
        if not instance:
            raise AirflowException("Dependency: Instance '{}' does not exist.".format(self.instance_id))

        try:
            self.hook.update_cluster(
                instance,
                self.cluster_id,
                self.nodes
            )
        except google.api_core.exceptions.NotFound:
            raise AirflowException("Dependency: Cluster '{}' does not exist for Instance '{}'.".format(
                self.cluster_id,
                self.instance_id
            ))
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.log.error('An error occurred. Exiting.')
            raise e


class BigTableTableWaitForReplicationSensor(BaseSensorOperator, BigTableValidationMixin):
    """
    Sensor that waits for BigTable Table to be fully replicated to its Clusters.
    No exception will be raised if the Instance or the Table does not exist.

    For more details about Cluster states for a Table have a look at the reference:
    https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.get_cluster_states # noqa: E501

    :type project_id: str
    :param project_id: The ID of the GCP Project.
    :type instance_id: str
    :param instance_id: The ID of the Instance.
    :type table_id: str
    :param table_id: The ID of the Table to check replication status.
    """
    REQUIRED_ATTRIBUTES = ('project_id', 'instance_id', 'table_id')
    template_fields = ['project_id', 'instance_id', 'table_id']

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 table_id,
                 *args, **kwargs):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self._validate_inputs()
        self.hook = BigTableHook()
        super(BigTableTableWaitForReplicationSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        instance = self.hook.get_instance(self.project_id, self.instance_id)
        if not instance:
            self.log.info("Dependency: Instance '%s' does not exist.", self.instance_id)
            return False

        try:
            cluster_states = self.hook.get_cluster_states_for_table(instance, self.table_id)
        except google.api_core.exceptions.NotFound:
            self.log.info(
                "Dependency: Table '%s' does not exist in Instance '%s'.", self.table_id, self.instance_id)
            return False

        ready_state = ClusterState(enums.Table.ClusterState.ReplicationState.READY)

        is_table_replicated = True
        for cluster_id in cluster_states.keys():
            if cluster_states[cluster_id] != ready_state:
                self.log.info("Table '%s' is not yet replicated on Cluster '%s'.", self.table_id, cluster_id)
                is_table_replicated = False

        if not is_table_replicated:
            return False

        self.log.info("Table '%s' is replicated.", self.table_id)
        return True
