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

from google.cloud.bigtable import Client
from google.cloud.bigtable.cluster import Cluster
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.table import Table
from google.cloud.bigtable_admin_v2 import enums
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


# noinspection PyAbstractClass
class BigTableHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud BigTable APIs.
    """

    _client = None

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(BigTableHook, self).__init__(gcp_conn_id, delegate_to)

    def get_client(self, project_id):
        if not self._client:
            self._client = Client(project=project_id, credentials=self._get_credentials(), admin=True)
        return self._client

    def get_instance(self, project_id, instance_id):
        """
        Retrieves and returns the specified BigTable Instance if it exists.
        Otherwise returns None.

        :param project_id: ID of the GCP Project.
        :type project_id: str
        :param instance_id: ID of the BigTable Instance.
        :type instance_id: str
        """

        client = self.get_client(project_id)

        instance = Instance(instance_id, client)
        if not instance.exists():
            return None
        return instance

    def delete_instance(self, project_id, instance_id):
        """
        Deletes the specified BigTable Instance.
        Raises google.api_core.exceptions.NotFound if the Instance does not exist.

        :param project_id: ID of the GCP Project.
        :type project_id: str
        :param instance_id: ID of the BigTable Instance.
        :type instance_id: str
        """
        instance = Instance(instance_id, self.get_client(project_id))
        instance.delete()

    def create_instance(self,
                        project_id,
                        instance_id,
                        main_cluster_id,
                        main_cluster_zone,
                        replica_cluster_id=None,
                        replica_cluster_zone=None,
                        instance_display_name=None,
                        instance_type=enums.Instance.Type.TYPE_UNSPECIFIED,
                        instance_labels=None,
                        cluster_nodes=None,
                        cluster_storage_type=enums.StorageType.STORAGE_TYPE_UNSPECIFIED,
                        timeout=None):
        """
        Creates new instance.

        :type project_id: str
        :param project_id: The ID of the GCP Project.
        :type instance_id: str
        :param instance_id: The ID for the new Instance.
        :type main_cluster_id: str
        :param main_cluster_id: The ID for main Cluster for the new Instance.
        :type main_cluster_zone: str
        :param main_cluster_zone: The zone for main Cluster.
            See https://cloud.google.com/bigtable/docs/locations for more details.
        :type replica_cluster_id: str
        :param replica_cluster_id: (optional) The ID for replica Cluster for the new Instance.
        :type replica_cluster_zone: str
        :param replica_cluster_zone: (optional)  The zone for replica Cluster.
        :type instance_type: enums.Instance.Type
        :param instance_type: (optional) The type of the Instance.
        :type instance_display_name: str
        :param instance_display_name: (optional) Human-readable name of the Instance.
                Defaults to ``instance_id``.
        :type instance_labels: dict
        :param instance_labels: (optional) Dictionary of labels to associate with the Instance.
        :type cluster_nodes: int
        :param cluster_nodes: (optional) Number of nodes for Cluster.
        :type cluster_storage_type: enums.StorageType
        :param cluster_storage_type: (optional) The type of storage.
        :type timeout: int
        :param timeout: (optional) timeout (in seconds) for Instance creation.
                        If None is not specified, Operator will wait indefinitely.
        """
        cluster_storage_type = enums.StorageType(cluster_storage_type)
        instance_type = enums.Instance.Type(instance_type)

        instance = Instance(
            instance_id,
            self.get_client(project_id),
            instance_display_name,
            instance_type,
            instance_labels,
        )

        clusters = [
            instance.cluster(
                main_cluster_id,
                main_cluster_zone,
                cluster_nodes,
                cluster_storage_type
            )
        ]
        if replica_cluster_id and replica_cluster_zone:
            clusters.append(instance.cluster(
                replica_cluster_id,
                replica_cluster_zone,
                cluster_nodes,
                cluster_storage_type
            ))
        operation = instance.create(
            clusters=clusters
        )
        operation.result(timeout)
        return instance

    # noinspection PyMethodMayBeStatic
    def create_table(self, instance, table_id, initial_split_keys, column_families):
        """
        Creates the specified BigTable Table.
        Raises google.api_core.exceptions.AlreadyExists if the Table already exists.

        :type instance: Instance
        :param instance: The Instance that owns the Table.
        :type table_id: str
        :param table_id: ID of the BigTable Table to create.
        :type initial_split_keys: list
        :param initial_split_keys: (Optional) list of row keys in bytes that will be used to
                initially split the Table.
        :type column_families: dict
        :param column_families: (Optional) A map columns to create. The key is the column_id str and
            the value is a GarbageCollectionRule.
        """
        table = Table(table_id, instance)
        table.create(initial_split_keys, column_families)

    def delete_table(self, project_id, instance_id, table_id):
        """
        Deletes the specified BigTable Table.
        Raises google.api_core.exceptions.NotFound if the Table does not exist.

        :type project_id: str
        :param project_id: ID of the GCP Project.
        :type instance_id: str
        :param instance_id: ID of the BigTable Instance.
        :type table_id: str
        :param table_id: ID of the BigTable Table.
        """
        instance = Instance(instance_id, self.get_client(project_id))
        table = Table(table_id, instance)
        table.delete()

    # noinspection PyMethodMayBeStatic
    def update_cluster(self, instance, cluster_id, nodes):
        """
        Updates number of nodes in specified Cluster.
        Raises google.api_core.exceptions.NotFound if the Cluster does not exist.

        :type instance: Instance
        :param instance: The Instance that owns the Cluster.
        :type cluster_id: str
        :param cluster_id: ID of the Cluster.
        :type nodes: int
        :param nodes: Number of desired nodes.
        """
        cluster = Cluster(cluster_id, instance)
        cluster.serve_nodes = nodes
        cluster.update()

    # noinspection PyMethodMayBeStatic
    def get_column_families_for_table(self, instance, table_id):
        """
        Fetches Column Families for the specified BigTable Table.

        :type instance: Instance
        :param instance: The Instance that owns the Table.
        :type table_id: str
        :param table_id: ID of the BigTable Table to fetch Column Families.
        """

        table = Table(table_id, instance)
        return table.list_column_families()

    # noinspection PyMethodMayBeStatic
    def get_cluster_states_for_table(self, instance, table_id):
        """
        Fetches ClusterStates for the specified BigTable Table.
        Raises google.api_core.exceptions.NotFound if the Table does not exist.

        :type instance: Instance
        :param instance: The Instance that owns the Table.
        :type table_id: str
        :param table_id: ID of the BigTable Table to fetch Column Families.
        """

        table = Table(table_id, instance)
        return table.get_cluster_states()
