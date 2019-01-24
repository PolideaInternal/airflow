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
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook, \
    GcpTransferOperationStatus
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class GcpStorageTransferOperationWaitForJobStatusSensor(BaseSensorOperator):

    template_fields = ['job_name']

    @apply_defaults
    def __init__(self,
                 operation_name,
                 job_name,
                 project_id,
                 expected_status=GcpTransferOperationStatus.SUCCESS,
                 gcp_conn_id='google_cloud_default',
                 *args, **kwargs):
        super(GcpStorageTransferOperationWaitForJobStatusSensor, self).__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.job_name = job_name
        self.expected_status = expected_status
        self.project_id = project_id
        self.gcp_cloud_conn_id = gcp_conn_id

    def poke(self, context):
        self.log.info('Sensor checks status of operation: %s',
                      self.operation_name)
        hook = GCPTransferServiceHook(
            gcp_conn_id=self.gcp_cloud_conn_id
        )
        operations = hook.list_transfer_operations(
            filter={
                'project_id': self.project_id,
                'job_names': [self.job_name],
            })
        return GCPTransferServiceHook\
            .check_operations_result(operations=operations,
                                     expected_status=self.expected_status)
