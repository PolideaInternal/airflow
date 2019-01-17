import json

from airflow import AirflowException
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook, \
    GcpTransferOperationStatus
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class GcpStorageTransferOperationWaitForJobStatusSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self,
                 operation_name,
                 job_anme,
                 project_id,
                 expected_status=GcpTransferOperationStatus.SUCCESS,
                 gcp_conn_id='google_cloud_default',
                 *args, **kwargs):
        super(GcpStorageTransferOperationWaitForJobStatusSensor, self).__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.job_name = job_anme
        self.expected_status = expected_status
        self.project_id = project_id
        self.gcp_cloud_conn_id = gcp_conn_id

    def poke(self, context):
        self.log.info('Sensor checks status of operation: %s',
                      self.operation_name)
        hook = GCPTransferServiceHook(
            gcp_conn_id=self.gcp_cloud_conn_id
        )
        result = hook.list_transfer_operations()\
            .list(
                filter=json.dumps({
                    'project_id': self.project_id,
                    'job_names': [self.job_name],
                })
            ).execute()
        return GCPTransferServiceHook\
            .check_operations_result(result,
                                     expected_status=self.expected_status)
