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
"""
This module contains Cassandra to Google Cloud Storage operator.
"""

import json
from base64 import b64encode
from datetime import datetime
from decimal import Decimal
from tempfile import NamedTemporaryFile
from uuid import UUID

from cassandra.util import Date, Time, SortedSet, OrderedMapSerializedKey

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.cassandra_hook import CassandraHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CassandraToGoogleCloudStorageOperator(BaseOperator):
    """
    Copy data from Cassandra to Google cloud storage in JSON format

    Note: Arrays of arrays are not supported.

    :param cql: The CQL to execute on the Cassandra table.
    :type cql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google cloud storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MySQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files (see notes in the
        filename param docs above). This param allows developers to specify the
        file size of the splits. Check https://cloud.google.com/storage/quotas
        to see the maximum allowed file size for a single object.
    :type approx_max_file_size_bytes: long
    :param cassandra_conn_id: Reference to a specific Cassandra hook.
    :type cassandra_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('cql', 'bucket', 'filename', 'schema_filename',)
    template_ext = ('.cql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 cql,
                 bucket,
                 filename,
                 schema_filename=None,
                 approx_max_file_size_bytes=1900000000,
                 cassandra_conn_id='cassandra_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.cql = cql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.cassandra_conn_id = cassandra_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.hook = None

    # Default Cassandra to BigQuery type mapping
    CQL_TYPE_MAP = {
        'BytesType': 'BYTES',
        'DecimalType': 'FLOAT',
        'UUIDType': 'BYTES',
        'BooleanType': 'BOOL',
        'ByteType': 'INTEGER',
        'AsciiType': 'STRING',
        'FloatType': 'FLOAT',
        'DoubleType': 'FLOAT',
        'LongType': 'INTEGER',
        'Int32Type': 'INTEGER',
        'IntegerType': 'INTEGER',
        'InetAddressType': 'STRING',
        'CounterColumnType': 'INTEGER',
        'DateType': 'TIMESTAMP',
        'SimpleDateType': 'DATE',
        'TimestampType': 'TIMESTAMP',
        'TimeUUIDType': 'BYTES',
        'ShortType': 'INTEGER',
        'TimeType': 'TIME',
        'DurationType': 'INTEGER',
        'UTF8Type': 'STRING',
        'VarcharType': 'STRING',
    }

    def execute(self, context):
        cursor = self._query_cassandra()
        files_to_upload = self._write_local_data_files(cursor)

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            files_to_upload.update(self._write_local_schema_file(cursor))

        # Flush all files before uploading
        for file_handle in files_to_upload.values():
            file_handle.flush()

        self._upload_to_gcs(files_to_upload)

        # Close all temp file handles.
        for file_handle in files_to_upload.values():
            file_handle.close()

        # Close all sessions and connection associated with this Cassandra cluster
        self.hook.shutdown_cluster()

    def _query_cassandra(self):
        """
        Queries cassandra and returns a cursor to the results.
        """
        self.hook = CassandraHook(cassandra_conn_id=self.cassandra_conn_id)
        session = self.hook.get_conn()
        cursor = session.execute(self.cql)
        return cursor

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.

        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        tmp_file_handles = {self.filename.format(file_no): tmp_file_handle}
        for row in cursor:
            row_dict = self._generate_data_dict(row._fields, row)
            dump = json.dumps(row_dict).encode('utf-8')
            tmp_file_handle.write(dump)

            # Append newline to make dumps BigQuery compatible.
            tmp_file_handle.write(b'\n')

            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                tmp_file_handles[self.filename.format(file_no)] = tmp_file_handle

        return tmp_file_handles

    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema for the results to a
        local file system.

        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        schema = []
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)

        for name, type_ in zip(cursor.column_names, cursor.column_types):
            schema.append(self._generate_schema_dict(name, type_))
        json_serialized_schema = json.dumps(schema).encode('utf-8')

        tmp_schema_file_handle.write(json_serialized_schema)
        return {self.schema_filename: tmp_schema_file_handle}

    def _upload_to_gcs(self, files_to_upload):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        for obj, tmp_file_handle in files_to_upload.items():
            hook.upload(self.bucket, obj, tmp_file_handle.name, 'application/json')

    def _generate_data_dict(self, names, values):
        row_dict = {}
        for name, value in zip(names, values):
            row_dict.update({name: self._convert_value(name, value)})
        return row_dict

    def _convert_value(self, name, value):  # pylint:disable=too-many-return-statements
        if not value:
            return value

        if isinstance(value, (str, int, float, bool, dict)):
            return value

        if isinstance(value, bytes):
            return b64encode(value).decode('ascii')

        if isinstance(value, UUID):
            return b64encode(value.bytes).decode('ascii')

        if isinstance(value, (datetime, Date)):
            return str(value)

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, Time):
            return str(value).split('.')[0]

        if isinstance(value, (list, SortedSet)):
            return self._convert_array_types(name, value)

        if hasattr(value, '_fields'):
            return self._convert_user_type(value)

        if isinstance(value, tuple):
            return self._convert_tuple_type(value)

        if isinstance(value, OrderedMapSerializedKey):
            return self._convert_map_type(value)

        raise AirflowException('Unexpected value: {}'.format(value))

    def _convert_array_types(self, name, value):
        return [self._convert_value(name, nested_value) for nested_value in value]

    def _convert_user_type(self, value):
        """
        Converts a user type to RECORD that contains n fields, where n is the
        number of attributes. Each element in the user type class will be converted to its
        corresponding data type in BQ.
        """
        names = value._fields
        values = [self._convert_value(name, getattr(value, name)) for name in names]
        return self._generate_data_dict(names, values)

    def _convert_tuple_type(self, value):
        """
        Converts a tuple to RECORD that contains n fields, each will be converted
        to its corresponding data type in bq and will be named 'field_<index>', where
        index is determined by the order of the tuple elements defined in cassandra.
        """
        names = ['field_' + str(i) for i in range(len(value))]
        values = [self._convert_value(name, value) for name, value in zip(names, value)]
        return self._generate_data_dict(names, values)

    def _convert_map_type(self, value):
        """
        Converts a map to a repeated RECORD that contains two fields: 'key' and 'value',
        each will be converted to its corresponding data type in BQ.
        """
        converted_map = []
        for k, v in zip(value.keys(), value.values()):
            converted_map.append({
                'key': self._convert_value('key', k),
                'value': self._convert_value('value', v)
            })
        return converted_map

    def _generate_schema_dict(self, name, type_):
        field_schema = dict()
        field_schema.update({'name': name})
        field_schema.update({'type': self._get_bq_type(type_)})
        field_schema.update({'mode': self._get_bq_mode(type_)})
        fields = self._get_bq_fields(type_)
        if fields:
            field_schema.update({'fields': fields})
        return field_schema

    def _get_bq_fields(self, type_):
        fields = []

        if not self._is_simple_type(type_):
            names, types = [], []

            if self._is_array_type(type_) and self._is_record_type(type_.subtypes[0]):
                names = type_.subtypes[0].fieldnames
                types = type_.subtypes[0].subtypes
            elif self._is_record_type(type_):
                names = type_.fieldnames
                types = type_.subtypes

            if types and not names and type_.cassname == 'TupleType':
                names = ['field_' + str(i) for i in range(len(types))]
            elif types and not names and type_.cassname == 'MapType':
                names = ['key', 'value']

            for name, type_ in zip(names, types):  # pylint:disable=redefined-argument-from-local
                field = self._generate_schema_dict(name, type_)
                fields.append(field)

        return fields

    @staticmethod
    def _is_simple_type(type_):
        return type_.cassname in CassandraToGoogleCloudStorageOperator.CQL_TYPE_MAP

    @staticmethod
    def _is_array_type(type_):
        return type_.cassname in ['ListType', 'SetType']

    @staticmethod
    def _is_record_type(type_):
        return type_.cassname in ['UserType', 'TupleType', 'MapType']

    def _get_bq_type(self, type_):
        if self._is_simple_type(type_):
            return CassandraToGoogleCloudStorageOperator.CQL_TYPE_MAP[type_.cassname]
        elif self._is_record_type(type_):
            return 'RECORD'
        elif self._is_array_type(type_):
            return self._get_bq_type(type_.subtypes[0])
        else:
            raise AirflowException('Not a supported type: ' + type_.cassname)

    def _get_bq_mode(self, type_):
        if self._is_array_type(type_) or type_.cassname == 'MapType':
            return 'REPEATED'
        elif self._is_record_type(type_) or self._is_simple_type(type_):
            return 'NULLABLE'
        else:
            raise AirflowException('Not a supported type: ' + type_.cassname)
