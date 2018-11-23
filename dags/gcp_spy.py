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

import requests

from airflow import utils, DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator

GCP_SERVICES = [
    ('sql', 'Cloud SQL'),
    ('spanner', 'Spanner'),
    ('bigtable', 'BigTable'),
    ('compute', 'Compute Engine'),
]
INSTANCES_IN_PROJECT_TITLE = "Instances in `polidea-airflow`:"


def send_slack_msg(**context):
    with_instances = []
    with_instances_label = INSTANCES_IN_PROJECT_TITLE
    no_instances = []

    for gcp_service in GCP_SERVICES:
        result = context['task_instance'].\
            xcom_pull(task_ids='gcp_service_list_instances_{}'.format(gcp_service[0]))

        if result == "Listed 0 items.":
            field = {
                "title": gcp_service[1],
                "value": "-",
                "short": "True"
            }
            no_instances.append(field)
        else:
            value = ""
            for word in result.split(' '):
                value += '`' + word + '`\n'
            field = {
                "title": gcp_service[1],
                "value": value,
                "short": "False"
            }
            with_instances.append(field)

    data = {
        "attachments": [
            {
                "fallback": with_instances_label,
                "pretext": with_instances_label,
                "fields": with_instances,
                "color": "#0f9d58",
                "mrkdwn_in": ["fields"]
            },
            {
                "fields": no_instances,
                "color": "#4285f4",
                "mrkdwn_in": ["fields"]
            }
        ]
    }

    requests.post(
        "https://hooks.slack.com/services/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",
        data=(json.dumps(data)),
        headers={'Content-type': 'application/json'}
    )


def prepare_email(**context):
    with_instances = []
    no_instances = []

    for gcp_service in GCP_SERVICES:
        result = context['task_instance']. \
            xcom_pull(task_ids='gcp_service_list_instances_{}'.format(gcp_service[0]))

        if result == "Listed 0 items.":
            value = "-<br>"
            field = '<b>{}</b>:<br>{}<br>'.format(gcp_service[1], value)
            no_instances.append(field)
        else:
            value = ""
            for word in result.split(' '):
                value += word + '<br>'
            field = '<b>{}</b>:<br>{}<br>'.format(gcp_service[1], value)
            with_instances.append(field)

    html_content = ""
    for with_instance in with_instances:
        html_content += with_instance
    for no_instance in no_instances:
        html_content += no_instance

    context['task_instance'].xcom_push(key='email', value=html_content)


dag = DAG(dag_id='gcp_spy',
          default_args={
              'start_date': utils.dates.days_ago(0)
          },
          schedule_interval='@hourly')

send_slack_msg = PythonOperator(
    python_callable=send_slack_msg,
    provide_context=True,
    task_id='send_slack_msg',
    dag=dag
)

prepare_email = PythonOperator(
    python_callable=prepare_email,
    provide_context=True,
    task_id='prepare_email',
    dag=dag
)

send_email = EmailOperator(
    to='szymon.przedwojski@polidea.com',
    subject=INSTANCES_IN_PROJECT_TITLE,
    html_content="{{ task_instance.xcom_pull(task_ids='prepare_email', key='email') }}",
    task_id='send_email',
    dag=dag
)
prepare_email >> send_email

for gcp_service in GCP_SERVICES:
    bash = BashOperator(
        xcom_push=True,
        bash_command=
        "gcloud %s instances list | tail -n +2 | grep -oE '^[^ ]+' | tr '\n' ' '"
        % gcp_service[0],
        task_id="gcp_service_list_instances_%s" % gcp_service[0],
        dag=dag
    )
    bash >> send_slack_msg
    bash >> prepare_email
