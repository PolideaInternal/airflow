Using Operators
===============

An operator represents a single, ideally idempotent, task. Operators
determine what actually executes when your DAG runs.

See the :ref:`Operators Concepts <concepts-operators>` documentation and the
:ref:`Operators API Reference <api-reference-operators>` for more
information.

.. contents:: :local:

BashOperator
------------

Use the :class:`~airflow.operators.bash_operator.BashOperator` to execute
commands in a `Bash <https://www.gnu.org/software/bash/>`__ shell.

.. literalinclude:: ../../airflow/example_dags/example_bash_operator.py
    :language: python
    :start-after: [START howto_operator_bash]
    :end-before: [END howto_operator_bash]

Templating
^^^^^^^^^^

You can use :ref:`Jinja templates <jinja-templating>` to parameterize the
``bash_command`` argument.

.. literalinclude:: ../../airflow/example_dags/example_bash_operator.py
    :language: python
    :start-after: [START howto_operator_bash_template]
    :end-before: [END howto_operator_bash_template]

Troubleshooting
^^^^^^^^^^^^^^^

Jinja template not found
""""""""""""""""""""""""

Add a space after the script name when directly calling a Bash script with
the ``bash_command`` argument. This is because Airflow tries to apply a Jinja
template to it, which will fail.

.. code-block:: python

    t2 = BashOperator(
        task_id='bash_example',

        # This fails with `Jinja template not found` error
        # bash_command="/home/batcher/test.sh",

        # This works (has a space after)
        bash_command="/home/batcher/test.sh ",
        dag=dag)

PythonOperator
--------------

Use the :class:`~airflow.operators.python_operator.PythonOperator` to execute
Python callables.

.. literalinclude:: ../../airflow/example_dags/example_python_operator.py
    :language: python
    :start-after: [START howto_operator_python]
    :end-before: [END howto_operator_python]

Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Use the ``op_args`` and ``op_kwargs`` arguments to pass additional arguments
to the Python callable.

.. literalinclude:: ../../airflow/example_dags/example_python_operator.py
    :language: python
    :start-after: [START howto_operator_python_kwargs]
    :end-before: [END howto_operator_python_kwargs]

Templating
^^^^^^^^^^

When you set the ``provide_context`` argument to ``True``, Airflow passes in
an additional set of keyword arguments: one for each of the :ref:`Jinja
template variables <macros>` and a ``templates_dict`` argument.

The ``templates_dict`` argument is templated, so each value in the dictionary
is evaluated as a :ref:`Jinja template <jinja-templating>`.

Google Cloud Platform Operators
-------------------------------

GoogleCloudStorageToBigQueryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the
:class:`~airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator`
to execute a BigQuery load job.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcs_to_bq_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_bq]
    :end-before: [END howto_operator_gcs_to_bq]

GceInstanceStartOperator
^^^^^^^^^^^^^^^^^^^^^^^^

Allows to start an existing Google Compute Engine instance.

In this example parameter values are extracted from OS environment variables.
Moreover, the ``default_args`` dict is used to pass common arguments to all operators in a single DAG.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :start-after: [START howto_operator_gce_args]
    :end-before: [END howto_operator_gce_args]


Define the :class:`~airflow.contrib.operators.gcp_compute_operator
.GceInstanceStartOperator` by passing the required arguments to the constructor.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_start]
    :end-before: [END howto_operator_gce_start]

GceInstanceStopOperator
^^^^^^^^^^^^^^^^^^^^^^^

Allows to stop an existing Google Compute Engine instance.

For parameter definition take a look at :class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator` above.

Define the :class:`~airflow.contrib.operators.gcp_compute_operator
.GceInstanceStopOperator` by passing the required arguments to the constructor.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_stop]
    :end-before: [END howto_operator_gce_stop]

GceSetMachineTypeOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Allows to change the machine type for a stopped instance to the specified machine type.

For parameter definition take a look at :class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator` above.

Define the :class:`~airflow.contrib.operators.gcp_compute_operator
.GceSetMachineTypeOperator` by passing the required arguments to the constructor.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_set_machine_type]
    :end-before: [END howto_operator_gce_set_machine_type]


GcfFunctionDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the ``default_args`` dict to pass arguments to the operator.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_delete_args]
    :end-before: [END howto_operator_gcf_delete_args]


Use the :class:`~airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator`
to delete a function from Google Cloud Functions.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_delete]
    :end-before: [END howto_operator_gcf_delete]

Troubleshooting
"""""""""""""""
If you want to run or deploy an operator using a service account and get “forbidden 403”
errors, it means that your service account does not have the correct
Cloud IAM permissions.

1. Assign your Service Account the Cloud Functions Developer role.
2. Grant the user the Cloud IAM Service Account User role on the Cloud Functions runtime
   service account.

The typical way of assigning Cloud IAM permissions with `gcloud` is
shown below. Just replace PROJECT_ID with ID of your Google Cloud Platform project
and SERVICE_ACCOUNT_EMAIL with the email ID of your service account.


.. code-block:: bash

  gcloud iam service-accounts add-iam-policy-binding \
    PROJECT_ID@appspot.gserviceaccount.com \
    --member="serviceAccount:[SERVICE_ACCOUNT_EMAIL]" \
    --role="roles/iam.serviceAccountUser"


See `Adding the IAM service agent user role to the runtime service <https://cloud.google.com/functions/docs/reference/iam/roles#adding_the_iam_service_agent_user_role_to_the_runtime_service_account>`_  for details

GcfFunctionDeployOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the :class:`~airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator`
to deploy a function from Google Cloud Functions.

The following examples of OS environment variables show various variants and combinations
of default_args that you can use. The variables are defined as follows:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_variables]
    :end-before: [END howto_operator_gcf_deploy_variables]

With those variables you can define the body of the request:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_body]
    :end-before: [END howto_operator_gcf_deploy_body]

When you create a DAG, the default_args dictionary can be used to pass the body and
other arguments:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_args]
    :end-before: [END howto_operator_gcf_deploy_args]

Note that the neither the body nor the default args are complete in the above examples.
Depending on the set variables, there might be different variants on how to pass source
code related fields. Currently, you can pass either sourceArchiveUrl, sourceRepository
or sourceUploadUrl as described in the
`CloudFunction API specification <https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#CloudFunction>`_.
Additionally, default_args might contain zip_path parameter to run the extra step of
uploading the source code before deploying it. In the last case, you also need to
provide an empty `sourceUploadUrl` parameter in the body.

Based on the variables defined above, example logic of setting the source code
related fields is shown here:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_variants]
    :end-before: [END howto_operator_gcf_deploy_variants]

The code to create the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy]
    :end-before: [END howto_operator_gcf_deploy]

Troubleshooting
"""""""""""""""

If you want to run or deploy an operator using a service account and get “forbidden 403”
errors, it means that your service account does not have the correct
Cloud IAM permissions.

1. Assign your Service Account the Cloud Functions Developer role.
2. Grant the user the Cloud IAM Service Account User role on the Cloud Functions runtime
   service account.

The typical way of assigning Cloud IAM permissions with `gcloud` is
shown below. Just replace PROJECT_ID with ID of your Google Cloud Platform project
and SERVICE_ACCOUNT_EMAIL with the email ID of your service account.

.. code-block:: bash

  gcloud iam service-accounts add-iam-policy-binding \
    PROJECT_ID@appspot.gserviceaccount.com \
    --member="serviceAccount:[SERVICE_ACCOUNT_EMAIL]" \
    --role="roles/iam.serviceAccountUser"


See `Adding the IAM service agent user role to the runtime service <https://cloud.google.com/functions/docs/reference/iam/roles#adding_the_iam_service_agent_user_role_to_the_runtime_service_account>`_  for details

If the source code for your function is in Google Source Repository, make sure that
your service account has the Source Repository Viewer role so that the source code
can be downloaded if necessary.

CloudSqlInstanceDatabaseCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a new database inside a Cloud SQL instance.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseCreateOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_db_create]
    :end-before: [END howto_operator_cloudsql_db_create]

Example request body:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_db_create_body]
    :end-before: [END howto_operator_cloudsql_db_create_body]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_db_create_template_fields]
  :end-before: [END gcp_sql_db_create_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for database insert
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert>`_.

CloudSqlInstanceDatabaseDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deletes a database from a Cloud SQL instance.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_db_delete]
    :end-before: [END howto_operator_cloudsql_db_delete]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_db_delete_template_fields]
  :end-before: [END gcp_sql_db_delete_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for database delete
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/delete>`_.

CloudSqlInstanceDatabasePatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Updates a resource containing information about a database inside a Cloud SQL instance
using patch semantics.
See: https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabasePatchOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_db_patch]
    :end-before: [END howto_operator_cloudsql_db_patch]

Example request body:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_db_patch_body]
    :end-before: [END howto_operator_cloudsql_db_patch_body]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_db_patch_template_fields]
  :end-before: [END gcp_sql_db_patch_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for database patch
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/patch>`_.

CloudSqlInstanceDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deletes a Cloud SQL instance in Google Cloud Platform.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_delete]
    :end-before: [END howto_operator_cloudsql_delete]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_delete_template_fields]
  :end-before: [END gcp_sql_delete_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for delete
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/delete>`_.

.. _CloudSqlInstanceCreateOperator:

CloudSqlInstanceCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a new Cloud SQL instance in Google Cloud Platform.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceCreateOperator`.

If an instance with the same name exists, no action will be taken and the operator
will succeed.

Arguments
"""""""""

Some arguments in the example DAG are taken from OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Example body defining the instance:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_create_body]
    :end-before: [END howto_operator_cloudsql_create_body]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_create]
    :end-before: [END howto_operator_cloudsql_create]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_create_template_fields]
  :end-before: [END gcp_sql_create_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for insert <https://cloud.google
.com/sql/docs/mysql/admin-api/v1beta4/instances/insert>`_.


.. _CloudSqlInstancePatchOperator:

CloudSqlInstancePatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Updates settings of a Cloud SQL instance in Google Cloud Platform (partial update).

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstancePatchOperator`.

This is a partial update, so only values for the settings specified in the body
will be set / updated. The rest of the existing instance's configuration will remain
unchanged.

Arguments
"""""""""

Some arguments in the example DAG are taken from OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Example body defining the instance:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_patch_body]
    :end-before: [END howto_operator_cloudsql_patch_body]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_patch]
    :end-before: [END howto_operator_cloudsql_patch]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_patch_template_fields]
  :end-before: [END gcp_sql_patch_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for patch <https://cloud.google
.com/sql/docs/mysql/admin-api/v1beta4/instances/patch>`_.


CloudSqQueryOperator
^^^^^^^^^^^^^^^^^^^^

Performs DDL or DML SQL queries in Google Cloud SQL instance. The DQL
(retrieving data from Google Cloud SQL) is not supported - you might run the SELECT
queries but results of those queries are discarded.

You can specify various connectivity methods to connect to running instance -
starting from public IP plain connection through public IP with SSL or both TCP and
socket connection via Cloud Sql Proxy. The proxy is downloaded and started/stopped
dynamically as needed by the operator.

New *cloudsql://* connection type is introduced that you should use to define what
kind of connectivity you want the operator to use. The connection is a "meta"
type of connection. It is not used to make an actual connectivity on it's own, but it
determines whether Cloud Sql Proxy should be started by `CloudSqlDatabaseHook`
and what kind of the database connection (Postgres or MySQL) should be created
dynamically - to either connect to Cloud SQL via public IP address or via the proxy.
The 'CloudSqlDatabaseHook` uses
:class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlProxyRunner` to manage Cloud Sql
Proxy lifecycle (each task has its own Cloud Sql Proxy)

When you build connection, you should use connection parameters as described in
:class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook`. You can see
examples of connections below for all the possible types of connectivity. Such connection
can be reused between different tasks (instances of `CloudSqlQueryOperator`) - each
task will get their own proxy started if needed with their own TCP or UNIX socket.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlQueryOperator`.

Since query operator can run arbitrary query - it cannot be guaranteed to be
idempotent. SQL query designer should design the queries to be idempotent. For example
both Postgres and MySql support CREATE TABLE IF NOT EXISTS statements that can be
used to create tables in an idempotent way.

Arguments
"""""""""

If you define connection via `AIRFLOW_CONN_*` URL defined in an environment
variable, make sure the URL components in the URL are URL-encoded.
See examples below for details.

Note that in case of SSL connections you need to have a mechanism to make the
certificate/key files available in predefined locations for all the workers on
which the operator can run. This can be provided for example by mounting
NFS-like volumes in the same path for all the workers.

Some arguments in the example DAG are taken from the OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql_query.py
      :language: python
      :start-after: [START howto_operator_cloudsql_query_arguments]
      :end-before: [END howto_operator_cloudsql_query_arguments]

Example connection definitions for all connectivity cases. Note that all the components
of the connection URI should be URL-encoded:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql_query.py
      :language: python
      :start-after: [START howto_operator_cloudsql_query_connections]
      :end-before: [END howto_operator_cloudsql_query_connections]

Using the operator
""""""""""""""""""

Example operators below are using all connectivity options (note connection id
from the operator matches the `AIRFLOW_CONN_*` postfix uppercase - this is
standard AIRFLOW notation for defining connection via environment variables):

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql_query.py
      :language: python
      :start-after: [START howto_operator_cloudsql_query_operators]
      :end-before: [END howto_operator_cloudsql_query_operators]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_query_template_fields]
    :end-before: [END gcp_sql_query_template_fields]

More information
""""""""""""""""

See `Google Cloud Sql Proxy documentation
<https://cloud.google.com/sql/docs/postgres/sql-proxy>`_
for details about Cloud Sql Proxy.

