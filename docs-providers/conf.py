# flake8: noqa
# Disable Flake8 because of all the sphinx imports
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


# Airflow documentation build configuration file, created by
# sphinx-quickstart on Thu Oct  9 20:50:01 2014.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.
"""Configuration of Airflow Docs"""
import glob
import os
import sys
from typing import List

import yaml

import airflow

try:
    import sphinx_airflow_theme  # pylint: disable=unused-import

    airflow_theme_is_available = True
except ImportError:
    airflow_theme_is_available = False

provider = os.environ.get('AIRFLOW_PROVIDER')
CONF_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
ROOT_DIR = os.path.abspath(os.path.join(CONF_DIR, os.pardir))
DOCS_DIR = os.path.abspath(os.path.join(CONF_DIR, provider))

autodoc_mock_imports = [
    'MySQLdb',
    'adal',
    'analytics',
    'azure',
    'azure.cosmos',
    'azure.datalake',
    'azure.kusto',
    'azure.mgmt',
    'boto3',
    'botocore',
    'bson',
    'cassandra',
    'celery',
    'cloudant',
    'cryptography',
    'cx_Oracle',
    'datadog',
    'distributed',
    'docker',
    'google',
    'google_auth_httplib2',
    'googleapiclient',
    'grpc',
    'hdfs',
    'httplib2',
    'jaydebeapi',
    'jenkins',
    'jira',
    'kubernetes',
    'msrestazure',
    'pandas',
    'pandas_gbq',
    'paramiko',
    'pinotdb',
    'psycopg2',
    'pydruid',
    'pyhive',
    'pyhive',
    'pymongo',
    'pymssql',
    'pysftp',
    'qds_sdk',
    'redis',
    'simple_salesforce',
    'slackclient',
    'smbclient',
    'snowflake',
    'sshtunnel',
    'tenacity',
    'vertica_python',
    'winrm',
    'zdesk',
]

# Hack to allow changing for piece of the code to behave differently while
# the docs are being built. The main objective was to alter the
# behavior of the utils.apply_default that was hiding function headers
os.environ['BUILDING_AIRFLOW_DOCS'] = 'TRUE'

sys.path.append(os.path.join(os.path.dirname(__file__), '../docs/exts'))

# -- General configuration ------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    # 'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
    # 'sphinx.ext.graphviz',
    'sphinxarg.ext',
    'sphinxcontrib.jinja',
    'sphinx.ext.intersphinx',
    'autoapi.extension',
    'exampleinclude',
    'docroles',
    'removemarktransform',
    'sphinx_copybutton',
    # 'redirects',
]

autodoc_default_options = {'show-inheritance': True, 'members': True}


def load_config():
    root_dir = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
        )
    )
    templates_dir = os.path.join(root_dir, "airflow", "providers", provider, 'config_templates')
    file_path = os.path.join(templates_dir, "config.yml")
    if not os.path.exists(file_path):
        return {}

    with open(file_path) as config_file:
        return yaml.safe_load(config_file)


jinja_contexts = {'config_ctx': {"configs": load_config()}}

viewcode_follow_imported_members = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ['templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'Airflow'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
# version = '1.0.0'
version = airflow.__version__
# The full version, including alpha/beta/rc tags.
# release = '1.0.0'
release = airflow.__version__

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns: List[str] = [
    # Templates or partials
    'operator/_partials',
]


def _get_rst_filepath_from_path(filepath: str):
    if os.path.isdir(filepath):
        result = filepath
    elif os.path.isfile(filepath) and filepath.endswith('/__init__.py'):
        result = filepath.rpartition("/")[0]
    else:
        result = filepath.rpartition(".")[0]
    result += "/index.rst"

    result = f"_api/{os.path.relpath(result, ROOT_DIR)}"
    return result


exclude_patterns.extend(
    _get_rst_filepath_from_path(f)
    for f in glob.glob(f"{ROOT_DIR}/airflow/providers/{provider}/example_dags/**/*.py")
)

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# If true, keep warnings as "system message" paragraphs in the built documents.
keep_warnings = True

intersphinx_mapping = {
    # airflow
    'airflow': ('https://airflow.readthedocs.io/en/latest/', None),
    # third-party
    'boto3': ('https://boto3.amazonaws.com/v1/documentation/api/latest/', None),
    'celery': ('https://docs.celeryproject.org/en/stable/', None),
    'hdfs': ('https://hdfscli.readthedocs.io/en/latest/', None),
    'jinja2': ('https://jinja.palletsprojects.com/en/master/', None),
    'mongodb': ('https://api.mongodb.com/python/current/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'python': ('https://docs.python.org/3/', None),
    'requests': ('https://requests.readthedocs.io/en/master/', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/en/latest/', None),
}
if provider == 'google':
    intersphinx_mapping.update(
        {
            'google-api-core': ('https://googleapis.dev/python/google-api-core/latest', None),
            'google-cloud-automl': ('https://googleapis.dev/python/automl/latest', None),
            'google-cloud-bigquery': ('https://googleapis.dev/python/bigquery/latest', None),
            'google-cloud-bigquery-datatransfer': (
                'https://googleapis.dev/python/bigquerydatatransfer/latest',
                None,
            ),
            'google-cloud-bigquery-storage': ('https://googleapis.dev/python/bigquerystorage/latest', None),
            'google-cloud-bigtable': ('https://googleapis.dev/python/bigtable/latest', None),
            'google-cloud-container': ('https://googleapis.dev/python/container/latest', None),
            'google-cloud-core': ('https://googleapis.dev/python/google-cloud-core/latest', None),
            'google-cloud-datacatalog': ('https://googleapis.dev/python/datacatalog/latest', None),
            'google-cloud-datastore': ('https://googleapis.dev/python/datastore/latest', None),
            'google-cloud-dlp': ('https://googleapis.dev/python/dlp/latest', None),
            'google-cloud-kms': ('https://googleapis.dev/python/cloudkms/latest', None),
            'google-cloud-language': ('https://googleapis.dev/python/language/latest', None),
            'google-cloud-monitoring': ('https://googleapis.dev/python/monitoring/latest', None),
            'google-cloud-pubsub': ('https://googleapis.dev/python/pubsub/latest', None),
            'google-cloud-redis': ('https://googleapis.dev/python/redis/latest', None),
            'google-cloud-spanner': ('https://googleapis.dev/python/spanner/latest', None),
            'google-cloud-speech': ('https://googleapis.dev/python/speech/latest', None),
            'google-cloud-storage': ('https://googleapis.dev/python/storage/latest', None),
            'google-cloud-tasks': ('https://googleapis.dev/python/cloudtasks/latest', None),
            'google-cloud-texttospeech': ('https://googleapis.dev/python/texttospeech/latest', None),
            'google-cloud-translate': ('https://googleapis.dev/python/translation/latest', None),
            'google-cloud-videointelligence': (
                'https://googleapis.dev/python/videointelligence/latest',
                None,
            ),
            'google-cloud-vision': ('https://googleapis.dev/python/vision/latest', None),
        }
    )

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_rtd_theme'

if airflow_theme_is_available:
    html_theme = 'sphinx_airflow_theme'

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
html_title = "Airflow Documentation"

html_favicon = "../airflow/www/static/pin_32.png"

# Custom sidebar templates, maps document names to template names.
if airflow_theme_is_available:
    html_sidebars = {
        '**': [
            'version-selector.html',
            'searchbox.html',
            'globaltoc.html',
        ]
    }

# Output file base name for HTML help builder.
htmlhelp_basename = 'Airflowdoc'


# ------ Options for sphinx-autoapi -------------------------------------------
# See:
# https://sphinx-autoapi.readthedocs.io/en/latest/config.html

# Paths (relative or absolute) to the source code that you wish to generate
# your API documentation from.
provider_path = provider.replace("-", "/")
package_dir = os.path.abspath(f'../airflow/providers')
autoapi_dirs = [
    os.path.join(package_dir, provider_path, d)
    for d in os.listdir(os.path.join(package_dir, provider_path))
    if os.path.isdir(os.path.join(package_dir, provider_path, d)) and not d.startswith('__')
]

# A directory that has user-defined templates to override our default templates.
autoapi_template_dir = 'autoapi_templates'

# A list of patterns to ignore when finding files
autoapi_ignore = [
    'airflow/configuration/',
    '*/example_dags/*',
    '*/backport_provider_setup/*',
    '*/backport_provider_setup.py',
    '*/_internal*',
    '*/node_modules/*',
    '*/migrations/*',
]

# Keep the AutoAPI generated files on the filesystem after the run.
# Useful for debugging.
autoapi_keep_files = True

# Relative path to output the AutoAPI files into. This can also be used to place the generated documentation
# anywhere in your documentation hierarchy.
autoapi_root = f'{provider}/_api'

autoapi_add_toctree_entry = False

# -- Options for example include ------------------------------------------
exampleinclude_sourceroot = os.path.abspath('..')

# -- Options for sphinxcontrib-redirects ----------------------------------
if os.path.exists(os.path.join(DOCS_DIR, 'redirects.txt')):
    extensions.append('redirects')
    redirects_file = 'redirects.txt'


# -- Additional HTML Context variable
html_context = {
    # Google Analytics ID.
    # For more information look at:
    # https://github.com/readthedocs/sphinx_rtd_theme/blob/master/sphinx_rtd_theme/layout.html#L222-L232
    'theme_analytics_id': 'UA-140539454-1',
}
if airflow_theme_is_available:
    html_context = {
        # Variables used to build a button for editing the source code
        #
        # The path is created according to the following template:
        #
        # https://{{ github_host|default("github.com") }}/{{ github_user }}/{{ github_repo }}/
        # {{ theme_vcs_pageview_mode|default("blob") }}/{{ github_version }}{{ conf_py_path }}
        # {{ pagename }}{{ suffix }}
        #
        # More information:
        # https://github.com/readthedocs/readthedocs.org/blob/master/readthedocs/doc_builder/templates/doc_builder/conf.py.tmpl#L100-L103
        # https://github.com/readthedocs/sphinx_rtd_theme/blob/master/sphinx_rtd_theme/breadcrumbs.html#L45
        # https://github.com/apache/airflow-site/blob/91f760c/sphinx_airflow_theme/sphinx_airflow_theme/suggest_change_button.html#L36-L40
        #
        'theme_vcs_pageview_mode': 'edit',
        'conf_py_path': '/docs/',
        'github_user': 'apache',
        'github_repo': 'airflow',
        'github_version': 'master',
        'display_github': 'master',
        'suffix': '.rst',
    }
