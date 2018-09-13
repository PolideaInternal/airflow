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

import time
import requests
import requests_toolbelt
from googleapiclient.discovery import build

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

import google.auth
import google.auth.app_engine
import google.auth.compute_engine.credentials
import google.auth.iam
from google.auth.transport.requests import Request
import google.oauth2.credentials
import google.oauth2.service_account

# Number of retries - used by googleapiclient method calls to perform retries
# For requests that are "retriable"
NUM_RETRIES = 5

# TODO
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 1


# noinspection PyAbstractClass
class GcfHook(GoogleCloudBaseHook):
    """
    Hook for the Google Cloud Functions APIs.
    """
    _conn = None

    def __init__(self,
                 api_version,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GcfHook, self).__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """
        Retrieves the connection to Cloud Functions.

        :return: Google Cloud Function services object
        :rtype: dict
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('cloudfunctions', self.api_version,
                               http=http_authorized, cache_discovery=False)
        return self._conn

    def get_function(self, name):
        """
        Returns the Cloud Function with the given name.

        :param name: name of the function
        :type name: str
        :return: a Cloud Functions object representing the function
        :rtype: dict
        """
        return self.get_conn().projects().locations().functions().get(
            name=name).execute(num_retries=NUM_RETRIES)

    def list_functions(self, full_location):
        """
        Lists all Cloud Functions created in the location.

        :param full_location: full location including the project in the form of
            of /projects/<PROJECT>/location/<LOCATION>
        :type full_location: str
        :return: array of Cloud Functions objects - representing functions in the location
        :rtype: [dict]
        """
        list_response = self.get_conn().projects().locations().functions().list(
            parent=full_location).execute(num_retries=NUM_RETRIES)
        return list_response.get("functions", [])

    def create_new_function(self, full_location, body):
        """
        Creates a new function in Cloud Function in the location specified in the body.

        :param full_location: full location including the project in the form of
            of /projects/<PROJECT>/location/<LOCATION>
        :type full_location: str
        :param body: body required by the Cloud Functions insert API
        :type body: dict
        :return: response returned by the operation
        :rtype: dict
        """
        response = self.get_conn().projects().locations().functions().create(
            location=full_location,
            body=body
        ).execute(num_retries=NUM_RETRIES)
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(operation_name)

    def update_function(self, name, body, update_mask):
        """
        Updates Cloud Functions according to the specified update mask.

        :param name: name of the function
        :type name: str
        :param body: body required by the cloud function patch API
        :type body: str
        :param update_mask: update mask - array of fields that should be patched
        :type update_mask: [str]
        :return: response returned by the operation
        :rtype: dict
        """
        response = self.get_conn().projects().locations().functions().patch(
            updateMask=",".join(update_mask),
            name=name,
            body=body
        ).execute(num_retries=NUM_RETRIES)
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(operation_name)

    def upload_function_zip(self, parent, zip_path):
        """
        Uploads zip file with sources.

        :param parent: Google Cloud Platform project id and region where zip file should
         be uploaded in the form of /projects/<PROJECT>/location/<LOCATION>
        :type parent: str
        :param zip_path: path of the valid .zip file to upload
        :type zip_path: str
        :return: Upload URL that was returned by generateUploadUrl method
        """
        response = self.get_conn().projects().locations().functions().generateUploadUrl(
            parent=parent
        ).execute(num_retries=NUM_RETRIES)
        upload_url = response.get('uploadUrl')
        with open(zip_path, 'rb') as fp:
            requests.put(
                url=upload_url,
                data=fp.read(),
                # Those two headers needs to be specified according to:
                # https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/generateUploadUrl
                # nopep8
                headers={
                    'Content-type': 'application/zip',
                    'x-goog-content-length-range': '0,104857600',
                }
            )
        return upload_url

    def delete_function(self, name):
        """
        Deletes the specified Cloud Function.

        :param name: name of the function
        :type name: str
        :return: response returned by the operation
        :rtype: dict
        """
        response = self.get_conn().projects().locations().functions().delete(
            name=name).execute(num_retries=NUM_RETRIES)
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(operation_name)

    def invoke_function(self, url):
        """
        Invokes an HTTP-triggered, IAM-protected Cloud Function under a given URL.
        Credentials used to authorize to the IAM Function are the credentials provided
        for the GCP connection.

        :param url: The Identity-Aware Proxy-protected URL to fetch.
        :type url: str
        :return: The page body, or raises an exception if the page couldn't be retrieved.
        :rtype: requests.Response
        """
        credentials = self._get_credentials()
        req = IAPRequest()
        return req.make_iap_request(url=url, credentials=credentials, method='GET')

    def _wait_for_operation_to_complete(self, operation_name):
        """
        Waits for the named operation to complete - checks status of the
        asynchronous call.

        :param operation_name: name of the operation
        :type operation_name: str
        :return: response  returned by the operation
        :rtype: dict
        :exception: AirflowException in case error is returned
        """
        service = self.get_conn()
        while True:
            operation_response = service.operations().get(
                name=operation_name,
            ).execute(num_retries=NUM_RETRIES)
            if operation_response.get("done"):
                response = operation_response.get("response")
                error = operation_response.get("error")
                # Note, according to documentation always either response or error is
                # set when "done" == True
                if error:
                    raise AirflowException(str(error))
                return response
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)


# noinspection PyProtectedMember
class IAPRequest(object):
    """
    This class is based on the example in the IAP docs and modified to match our needs.
    Source: https://cloud.google.com/iap/docs/authentication-howto#iap_make_request-python
    """
    def make_iap_request(self, url,
                         credentials,
                         method='GET', **kwargs):
        """
        Makes a request to an application protected by Identity-Aware Proxy.
        Args:
            url: The Identity-Aware Proxy-protected URL to fetch.
            credentials: The Service Account credentials as an instance of:
                       google.auth.app_engine.Credentials
            method: The request method to use
                  ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
            **kwargs: Any of the parameters defined for the request function:
                    https://github.com/requests/requests/blob/master/requests/api.py
        Returns:
            The page body, or raises an exception if the page couldn't be retrieved.
        """
        # Figure out what environment we're running in and get some preliminary
        # information about the service account.
        if isinstance(credentials, google.oauth2.credentials.Credentials):
            raise Exception('make_iap_request is only supported for service accounts.')
        elif isinstance(credentials, google.auth.app_engine.Credentials):
            requests_toolbelt.adapters.appengine.monkeypatch()
        # For service account's using the Compute Engine metadata service,
        # service_account_email isn't available until refresh is called.
        credentials.refresh(Request())
        signer_email = credentials.service_account_email

        if isinstance(credentials, google.auth.compute_engine.credentials.Credentials):
            # Since the Compute Engine metadata service doesn't expose the service
            # account key, we use the IAM signBlob API to sign instead.
            # In order for this to work:
            #
            # 1. Your VM needs the https://www.googleapis.com/auth/iam scope.
            #    You can specify this specific scope when creating a VM
            #    through the API or gcloud. When using Cloud Console,
            #    you'll need to specify the "full access to all Cloud APIs"
            #    scope. A VM's scopes can only be specified at creation time.
            #
            # 2. The VM's default service account needs the "Service Account Actor"
            #    role. This can be found under the "Project" category in Cloud
            #    Console, or roles/iam.serviceAccountActor in gcloud.
            signer = google.auth.iam.Signer(Request(), credentials, signer_email)
        else:
            # A Signer object can sign a JWT using the service account's key.
            signer = credentials.signer
            # Construct OAuth 2.0 service account credentials using the signer
            # and email acquired from the bootstrap credentials.
        service_account_credentials = google.oauth2.service_account.Credentials(
            signer, signer_email, token_uri=OAUTH_TOKEN_URI, additional_claims={
                'target_audience': url
            })
        # service_account_credentials gives us a JWT signed by the service
        # account. Next, we use that to obtain an OpenID Connect token,
        # which is a JWT signed by Google.
        google_open_id_connect_token = self.get_google_open_id_connect_token(
            service_account_credentials)
        # Fetch the Identity-Aware Proxy-protected URL, including an
        # Authorization header containing "Bearer " followed by a
        # Google-issued OpenID Connect token for the service account.
        resp = requests.request(
            method, url,
            headers={'Authorization': 'Bearer {}'.format(
                google_open_id_connect_token)}, **kwargs)
        return resp

    @staticmethod
    def get_google_open_id_connect_token(service_account_credentials):
        """
        Get an OpenID Connect token issued by Google for the service account.
        This function:

        1. Generates a JWT signed with the service account's private key
        containing a special "target_audience" claim.

        2. Sends it to the OAUTH_TOKEN_URI endpoint. Because the JWT in #1
        has a target_audience claim, that endpoint will respond with
        an OpenID Connect token for the service account -- in other words,
        a JWT signed by *Google*. The aud claim in this JWT will be
        set to the value from the target_audience claim in #1.

        For more information, see
        https://developers.google.com/identity/protocols/OAuth2ServiceAccount .

        The HTTP/REST example on that page describes the JWT structure and
        demonstrates how to call the token endpoint. (The example on that page
        shows how to get an OAuth2 access token; this code is using a
        modified version of it to get an OpenID Connect token.)
        """
        service_account_jwt = (
            service_account_credentials._make_authorization_grant_assertion())
        request = google.auth.transport.requests.Request()
        body = {
            'assertion': service_account_jwt,
            'grant_type': google.oauth2._client._JWT_GRANT_TYPE,
        }
        token_response = google.oauth2._client._token_endpoint_request(
            request, OAUTH_TOKEN_URI, body)
        return token_response['id_token']
