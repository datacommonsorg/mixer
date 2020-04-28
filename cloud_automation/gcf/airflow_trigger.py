""" This module triggers when a file is writtent to prophet-cache GCS bucket.

This create a cloud BT table, scales it up and then triggers a dataflow job.
"""

import google.auth
import google.auth.compute_engine.credentials
import google.auth.iam
from google.auth.transport.requests import Request
from google.cloud import storage
import google.oauth2.credentials
import google.oauth2.service_account
import pathlib
import requests


IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'
PROJECT_ID          = 'google.com:datcom-store-dev'
DATAFLOW_TMPL   = 'gs://datcom-dataflow-templates/templates/csv_to_bt'
PIPELINE_TRIGGER_FILE = 'airflow_trigger.txt'
SUCCESS_FILE = 'success.txt'
FAILURE_FILE = 'failure.txt'

def read_contents(bucket, name):
  client = storage.Client()
  bucket = client.get_bucket(bucket)
  blob = bucket.get_blob(name)
  return blob.download_as_string()


def gcs_trigger(data, context=None):
  bucket = data['bucket']
  name = data['name']

  # Check if this is triggered after flume job completion
  if name.endswith(PIPELINE_TRIGGER_FILE):
    csv_file = read_contents(bucket, name)
    path = pathlib.PurePath(csv_file)
    # path.parent.name gives the last directory in the path. We use this as
    # bt table id.
    bt_table_id = path.parent.name
    trigger_dag(bt_table_id, csv_file)
  return


def trigger_dag(table_id, csv_file):
    """Makes a POST request to the Cloud Composer(Airflow) DAG Trigger API.
    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.
    For more info, refer to
    https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-python
    To call this function from a Python script, omit the ``context`` argument
    and pass in a non-null value for the ``data`` argument.
    """
    # Instructions to find client id can be found at:
    # https://medium.com/google-cloud/using-airflow-experimental-rest-api-on-google-cloud-platform-cloud-composer-and-iap-9bd0260f095a
    # snarsale@ figure out where to store this in long-term.
    client_id = '386888630136-tjng343n77bnjvhdv240cc6rmr5ej8on.apps.googleusercontent.com'
    # This should be part of your airflow's URL:
    # {tenant-project-id}.appspot.com
    webserver_id = 'x7fea1724da0f8c91p-tp'
    # The name of the DAG is configured as part of the airflow job.
    dag_name = 'GcsToBTCache'
    webserver_url = (
        'https://'
        + webserver_id
        + '.appspot.com/api/experimental/dags/'
        + dag_name
        + '/dag_runs'
    )
    # bigtable_id and input_file are passed to airflow as these are required to
    # trigger the dataflow pipeline to read from CSV to cloud BT.
    data['bigtable_id'] = table_id
    data['input_file']  = csv_file
    # Make a POST request to IAP which then Triggers the DAG
    make_iap_request(webserver_url, client_id, method='POST', json={"conf":data})


# This code is copied from
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/iap/make_iap_request.py
# START COPIED IAP CODE
def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.
    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.
    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    bootstrap_credentials, _ = google.auth.default(
        scopes=[IAM_SCOPE])

    bootstrap_credentials.refresh(Request())

    signer_email = bootstrap_credentials.service_account_email
    if isinstance(bootstrap_credentials,
                  google.auth.compute_engine.credentials.Credentials):
        signer = google.auth.iam.Signer(
            Request(), bootstrap_credentials, signer_email)
    else:
        signer = bootstrap_credentials.signer

    service_account_credentials = google.oauth2.service_account.Credentials(
        signer, signer_email, token_uri=OAUTH_TOKEN_URI, additional_claims={
            'target_audience': client_id
        })

    google_open_id_connect_token = get_google_open_id_connect_token(
        service_account_credentials)

    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account {} does not have permission to '
                        'access the IAP-protected application.'.format(
                            signer_email))
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text


def get_google_open_id_connect_token(service_account_credentials):
    """Get an OpenID Connect token issued by Google for the service account."""
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
# END COPIED IAP CODE

