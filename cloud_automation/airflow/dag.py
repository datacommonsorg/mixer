"""A simple Airflow DAG that is triggered externally by a Cloud Function when a
file lands in a GCS bucket.
Once triggered the DAG performs the following steps:
1. Triggers a Google Cloud Dataflow job with the input file information received
   from the Cloud Function trigger.
2. Upon completion of the Dataflow job, writes status .
"""

import datetime
import logging
import os

from airflow import configuration
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import dataflow_operator
from airflow.operators import python_operator
from airflow.utils.trigger_rule import TriggerRule

# Set start_date of the DAG to the -1 day. This will
# make the DAG immediately available for scheduling.
YESTERDAY = datetime.datetime.combine(
  datetime.datetime.today() - datetime.timedelta(1),
  datetime.datetime.min.time())

SUCCESS_TAG = 'success.txt'
FAILURE_TAG = 'failure.txt'

# Reads environment variables set as part of the airflow pipeline.
# The following Airflow variables are set:
# gcp_project:                  Google Cloud Platform project id.
# dataflow_template_location:   GCS location of Dataflow template.
# dataflow_staging_location:    GCS location of stagining directory for Dataflow.
# email:                        Email address to send failure notifications.
# completion_status_file_bucket: Path of GCS file to be updated on airflow completion.
config = models.Variable.get("variables_config", deserialize_json=True)


# Default arguments for airflow task. It is recommended to specify dataflow args
# here, instead of in dataflow job config.
DEFAULT_DAG_ARGS = {
  'start_date': YESTERDAY,
  'email': config['email'],
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 0,
  'project_id': config['gcp_project'],
  'dataflow_default_options': {
     'project': config['gcp_project'],
     'template_location': config['dataflow_template_location'],
     'runner': 'DataflowRunner',
     'region': 'us-central1',
     'zone': 'us-central1-a',
     'ip_configuration': 'WORKER_IP_PRIVATE',
     'staging_location': config['dataflow_staging_location'],
     'no_use_public_ips': True, 
  }
}

def update_on_completion(src, dst, **kwargs):
  """Write to GCS on completion of dataflow task.

  Update the completion status. This writes to either success.txt or
  failure.txt. gcs_hook doesn't have update api, so we use copy.
  """
  conn = gcs_hook.GoogleCloudStorageHook()
  bucket = config['completion_status_file_bucket']
  conn.copy(bucket, dst, bucket, src)

with models.DAG(dag_id='GcsToBTCache',
                description='A DAG triggered by an external Cloud Function',
                schedule_interval=None, default_args=DEFAULT_DAG_ARGS) as dag:


  # Build arguments for dataflow task. The dag_run.conf is a way of accessing
  # input variables passed by calling GCF function.
  job_args = {
    'bigtableInstanceId': config['bt_instance'],
    'bigtableTableId':    '{{ dag_run.conf["bigtable_id"] }}',
    'inputFile':         '{{ dag_run.conf["input_file"] }}',
    'bigtableProjectId':  config['gcp_project'],
  }

  # Main Dataflow task that will process and load the input csv file.
  dataflow_task = dataflow_operator.DataflowTemplateOperator(
    task_id='csv_to_bt',
    template=config['dataflow_template_location'],
    parameters=job_args)

  success_task = python_operator.PythonOperator(task_id='success-move-to-completion',
                                                python_callable=update_on_completion,
                                                op_args=[SUCCESS_TAG, FAILURE_TAG],
                                                provide_context=True,
                                                trigger_rule=TriggerRule.ALL_SUCCESS)

  failure_task = python_operator.PythonOperator(task_id='failure-move-to-completion',
                                                python_callable=update_on_completion,
                                                op_args=[FAILURE_TAG, SUCCESS_TAG],
                                                provide_context=True,
                                                trigger_rule=TriggerRule.ALL_FAILED)

  # The success_task and failure_task both wait on completion of
  # dataflow_task.
  dataflow_task >> success_task
  dataflow_task >> failure_task
