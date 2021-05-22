# pylint: disable=import-error
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)

DEFAULT_ARGS = {
    'owner': 'Filipe',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

JOB_FLOW_OVERRIDES = {
    'Name': 'test_cluster',
    'ReleaseLabel': 'emr-5.28.0',
    'Instances': {
        'Ec2KeyName': 'XXX',
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 3,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': True,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

with DAG(
    dag_id="test_emr_cluster_creation",
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily'
) as dag:

    # Start the cluster
    cluster_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
    )

cluster_creator