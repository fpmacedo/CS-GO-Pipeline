# pylint: disable=import-error
from datetime import datetime, timedelta
import os
from os import path
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.local_to_s3 import LocalToS3Operator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import Variable

PROJECT_PATH = path.abspath(path.join(path.dirname(__file__), '../..'))

default_args = {
    'owner': 'Filipe',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


dag = DAG('CS_GO_Pipeline',
          default_args=default_args,
          description='Extract, Load and transform data from HLTV with Airflow',
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

match_result_scraper = BashOperator(task_id='Matches_Results_Scraping',
                                    bash_command='cd {}/airflow/scrapers && scrapy runspider match_results.py -o match_results.json'.format(PROJECT_PATH),
                                    dag=dag
                                    )

match_players_scraper = BashOperator(task_id='Matches_Players_Scraping',
                                    bash_command='cd {}/airflow/scrapers && scrapy runspider match_players.py -o match_players.json'.format(PROJECT_PATH),
                                    dag=dag
                                    )

results_to_s3_operator = LocalToS3Operator( task_id='Results_to_S3',
                                            filepath="{}/airflow/scrapers".format(PROJECT_PATH),
                                            filename="match_results.json",
                                            key="match_results",
                                            aws_credentials_id="aws_credentials",
                                            bucket_name="fpmacedo",
                                            dag=dag
                                          )

players_to_s3_operator = LocalToS3Operator( task_id='Players_to_S3',
                                            filepath="{}/airflow/scrapers".format(PROJECT_PATH),
                                            filename="match_players.json",
                                            key="match_players",
                                            aws_credentials_id="aws_credentials",
                                            bucket_name="fpmacedo",
                                            dag=dag
                                          )


"""
cluster_creator = EmrCreateJobFlowOperator(
                                            task_id='Create_EMR_Cluster',
                                            job_flow_overrides=JOB_FLOW_OVERRIDES,
                                            dag=dag,
                                            aws_conn_id='aws_credentials',
                                            emr_conn_id='emr_default',
                                          )
"""

start_operator>>match_result_scraper
start_operator>>match_players_scraper

match_result_scraper>>results_to_s3_operator
match_players_scraper>>players_to_s3_operator