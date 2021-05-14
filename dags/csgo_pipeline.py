# pylint: disable=import-error
from datetime import datetime, timedelta
import os
from os import path
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
#from operators.data_quality import DataQualityOperator
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

start_operator>>match_result_scraper
start_operator>>match_players_scraper