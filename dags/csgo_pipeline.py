# pylint: disable=import-error
from datetime import datetime, timedelta
import os
from os import path
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.local_to_s3 import LocalToS3Operator
from operators.emr_spark import EMRSparkOperator
from airflow.utils.dates import days_ago
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
          schedule_interval='@weekly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

match_result_scraper = BashOperator(task_id='Matches_Results_Scraping',
                                    bash_command='cd {}/airflow/scrapers/scrapers && scrapy crawl results -o match_results.json'.format(PROJECT_PATH),
                                    dag=dag
                                    )

match_players_scraper = BashOperator(task_id='Matches_Players_Scraping',
                                    bash_command='cd {}/airflow/scrapers/scrapers && scrapy crawl players -o match_players.json'.format(PROJECT_PATH),
                                    dag=dag
                                    )

stats_players_scraper = BashOperator(task_id='Stats_Players_Scraping',
                                    bash_command='cd {}/airflow/scrapers/scrapers && scrapy crawl stats -o player_stats.json'.format(PROJECT_PATH),
                                    dag=dag
                                    )

results_to_s3_operator = LocalToS3Operator( task_id='Results_to_S3',
                                            filepath="{}/airflow/scrapers/scrapers".format(PROJECT_PATH),
                                            filename="match_results.json",
                                            key="match_results",
                                            aws_credentials_id="aws_credentials",
                                            bucket_name="fpmacedo",
                                            dag=dag
                                          )

players_to_s3_operator = LocalToS3Operator( task_id='Players_to_S3',
                                            filepath="{}/airflow/scrapers/scrapers".format(PROJECT_PATH),
                                            filename="match_players.json",
                                            key="match_players",
                                            aws_credentials_id="aws_credentials",
                                            bucket_name="fpmacedo",
                                            dag=dag
                                          )

stats_to_s3_operator = LocalToS3Operator( task_id='Stats_to_S3',
                                            filepath="{}/airflow/scrapers/scrapers".format(PROJECT_PATH),
                                            filename="players_stats.json",
                                            key="players_stats",
                                            aws_credentials_id="aws_credentials",
                                            bucket_name="fpmacedo",
                                            dag=dag
                                          )



emr_and_spark_operator = EMRSparkOperator        (
                                            task_id='EMR_AND_PySpark_Executor',
                                            emr_cluster_prefix="CS_GO_PIPELINE",
                                            s3_script_key="spark/pyspark_script.py",
                                            local_script_key="{}/airflow/dags/pyspark_script.py".format(PROJECT_PATH),
                                            bucket_name="fpmacedo",
                                            region_name=Variable.get("region_name"),
                                            aws_access_key_id=Variable.get("aws_access_key_id_secret"),
                                            aws_secret_access_key=Variable.get("aws_secret_access_key"),
                                            dag=dag
                                          )


end_operator = DummyOperator(task_id='Finish_execution',  dag=dag)

start_operator>>match_result_scraper>>match_players_scraper>>stats_players_scraper

match_result_scraper>>results_to_s3_operator
match_players_scraper>>players_to_s3_operator
stats_players_scraper>>stats_to_s3_operator

results_to_s3_operator>>emr_and_spark_operator>>end_operator
players_to_s3_operator>>emr_and_spark_operator>>end_operator
stats_to_s3_operator>>emr_and_spark_operator>>end_operator