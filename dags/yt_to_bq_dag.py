from airflow import DAG

from datetime import datetime,timedelta

from plugins.operators.yt_bq_operator import YtToBQOperator

from airflow.utils.dates import days_ago


args = {
        
    'owner': 'rahul',
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),

}



with DAG(
    dag_id='yt_to_bq_complete',
    default_args=args,
    schedule_interval=None,
    tags=['Testing'],
) as dag:
    
    yt_to_bq = YtToBQOperator(
    task_id='yt_to_bq',
    playlist_id = 'PL08903FB7ACA1C2FB',
    bigquery_dataset= 'Youtube_Data',
    yt_conn= 'yt_conn',
    bq_conn= 'bq_conn'
    )
    






