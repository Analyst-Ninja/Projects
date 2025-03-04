from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
from redditETL.reddit_etl_copy import eshtablishMySQLconnection

default_args = {
    'owner': 'rohit',
    'start_date': days_ago(0),
    'email': 'r.kumar01@hotmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='reddit_etl',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    description='Reddit ETL to store the new subreddit posts with comments'
)

connectToDB = PythonOperator(
    task_id='transform_data',
    dag=dag,
    python_callable=eshtablishMySQLconnection
)

connectToDB