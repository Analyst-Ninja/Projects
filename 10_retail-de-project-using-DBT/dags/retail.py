from airflow.decorators import task, dag
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import datetime

@dag(
    start_date = datetime(2023, 1, 1),
    schedule = None,
    catchup = False,
    tags = ['retail'], 
    dag_id = 'retail'
)
def retail():
    
    upload_csv_to_s3 = LocalFilesystemToS3Operator(
        task_id = 'upload_csv_to_s3',
        filename='/usr/local/airflow/include/dataset/online_retail.csv',
        dest_key='raw/online_retail.csv',
        dest_bucket="retail-data-for-dbt-project",
        aws_conn_id="AWS",
        verify=False,
        replace=False,
        encrypt=False,
        gzip=False,
        acl_policy=None,
    )

retail()
