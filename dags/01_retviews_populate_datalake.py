import datetime as dt
import os
from scripts.convert_csv import encode_csv_utf8
from scripts.load_s3_raw import upload_file_to_s3_raw

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.utils.dates import days_ago
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

DAG_ID = os.path.basename(__file__).replace(".py", "")
S3_BUCKET = 'retviews-demo'

DEFAULT_ARGS = {
    'owner': 'aveek',
    'start_date': dt.datetime(2022, 1, 30, 17, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG(
        dag_id=DAG_ID,
        description='Retviews Datalake Demo',
        dagrun_timeout=dt.timedelta(minutes=5),
        start_date=days_ago(1),
        schedule_interval=None,
        default_args=DEFAULT_ARGS,
) as dag:
    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    convert_csv = PythonOperator(
        task_id="convert_csv",
        python_callable=encode_csv_utf8,
        dag=dag
    )

    upload_s3 = PythonOperator(
        task_id="upload_s3_raw",
        python_callable=upload_file_to_s3_raw,
        dag=dag
    )

    glue_raw_curated = AwsGlueJobOperator(
        task_id='glue_raw_curated',
        job_name='retviews_superstore_raw_to_curated'
    )

    glue_curated_aggregated = AwsGlueJobOperator(
        task_id='glue_curated_aggregated',
        job_name='retviews_superstore_curated_to_aggregated'
    )

    slack_notify = SlackWebhookOperator(
        task_id='slack_notify',
        http_conn_id='slack-aveekd22',
        webhook_token='/TDWM3QJ59/B0318QRE6Q2/umwJvMrBHylcZXdVuOUQ2SXN',
        message=f'Airflow Alert - Job completed successfully at {dt.datetime.now()}.',
        channel='#retviews',
        username='airflow_notification',
        dag=dag
    )

    begin >> convert_csv >> upload_s3 >> glue_raw_curated >> glue_curated_aggregated >> slack_notify >> end
