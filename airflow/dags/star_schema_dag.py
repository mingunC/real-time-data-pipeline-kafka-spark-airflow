from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 5, 5),
    'retries': 1,
}

with DAG(
    dag_id='weather_star_schema_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1) 스트리밍 작업: Kafka -> Spark Streaming 스크립트 실행
    def run_stream():
        subprocess.check_call(['bash', '/tmp/run_spark.sh'])

    stream_task = PythonOperator(
        task_id='stream_weather_data',
        python_callable=run_stream,
    )

    # 2) 배치 변환 작업: 스타 스키마 생성 스크립트 실행
    def run_transform():
        subprocess.check_call(['spark-submit', '/opt/project/spark_app/batch_etl.py'])

    transform_task = PythonOperator(
        task_id='transform_star_schema',
        python_callable=run_transform,
    )

    stream_task >> transform_task
