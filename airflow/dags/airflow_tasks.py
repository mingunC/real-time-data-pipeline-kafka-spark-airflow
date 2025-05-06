import subprocess
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, DoubleType

logging.basicConfig(level=logging.INFO)

# Kafka→Spark Streaming 예제 로직
# 전역 함수로 정의하여 Airflow PythonOperator에서 직렬화 에러 방지

def run_spark_job():
    logging.info("===== run_spark_job 시작 =====")
    spark = SparkSession.builder \
        .appName("KafkaSparkWeatherStream") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    schema = StructType() \
        .add("location", StringType()) \
        .add("temperature", DoubleType()) \
        .add("humidity", DoubleType())

    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "weather_events") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()

        parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
            .withColumn("parsed", from_json(col("json_str"), schema)) \
            .select("parsed.*") \
            .withColumn("temp_fahrenheit", expr("temperature * 1.8 + 32"))

        query = parsed.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query.awaitTermination(10)
        logging.info("Completed Spark Streaming ")
    except Exception as e:
        logging.error(f"스파크 작업 중 오류: {e}", exc_info=True)
    finally:
        spark.stop()


# 배치 스타 스키마 변환 로직
# star_schema batch_etl.py를 직접 spark-submit으로 실행

def transform_star_schema_job():
    logging.info("===== transform_star_schema_job 시작 =====")
    subprocess.check_call([
        'spark-submit',
        '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
        '/opt/project/spark_app/batch_etl.py',
        '--raw-path', 'output/weather_data',
        '--output-dir', 'output/star_schema'
    ])
    logging.info("Completed the converting of Star schema")
