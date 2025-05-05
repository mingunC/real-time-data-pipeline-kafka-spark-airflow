from pyspark.sql.functions import from_json, col, expr
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

def parse_kafka_message(df: DataFrame, schema: StructType) -> DataFrame:
    return df.selectExpr("CAST(value AS STRING) as json_str") \
             .withColumn("parsed", from_json(col("json_str"), schema)) \
             .select("parsed.*")

def enrich_with_temp_fahrenheit(df: DataFrame) -> DataFrame:
    return df.withColumn("temp_fahrenheit", expr("temperature * 1.8 + 32"))

def drop_nulls_and_outliers(df):
    return df \
        .dropna(subset=["location", "temperature", "humidity"]) \
        .filter((col("temperature") > -100) & (col("temperature") < 100)) \
        .filter((col("humidity") >= 0) & (col("humidity") <= 1))