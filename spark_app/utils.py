from pyspark.sql.functions import from_json, col, expr, udf
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType
import hashlib

def parse_kafka_message(df: DataFrame, schema: StructType) -> DataFrame:
    return (
        df
        .selectExpr("CAST(value AS STRING) as json_str")
        .withColumn("parsed", from_json(col("json_str"), schema))
        .select("parsed.*")
    )

def enrich_with_temp_fahrenheit(df: DataFrame) -> DataFrame:
    return df.withColumn("temp_fahrenheit", expr("temperature * 1.8 + 32"))

def drop_nulls_and_outliers(df: DataFrame) -> DataFrame:
    return (
        df
        .dropna(subset=["location", "temperature", "humidity"])
        .filter((col("temperature") > -100) & (col("temperature") < 100))
        .filter((col("humidity") >= 0) & (col("humidity") <= 1))
    )

def hash_location(location: str) -> str:
    return hashlib.md5(location.encode()).hexdigest()

@udf(StringType())
def udf_hash_location(location: str) -> str:
    return hash_location(location)

@udf(IntegerType())
def udf_time_key(timestamp) -> int:
    return int(timestamp.strftime('%Y%m%d%H')) if timestamp else None