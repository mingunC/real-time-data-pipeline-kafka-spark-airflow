from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType

# Kafka 메시지 스키마
weather_schema = StructType() \
    .add("location", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType())

# location_dim
location_dim_schema = StructType() \
    .add("location_key", StringType()) \
    .add("location_name", StringType())

# time_dim
time_dim_schema = StructType() \
    .add("time_key", IntegerType()) \
    .add("date", StringType()) \
    .add("hour", IntegerType()) \
    .add("weekday", StringType())

# weather_fact
weather_fact_schema = StructType() \
    .add("id", StringType()) \
    .add("location_key", StringType()) \
    .add("time_key", IntegerType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("temp_fahrenheit", DoubleType()) \
    .add("ingestion_time", TimestampType())