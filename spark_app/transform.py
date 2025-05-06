from pyspark.sql.functions import current_timestamp

# timestamp 컬럼 생성
with_ts = clean_df.withColumn("ingestion_time", current_timestamp())

# dim_location 생성
location_dim = with_ts \
    .select("location") \
    .distinct() \
    .withColumn("location_key", udf_hash_location(col("location"))) \
    .select("location_key", "location")

# dim_time 생성
time_dim = with_ts \
    .withColumn("time_key", udf_time_key(col("ingestion_time"))) \
    .withColumn("date", date_format("ingestion_time", "yyyy-MM-dd")) \
    .withColumn("hour", hour("ingestion_time")) \
    .withColumn("weekday", dayofweek("ingestion_time")) \
    .select("time_key", "date", "hour", "weekday") \
    .distinct()

# fact 테이블 생성
fact_table = with_ts \
    .withColumn("location_key", udf_hash_location(col("location"))) \
    .withColumn("time_key", udf_time_key(col("ingestion_time"))) \
    .select("location_key", "time_key", "temperature", "humidity", "temp_fahrenheit", "ingestion_time")