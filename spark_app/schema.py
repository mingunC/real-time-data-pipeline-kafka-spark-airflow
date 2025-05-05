from pyspark.sql.types import StructType, StringType, DoubleType

weather_schema = StructType() \
    .add("location", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType())