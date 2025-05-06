from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(raw_path=None, output_dir=None):
    """
    실행 로직을 담당하는 메인 함수
    """
    logger.info("▶ Batch ETL 시작")
    
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("StarSchemaBatchETL") \
        .getOrCreate()
    
    try:
        # 파일이 없으므로 샘플 데이터로 대체
        # 실제 데이터가 있다면 아래 주석을 해제
        # if raw_path and os.path.exists(raw_path):
        #     raw_df = spark.read.parquet(raw_path)
        # else:
        logger.info("샘플 데이터 생성")
        # 샘플 데이터 생성
        data = [
            ("Seoul", 25.5, 60.0, "2025-05-06 12:00:00"),
            ("Busan", 28.0, 70.0, "2025-05-06 12:00:00"),
            ("Daegu", 27.0, 65.0, "2025-05-06 12:00:00"),
            ("Inchon", 24.5, 55.0, "2025-05-06 12:00:00"),
            ("Gwangju", 26.0, 62.0, "2025-05-06 12:00:00")
        ]
        
        schema = StructType([
            StructField("city", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        raw_df = spark.createDataFrame(data, schema)
        raw_df = raw_df.withColumn("timestamp", to_timestamp(col("timestamp")))
        
        # 스타 스키마 생성
        # 1. 시간 차원 테이블
        logger.info("시간 차원 테이블 생성")
        time_dim = raw_df.select(
            col("timestamp"),
            year(col("timestamp")).alias("year"),
            month(col("timestamp")).alias("month"),
            dayofmonth(col("timestamp")).alias("day"),
            hour(col("timestamp")).alias("hour")
        ).distinct()
        
        # 2. 위치 차원 테이블
        logger.info("위치 차원 테이블 생성")
        location_dim = raw_df.select("city").distinct()
        
        # 3. 팩트 테이블
        logger.info("팩트 테이블 생성")
        fact_table = raw_df.select(
            col("city"),
            col("timestamp"),
            col("temperature"),
            col("humidity")
        )
        
        # 결과 출력
        logger.info("생성된 테이블 출력")
        logger.info("시간 차원 테이블:")
        time_dim.show()
        
        logger.info("위치 차원 테이블:")
        location_dim.show()
        
        logger.info("팩트 테이블:")
        fact_table.show()
        
        # 결과 저장 (선택적)
        if output_dir:
            save_dir = output_dir
        else:
            save_dir = "/tmp/weather_star_schema"
        
        logger.info(f"데이터 저장 경로: {save_dir}")
        os.makedirs(save_dir, exist_ok=True)
        
        time_dim.write.mode("overwrite").parquet(f"{save_dir}/time_dim")
        location_dim.write.mode("overwrite").parquet(f"{save_dir}/location_dim")
        fact_table.write.mode("overwrite").parquet(f"{save_dir}/fact_table")
        
        logger.info("✅ Batch ETL 작업 완료")
        
    except Exception as e:
        logger.error(f" X 오류 발생: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Data Star Schema ETL")
    parser.add_argument("--raw-path", dest="raw_path", help="원본 데이터 경로")
    parser.add_argument("--output-dir", dest="output_dir", help="결과 저장 경로")
    
    args = parser.parse_args()
    main(args.raw_path, args.output_dir)
