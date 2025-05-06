cat > simplified_star_schema_dag.py << 'EOF'
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# DAG 설정
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'weather_star_schema',
    default_args=default_args,
    description='Weather data dimensional modeling with Star Schema',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# 차원 테이블 생성 SQL 스크립트
create_dim_location = """
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    timezone VARCHAR(50),
    region VARCHAR(50),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);
"""

create_dim_date = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE,
    day INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    season VARCHAR(20)
);
"""

create_dim_time = """
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    hour INTEGER,
    minute INTEGER,
    second INTEGER,
    time_of_day VARCHAR(20),
    is_business_hours BOOLEAN
);
"""

create_dim_weather_type = """
CREATE TABLE IF NOT EXISTS dim_weather_type (
    weather_type_id SERIAL PRIMARY KEY,
    weather_main VARCHAR(50),
    weather_description VARCHAR(200),
    weather_category VARCHAR(50),
    severity_level INTEGER
);
"""

create_fact_weather_events = """
CREATE TABLE IF NOT EXISTS fact_weather_events (
    event_id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES dim_location(location_id),
    date_id INTEGER REFERENCES dim_date(date_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    weather_type_id INTEGER REFERENCES dim_weather_type(weather_type_id),
    temperature DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    humidity INTEGER,
    pressure INTEGER,
    wind_speed DECIMAL(5,2),
    wind_direction INTEGER,
    visibility INTEGER,
    cloudiness INTEGER,
    precipitation DECIMAL(5,2),
    event_timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP
);
"""

# 테이블 생성 작업
create_dim_location_task = PostgresOperator(
    task_id='create_dim_location',
    postgres_conn_id='postgres_default',
    sql=create_dim_location,
    dag=dag
)

create_dim_date_task = PostgresOperator(
    task_id='create_dim_date',
    postgres_conn_id='postgres_default',
    sql=create_dim_date,
    dag=dag
)

create_dim_time_task = PostgresOperator(
    task_id='create_dim_time',
    postgres_conn_id='postgres_default',
    sql=create_dim_time,
    dag=dag
)

create_dim_weather_type_task = PostgresOperator(
    task_id='create_dim_weather_type',
    postgres_conn_id='postgres_default',
    sql=create_dim_weather_type,
    dag=dag
)

create_fact_weather_events_task = PostgresOperator(
    task_id='create_fact_weather_events',
    postgres_conn_id='postgres_default',
    sql=create_fact_weather_events,
    dag=dag
)

# 데이터 적재 함수
def load_dimension_tables(**context):
    # 실제 구현: Kafka에서 데이터를 읽어 차원 테이블에 데이터 적재
    print("Loading dimension tables...")
    return "Dimension tables loaded successfully"

load_dimension_task = PythonOperator(
    task_id='load_dimension_tables',
    python_callable=load_dimension_tables,
    provide_context=True,
    dag=dag
)

def load_fact_table(**context):
    # 실제 구현: 차원 테이블의 키를 사용하여 팩트 테이블에 데이터 적재
    print("Loading fact table...")
    return "Fact table loaded successfully"

load_fact_task = PythonOperator(
    task_id='load_fact_table',
    python_callable=load_fact_table,
    provide_context=True,
    dag=dag
)

# 워크플로우 정의
[create_dim_location_task, create_dim_date_task, create_dim_time_task, 
 create_dim_weather_type_task] >> create_fact_weather_events_task >> load_dimension_task >> load_fact_task
EOF