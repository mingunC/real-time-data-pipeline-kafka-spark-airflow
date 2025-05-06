from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import datacatalog_v1

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
    'metadata_catalog_integration',
    default_args=default_args,
    description='Metadata catalog integration for weather data pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False
)


# Google Cloud Data Catalog 클라이언트 초기화
def initialize_datacatalog_client():
    return datacatalog_v1.DataCatalogClient()


# 데이터셋 등록 함수
def register_dataset(dataset_id, display_name, description, schema_fields):
    client = initialize_datacatalog_client()

    # 엔트리 그룹 생성/가져오기
    entry_group_id = "weather_data_group"
    project_id = "future-cat-458304-j8"
    location_id = "us-central1"

    entry_group_name = client.entry_group_path(project_id, location_id, entry_group_id)

    try:
        entry_group = client.get_entry_group(name=entry_group_name)
    except Exception:
        # 엔트리 그룹이 존재하지 않으면 새로 생성
        entry_group = client.create_entry_group(
            parent=f"projects/{project_id}/locations/{location_id}",
            entry_group_id=entry_group_id,
            entry_group=datacatalog_v1.EntryGroup(
                display_name="Weather Data Group",
                description="Group containing weather data pipeline metadata"
            )
        )

    # 엔트리 생성/업데이트
    entry_id = dataset_id.replace("_", "-")
    entry_name = client.entry_path(project_id, location_id, entry_group_id, entry_id)

    # 스키마 정의
    schema = datacatalog_v1.Schema()
    for field in schema_fields:
        column = datacatalog_v1.ColumnSchema()
        column.column = field['name']
        column.type_ = field['type']
        column.description = field.get('description', '')
        schema.columns.append(column)

    # 엔트리 정의
    entry = datacatalog_v1.Entry(
        display_name=display_name,
        description=description,
        schema=schema,
        user_specified_type="custom_table"
    )

    try:
        existing_entry = client.get_entry(name=entry_name)
        # 엔트리가 존재하면 업데이트
        entry.name = entry_name
        client.update_entry(entry=entry)
        print(f"Updated entry: {entry_name}")
    except Exception:
        # 엔트리가 존재하지 않으면 새로 생성
        created_entry = client.create_entry(
            parent=entry_group_name,
            entry_id=entry_id,
            entry=entry
        )
        print(f"Created entry: {created_entry.name}")

    return entry_name


# 차원 테이블 등록 작업
def register_dimension_tables(**context):
    # DIM_LOCATION 등록
    register_dataset(
        dataset_id="dim_location",
        display_name="Location Dimension",
        description="Dimension table containing geographical location data",
        schema_fields=[
            {"name": "location_id", "type": "INTEGER", "description": "Primary key for location dimension"},
            {"name": "city", "type": "STRING", "description": "City name"},
            {"name": "state", "type": "STRING", "description": "State or province"},
            {"name": "country", "type": "STRING", "description": "Country name"},
            {"name": "latitude", "type": "FLOAT", "description": "Latitude coordinate"},
            {"name": "longitude", "type": "FLOAT", "description": "Longitude coordinate"},
            {"name": "timezone", "type": "STRING", "description": "Timezone of the location"},
            {"name": "region", "type": "STRING", "description": "Region or area classification"},
            {"name": "valid_from", "type": "TIMESTAMP", "description": "SCD Type 2 validity start"},
            {"name": "valid_to", "type": "TIMESTAMP", "description": "SCD Type 2 validity end"},
            {"name": "is_current", "type": "BOOLEAN", "description": "Flag for current record"}
        ]
    )

    # DIM_DATE 등록
    register_dataset(
        dataset_id="dim_date",
        display_name="Date Dimension",
        description="Dimension table containing date attributes",
        schema_fields=[
            {"name": "date_id", "type": "INTEGER", "description": "Primary key for date dimension"},
            {"name": "date", "type": "DATE", "description": "Calendar date"},
            {"name": "day", "type": "INTEGER", "description": "Day of month (1-31)"},
            {"name": "month", "type": "INTEGER", "description": "Month (1-12)"},
            {"name": "quarter", "type": "INTEGER", "description": "Quarter (1-4)"},
            {"name": "year", "type": "INTEGER", "description": "Calendar year"},
            {"name": "is_weekend", "type": "BOOLEAN", "description": "Flag for weekend days"},
            {"name": "is_holiday", "type": "BOOLEAN", "description": "Flag for holiday dates"},
            {"name": "season", "type": "STRING", "description": "Season (Spring, Summer, etc.)"}
        ]
    )

    # DIM_TIME 등록
    register_dataset(
        dataset_id="dim_time",
        display_name="Time Dimension",
        description="Dimension table containing time attributes",
        schema_fields=[
            {"name": "time_id", "type": "INTEGER", "description": "Primary key for time dimension"},
            {"name": "hour", "type": "INTEGER", "description": "Hour (0-23)"},
            {"name": "minute", "type": "INTEGER", "description": "Minute (0-59)"},
            {"name": "second", "type": "INTEGER", "description": "Second (0-59)"},
            {"name": "time_of_day", "type": "STRING", "description": "Time classification (Morning, Afternoon, etc.)"},
            {"name": "is_business_hours", "type": "BOOLEAN", "description": "Flag for business hours"}
        ]
    )

    # DIM_WEATHER_TYPE 등록
    register_dataset(
        dataset_id="dim_weather_type",
        display_name="Weather Type Dimension",
        description="Dimension table containing weather type classifications",
        schema_fields=[
            {"name": "weather_type_id", "type": "INTEGER", "description": "Primary key for weather type dimension"},
            {"name": "weather_main", "type": "STRING", "description": "Main weather category"},
            {"name": "weather_description", "type": "STRING", "description": "Detailed weather description"},
            {"name": "weather_category", "type": "STRING", "description": "Categorized weather group"},
            {"name": "severity_level", "type": "INTEGER", "description": "Weather severity level (1-5)"}
        ]
    )

    return "Dimension tables registered in Data Catalog"


# 팩트 테이블 등록 작업
def register_fact_table(**context):
    # FACT_WEATHER_EVENTS 등록
    register_dataset(
        dataset_id="fact_weather_events",
        display_name="Weather Events Fact Table",
        description="Fact table containing weather events data with dimensional references",
        schema_fields=[
            {"name": "event_id", "type": "INTEGER", "description": "Primary key for weather event"},
            {"name": "location_id", "type": "INTEGER", "description": "Foreign key to location dimension"},
            {"name": "date_id", "type": "INTEGER", "description": "Foreign key to date dimension"},
            {"name": "time_id", "type": "INTEGER", "description": "Foreign key to time dimension"},
            {"name": "weather_type_id", "type": "INTEGER", "description": "Foreign key to weather type dimension"},
            {"name": "temperature", "type": "FLOAT", "description": "Temperature in Celsius"},
            {"name": "feels_like", "type": "FLOAT", "description": "Perceived temperature in Celsius"},
            {"name": "humidity", "type": "INTEGER", "description": "Humidity percentage"},
            {"name": "pressure", "type": "INTEGER", "description": "Atmospheric pressure in hPa"},
            {"name": "wind_speed", "type": "FLOAT", "description": "Wind speed in m/s"},
            {"name": "wind_direction", "type": "INTEGER", "description": "Wind direction in degrees"},
            {"name": "visibility", "type": "INTEGER", "description": "Visibility in meters"},
            {"name": "cloudiness", "type": "INTEGER", "description": "Cloudiness percentage"},
            {"name": "precipitation", "type": "FLOAT", "description": "Precipitation amount in mm"},
            {"name": "event_timestamp", "type": "TIMESTAMP", "description": "Timestamp when weather event occurred"},
            {"name": "processing_timestamp", "type": "TIMESTAMP", "description": "Timestamp when data was processed"}
        ]
    )

    return "Fact table registered in Data Catalog"


# 워크플로우 정의
register_dimension_tables_task = PythonOperator(
    task_id='register_dimension_tables',
    python_callable=register_dimension_tables,
    provide_context=True,
    dag=dag
)

register_fact_table_task = PythonOperator(
    task_id='register_fact_table',
    python_callable=register_fact_table,
    provide_context=True,
    dag=dag
)


# 메타데이터 검증 태스크
def validate_metadata_registration(**context):
    client = initialize_datacatalog_client()
    project_id = "future-cat-458304-j8"
    location_id = "us-central1"

    # 모든 등록된 테이블 조회
    scope = datacatalog_v1.SearchCatalogRequest.Scope()
    scope.include_project_ids.append(project_id)

    query = 'user_specified_type="custom_table"'

    request = datacatalog_v1.SearchCatalogRequest(
        scope=scope,
        query=query
    )

    result_count = 0
    for result in client.search_catalog(request=request):
        print(f"Found entry: {result.relative_resource_name}")
        result_count += 1

    if result_count >= 5:  # 5개의 테이블(4개의 차원 + 1개의 팩트) 확인
        return "Metadata validation successful"
    else:
        raise ValueError(f"Expected at least 5 registered tables, but found {result_count}")


validate_metadata_task = PythonOperator(
    task_id='validate_metadata_registration',
    python_callable=validate_metadata_registration,
    provide_context=True,
    dag=dag
)
