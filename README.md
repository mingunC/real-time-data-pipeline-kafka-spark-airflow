# Real-Time Weather Data Pipeline

## Project Overview

This project implements a comprehensive real-time data pipeline for weather data processing, demonstrating modern data engineering practices and architecture. The pipeline ingests streaming weather data, processes it in real-time, transforms it according to a dimensional model, and implements data quality validation and metadata management.

## Key Components

### Data Ingestion & Streaming

The data ingestion layer uses Apache Kafka as a message broker to handle real-time weather data streams. The system is designed to be scalable and fault-tolerant, with the following features:

- Configurable Kafka topics for weather event data
- Real-time data producers simulating weather station feeds
- Robust error handling and message retry mechanisms
- Scalable architecture supporting multiple partitions for parallel processing

### Data Processing & Transformation

Apache Spark Structured Streaming forms the core of our data processing layer, enabling real-time analytics and transformation:

- Event-based processing of weather measurements
- Data cleansing and normalization operations
- Feature enhancement (e.g., temperature conversion between scales)
- Efficient storage in optimized Parquet format

### Data Modeling

The project implements a dimensional data model following star schema principles:

- Fact table containing weather measurements and metrics
- Dimension tables for locations, dates, times, and weather types
- Well-documented entity relationships with proper primary/foreign key constraints
- Support for historical tracking of dimensional changes (SCD Type 2)

### Metadata Management

A robust metadata management system ensures data governance and lineage tracking:

- Data lineage tracking via OpenLineage integration with Airflow
- Metadata registration with Google Cloud Data Catalog
- Complete documentation of data schemas and relationships
- Centralized repository for data discovery and exploration

### Data Quality Validation

Automated data quality checks ensure the reliability and consistency of processed data:

- Null value detection for required fields
- Range validation for measurement values (temperature, humidity)
- Statistical analysis of data distributions
- Automated alerts for quality issues

### Workflow Orchestration

Apache Airflow orchestrates the entire pipeline with scheduled workflows:

- Automated DAGs for data processing, validation, and metadata registration
- Task dependency management with proper error handling
- Monitoring and alerting capabilities
- Pipeline execution history and audit logging

## Technical Stack

- **Apache Kafka**: Real-time data streaming
- **Apache Spark**: Large-scale data processing
- **Apache Airflow**: Workflow orchestration
- **Google Cloud Platform**: Metadata catalog services
- **Docker/Kubernetes**: Containerization and deployment
- **Python**: Core implementation language

## Architecture

The system follows a layered architecture pattern:

![Architecture Diagram](weather_pipeline_architecture.png)

- **Data Processing Layer**: Handles data ingestion, processing, and storage
- **Metadata Management Layer**: Maintains metadata and lineage information
- **Quality Management Layer**: Ensures data quality and validation

## Documentation & Resources

- [Metadata Management Strategy](metadata_management.md)
- [Star Schema Data Model](weather_star_schema_erd.png)
- [Data Quality Framework](data_quality/)