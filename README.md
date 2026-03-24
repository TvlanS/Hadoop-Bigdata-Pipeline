# Docker Hadoop Ecosystem - Airbnb ELT Pipeline

A containerized big data ecosystem for ingesting, processing, and analyzing Airbnb listing data using Hadoop, Spark, Hive, Trino, ClickHouse, and Airflow

![Architecture Image](https://github.com/TvlanS/Hadoop-Bigdata-Pipeline/blob/469c60c31e0456b82ceefb51dd7f0796d6f2d101/readme_images/data_architecture.png)


## Features

- **Multi-layer Data Architecture**: Bronze (raw), Silver (cleaned), Gold (modeled) layers
- **Containerized Services**: All components run in Docker containers
- **Data Ingestion**: Web scraper for Inside Airbnb data with schema standardization
- **Distributed Storage**: HDFS for scalable file storage
- **Data Processing**: Apache Spark for batch processing and transformations
- **SQL Querying**: Hive for metadata management and Trino for interactive queries
- **OLAP Database**: ClickHouse for high-performance analytics
- **Orchestration**: Apache Airflow for pipeline scheduling and monitoring
- **Notebook Environment**: Jupyter with PySpark for development and exploration
- **Visualization**: Power BI integration for dashboard creation

## Prerequisites

- Docker and Docker Compose
- 8GB+ RAM available for containers
- Git (for cloning the repository)

## Building the Ingestion Image

The web scraper for Airbnb data requires building a custom Docker image:

```bash
cd raw_ingest_webscrap
docker build -t raw-ingest-webscrap .
```

## Getting Started

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd docker-hadoop-ecosystem
   ```

2. **Start the ecosystem**
   ```bash
   docker-compose up -d
   ```

3. **Wait for services to initialize** (may take 2-3 minutes on first run)

4. **Run the ELT pipeline**
   - Access Airflow at `http://localhost:8081` (admin/admin)
   - Trigger the `elt_pipeline_listings` DAG

## Accessing Services

| Service | URL | Port | Credentials |
|---------|-----|------|-------------|
| **Airflow** | http://localhost:8081 | 8081 | admin/admin |
| **Jupyter Notebook** | http://localhost:8888 | 8888 | token in logs |
| **HDFS NameNode** | http://localhost:9870 | 9870 | - |
| **YARN ResourceManager** | http://localhost:8088 | 8088 | - |
| **Trino** | http://localhost:8080 | 8080 | - |
| **ClickHouse** | http://localhost:8123 | 8123 | - |
| **Hive Server** | jdbc:hive2://localhost:10000 | 10000 | - |

## Project Structure

```
├── airflow/dags/           # Airflow pipeline definitions
├── bronze_layer/           # Raw data storage (partitioned by city/date)
├── hadoop-conf/            # Hadoop configuration files
├── notebooks/              # Jupyter notebooks for development
│   ├── bronze/            # Data ingestion scripts
│   ├── silver/            # Data cleaning scripts
│   └── gold/              # Data modeling scripts
├── raw_ingest_webscrap/   # Web scraper Docker image
├── scripts/               # SQL setup scripts
└── docker-compose.yml     # Service definitions
```

## Data Flow

1. **Ingestion**: Web scraper downloads Airbnb listings data (London, Bristol, Edinburgh)
2. **Bronze**: Raw data uploaded to HDFS as Parquet files
3. **Silver**: Data cleaned, deduplicated, and standardized using Spark
4. **Gold**: Data modeled into star schema (dim_host, dim_listings, fct_listings)
5. **Analytics**: Processed data loaded to ClickHouse for querying and visualization

## Development

- Use Jupyter notebooks at `http://localhost:8888` for Spark development
- Notebooks are mounted at `/home/jovyan/work` in the container
- Spark can read/write to HDFS at `hdfs://namenode:9000`
- Hive tables can be queried via Trino or directly through Spark

## Notes

- First run may require downloading multiple Docker images (~5GB)
- Update `.env` file with the correct `raw_file_path` for your system
- Spark driver memory is configured as 1GB (adjust in docker-compose if needed)
- ClickHouse setup includes pre-configured users and database
- The web scraper image must be built before running the pipeline (see above)

## License

MIT
