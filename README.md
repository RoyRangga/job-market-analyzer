# 🚀 Job Market Analyzer: End-to-End Data Pipeline

An automated, End-to-End Data Engineering pipeline designed to extract, process, enrich, and model data job vacancies in Indonesia (Kalibrr & Jobstreet). 

This project demonstrates a modern data stack architecture, featuring **Local LLM integration** for intelligent data enrichment.

## 🏗️ Architecture & Tech Stack

1. **Orchestration:** [Apache Airflow] (Dockerized)
2. **Ingestion (Scraping):** [Python + Selenium] (Headless Chrome)
3. **Data Lake (Object Storage):** [MinIO] (S3 Compatible)
4. **Data Processing:** [Apache Spark / PySpark 3.3.4]
5. **AI Enrichment:** [Ollama - Llama 3.2] (Extracting Hard Skills & Education levels via prompt engineering)
6. **Data Warehouse:** [PostgreSQL]
7. **Data Modeling:** [dbt (Data Build Tool)]

## 🔄 Pipeline Workflow

1. **Extract:** Python script scrapes job listings (Kalibrr & Jobstreet) and uploads raw `.parquet` files to **MinIO** buckets (`job-market-kalibrr-raw` & `job-market-jobstreet-raw`).
2. **Transform (Spark):** PySpark reads raw files from MinIO, cleanses the data, standardizes formats, and loads it into staging tables in SQL Server/Postgres.
3. **Enrich (Ollama):** The pipeline fetches unstructured job descriptions and minimum qualifications, feeding them to a local **Llama 3.2** model via Ollama API to strictly extract Technical Hard Skills, Majors, and Minimum Education.
4. **Model (dbt):** dbt transforms and joins the cleaned staging tables and AI-enriched dimension tables into a final, denormalized Master Table ready for BI tools (Tableau/Power BI).

## 🚀 How to Run Locally

1. Clone this repository.
2. Ensure Docker and Docker Compose are installed.
3. Configure your credentials in `profiles.yml` and the respective `.py` files.
4. Build and start the containers:
   ```bash
   docker-compose build airflow
   docker-compose up -d
