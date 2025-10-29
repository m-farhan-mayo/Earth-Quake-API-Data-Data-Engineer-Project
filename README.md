# Earth-Quake-API-Data-Data-Engineer-Project


This project demonstrates a fully parameterized, end-to-end ELT (Extract, Load, Transform) process built on the Azure platform. Instead of using a dedicated pipeline tool, this workflow is driven by pure Python code executed within a series of Azure Databricks notebooks.

The entire process, from data ingestion to final reporting tables, is fully parameterized. This means that crucial variables—such as API endpoints, storage paths, and table names—are defined as parameters at runtime, making the solution highly reusable and configurable without needing to modify the source code.

The project ingests real-time earthquake data from the USGS Earthquake API, processes it using the Medallion Architecture (Bronze, Silver, Gold), and stores the refined data in Azure Data Lake Storage (ADLS) Gen2.

Workflow & Project Recall
This project is structured as a three-stage data flow, with each stage executed by a separate, parameterized Databricks notebook.

1. Bronze Layer (Raw Data Ingestion)

Script: 01_Ingest_to_Bronze.py

Action: This notebook runs a pure Python script. It uses the requests library to make a live API call to the USGS Earthquake API.

Parameterization: The API endpoint URL (including date ranges or magnitude filters) and the raw ADLS save path (bronze_path) are supplied as parameters.

Storage: The raw, unaltered JSON data is written directly to the Bronze layer in the Azure Storage Account.

Purpose: To capture the source data exactly as it was received, creating an immutable, timestamped copy for auditing and reprocessing.

2. Silver Layer (Cleaned & Transformed Data)

Script: 02_Transform_to_Silver.py

Action: This notebook reads the raw JSON files from the Bronze layer using parameterized input paths.

Transformation (Cleaning): Using PySpark (the Python API for Spark), this script applies key transformations:

Schema Enforcement: Applies a defined schema to the raw VARIANT data.

Flattening: Un-nests the complex JSON structure (e.g., properties.mag, geometry.coordinates).

Cleaning: Filters out non-earthquake events (like 'quarry blasts'), handles null values, and renames columns to be user-friendly.

Type Casting: Converts epoch timestamps to standard UTC timestamps and ensures numerical fields (like magnitude, depth) are correctly typed.

Storage: The cleaned, structured data is written as a Delta Lake table to the Silver layer, using a parameterized output path (silver_path).

Purpose: To create a validated, de-duplicated, and structured single source of truth for all earthquake events.

3. Gold Layer (Business & Aggregated Data)

Script: 03_Aggregate_to_Gold.py

Action: This final notebook reads the cleaned data from the Silver Delta table.

Transformation (Aggregation): This script uses PySpark and Spark SQL to perform business-level aggregations, creating specific tables for analytics.

Examples:

daily_earthquake_summary: A table showing the total count of earthquakes and the average magnitude per day.

significant_earthquakes: A filtered table of all earthquakes with a magnitude > 4.5.

Storage: These final, aggregated tables are stored in the Gold layer (also as Delta tables), ready for BI tools like Power BI. All input and output paths/table names (silver_path, gold_path) are parameterized.

Purpose: To provide high-value, optimized, and ready-to-use data for business users and dashboards.
