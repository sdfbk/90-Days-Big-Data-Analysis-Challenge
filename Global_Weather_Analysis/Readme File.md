Global Weather Analytics & Forecasting Pipeline

An end-to-end data engineering and business intelligence project designed to ingest live and historical weather metrics across major international hubs and Indian cities, structure them into a high-performance relational star schema via PySpark, and present them through an interactive Power BI dashboard.

🛠️ Tech Stack & Tools Applied

Programming Language: Python 3.x & PySpark (for distributed data processing and ETL pipeline automation).

API Source: Open-Meteo Weather API (free-tier endpoints for historical and forecast data).

Database Management System: Microsoft SQL Server (MSSQL) / Local SQL Server (for relational data storage).

ETL & Automation: Python requests, PySpark DataFrames, sqlalchemy / JDBC, and Windows Task Scheduler.

Data Modeling & BI: Power BI Desktop (Star Schema design, DAX, and Azure Maps).

🚀 Step-by-Step Implementation Guide

Phase 1: Data Ingestion & ETL Pipeline (PySpark)

API Connection: Python scripts connect to the Open-Meteo API to fetch hourly and daily weather metrics (Temperature, Relative Humidity, Precipitation, Wind Speed, and Weather Codes) for target cities (Pune, New Delhi, Bangalore, London, Tokyo, New York).

Transformation (PySpark): Raw JSON payloads are ingested, flattened, parsed into structured date and time attributes using PySpark DataFrame transformations, and loaded cleanly into our staging workflow.

Database Insertion: Processed dataframes are loaded into a local Microsoft SQL Server instance using automated database connectivity.

Phase 2: Data Modeling & Star Schema Architecture

To ensure high query performance and accurate cross-filtering, the data was modeled into a relational star schema:

Fact_Weather_Metrics: Contains numerical observations (Temperature, Humidity, Wind Speed, Weather Codes) linked via foreign keys.

Dim_Location: Contains dimension attributes for cities, countries, and geographical coordinates (Latitude/Longitude).

Dim_Date: Comprehensive calendar dimension supporting time intelligence calculations and future forecasting horizons.

Phase 3: Data Transformation & Modeling (Power BI & DAX)

Unified Date & Time Formatting: Configured date relationships to link fact and dimension tables cleanly.

Custom DAX Measures: Developed business logic measures for metrics such as historical baselines and summaries to power the analytical visuals.

Phase 4: Dashboard Design & Visualization

The Power BI report was structured into four major analytical sections:

Global Location Slicer Panel: Clean dropdown and button filters enabling instant isolation of regional hubs.

Current Conditions Matrix & Azure Map: Real-time spatial tracking mapping global weather distribution scaled by temperature and wind metrics.

Historical Analysis Deep-Dive: Line and area charts visualizing past weather trends and temperature fluctuations.

Future Forecast Outlook: Comparative line charts contrasting upcoming predicted temperatures against 30-day historical averages.