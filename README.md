Rust Data Engineering Project: NYC Yellow Taxi 2025 Analysis

This project explores the NYC TLC Yellow Taxi Trip dataset for 2025 using Rust together with Apache DataFusion for analytical processing.

The application loads the monthly Parquet datasets, combines them into a unified dataset, and executes analytical queries using two approaches:

DataFusion DataFrame API

DataFusion SQL query interface

All computed results are displayed as formatted tables directly in the terminal. A screenshot showing the output is also included in the repository.

Project Overview

The goal of this project is to perform analytical queries on NYC taxi data while demonstrating the use of Rust for data engineering workflows.

Main tasks performed in this project:

Import all 12 monthly Parquet files for 2025

Register the data as a single logical table

Perform aggregation queries using DataFrame API and SQL

Display query outputs in structured terminal tables

Save the final program output screenshot

Dataset Information

The dataset is publicly available from the NYC Taxi & Limousine Commission (TLC).

Trip Record Dataset
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Data Dictionary Documentation
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

Data Download Instructions

Follow these steps to download and prepare the dataset:

Go to the NYC TLC trip record data webpage.

Find the section titled Yellow Taxi Trip Records.

Download the Parquet files for all months of 2025.

Create a folder named data in the project directory.

Move the downloaded files into this folder.

Expected file naming format:

yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
yellow_tripdata_2025-03.parquet
...
yellow_tripdata_2025-12.parquet
Running the Application

Navigate to the project root directory and execute:

cargo run

The program will automatically load every .parquet file located in the data/ directory.

Data Analysis Queries

Two main analytical queries are implemented in this project.

Monthly Trip & Revenue Summary

Trips are grouped by pickup month, extracted from the column tpep_pickup_datetime.

Metrics calculated include:

Total number of trips

Total revenue (SUM(total_amount))

Average fare (AVG(fare_amount))

The results are ordered chronologically by month.

Both implementations are provided using:

DataFusion DataFrame API

DataFusion SQL queries

Tip Statistics by Payment Type

Trips are grouped based on the payment_type field.

Metrics calculated:

Total trip count

Average tip value

Tip percentage (SUM(tip_amount) / SUM(total_amount))

Results are sorted by the number of trips in descending order.

This analysis is implemented using both:

DataFrame API

SQL API

Program Output

The repository includes a screenshot that shows:

Output generated using the DataFrame API

Output generated using the SQL API

A success message confirming the completion of all aggregations

Screenshot file location:

screenshots/output.png
Repository Layout
nyc_taxi_analysis_rust/
│
├── data/                (ignored in git to avoid large files)
├── screenshots/
│   └── output.png
├── src/
│   └── main.rs
│
├── Cargo.toml
├── .gitignore
└── README.md
Additional Notes

The data/ directory is excluded from Git using .gitignore because the dataset files are large.

Apache DataFusion is used for both SQL-based and DataFrame-based processing.

All required aggregation tasks have been implemented as part of the project.


**Author**
**Dev Patel**
**MS Data Science**
**Rowan University**