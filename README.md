# Data_engineering_Airflow_salesforce_rest-api

Airflow for processing Salesforce data using the Simple-Salesforce Rest-api

# Salesforce Data Processing DAG

This project demonstrates the integration of Salesforce data using Apache Airflow. The project fetches data from Salesforce using the Salesforce REST API, processes it, and stores it in CSV files, automtate this script to run every 1 min.

## Project Overview

The goal of this project is to automate the data extraction and processing of Salesforce data using Apache Airflow. The project is divided into several steps, each implemented as an Airflow task:

## Task Flow

Here is the visual representation of the task flow:

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [Schedule](#schedule)
- [License](#license)

## Introduction


1. **make_api_call**: Establishes a connection to Salesforce using credentials from a configuration file.
2. **query_data**: Executes a Salesforce query to retrieve data.
3. **process_results**: Processes the retrieved data and extracts specific attributes.
4. **download_data_to_tm_file**: Downloads processed data to a CSV file.
5. **to_st_file**: Cleans and formats data for a specific use case.
6. **to_etl_file**: Appends cleaned data to an ETL CSV file.



## Prerequisites

- Python 3.x
- Apache Airflow (installed and configured)
- Simple Salesforce library (`pip install simple-salesforce`)
- Docker (if using Dockerized Airflow)
- Configuration YAML file (`configuration_files/config.yaml`) with Salesforce credentials.

## Setup

1. Clone this repository to your local machine:

git clone https://github.com/pavan-forlooper/Data_engineering_Airflow_salesforce_rest-api.git


2. Install the required packages:
pip install simple-salesforce


3. Configure your Salesforce credentials in the `configuration_files/config.yaml` file.

## Usage

1. Run the Apache Airflow webserver and scheduler.

2. Upload the DAG file (`main_DAG.py`) to your Airflow DAGs directory.

3. Access the Airflow web interface (usually at http://localhost:8080) and enable the `salesforce_data_processing` DAG.

4. The DAG will run based on the specified schedule and execute the defined tasks.

## Configuration

- Modify the Salesforce query in the `sql_files/soql_query.sql` file as needed.
- Adjust other file paths and task logic as required by your use case.

## Schedule

The DAG is scheduled to run every minute. You can modify the `schedule_interval` parameter in the `main_DAG.py` file to change the frequency.

