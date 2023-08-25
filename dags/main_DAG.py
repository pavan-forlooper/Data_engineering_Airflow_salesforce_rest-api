from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

from simple_salesforce import Salesforce
import yaml
import csv
from helper_functions import extract_account_type

import logging
log_path = "/usr/local/airflow/logs/data_processing.log"
# Create the file if it does not exist
if not os.path.exists(log_path):
    open(log_path, "w").close()

dag_directory = os.path.dirname(__file__)
# Modify paths to be relative to the DAG file's directory
config_path = "/usr/local/airflow/configuration_files/config.yaml"
sql_query_path = "/usr/local/airflow/sql_files/soql_query.sql"
tm_file_path = "/usr/local/airflow/data/accounts_tm.csv"
st_file_path = "/usr/local/airflow/data/accounts_st.csv"
etl_file_path = "/usr/local/airflow/data/accounts_etl.csv"

print("Current working directory:", os.getcwd())
print("DAG directory:", os.path.dirname(__file__))

default_args = {
    'owner': 'pavan',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'salesforce_data_processing',
    default_args=default_args,
    description='DAG for processing Salesforce data',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)


# Import your functions here

def make_api_call(**kwargs):
    print("Current working directory:", os.getcwd())
    print("DAG directory:", os.path.dirname(__file__))
    # 1 getting credentials from config file
    try:
        current_working_directory = os.getcwd()
        print("Current working directory:", current_working_directory)

        dag_directory = os.path.dirname(__file__)
        with open(config_path) as file:
            config = yaml.safe_load(file)
            username = config['username']
            password = config['password']
            security_token = config['security_token']
        print("api_call_successful")
    except Exception as e:
        logging.error("Error in step 1: %s", e)
        print("api_call_fail")
        return

    # 2 api instance creation
    try:
        salesforce_instance = Salesforce(username=username, password=password, security_token=security_token)
        print("2, salesforce_instance_successful")
        kwargs['ti'].xcom_push(key='salesforce_instance', value=salesforce_instance)
    except Exception as e:
        logging.error("Error in step 2: %s", e)
        return


def query_data(**kwargs):
    # 3 query data
    try:
        salesforce_instance = kwargs['ti'].xcom_pull(task_ids='make_api_call', key='salesforce_instance')
        with open(sql_query_path) as file:
            query = file.read()
            result = salesforce_instance.query_all(query)
        print("query_data_successful")
        kwargs['ti'].xcom_push(key='result', value=result)

    except Exception as e:
        logging.error("Error in step 3: %s", e)
        print("query_data_fail")
        return


def process_results(**kwargs):
    # 4 Process the result
    try:
        result = kwargs['ti'].xcom_pull(task_ids='query_data', key='result')
        records = result["records"]
        fieldnames = ["Id", "Name"]  # Rename the columns
        # Extract the required attributes and format the records
        processed_records = []
        for record in records:
            account_type = extract_account_type(record["attributes"])
            if account_type:
                processed_record = {
                    "Id": record["Id"],
                    "Name": record["Name"]
                }
                processed_records.append(processed_record)
        print("process results success", processed_records)
        kwargs['ti'].xcom_push(key='processed_records', value=processed_records)
    except Exception as e:
        logging.error("Error in step 4: %s", e)
        print("process results fail")
        return


def download_data_to_tm_file(**kwargs):
    # 5 Define new file paths based on the updated directory structure
    try:
        processed_records = kwargs['ti'].xcom_pull(task_ids='process_results', key='processed_records')
        print("testing in 5", processed_records)
        fieldnames = ["Id", "Name"]
        with open(tm_file_path, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if file.tell() == 0:
                writer.writeheader()
            writer.writerows(processed_records)
            print("download data tm successful", processed_records)
    except Exception as e:
        logging.error("Error in step 5: %s", e)
        print("download data tm successful")
        return


def to_st_file(**kwargs):
    # 6 clean the data and copy to ST table
    data = []
    try:
        with open(tm_file_path, "r") as file:
            reader = csv.reader(file)
            header = next(reader)
            data.append(header)

            for row in reader:
                new_id = row[0][10:]
                modified_row = [new_id] + row[1:]
                data.append(modified_row)

        with open(st_file_path, "w", newline="") as file:
            writer = csv.writer(file)
            for row in data:
                writer.writerow(row)
        print("6 successful", data)
    except Exception as e:
        logging.error("Error in step 6: %s", e)
        return


def to_etl_file(**kwargs):
    # 7 update data to ETL csv file
    try:
        with open(st_file_path, "r") as input_file, open(etl_file_path, "a", newline="") as etl_file:
            reader = csv.reader(input_file)
            writer = csv.writer(etl_file, lineterminator='\n')

            etl_file_empty = os.path.getsize(etl_file_path) == 0

            if etl_file_empty:
                writer.writerows(reader)
            else:
                next(reader)
                writer.writerows(reader)
        print("7 successful", etl_file)
    except Exception as e:
        logging.error("Error in step 7: %s", e)
        return


# Define tasks for each step
make_api_call_task = PythonOperator(
    task_id='make_api_call',
    python_callable=make_api_call,
    provide_context=True,
    dag=dag,
)

query_data_task = PythonOperator(
    task_id='query_data',
    python_callable=query_data,
    provide_context=True,
    dag=dag,
)

process_results_task = PythonOperator(
    task_id='process_results',
    python_callable=process_results,
    provide_context=True,
    dag=dag,
)

download_data_to_tm_file_task = PythonOperator(
    task_id='download_data_to_tm_file',
    python_callable=download_data_to_tm_file,
    provide_context=True,
    dag=dag,
)

to_st_file_task = PythonOperator(
    task_id='to_st_file',
    python_callable=to_st_file,
    provide_context=True,
    dag=dag,
)

to_etl_file_task = PythonOperator(
    task_id='to_etl_file',
    python_callable=to_etl_file,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
make_api_call_task >> query_data_task >> process_results_task >> download_data_to_tm_file_task >> to_st_file_task >> to_etl_file_task
