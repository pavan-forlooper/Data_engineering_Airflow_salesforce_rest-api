from simple_salesforce import Salesforce
import yaml
import csv
import os
from helper_functions import extract_account_type

import logging
current_dir = os.getcwd()
log_path = os.path.join(current_dir, "..", "logs", "data_processing.log")
# Create the file if it does not exist
if not os.path.exists(log_path):
    open(log_path, "w").close()

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def salesforce_download_to_computer():
    # 1 getting credentials from config file
    try:
        with open("../configuration_files/config.yaml") as file:
            config = yaml.safe_load(file)
            username = config['username']
            password = config['password']
            security_token = config['security_token']
    except Exception as e:
        logging.error("Error in step 1: %s", e)
        return

    # 2 api instance creation
    try:
        salesforce_instance = Salesforce(username=username, password=password, security_token=security_token)
    except Exception as e:
        logging.error("Error in step 2: %s", e)
        return

    # 3 query data
    try:
        with open("../sql_files/soql_query.sql") as file:
            query = file.read()
            result = salesforce_instance.query_all(query)
    except Exception as e:
        logging.error("Error in step 3: %s", e)
        return

    # 4 Process the result
    try:
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
    except Exception as e:
        logging.error("Error in step 4: %s", e)
        return

    # 5 DATA download to TM table
    # Define new file paths based on the updated directory structure
    tm_file_path = "../data/accounts_tm.csv"
    st_file_path = "../data/accounts_st.csv"
    etl_file_path = "../data/accounts_etl.csv"
    try:
        with open(tm_file_path, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if file.tell() == 0:
                writer.writeheader()
            writer.writerows(processed_records)
    except Exception as e:
        logging.error("Error in step 5: %s", e)
        return

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

    except Exception as e:
        logging.error("Error in step 5: %s", e)
        return

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

    except Exception as e:
        logging.error("Error in step 7: %s", e)
        return

    # 8 close the connection
    salesforce_instance = None


if __name__ == "__main__":
    salesforce_download_to_computer()
