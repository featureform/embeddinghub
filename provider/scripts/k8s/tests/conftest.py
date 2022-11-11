import os 

import dill
import pytest
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


@pytest.fixture(scope="module")
def local_variables_success():
    return {
        "MODE": "local",
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test/",
        "SOURCES": f"{dir_path}/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }


@pytest.fixture(scope="module")
def local_variables_parquet_success():
    return {
        "MODE": "local",
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test/",
        "SOURCES": f"{dir_path}/test_files/inputs/transaction_short",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }


@pytest.fixture(scope="module")
def local_df_variables_success():
    return {
        "MODE": "local",
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test/",
        "SOURCES": f"{dir_path}/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": f"{dir_path}/test_files/transformations/same_df.pkl",
    }


@pytest.fixture(scope="module")
def local_df_parquet_variables_success():
    return {
        "MODE": "local",
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test/",
        "SOURCES": f"{dir_path}/test_files/inputs/transaction_short",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": f"{dir_path}/test_files/transformations/same_df.pkl",
    }


@pytest.fixture(scope="module")
def local_variables_failure():
    return {}


@pytest.fixture(scope="module")
def k8s_sql_variables_success():
    return {
        "MODE": "k8s",
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test",
        "SOURCES": f"{dir_path}/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }


@pytest.fixture(scope="module")
def k8s_df_variables_success():
    return {
        "MODE": "k8s",
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test",
        "SOURCES": f"{dir_path}/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": "/path/to/transformation",
        "ETCD_HOST": "127.0.0.1",
        "ETCD_PORT": "2379,2380",
        "ETCD_USERNAME": "username",
        "ETCD_PASSWORD": "password",
        "AZURE_CONNECTION_STRING": "connection_string",
    }


@pytest.fixture(scope="module")
def k8s_df_variables_single_port_success():
    return {
        "MODE": "k8s",
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test",
        "SOURCES": f"{dir_path}/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "df",
        "TRANSFORMATION": "/path/to/transformation",
        "ETCD_HOST": "127.0.0.1",
        "ETCD_PORT": "2379",
        "ETCD_USERNAME": "username",
        "ETCD_PASSWORD": "password",
        "AZURE_CONNECTION_STRING": "connection_string",
    }


@pytest.fixture(scope="module")
def k8s_variables_failure():
    return {
        "MODE": "k8s",
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test",
        "SOURCES": f"{dir_path}/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
    }


@pytest.fixture(scope="module")
def k8s_variables_port_not_provided_failure():
    return {
        "MODE": "k8s",
        "OUTPUT_URI": f"{dir_path}/test_files/output/local_test",
        "SOURCES": f"{dir_path}/test_files/inputs/transactions_short.csv",
        "TRANSFORMATION_TYPE": "sql",
        "TRANSFORMATION": "SELECT * FROM source_0",
        "ETCD_HOST": "HOST_PATH",
        "ETCD_USERNAME": "username",
        "ETCD_PASSWORD": "password",
    }


@pytest.fixture(scope="module")
def df_transformation():
    file_path = f"{dir_path}/test_files/transformations/same_df.pkl"

    def transformation(transaction):
        return transaction

    with open(file_path, "wb") as f:
        dill.dump(transformation.__code__, f)
    return file_path


@pytest.fixture(scope="module")
def container_client():
    connection_string = os.getenv("AZURE_CONNECTION_STRING")
    if connection_string == None:
        # get the path to .env in root directory
        env_file = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(real_path)))))   
        load_dotenv(f"{env_file}/.env")

        connection_string = os.getenv("AZURE_CONNECTION_STRING")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(os.getenv("AZURE_CONTAINER_NAME"))
    return container_client

