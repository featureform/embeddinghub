from behave import *
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import urllib.request
import pandas as pd
import numpy as np
import dotenv

import logging
import boto3
from botocore.exceptions import ClientError
from google.cloud import storage
import os

dotenv.load_dotenv("../../.env")


def get_filename_and_uri(filesize, filetype):
    if filesize == "small" and filetype == "csv":
        file_uri = (
            "https://featureform-demo-files.s3.amazonaws.com/transactions_short.csv"
        )
        filename = "transactions_short.csv"
    elif filesize == "small" and filetype == "parquet":
        file_uri = (
            "https://featureform-demo-files.s3.amazonaws.com/transactions_short.csv"
        )
        filename = "transactions_short.parquet"
    elif filesize == "large" and filetype == "csv":
        file_uri = "https://featureform-demo-files.s3.amazonaws.com/transactions.csv"
        filename = "transactions.csv"
    elif filesize == "large" and filetype == "parquet":
        file_uri = "https://featureform-demo-files.s3.amazonaws.com/transactions.csv"
        filename = "transactions.parquet"
    elif filesize == "small" and filetype == "directory":
        file_uri = (
            "https://featureform-demo-files.s3.amazonaws.com/transactions_short.csv"
        )
        filename = "transactions_short"
    elif filesize == "large" and filetype == "directory":
        file_uri = "https://featureform-demo-files.s3.amazonaws.com/transactions.csv"
        filename = "transactions"
    else:
        raise Exception("Invalid file size or type")
    return filename, file_uri


def download_file(file_uri, local_file_name, filetype):
    if not os.path.exists(local_file_name):
        if filetype == "csv":
            urllib.request.urlretrieve(
                file_uri,
                local_file_name,
            )
        elif filetype == "parquet":
            df = pd.read_csv(file_uri)
            df.to_parquet(local_file_name)
        elif filetype == "directory":
            if not os.path.exists(local_file_name):
                os.mkdir(local_file_name)
            df = pd.read_csv(file_uri)
            dfs = np.array_split(df, 4)
            for i in range(4):
                dfs[i].to_parquet(f"{local_file_name}/part-{i}.parquet")


def get_file_rows(local_file_name, filetype):
    if filetype == "csv":
        return len(pd.read_csv(local_file_name))
    elif filetype == "parquet":
        return len(pd.read_parquet(local_file_name))


def create_local_path(local_path):
    if not os.path.exists(local_path):
        os.mkdir(local_path)


def upload_to_azure(
    connection_string, container_name, local_file_name, upload_file_path, filetype
):
    def upload_single_file(
        connection_string, container_name, local_file_name, upload_file_path
    ):
        blob = BlobClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
            blob_name=local_file_name,
        )
        if blob.exists():
            return

        print("\nUploading to Azure Storage as blob:\n\t" + upload_file_path)
        # Upload the created file
        with open(file=upload_file_path, mode="rb") as data:
            blob.upload_blob(data)

    if filetype == "directory":
        files = os.listdir(local_file_name)
        for file in files:
            upload_single_file(
                connection_string,
                container_name,
                f"{local_file_name}/{file}",
                f"{upload_file_path}/{file}",
            )
    else:
        upload_single_file(
            connection_string, container_name, local_file_name, upload_file_path
        )


def upload_to_aws(bucket, local_file_name, upload_file_path, filetype):
    def upload_single_file(bucket, local_file_name, upload_file_path):
        s3_client = boto3.client("s3")
        try:
            response = s3_client.upload_file(local_file_name, bucket, upload_file_path)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    if filetype == "directory":
        files = os.listdir(local_file_name)
        for file in files:
            upload_single_file(
                bucket,
                f"{local_file_name}/{file}",
                f"{upload_file_path}/{file}",
            )
    else:
        upload_single_file(bucket, local_file_name, upload_file_path)


def upload_to_gcs(bucket_name, local_file_name, upload_file_path, filetype):
    def upload_single_file(bucket_name, local_file_name, upload_file_path):
        storage_client = storage.Client(project="testing-352123")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(upload_file_path)
        blob.upload_from_filename(local_file_name)

    if filetype == "directory":
        files = os.listdir(local_file_name)
        for file in files:
            upload_single_file(
                bucket_name,
                f"{local_file_name}/{file}",
                f"{upload_file_path}/{file}",
            )
    else:
        upload_single_file(bucket_name, local_file_name, upload_file_path)


@when('I upload a "{filesize}" "{filetype}" file to "{storage_provider}"')
def step_impl(context, filesize, filetype, storage_provider):
    filename, file_uri = get_filename_and_uri(filesize, filetype)

    local_path = "data"
    create_local_path(local_path)

    local_file_name = f"{local_path}/{filename}"

    remote_path = "data"

    upload_file_path = os.path.join(remote_path, filename)
    context.filename = upload_file_path

    # Create a file in the local data directory to upload and download
    download_file(file_uri, local_file_name, filetype)

    context.file_length = get_file_rows(local_file_name, filetype)

    if storage_provider == "azure":
        context.cloud_file_path = (
            f"abfss://test@testingstoragegen.dfs.core.windows.net/{upload_file_path}"
        )
        connection_string = os.getenv("AZURE_CONN_STRING")
        if connection_string is None:
            raise Exception("No Blob Store connection string found")
        upload_to_azure(
            connection_string, "test", local_file_name, upload_file_path, filetype
        )
    elif storage_provider == "s3":
        context.cloud_file_path = f"s3://featureform-spark-testing/{upload_file_path}"
        upload_to_aws(
            "featureform-spark-testing", local_file_name, upload_file_path, filetype
        )

    elif storage_provider == "gcs":
        context.cloud_file_path = f"gs://featureform-spark-testing/{upload_file_path}"
        upload_to_gcs("featureform-test", local_file_name, upload_file_path, filetype)

    else:
        raise Exception(f"Invalid storage provider {storage_provider}")
