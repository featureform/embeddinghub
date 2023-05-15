import io
import os
import types

from typing import List
from datetime import datetime
from argparse import Namespace

import dill

import boto3
import pandas as pd
from pandasql import sqldf
from azure.storage.blob import BlobServiceClient

LOCAL_MODE = "local"
K8S_MODE = "k8s"

# Blob Store Types
LOCAL = "local"
AZURE = "azure"
GCS = "gcs"
S3 = "s3"

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

LOCAL_DATA_PATH = f"{dir_path}/.featureform/data"


class BlobStore:
    def __init__(self, store_credentials):
        self._credentials = store_credentials
        self.type = store_credentials.type
        self._client = self._create_client()

    def _create_client(self):
        return "client"

    def get_client(self):
        return self._client

    def upload(self, file_path, blob_path):
        if os.path.isfile(file_path):
            response = self.upload_file(file_path, blob_path)
        elif os.path.isdir(file_path):
            response = self.upload_directory(file_path, blob_path)
        else:
            raise Exception(f"the file path {file_path} is not a file or a directory.")

        return response

    def upload_file(self, file_path, blob_path):
        return "response"

    def upload_directory(self, directory_path, blob_path):
        pass

    def download(self, blob_path, file_path):
        print(f"downloading {blob_path} to {LOCAL_DATA_PATH}/{file_path}")
        if not os.path.isdir(LOCAL_DATA_PATH):
            os.makedirs(LOCAL_DATA_PATH, exist_ok=True)

        full_path = f"{LOCAL_DATA_PATH}/{file_path}"

        if (
            blob_path.endswith(".csv")
            or blob_path.endswith(".parquet")
            or blob_path.endswith(".pkl")
        ):
            response = self.download_file(blob_path, full_path)
        else:
            print("downloading directory...")
            if not os.path.isdir(full_path):
                os.mkdir(full_path)
            response = self.download_directory(blob_path, full_path)

        return response

    def download_file(self, blob_path, file_path):
        pass

    def download_directory(self, blob_path, directory_path):
        pass


class S3BlobStore(BlobStore):
    def __init__(self, store_credentials):
        super().__init__(store_credentials)
        self._bucket_name = store_credentials.bucket_name

    def _create_client(self):
        session = boto3.Session(
            aws_access_key_id=self._credentials.aws_access_key_id,
            aws_secret_access_key=self._credentials.aws_secret_key,
        )
        s3_resource_client = session.resource(
            "s3", region_name=self._credentials.bucket_region
        )

        return s3_resource_client

    def upload_file(self, local_file_path, blob_path):
        bucket = self._client.Bucket(self._bucket_name)
        _ = bucket.upload_file(local_file_path, blob_path)
        return blob_path

    def upload_directory(self, directory_path, blob_path):
        file_count = 0
        for file in os.listdir(directory_path):
            local_file_path = os.path.join(directory_path, file)
            _ = self.upload_file(local_file_path, f"{blob_path}/{file}")
            file_count += 1

        return blob_path

    def download_file(self, blob_path, local_file_path):
        s3_object = self._client.Object(
            bucket_name=self._bucket_name,
            key=blob_path,
        )

        with open(local_file_path, "wb") as file:
            s3_object.download_fileobj(Fileobj=file)
        return local_file_path

    def download_directory(self, blob_path, directory_path):
        print("downloading directory...")
        if not os.path.isdir(directory_path):
            os.mkdir(directory_path)

        bucket = self._client.Bucket(self._bucket_name)

        file_count = 0
        for blob in bucket.objects.filter(Prefix=blob_path):
            print("downloading file: ", blob.key)
            filename = blob.key.split("/")[-1]
            local_file = os.path.join(directory_path, filename)
            _ = self.download_file(blob.key, local_file)

            file_count += 1

        return directory_path


class AzureBlobStore(BlobStore):
    def __init__(self, store_credentials):
        super().__init__(store_credentials)

    def _create_client(self):
        blob_service_client = BlobServiceClient.from_connection_string(
            self._credentials.connection_string
        )
        container_client = blob_service_client.get_container_client(
            self._credentials.container
        )
        return container_client

    def upload_file(self, local_filename, blob_path):
        print(f"uploading {local_filename} file to {blob_path} as file")
        blob_upload = self._client.get_blob_client(blob_path)
        with open(local_filename, "rb") as data:
            blob_upload.upload_blob(data, blob_type="BlockBlob")

        return blob_path

    def upload_directory(self, directory_path, blob_path):
        print(f"uploading {directory_path} file to {blob_path} as partitioned files")
        for file in os.listdir(directory_path):
            blob_upload = self._client.get_blob_client(f"{blob_path}/{file}")
            full_file_path = os.path.join(directory_path, file)
            with open(full_file_path, "rb") as data:
                blob_upload.upload_blob(data, blob_type="BlockBlob")

        return blob_path

    def download_file(self, blob_path, local_file_path):
        blob_client = self._client.get_blob_client(blob_path)

        with open(local_file_path, "wb") as my_blob:
            download_stream = blob_client.download_blob()
            my_blob.write(download_stream.readall())

        return local_file_path

    def download_directory(self, blob_path, directory_path):
        print(f"downloading directory: {blob_path}")
        if not os.path.isdir(directory_path):
            os.mkdir(directory_path)

        blob_list = self._client.list_blobs(name_starts_with=blob_path)
        for b in blob_list:
            # skip the directory itself
            if b.name == blob_path:
                continue

            blob_client = self._client.get_blob_client(b)

            ## Download
            with open(f"{directory_path}/{b.name.split('/')[-1]}", "wb") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())

        return directory_path


class LocalBlobStore(BlobStore):
    def __init__(self, store_credentials):
        super().__init__(store_credentials)


def main(args):
    """
    Executes the Transformation Job:
    Parameters:
        args: (argparse.Namespace) arguments passed to the script
    Returns:
        output_location: (str) location of the output data
    """

    blob_store = get_blob_store(args.blob_credentials)
    print(f"retrieved blob store of type {blob_store.type}")

    if args.transformation_type == "sql":
        print(f"starting execution for SQL Transformation in {args.mode} mode")
        output_location = execute_sql_job(
            args.mode,
            args.output_uri,
            args.transformation,
            args.sources,
            blob_store,
        )
    elif args.transformation_type == "df":
        print(f"starting execution for DF Transformation in {args.mode} mode")
        output_location = execute_df_job(
            args.mode,
            args.output_uri,
            args.transformation,
            args.sources,
            blob_store,
        )
    return output_location


def execute_sql_job(mode, output_uri, transformation, source_list, blob_store):
    """
    Executes the SQL Queries:

    Parameters:
        mode:           string ("local", "k8s")
        output_uri:     string (path to blob store)
        transformation: string (eg. "SELECT * FROM source_0)
        source_list:    List(string) (a list of input sources)
        blob_store:     BlobStore (blob store object)

    Returns:
        output_uri_with_timestamp: string (output path of blob storage)
    """
    try:
        for i, source in enumerate(source_list):
            if blob_store.type == LOCAL:
                output_path = source
            else:
                # download blob to local & set source to local path
                local_file = (
                    f"source_{i}.csv" if source.endswith(".csv") else f"source_{i}"
                )
                output_path = blob_store.download(source, local_file)

            if output_path.endswith(".csv"):
                globals()[f"source_{i}"] = pd.read_csv(output_path)
            else:
                globals()[f"source_{i}"] = pd.read_parquet(output_path)

        pysqldf = lambda q: sqldf(q, globals())
        transformation_df = pysqldf(transformation)
        output_dataframe = set_bool_columns(transformation_df)

        dt = datetime.now()
        output_uri_with_timestamp = f"{output_uri}/{dt}.parquet"

        if blob_store.type == LOCAL:
            os.makedirs(output_uri, exist_ok=True)
            output_dataframe.to_parquet(output_uri_with_timestamp)
        else:
            local_output = f"{LOCAL_DATA_PATH}/output.parquet"
            output_dataframe.to_parquet(local_output)
            # upload blob to blob store
            output_uri = blob_store.upload(local_output, output_uri_with_timestamp)

        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(e)
        raise e


def execute_df_job(mode, output_uri, code, sources, blob_store):
    """
    Executes the DF transformation:

    Parameters:
        mode:             string ("local", "k8s")
        output_uri:       string (blob store path)
        code:             code (python code)
        sources:          List(string) (a list of input sources)
        blob_store:       BlobStore (blob store object)

    Returns:
        output_uri_with_timestamp: string (output s3 path)
    """

    func_parameters = []
    print(f"reading '{len(sources)}' source files")
    for i, source in enumerate(sources):
        if blob_store.type == LOCAL:
            source_path = source
        else:
            # download blob to local & set source to local path
            local_file = f"source_{i}.csv" if source.endswith(".csv") else f"source_{i}"

            print(f"downloading {source} to {local_file}")
            source_path = blob_store.download(source, local_file)

        print(f"reading '{source}' source file into dataframe")
        if source_path.endswith(".csv"):
            func_parameters.append(pd.read_csv(source_path))
        else:
            func_parameters.append(pd.read_parquet(source_path))

    try:
        df_path = "transformation.pkl"

        print(f"retrieving code from {code} in {blob_store.type}")
        if blob_store.type == LOCAL:
            code_path = code
        else:
            code_path = blob_store.download(code, df_path)

        print("executing transformation code")
        code = get_code_from_file(mode, code_path)
        func = types.FunctionType(code, globals(), "df_transformation")
        output_df = pd.DataFrame(func(*func_parameters))

        dt = datetime.now()
        output_uri_with_timestamp = f"{output_uri}/{dt}.parquet"

        print(f"storing output dataframe to {output_uri_with_timestamp}")
        if blob_store.type == LOCAL:
            os.makedirs(output_uri, exist_ok=True)
            output_df.to_parquet(output_uri_with_timestamp)
        else:
            local_output = f"{LOCAL_DATA_PATH}/output.parquet"
            output_df.to_parquet(local_output)

            # upload blob to blob store
            output_uri = blob_store.upload(local_output, output_uri_with_timestamp)

        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(f"Issue with execution of the transformation: {e}")
        raise e


def get_code_from_file(mode, file_path):
    """
    Reads the code from a pkl file into a python code object.
    Then this object will be used to execute the transformation.

    Parameters:
        mode:             string ("local", "k8s")
        file_path:        string (path to file)

    Returns:
        code: code object that could be executed
    """
    print(f"Retrieving transformation code from '{file_path}' file in {mode} mode.")
    code = None
    with open(file_path, "rb") as f:
        f.seek(0)
        code = dill.load(f)

    return code


def get_blob_store(store_credentials):
    """
    Returns a BlobStore object based on the store_credentials type
    Parameters:
        store_credentials: Namespace (used to download/upload files)

    Returns:
        BlobStore
    """

    if store_credentials.type == S3:
        return S3BlobStore(store_credentials)
    elif store_credentials.type == AZURE:
        return AzureBlobStore(store_credentials)
    elif store_credentials.type == LOCAL:
        return LocalBlobStore(store_credentials)
    else:
        raise Exception(f"blob store type {store_credentials.type} is not supported.")


def column_is_bool(df: pd.DataFrame, column: str):
    for _, row in df.iterrows():
        if row[column] != 0 and row[column] != 1:
            return False
    return True


def set_bool_columns(df: pd.DataFrame):
    for col in df.columns:
        if column_is_bool(df, col):
            df[col] = df[col].astype("bool")
    return df


def get_args():
    """
    Gets input arguments from environment variables.

    Parameters:
        None

    Returns:
        Namespace
    """

    mode = os.getenv("MODE")
    blob_store_type = os.getenv("BLOB_STORE_TYPE")
    output_uri = os.getenv("OUTPUT_URI")
    sources = os.getenv("SOURCES", "").split(",")
    transformation_type = os.getenv("TRANSFORMATION_TYPE")
    transformation = os.getenv("TRANSFORMATION")

    blob_credentials = get_blob_credentials(mode, blob_store_type)

    args = Namespace(
        mode=mode,
        transformation_type=transformation_type,
        transformation=transformation,
        output_uri=output_uri,
        sources=sources,
        blob_credentials=blob_credentials,
    )

    validate_args(args)
    return args


def validate_args(args):
    """
    Validates the input arguments.

    Parameters:
        args: Namespace

    Returns:
        None (raises error if validation fails)
    """

    if args.mode not in (
        LOCAL_MODE,
        K8S_MODE,
    ):
        raise ValueError(
            f"the {args.mode} mode is not supported. supported modes are '{LOCAL_MODE}' and '{K8S_MODE}'."
        )

    if args.transformation_type not in (
        "sql",
        "df",
    ):
        raise ValueError(
            f"the {args.transformation_type} transformation type is not supported. supported types are 'sql', and 'df'."
        )

    if not (args.output_uri and args.sources != [""] and args.transformation != ""):
        raise Exception(
            "the environment variables are not set properly; output_uri, sources, and transformation are not set correctly."
        )


def get_blob_credentials(mode, blob_store_type):
    """
    Retrieve credentials for the blob store. Currently, only azure blob store and aws s3 is supported.

    Parameters:
        mode: string ("local", "k8s")
        blob_store_type: string ("azure", "gcs", "s3")

    Returns:
        credentials: Namespace(type="", ...) (includes credentials needed for each blob store.)
    """

    if mode == K8S_MODE and blob_store_type == AZURE:
        azure_connection_string = os.getenv("AZURE_CONNECTION_STRING")
        azure_container_name = os.getenv("AZURE_CONTAINER_NAME")

        if not (azure_connection_string and azure_container_name):
            raise Exception(
                "azure blob store requires connection string and container name."
            )

        return Namespace(
            type=AZURE,
            connection_string=azure_connection_string,
            container=azure_container_name,
        )
    elif mode == K8S_MODE and blob_store_type == S3:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_KEY")
        bucket_name = os.getenv("S3_BUCKET_NAME")
        bucket_region = os.getenv("S3_BUCKET_REGION")

        if not (aws_access_key_id and aws_secret_key and bucket_name and bucket_region):
            raise Exception(
                "s3 blob store requires access key id, secret access key, bucket name, and bucket region."
            )

        return Namespace(
            type=S3,
            aws_access_key_id=aws_access_key_id,
            aws_secret_key=aws_secret_key,
            bucket_name=bucket_name,
            bucket_region=bucket_region,
        )
    elif mode == K8S_MODE and blob_store_type == GCS:
        raise NotImplementedError("gcs blob store is not supported yet.")
    else:
        return Namespace(
            type=LOCAL,
        )


if __name__ == "__main__":
    main(get_args())
