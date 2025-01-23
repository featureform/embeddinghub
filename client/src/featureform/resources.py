#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import json
import logging
import os
import re
import sys
import uuid
from abc import ABC, abstractmethod
from dataclasses import field
from datetime import timedelta
from typing import List, Tuple, Union, Optional, Any, Dict
from urllib.parse import urlencode, urlunparse

import dill
import grpc
from google.protobuf.duration_pb2 import Duration
from google.rpc import error_details_pb2
import warnings

from . import feature_flag
from .enums import *
from .exceptions import *
from .types import VectorType, type_from_proto
from .version import check_up_to_date

logger = logging.getLogger(__name__)

NameVariant = Tuple[str, str]

# Constants for Pyspark Versions
MAJOR_VERSION = "3"
MINOR_VERSIONS = ["7", "8", "9", "10", "11"]


@typechecked
def valid_name_variant(nvar: NameVariant) -> bool:
    return nvar[0] != "" and nvar[1] != ""


@typechecked
@dataclass
class Schedule:
    name: str
    variant: str
    resource_type: int
    schedule_string: str

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.SCHEDULE

    def _create(self, req_id, stub) -> Tuple[None, None]:
        serialized = pb.SetScheduleChangeRequest(
            resource=pb.ResourceId(
                pb.NameVariant(name=self.name, variant=self.variant),
                resource_type=self.resource_type,
            ),
            schedule=self.schedule_string,
        )
        stub.RequestScheduleChange(serialized)
        return None, None


class HashPartition:
    def __init__(self, column, num_buckets):
        self.column = column
        self.num_buckets = num_buckets

    def proto_kwargs(self):
        return {
            "HashPartition": pb.HashPartition(
                column=self.column, buckets=self.num_buckets
            )
        }


class DailyPartition:
    def __init__(self, column):
        self.column = column

    def proto_kwargs(self):
        return {"DailyPartition": pb.DailyPartition(column=self.column)}


PartitionType = Union[HashPartition, DailyPartition]


@typechecked
@dataclass
class RedisConfig:
    host: str
    port: int
    password: str
    db: int

    def software(self) -> str:
        return "redis"

    def type(self) -> str:
        return "REDIS_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Addr": f"{self.host}:{self.port}",
            "Password": self.password,
            "DB": self.db,
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, RedisConfig):
            return False
        return (
            self.host == __value.host
            and self.port == __value.port
            and self.password == __value.password
            and self.db == __value.db
        )


@typechecked
@dataclass
class PineconeConfig:
    project_id: str = ""
    environment: str = ""
    api_key: str = ""

    def software(self) -> str:
        return "pinecone"

    def type(self) -> str:
        return "PINECONE_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "ProjectID": self.project_id,
            "Environment": self.environment,
            "ApiKey": self.api_key,
        }
        return bytes(json.dumps(config), "utf-8")

    def deserialize(self, config):
        config = json.loads(config)
        self.project_id = config["ProjectID"]
        self.environment = config["Environment"]
        self.api_key = config["ApiKey"]
        return self

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, PineconeConfig):
            return False
        return (
            self.project_id == __value.project_id
            and self.environment == __value.environment
            and self.api_key == __value.api_key
        )


@typechecked
@dataclass
class WeaviateConfig:
    url: str = ""
    api_key: str = ""

    def software(self) -> str:
        return "weaviate"

    def type(self) -> str:
        return "WEAVIATE_ONLINE"

    def serialize(self) -> bytes:
        if self.url == "":
            raise Exception("URL cannot be empty")
        config = {
            "URL": self.url,
            "ApiKey": self.api_key,
        }
        return bytes(json.dumps(config), "utf-8")

    def deserialize(self, config):
        config = json.loads(config)
        self.url = config["URL"]
        self.api_key = config["ApiKey"]
        return self


@typechecked
@dataclass
class AWSStaticCredentials:
    """

    Static Credentials for an AWS services.

    **Example**
    ```
    aws_credentials = ff.AWSStaticCredentials(
        access_key="<AWS_ACCESS_KEY>",
        secret_key="<AWS_SECRET_KEY>"
    )
    ```

    Args:
        access_key (str): AWS Access Key.
        secret_key (str): AWS Secret Key.
    """

    access_key: str = field(default="")
    secret_key: str = field(default="")
    _type: str = field(default="AWS_STATIC_CREDENTIALS")

    def __post_init__(
        self,
    ):
        if self.access_key == "":
            raise Exception("'AWSStaticCredentials' access_key cannot be empty")

        if self.secret_key == "":
            raise Exception("'AWSStaticCredentials' secret_key cannot be empty")

    @staticmethod
    def type() -> str:
        return "AWS_STATIC_CREDENTIALS"

    def config(self):
        return {
            "AccessKeyId": self.access_key,
            "SecretKey": self.secret_key,
            "Type": self._type,
        }


@typechecked
@dataclass
class AWSAssumeRoleCredentials:
    """

    Assume Role Credentials for an AWS services.

    If an IAM role for service account (IRSA) has been configured, the default credentials provider chain
        will be used to get the credentials stored on the pod. See the following link for more information:
        https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html

    **Example**
    ```
    aws_credentials = ff.AWSAssumeRoleCredentials()
    ```
    """

    _type: str = field(default="AWS_ASSUME_ROLE_CREDENTIALS")

    @staticmethod
    def type() -> str:
        return "AWS_ASSUME_ROLE_CREDENTIALS"

    def config(self):
        return {
            "Type": self._type,
        }


@typechecked
@dataclass
class GCPCredentials:
    def __init__(
        self, project_id: str, credentials_path: str = "", credential_json: dict = None
    ):
        """

        Credentials for an GCP.

        **Example**
        ```
        gcp_credentials = ff.GCPCredentials(
            project_id="<project_id>",
            credentials_path="<path_to_credentials>"
        )
        ```

        Args:
            project_id (str): The project id.
            credentials_path (str): The path to the credentials file.
        """
        if project_id == "":
            raise Exception("'GCPCredentials' project_id cannot be empty")

        self.project_id = project_id
        self.credentials_path = credentials_path

        if self.credentials_path == "" and credential_json is None:
            raise ValueError(
                "Either a path to credentials or json credentials must be provided"
            )
        elif self.credentials_path != "" and credential_json is not None:
            raise ValueError(
                "Only one of credentials_path or credentials_json can be provided"
            )
        elif self.credentials_path != "" and credential_json is None:
            if not os.path.isfile(credentials_path):
                raise Exception(
                    f"'GCPCredentials' credentials_path '{credentials_path}' file not found"
                )
            with open(credentials_path) as f:
                self.credentials = json.load(f)
        else:
            self.credentials = credential_json

    def type(self):
        return "GCPCredentials"

    def config(self):
        return {
            "ProjectId": self.project_id,
            "JSON": self.credentials,
        }

    def to_json(self):
        return self.credentials


@typechecked
@dataclass
class BasicCredentials:
    def __init__(
        self,
        username: str,
        password: str = "",
    ):
        """

        Credentials for an EMR cluster.

        **Example**
        ```
        creds = ff.BasicCredentials(
            username="<username>",
            password="<password>",
        )

        hdfs = ff.register_hdfs(
            name="hdfs",
            credentials=creds,
            ...
        )
        ```

        Args:
            username (str): the username of account.
            password (str): the password of account (optional).
        """
        self.username = username
        self.password = password

    def type(self):
        return "BasicCredential"

    def config(self):
        return {
            "Username": self.username,
            "Password": self.password,
        }


@typechecked
@dataclass
class KerberosCredentials:
    def __init__(
        self,
        username: str,
        password: str,
        krb5s_file: str,
    ):
        """

        Credentials for an EMR cluster.

        **Example**
        ```
        kerberos = ff.KerberosCredentials(
            username="<username>",
            password="<password>",
            krb5s_file="<path/to/krb5s_file>",
        )

        hdfs = ff.register_hdfs(
            name="hdfs",
            credentials=kerberos,
            ...
        )
        ```

        Args:
            username (str): the username of account.
            password (str): the password of account.
            krb5s_file (str): the path to krb5s file.
        """
        self.username = username
        self.password = password
        self.krb5s_conf = read_file(krb5s_file)

    def type(self):
        return "KerberosCredential"

    def config(self):
        return {
            "Username": self.username,
            "Password": self.password,
            "Krb5Conf": self.krb5s_conf,
        }


@typechecked
@dataclass
class GCSFileStoreConfig:
    credentials: GCPCredentials
    bucket_name: str
    bucket_path: str = ""

    def software(self) -> str:
        return "gcs"

    def type(self) -> str:
        return "GCS"

    def serialize(self) -> bytes:
        config = {
            "BucketName": self.bucket_name,
            "BucketPath": self.bucket_path,
            "Credentials": self.credentials.config(),
        }
        return bytes(json.dumps(config), "utf-8")

    def config(self):
        return {
            "BucketName": self.bucket_name,
            "BucketPath": self.bucket_path,
            "Credentials": self.credentials.config(),
        }

    def store_type(self):
        return self.type()


@typechecked
@dataclass
class AzureFileStoreConfig:
    account_name: str
    account_key: str
    container_name: str
    root_path: str

    def software(self) -> str:
        return "azure"

    def type(self) -> str:
        return "AZURE"

    def serialize(self) -> bytes:
        config = {
            "AccountName": self.account_name,
            "AccountKey": self.account_key,
            "ContainerName": self.container_name,
            "Path": self.root_path,
        }
        return bytes(json.dumps(config), "utf-8")

    def config(self):
        return {
            "AccountName": self.account_name,
            "AccountKey": self.account_key,
            "ContainerName": self.container_name,
            "Path": self.root_path,
        }

    def store_type(self):
        return self.type()


@typechecked
@dataclass
class S3StoreConfig:
    bucket_path: str
    bucket_region: str
    credentials: Union[AWSStaticCredentials, AWSAssumeRoleCredentials]
    path: str = ""

    def __post_init__(self):
        # Validate that the bucket_path does not end with a slash
        if self.bucket_path.endswith("/"):
            raise ValueError("The 'bucket_path' cannot end with '/'.")

    def software(self) -> str:
        return "S3"

    def type(self) -> str:
        return "S3"

    def serialize(self) -> bytes:
        config = self.config()
        return bytes(json.dumps(config), "utf-8")

    def config(self):
        return {
            "Credentials": self.credentials.config(),
            "BucketRegion": self.bucket_region,
            "BucketPath": self.bucket_path,
            "Path": self.path,
        }

    def store_type(self):
        return self.type()


@typechecked
@dataclass
class HDFSConfig:
    def __init__(
        self,
        host: str,
        port: str,
        path: str,
        hdfs_site_file: str = "",
        core_site_file: str = "",
        hdfs_site_contents: str = "",
        core_site_contents: str = "",
        credentials: Union[BasicCredentials, KerberosCredentials] = BasicCredentials(
            ""
        ),
    ):
        bucket_path_ends_with_slash = len(path) != 0 and path[-1] == "/"

        if bucket_path_ends_with_slash:
            raise Exception("The 'bucket_path' cannot end with '/'.")

        self.path = path
        self.host = host
        self.port = port
        self.credentials = credentials

        self.hdfs_site_conf = self.__get_hdfs_site_contents(
            hdfs_site_file, hdfs_site_contents
        )
        self.core_site_conf = self.__get_core_site_contents(
            core_site_file, core_site_contents
        )

    def __get_hdfs_site_contents(self, site_file, site_contents):
        return self.__get_site_contents(site_file, site_contents, "hdfs")

    def __get_core_site_contents(self, site_file, site_contents):
        return self.__get_site_contents(site_file, site_contents, "core")

    @staticmethod
    def __get_site_contents(site_file, site_contents, config_type):
        if site_file == "" and site_contents == "":
            raise ValueError(
                f"{config_type} site config must be provided. Either provide a path to the xml file "
                f"({config_type}_site_file) or the contents of the file ({config_type}_site_contents)."
            )

        elif site_file != "" and site_contents != "":
            raise ValueError(
                f"Only one of {config_type}_site_file or {config_type}_site_contents should be provided."
            )

        elif site_file != "" and site_contents == "":
            return read_file(site_file)

        else:
            return site_contents

    def software(self) -> str:
        return "HDFS"

    def type(self) -> str:
        return "HDFS"

    def serialize(self) -> bytes:
        config = self.config()
        return bytes(json.dumps(config), "utf-8")

    def config(self):
        return {
            "Host": self.host,
            "Port": self.port,
            "Path": self.path,
            "HDFSSiteConf": self.hdfs_site_conf,
            "CoreSiteConf": self.core_site_conf,
            "CredentialType": self.credentials.type(),
            "CredentialConfig": self.credentials.config(),
        }

    def store_type(self):
        return self.type()


@typechecked
@dataclass
class OnlineBlobConfig:
    store_type: str
    store_config: dict

    def software(self) -> str:
        return self.store_type

    def type(self) -> str:
        return "BLOB_ONLINE"

    def config(self):
        return self.store_config

    def serialize(self) -> bytes:
        config = {
            "Type": self.store_type,
            "Config": self.store_config,
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, OnlineBlobConfig):
            return False
        return (
            self.store_type == __value.store_type
            and self.store_config == __value.store_config
        )


@typechecked
@dataclass
class FirestoreConfig:
    collection: str
    project_id: str
    credentials: GCPCredentials

    def software(self) -> str:
        return "firestore"

    def type(self) -> str:
        return "FIRESTORE_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Collection": self.collection,
            "ProjectID": self.project_id,
            "Credentials": self.credentials.to_json(),
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, FirestoreConfig):
            return False
        return (
            self.collection == __value.collection
            and self.project_id == __value.project_id
        )


@typechecked
@dataclass
class CassandraConfig:
    keyspace: str
    host: str
    port: int
    username: str
    password: str
    consistency: str
    replication: int

    def software(self) -> str:
        return "cassandra"

    def type(self) -> str:
        return "CASSANDRA_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Keyspace": self.keyspace,
            "Addr": f"{self.host}:{self.port}",
            "Username": self.username,
            "Password": self.password,
            "Consistency": self.consistency,
            "Replication": self.replication,
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, CassandraConfig):
            return False
        return (
            self.keyspace == __value.keyspace
            and self.host == __value.host
            and self.port == __value.port
            and self.username == __value.username
            and self.password == __value.password
            and self.consistency == __value.consistency
            and self.replication == __value.replication
        )


@typechecked
@dataclass
class DynamodbConfig:
    region: str
    credentials: Union[AWSStaticCredentials, AWSAssumeRoleCredentials]
    should_import_from_s3: bool

    def software(self) -> str:
        return "dynamodb"

    def type(self) -> str:
        return "DYNAMODB_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Region": self.region,
            "Credentials": self.credentials.config(),
            "ImportFromS3": self.should_import_from_s3,
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, DynamodbConfig):
            return False

        if type(self.credentials) != type(__value.credentials):
            return False

        if (
            isinstance(self.credentials, AWSStaticCredentials)
            and self.credentials.access_key != __value.credentials.access_key
            and self.credentials.secret_key != __value.credentials.secret_key
        ):
            return False

        return (
            self.region == __value.region
            and self.should_import_from_s3 == __value.should_import_from_s3
        )


@typechecked
@dataclass
class MongoDBConfig:
    username: str
    password: str
    host: str
    port: str
    database: str
    throughput: int

    def software(self) -> str:
        return "mongodb"

    def type(self) -> str:
        return "MONGODB_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Username": self.username,
            "Password": self.password,
            "Host": self.host,
            "Port": self.port,
            "Database": self.database,
            "Throughput": self.throughput,
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, MongoDBConfig):
            return False
        return (
            self.username == __value.username
            and self.password == __value.password
            and self.host == __value.host
            and self.port == __value.port
            and self.database == __value.database
            and self.throughput == __value.throughput
        )


@typechecked
@dataclass
class PostgresConfig:
    host: str
    port: str
    database: str
    user: str
    password: str
    sslmode: str

    def software(self) -> str:
        return "postgres"

    def type(self) -> str:
        return "POSTGRES_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "Host": self.host,
            "Port": self.port,
            "Username": self.user,
            "Password": self.password,
            "Database": self.database,
            "SSLMode": self.sslmode,
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, PostgresConfig):
            return False
        return (
            self.host == __value.host
            and self.port == __value.port
            and self.database == __value.database
            and self.user == __value.user
            and self.password == __value.password
            and self.sslmode == __value.sslmode
        )


@typechecked
@dataclass
class ClickHouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    ssl: bool

    def software(self) -> str:
        return "clickhouse"

    def type(self) -> str:
        return "CLICKHOUSE_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "Host": self.host,
            "Port": self.port,
            "Username": self.user,
            "Password": self.password,
            "Database": self.database,
            "SSL": self.ssl,
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, ClickHouseConfig):
            return False
        return (
            self.host == __value.host
            and self.port == __value.port
            and self.database == __value.database
            and self.user == __value.user
            and self.password == __value.password
            and self.ssl == __value.ssl
        )


@typechecked
@dataclass
class RedshiftConfig:
    host: str
    port: str
    database: str
    user: str
    password: str
    sslmode: str

    def software(self) -> str:
        return "redshift"

    def type(self) -> str:
        return "REDSHIFT_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "Host": self.host,
            "Port": self.port,
            "Username": self.user,
            "Password": self.password,
            "Database": self.database,
            "SSLMode": self.sslmode,
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, RedshiftConfig):
            return False
        return (
            self.host == __value.host
            and self.port == __value.port
            and self.database == __value.database
            and self.user == __value.user
            and self.password == __value.password
            and self.sslmode == __value.sslmode
        )


@typechecked
@dataclass
class BigQueryConfig:
    project_id: str
    dataset_id: str
    credentials: GCPCredentials

    def software(self) -> str:
        return "bigquery"

    def type(self) -> str:
        return "BIGQUERY_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "ProjectID": self.project_id,
            "DatasetID": self.dataset_id,
            "Credentials": self.credentials.to_json(),
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, BigQueryConfig):
            return False
        return (
            self.project_id == __value.project_id
            and self.dataset_id == __value.dataset_id
        )


class Catalog(ABC):
    @abstractmethod
    def config(self):
        pass


@typechecked
@dataclass
class GlueCatalog(Catalog):
    region: str
    database: str
    warehouse: str = ""
    assume_role_arn: str = ""
    table_format: TableFormat = field(default_factory=lambda: TableFormat.ICEBERG)

    def __post_init__(self):
        self._validate_database_name()
        self._validate_iceberg_configuration()

    def config(self):
        return {
            "Database": self.database,
            "Warehouse": self.warehouse,
            "Region": self.region,
            "AssumeRoleArn": self.assume_role_arn,
            "TableFormat": self.table_format,
        }

    def _validate_database_name(self):
        if self.database == "":
            raise ValueError("Database name cannot be empty")
        if not all(c.isalnum() or c == "_" for c in self.database):
            raise ValueError("Database name must be alphanumeric and/or underscores")

    def _validate_iceberg_configuration(self):
        if self.table_format is TableFormat.ICEBERG:
            errors = []
            if self.warehouse == "":
                errors.append("warehouse is required for Iceberg tables")
            if self.region == "":
                errors.append("region is required for Iceberg tables")

            if len(errors) > 0:
                raise ValueError(";".join(errors))


@dataclass
class SnowflakeDynamicTableConfig:
    target_lag: Optional[str] = None
    refresh_mode: Optional[RefreshMode] = None
    initialize: Optional[Initialize] = None

    def config(self) -> dict:
        return {
            "TargetLag": self.target_lag,
            "RefreshMode": self.refresh_mode.to_string() if self.refresh_mode else None,
            "Initialize": self.initialize.to_string() if self.initialize else None,
        }

    def to_proto(self):
        return pb.SnowflakeDynamicTableConfig(
            target_lag=self.target_lag,
            refresh_mode=self.refresh_mode.to_proto() if self.refresh_mode else None,
            initialize=self.initialize.to_proto() if self.initialize else None,
        )


@dataclass
class ResourceSnowflakeConfig:
    dynamic_table_config: Optional[SnowflakeDynamicTableConfig] = None
    warehouse: Optional[str] = None

    def config(self) -> dict:
        return {
            "DynamicTableConfig": (
                self.dynamic_table_config.config()
                if self.dynamic_table_config
                else None
            ),
            "Warehouse": self.warehouse,
        }

    def to_proto(self):
        return pb.ResourceSnowflakeConfig(
            dynamic_table_config=(
                self.dynamic_table_config.to_proto()
                if self.dynamic_table_config
                else None
            ),
            warehouse=self.warehouse,
        )


@typechecked
@dataclass
class SnowflakeCatalog(Catalog):
    external_volume: str
    base_location: str
    table_config: Optional[SnowflakeDynamicTableConfig] = None

    def __post_init__(self):
        if self.table_config is None:
            self.table_config = SnowflakeDynamicTableConfig(
                target_lag="DOWNSTREAM",
                refresh_mode=RefreshMode.AUTO,
                initialize=Initialize.ON_CREATE,
            )
        if not self._validate_target_lag():
            raise ValueError(
                "target_lag must be in the format of '<num> { seconds | minutes | hours | days }' or 'DOWNSTREAM'; the minimum value is 1 minute"
            )

    def config(self) -> dict:
        return {
            "ExternalVolume": self.external_volume,
            "BaseLocation": self.base_location,
            "TableFormat": TableFormat.ICEBERG,
            "TableConfig": self.table_config.config(),
        }

    def _validate_target_lag(self) -> bool:
        if self.table_config is None or self.table_config.target_lag is None:
            return True
        # See https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#create-dynamic-iceberg-table
        # for more information on the target_lag parameter
        pattern = r"^(\d+)\s+(seconds|minutes|hours|days)$|^(?i:DOWNSTREAM)$"
        match = re.match(pattern, self.table_config.target_lag)

        if not match:
            return False

        if self.table_config.target_lag.lower() == "downstream":
            return True

        value, unit = int(match.group(1)), match.group(2)

        if unit == "seconds" and value < 60:
            return False

        if unit == "minutes" and value < 1:
            return False

        return True


@typechecked
@dataclass
class SnowflakeConfig:
    username: str
    password: str
    schema: str
    account: str = ""
    organization: str = ""
    database: str = ""
    account_locator: str = ""
    warehouse: str = ""
    role: str = ""
    catalog: Optional[SnowflakeCatalog] = None
    session_params: Optional[Dict[str, str]] = None

    def __post_init__(self):

        if self.session_params:
            for key, _ in self.session_params.items():
                if not SnowflakeSessionParamKey.validate_key(key):
                    warnings.warn(
                        f"Invalid key '{key}' in session_params. This key will be ignored in the Snowflake session."
                    )

        if self.__has_legacy_credentials() and self.__has_current_credentials():
            raise ValueError(
                "Cannot create configure Snowflake with both current and legacy credentials"
            )

        if not self.__has_legacy_credentials() and not self.__has_current_credentials():
            raise ValueError("Cannot create configure Snowflake without credentials")

    def __has_legacy_credentials(self) -> bool:
        return self.account_locator != ""

    def __has_current_credentials(self) -> bool:
        if (self.account != "" and self.organization == "") or (
            self.account == "" and self.organization != ""
        ):
            raise ValueError("Both Snowflake organization and account must be included")
        elif self.account != "" and self.organization != "":
            return True
        else:
            return False

    def software(self) -> str:
        return "Snowflake"

    def type(self) -> str:
        return "SNOWFLAKE_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "Username": self.username,
            "Password": self.password,
            "Organization": self.organization,
            "AccountLocator": self.account_locator,
            "Account": self.account,
            "Schema": self.schema,
            "Database": self.database,
            "Warehouse": self.warehouse,
            "Role": self.role,
            "Catalog": self.catalog.config() if self.catalog is not None else None,
            "SessionParams": self.session_params,
        }
        return bytes(json.dumps(config), "utf-8")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, SnowflakeConfig):
            return False
        is_catalog_equal = (self.catalog is None and self.catalog is None) or (
            self.catalog is not None
            and __value.catalog is not None
            and self.catalog.external_volume == __value.catalog.external_volume
            and self.catalog.base_location == __value.catalog.base_location
            and self.catalog.table_config.target_lag
            == __value.catalog.table_config.target_lag
            and self.catalog.table_config.refresh_mode
            == __value.catalog.table_config.refresh_mode
            and self.catalog.table_config.initialize
            == __value.catalog.table_config.initialize
            and self.catalog.table_config.table_format
            == __value.catalog.table_config.table_format
        )
        return (
            self.username == __value.username
            and self.password == __value.password
            and self.schema == __value.schema
            and self.account == __value.account
            and self.organization == __value.organization
            and self.database == __value.database
            and self.account_locator == __value.account_locator
            and self.warehouse == __value.warehouse
            and self.role == __value.role
            and is_catalog_equal
            and self.session_params == __value.session_params
        )


@dataclass
class SparkFlags:
    spark_params: Dict[str, str] = field(default_factory=dict)
    write_options: Dict[str, str] = field(default_factory=dict)
    table_properties: Dict[str, str] = field(default_factory=dict)

    def serialize(self) -> dict:
        return {
            "SparkParams": self.spark_params,
            "WriteOptions": self.write_options,
            "TableProperties": self.table_properties,
        }

    @classmethod
    def deserialize(cls, config: dict) -> Optional["SparkFlags"]:
        """
        Deserialize a dictionary into a SparkFlags object.

        Args:
            config (dict): The dictionary containing SparkFlags configuration.

        Returns:
            Optional[SparkFlags]: The deserialized SparkFlags object, or None if the config is missing.
        """
        if not config:
            return None
        return cls(
            spark_params=config.get("SparkParams", {}),
            write_options=config.get("WriteOptions", {}),
            table_properties=config.get("TableProperties", {}),
        )

    def to_proto(self) -> pb.SparkFlags:
        return pb.SparkFlags(
            spark_params=[
                pb.SparkParam(key=k, value=v) for k, v in self.spark_params.items()
            ],
            write_options=[
                pb.WriteOption(key=k, value=v) for k, v in self.write_options.items()
            ],
            table_properties=[
                pb.TableProperty(key=k, value=v)
                for k, v in self.table_properties.items()
            ],
        )


EmptySparkFlags = SparkFlags()


@typechecked
@dataclass
class SparkConfig:
    executor_type: str
    executor_config: dict
    store_type: str
    store_config: dict
    catalog: Optional[dict] = None

    def software(self) -> str:
        return "spark"

    def type(self) -> str:
        return "SPARK_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "ExecutorType": self.executor_type,
            "StoreType": self.store_type,
            "ExecutorConfig": self.executor_config,
            "StoreConfig": self.store_config,
        }

        if self.catalog is not None:
            config["GlueConfig"] = self.catalog  # change to catalog later

        return bytes(json.dumps(config), "utf-8")

    @classmethod
    def deserialize(cls, json_bytes: bytes) -> "SparkConfig":
        deserialized_config = json.loads(json_bytes.decode("utf-8"))

        try:
            return cls(
                executor_type=deserialized_config["ExecutorType"],
                executor_config=deserialized_config["ExecutorConfig"],
                store_type=deserialized_config["StoreType"],
                store_config=deserialized_config["StoreConfig"],
                catalog=deserialized_config.get("GlueConfig"),
            )
        except KeyError as e:
            raise ValueError(f"Missing expected config key: {e}")


@typechecked
@dataclass
class K8sResourceSpecs:
    cpu_request: str = ""
    cpu_limit: str = ""
    memory_request: str = ""
    memory_limit: str = ""


@typechecked
@dataclass
class K8sArgs:
    # TODO Delete and Deprecate
    docker_image: str
    specs: Union[K8sResourceSpecs, None] = None

    def apply(self, transformation: pb.Transformation):
        transformation.kubernetes_args.docker_image = self.docker_image
        if self.specs is not None:
            transformation.kubernetes_args.specs.cpu_request = self.specs.cpu_request
            transformation.kubernetes_args.specs.cpu_limit = self.specs.cpu_limit
            transformation.kubernetes_args.specs.memory_request = (
                self.specs.memory_request
            )
            transformation.kubernetes_args.specs.memory_limit = self.specs.memory_limit
        return transformation


@typechecked
@dataclass
class K8sConfig:
    store_type: str
    store_config: dict
    docker_image: str = ""

    def software(self) -> str:
        return "k8s"

    def type(self) -> str:
        return "K8S_OFFLINE"

    def serialize(self) -> bytes:
        config = {
            "ExecutorType": "K8S",
            "ExecutorConfig": {"docker_image": self.docker_image},
            "StoreType": self.store_type,
            "StoreConfig": self.store_config,
        }
        return bytes(json.dumps(config), "utf-8")


@typechecked
@dataclass
class EmptyConfig:
    def software(self) -> str:
        return ""

    def type(self) -> str:
        return ""

    def serialize(self) -> bytes:
        return bytes("", "utf-8")

    def deserialize(self, config):
        return self


Config = Union[
    RedisConfig,
    PineconeConfig,
    SnowflakeConfig,
    PostgresConfig,
    ClickHouseConfig,
    RedshiftConfig,
    PineconeConfig,
    BigQueryConfig,
    FirestoreConfig,
    SparkConfig,
    OnlineBlobConfig,
    AzureFileStoreConfig,
    S3StoreConfig,
    K8sConfig,
    MongoDBConfig,
    GCSFileStoreConfig,
    EmptyConfig,
    HDFSConfig,
    WeaviateConfig,
    DynamodbConfig,
    CassandraConfig,
]


@typechecked
@dataclass
class Properties:
    properties: dict

    def __post_init__(self):
        self.serialized = pb.Properties()
        for key, val in self.properties.items():
            self.serialized.property[key].string_value = val


@dataclass
class ErrorInfo:
    code: int
    message: str
    reason: str
    metadata: Dict[str, str]


@dataclass
class ServerStatus:
    status: ResourceStatus
    error_info: Optional[ErrorInfo]

    @staticmethod
    def from_proto(resource_status_proto: pb.ResourceStatus):
        error_info = None
        if resource_status_proto.HasField("error_status"):
            code = resource_status_proto.error_status.code
            message = resource_status_proto.error_status.message

            error = resource_status_proto.error_status.details[0]
            error_info = error_details_pb2.ErrorInfo()
            if error.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                error.Unpack(error_info)
                error_info = ErrorInfo(
                    code=code,
                    message=message,
                    reason=error_info.reason,
                    metadata=error_info.metadata,
                )
            else:
                print("The Any field does not contain an ErrorInfo")

        return ServerStatus(
            status=ResourceStatus.from_proto(resource_status_proto),
            error_info=error_info,
        )


@typechecked
@dataclass
class Provider:
    name: str
    description: str
    config: Config
    function: str
    team: str = ""
    status: str = "NO_STATUS"
    tags: list = field(default_factory=list)
    properties: dict = field(default_factory=dict)
    error: Optional[str] = None
    has_health_check: bool = False
    server_status: Optional["ServerStatus"] = None

    def __post_init__(self):
        self.software = self.config.software() if self.config is not None else None
        if self.config.type() in [
            "REDIS_ONLINE",
            "DYNAMODB_ONLINE",
            "POSTGRES_OFFLINE",
            "SPARK_OFFLINE",
            "REDSHIFT_OFFLINE",
            "CLICKHOUSE_OFFLINE",
        ]:
            self.has_health_check = True

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.PROVIDER

    def get(self, stub) -> "Provider":
        name = pb.NameRequest(name=pb.Name(name=self.name))
        provider = next(stub.GetProviders(iter([name])))

        return Provider(
            name=provider.name,
            description=provider.description,
            function=provider.type,
            team=provider.team,
            config=EmptyConfig(),  # TODO add deserializer to configs
            tags=list(provider.tags.tag),
            properties={k: v for k, v in provider.properties.property.items()},
            status=provider.status.Status._enum_type.values[
                provider.status.status
            ].name,
            error=provider.status.error_message,
            server_status=ServerStatus.from_proto(provider.status),
        )

    def _create(self, req_id, stub) -> Tuple[None, None]:
        serialized = pb.ProviderRequest(
            provider=pb.Provider(
                name=self.name,
                description=self.description,
                type=self.config.type(),
                software=self.config.software(),
                team=self.team,
                serialized_config=self.config.serialize(),
                tags=pb.Tags(tag=self.tags),
                properties=Properties(self.properties).serialized,
            ),
            request_id="",
        )
        stub.CreateProvider(serialized)
        return None, None

    def to_dictionary(self):
        return {
            "name": self.name,
            "description": self.description,
            "team": self.team,
            "config": "todox",
            "function": "todox",
            "status": self.status,
            "tags": self.tags,
            "properties": self.properties,
            "error": self.error,
        }


@typechecked
@dataclass
class User:
    name: str
    status: str = ""
    tags: list = field(default_factory=list)
    properties: dict = field(default_factory=dict)

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.USER

    def _create(self, req_id, stub) -> Tuple[None, None]:
        serialized = pb.UserRequest(
            user=pb.User(
                name=self.name,
                tags=pb.Tags(tag=self.tags),
                properties=Properties(self.properties).serialized,
            ),
            request_id="",
        )

        stub.CreateUser(serialized)
        return None, None

    def to_dictionary(self):
        return {
            "name": self.name,
            "status": self.status,
            "tags": self.tags,
            "properties": self.properties,
        }


class Location(ABC):
    def resource_identifier(self):
        """
        Return the location of the resource.
        :return:
        """
        raise NotImplementedError


@typechecked
@dataclass
class SQLTable(Location):
    name: str
    schema: str = ""
    database: str = ""

    def resource_identifier(self):
        return f"{self.database}.{self.schema}.{self.name}"

    @staticmethod
    def from_proto(source_table):
        return SQLTable(
            schema=source_table.schema,
            database=source_table.database,
            name=source_table.name,
        )


@typechecked
@dataclass
class FileStore(Location):
    """
    Contains the location of a path in a cloud file store (S3, GCS, Azure)
    """

    path_uri: str

    def resource_identifier(self):
        return self.path_uri

    @staticmethod
    def from_proto(source_filestore):
        return FileStore(path_uri=source_filestore.path)


@typechecked
@dataclass
class KafkaTopic(Location):
    topic: str

    def resource_identifier(self):
        return self.topic

    @staticmethod
    def from_proto(source_kafka_topic):
        return KafkaTopic(topic=source_kafka_topic.topic)


@typechecked
@dataclass
class GlueCatalogTable(Location):
    database: str
    table: str
    table_format: TableFormat = field(default_factory=lambda: TableFormat.ICEBERG)

    def resource_identifier(self):
        return f"{self.database}.{self.table}"

    @staticmethod
    def from_proto(source_catalog_table):
        return GlueCatalogTable(
            database=source_catalog_table.database,
            table=source_catalog_table.table,
            table_format=TableFormat.get_format(source_catalog_table.table_format),
        )


@typechecked
@dataclass
class Directory(Location):
    path: str

    def path(self):
        return self.path


class ResourceVariant(ABC):
    name: str
    variant: str
    server_status: ServerStatus

    @staticmethod
    def get_resource_type():
        raise NotImplementedError

    def name_variant(self):
        return self.name, self.variant

    def to_key(self) -> Tuple[ResourceType, str, str]:
        return self.get_resource_type(), self.name, self.variant


@typechecked
@dataclass
class PrimaryData:
    location: Location
    timestamp_column: str = ""

    def kwargs(self) -> Dict[str, Any]:
        primary_data_kwargs = {
            "timestamp_column": self.timestamp_column,
        }

        if isinstance(self.location, SQLTable):
            primary_data_kwargs["table"] = pb.SQLTable(
                schema=self.location.schema,
                database=self.location.database,
                name=self.location.name,
            )
        elif isinstance(self.location, FileStore):
            primary_data_kwargs["filestore"] = pb.FileStoreTable(
                path=self.location.resource_identifier(),
            )
        elif isinstance(self.location, GlueCatalogTable):
            primary_data_kwargs["catalog"] = pb.CatalogTable(
                database=self.location.database,
                table=self.location.table,
                table_format=self.location.table_format,
            )
        elif isinstance(self.location, KafkaTopic):
            primary_data_kwargs["kafka"] = pb.Kafka(topic=self.location.topic)
        else:
            raise ValueError(f"Unsupported location type: {type(self.location)}")

        return {"primaryData": pb.PrimaryData(**primary_data_kwargs)}

    def path(self) -> str:
        return self.location.resource_identifier()

    @staticmethod
    def from_proto(source_primary_data):
        primary_type = source_primary_data.WhichOneof("location")
        if primary_type == LocationType.TABLE:
            location = SQLTable.from_proto(source_primary_data.table)
        elif primary_type == LocationType.FILESTORE:
            location = FileStore.from_proto(source_primary_data.filestore)
        elif primary_type == LocationType.CATALOG:
            location = GlueCatalogTable.from_proto(source_primary_data.catalog)
        else:
            raise Exception(f"Invalid primary data type {source_primary_data}")

        return PrimaryData(location, source_primary_data.timestamp_column)


class Transformation(ABC):
    @classmethod
    def from_proto(cls, source_transformation: pb.Transformation):
        if source_transformation.DFTransformation.query != bytes():
            return DFTransformation.from_proto(source_transformation.DFTransformation)
        elif source_transformation.SQLTransformation.query != "":
            return SQLTransformation.from_proto(source_transformation.SQLTransformation)
        else:
            raise Exception(f"Invalid transformation type {source_transformation}")


@typechecked
@dataclass
class SQLTransformation(Transformation):
    query: str
    args: Optional[K8sArgs] = None
    func_params_to_inputs: Dict[str, Any] = field(default_factory=dict)
    partition_options: Optional[PartitionType] = None
    spark_flags: SparkFlags = field(default_factory=lambda: EmptySparkFlags)
    resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None

    _sql_placeholder_regex: str = field(
        default=r"\{\{\s*\w+\s*\}\}", init=False, repr=False
    )

    def __post_init__(self):
        self._validate_inputs_to_func_params(self.func_params_to_inputs)

    def type(self) -> str:
        """Return the type of the SQL transformation."""
        return SourceType.SQL_TRANSFORMATION.value

    def kwargs(self) -> Dict[str, pb.Transformation]:
        return {"transformation": self.to_proto()}

    def _validate_inputs_to_func_params(self, inputs: Dict[str, Any]) -> None:
        # Find and replace placeholders in the query with source name variants
        for placeholder in self._get_placeholders():
            clean_placeholder = placeholder.strip(" {}")
            if clean_placeholder not in self.func_params_to_inputs.keys():
                raise ValueError(
                    f"SQL placeholder '{placeholder}' not found in input arguments. "
                    f"Available input arguments: {', '.join(inputs.keys())}.\n"
                    f"Expected inputs based on function parameters: {', '.join(self.func_params_to_inputs.keys())}."
                )

    def _resolve_input_variants(self) -> Dict[str, Any]:
        """Resolve inputs to their name variants."""
        return {
            func_param: inp.name_variant() if hasattr(inp, "name_variant") else inp
            for func_param, inp in self.func_params_to_inputs.items()
        }

    def _get_placeholders(self) -> List[str]:
        """Get placeholders from the query."""
        return re.findall(self._sql_placeholder_regex, self.query)

    def to_proto(self) -> pb.Transformation:
        input_to_name_variant = self._resolve_input_variants()

        # Find and replace placeholders in the query with source name variants
        final_query = self.query
        for placeholder in self._get_placeholders():
            clean_placeholder = placeholder.strip(" {}")
            name_variant = input_to_name_variant[clean_placeholder]
            replacement = "{{ " + f"{name_variant[0]}.{name_variant[1]}" + " }}"
            final_query = final_query.replace(placeholder, replacement)

        partition_kwargs = {}
        if self.partition_options is not None:
            partition_kwargs = self.partition_options.proto_kwargs()

        # Construct the SQLTransformation protobuf message
        transformation = pb.Transformation(
            SQLTransformation=pb.SQLTransformation(
                query=final_query,
                resource_snowflake_config=(
                    self.resource_snowflake_config.to_proto()
                    if self.resource_snowflake_config
                    else None
                ),
            ),
            spark_flags=self.spark_flags.to_proto() if self.spark_flags else None,
            **partition_kwargs,
        )

        # Apply args transformations if any
        if self.args is not None:
            transformation = self.args.apply(transformation)

        return transformation

    @classmethod
    def from_proto(cls, sql_transformation: pb.SQLTransformation):
        return SQLTransformation(sql_transformation.query)


@typechecked
@dataclass
class DFTransformation(Transformation):
    query: bytes
    inputs: list
    args: K8sArgs = None
    source_text: str = ""
    canonical_func_text: str = ""
    partition_options: Optional[PartitionType] = None
    spark_flags: SparkFlags = field(default_factory=lambda: EmptySparkFlags)

    def type(self):
        return SourceType.DF_TRANSFORMATION.value

    def to_proto(self) -> pb.Transformation:
        # Create a new list of name variants without modifying self.inputs
        name_variants = []
        for inp in self.inputs:
            if hasattr(inp, "name_variant"):
                name_variants.append(inp.name_variant())
            else:
                name_variants.append(inp)

        name_variant_protos = [
            pb.NameVariant(name=inp[0], variant=inp[1]) for inp in name_variants
        ]

        partition_kwargs = {}
        if self.partition_options is not None:
            partition_kwargs = self.partition_options.proto_kwargs()

        transformation = pb.Transformation(
            DFTransformation=pb.DFTransformation(
                query=self.query,
                inputs=name_variant_protos,
                source_text=self.source_text,
                canonical_func_text=self.canonical_func_text,
            ),
            spark_flags=self.spark_flags.to_proto() if self.spark_flags else None,
            **partition_kwargs,
        )

        if self.args is not None:
            transformation = self.args.apply(transformation)

        return transformation

    def kwargs(self):
        return {"transformation": self.to_proto()}

    @classmethod
    def from_proto(cls, df_transformation: pb.DFTransformation):
        return DFTransformation(
            query=df_transformation.query,
            inputs=[(input.name, input.variant) for input in df_transformation.inputs],
            source_text=df_transformation.source_text,
        )


SourceDefinition = Union[PrimaryData, Transformation, str]


@typechecked
@dataclass
class Source:
    name: str
    default_variant: str
    variants: List[str]

    def to_dictionary(self):
        return {
            "name": self.name,
            "default_variant": self.default_variant,
            "variants": self.variants,
        }


@typechecked
@dataclass
class SourceVariant(ResourceVariant):
    name: str
    definition: SourceDefinition
    owner: str
    provider: str
    description: str
    tags: list
    properties: dict
    variant: str
    created: str = None
    status: str = (
        "ready"  # there is no associated status by default but it always stores ready
    )
    schedule: str = ""
    schedule_obj: Schedule = None
    is_transformation = (
        SourceType.PRIMARY_SOURCE.value
    )  # TODO this is the same as source_type below; pick one
    source_text: str = ""
    source_type: str = ""
    transformation: str = ""
    inputs: list = ([],)
    error: Optional[str] = None
    server_status: Optional[ServerStatus] = None
    max_job_duration: timedelta = timedelta(hours=48)

    def update_schedule(self, schedule) -> None:
        self.schedule_obj = Schedule(
            name=self.name,
            variant=self.variant,
            resource_type=7,
            schedule_string=schedule,
        )
        self.schedule = schedule

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.SOURCE_VARIANT

    def name_variant(self) -> NameVariant:
        return (self.name, self.variant)

    def get(self, stub):
        return SourceVariant.get_by_name_variant(stub, self.name, self.variant)

    @staticmethod
    def get_by_name_variant(stub, name, variant):
        name_variant = pb.NameVariantRequest(
            name_variant=pb.NameVariant(name=name, variant=variant)
        )
        source = next(stub.GetSourceVariants(iter([name_variant])))
        definition = SourceVariant._get_source_definition(source)

        return SourceVariant(
            created=None,
            name=source.name,
            definition=definition,
            owner=source.owner,
            provider=source.provider,
            description=source.description,
            variant=source.variant,
            tags=list(source.tags.tag),
            properties={k: v for k, v in source.properties.property.items()},
            status=source.status.Status._enum_type.values[source.status.status].name,
            error=source.status.error_message,
            server_status=ServerStatus.from_proto(source.status),
            max_job_duration=source.max_job_duration.ToTimedelta(),
        )

    @staticmethod
    def _get_source_definition(source):
        definition_type = source.WhichOneof("definition")
        if definition_type == "primaryData":
            return PrimaryData.from_proto(source.primaryData)
        elif definition_type == "transformation":
            return Transformation.from_proto(source.transformation)
        else:
            raise Exception(f"Invalid source definition type {definition_type}")

    def _get_and_set_equivalent_variant(self, req_id, stub):
        defArgs = self.definition.kwargs()
        duration = Duration()
        duration.FromTimedelta(self.max_job_duration)

        serialized = pb.SourceVariantRequest(
            source_variant=pb.SourceVariant(
                created=None,
                name=self.name,
                variant=self.variant,
                owner=self.owner,
                description=self.description,
                schedule=self.schedule,
                provider=self.provider,
                tags=pb.Tags(tag=self.tags),
                properties=Properties(self.properties).serialized,
                status=pb.ResourceStatus(status=pb.ResourceStatus.NO_STATUS),
                max_job_duration=duration,
                **defArgs,
            ),
            request_id="",
        )

        existing_variant = _get_and_set_equivalent_variant(
            req_id, serialized, "source_variant", stub
        )
        return serialized, existing_variant, "source_variant"

    def _create(self, req_id, stub) -> Tuple[Optional[str], Optional[str]]:
        serialized, existing_variant, _ = self._get_and_set_equivalent_variant(
            req_id, stub
        )
        if existing_variant is None:
            stub.CreateSourceVariant(serialized)
        return serialized.source_variant.variant, existing_variant

    def get_status(self):
        return ResourceStatus(self.status)

    def is_transformation_type(self):
        return isinstance(self.definition, Transformation)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value


@typechecked
@dataclass
class Entity:
    name: str
    description: str
    status: str = "NO_STATUS"
    tags: list = field(default_factory=list)
    properties: dict = field(default_factory=dict)

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.ENTITY

    def _create(self, req_id, stub) -> Tuple[None, None]:
        serialized = pb.EntityRequest(
            entity=pb.Entity(
                name=self.name,
                description=self.description,
                tags=pb.Tags(tag=self.tags),
                properties=Properties(self.properties).serialized,
            ),
            request_id="",
        )

        stub.CreateEntity(serialized)
        return None, None

    def to_dictionary(self):
        return {
            "name": self.name,
            "description": self.description,
            "status": self.status,
            "tags": self.tags,
            "properties": self.properties,
        }


@typechecked
@dataclass
class ResourceColumnMapping:
    entity: str
    value: str
    timestamp: str

    def proto(self) -> pb.Columns:
        return pb.Columns(
            entity=self.entity,
            value=self.value,
            ts=self.timestamp,
        )


ResourceLocation = ResourceColumnMapping


@typechecked
@dataclass
class Feature:
    name: str
    default_variant: str
    variants: List[str]

    def to_dictionary(self):
        return {
            "name": self.name,
            "default_variant": self.default_variant,
            "variants": self.variants,
        }


class PrecomputedFeatureParameters:
    pass


@typechecked
@dataclass
class OndemandFeatureParameters:
    definition: str = ""

    def proto(self) -> pb.FeatureParameters:
        ondemand_feature_parameters = pb.OndemandFeatureParameters(
            definition=self.definition
        )
        feature_parameters = pb.FeatureParameters()
        feature_parameters.ondemand.CopyFrom(ondemand_feature_parameters)
        return feature_parameters


Additional_Parameters = Union[
    PrecomputedFeatureParameters, OndemandFeatureParameters, None
]


@typechecked
@dataclass
class FeatureVariant(ResourceVariant):
    name: str
    source: Any
    value_type: Union[VectorType, ScalarType, str]
    entity: str
    owner: str
    location: ResourceLocation
    description: str
    variant: str
    provider: Optional[str] = None
    created: str = None
    tags: Optional[list] = None
    properties: Optional[dict] = None
    schedule: str = ""
    schedule_obj: Schedule = None
    status: str = "NO_STATUS"
    error: Optional[str] = None
    additional_parameters: Optional[Additional_Parameters] = None
    server_status: Optional[ServerStatus] = None
    resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None

    def __post_init__(self):
        if isinstance(self.value_type, str):
            self.value_type = ScalarType(self.value_type)

    def update_schedule(self, schedule) -> None:
        self.schedule_obj = Schedule(
            name=self.name,
            variant=self.variant,
            resource_type=4,
            schedule_string=schedule,
        )
        self.schedule = schedule

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.FEATURE_VARIANT

    def name_variant(self) -> NameVariant:
        return (self.name, self.variant)

    def get(self, stub) -> "FeatureVariant":
        return FeatureVariant.get_by_name_variant(stub, self.name, self.variant)

    @staticmethod
    def get_by_name_variant(stub, name, variant):
        name_variant = pb.NameVariantRequest(
            name_variant=pb.NameVariant(name=name, variant=variant)
        )
        feature = next(stub.GetFeatureVariants(iter([name_variant])))

        return FeatureVariant(
            created=None,
            name=feature.name,
            variant=feature.variant,
            source=(feature.source.name, feature.source.variant),
            value_type=type_from_proto(feature.type),
            entity=feature.entity,
            owner=feature.owner,
            provider=feature.provider,
            location=ResourceColumnMapping("", "", ""),
            description=feature.description,
            tags=list(feature.tags.tag),
            properties={k: v for k, v in feature.properties.property.items()},
            status=feature.status.Status._enum_type.values[feature.status.status].name,
            error=feature.status.error_message,
            server_status=ServerStatus.from_proto(feature.status),
            additional_parameters=None,
        )

    def _get_and_set_equivalent_variant(self, req_id, stub):
        if hasattr(self.source, "name_variant"):
            self.source = self.source.name_variant()

        feature_variant_message = pb.FeatureVariant(
            name=self.name,
            variant=self.variant,
            source=pb.NameVariant(
                name=self.source[0],
                variant=self.source[1],
            ),
            type=self.value_type.to_proto(),
            entity=self.entity,
            owner=self.owner,
            description=self.description,
            schedule=self.schedule,
            provider=self.provider,
            columns=self.location.proto(),
            mode=ComputationMode.PRECOMPUTED.proto(),
            tags=pb.Tags(tag=self.tags),
            properties=Properties(self.properties).serialized,
            status=pb.ResourceStatus(status=pb.ResourceStatus.NO_STATUS),
            additional_parameters=None,
            resource_snowflake_config=(
                self.resource_snowflake_config.to_proto()
                if self.resource_snowflake_config
                else None
            ),
        )

        # Initialize the FeatureVariantRequest message with the FeatureVariant message
        serialized = pb.FeatureVariantRequest(
            feature_variant=feature_variant_message,
            request_id="",
        )

        return (
            serialized,
            _get_and_set_equivalent_variant(
                req_id, serialized, "feature_variant", stub
            ),
            "feature_variant",
        )

    def _create(self, req_id, stub) -> Tuple[Optional[str], Optional[str]]:
        serialized, existing_variant, _ = self._get_and_set_equivalent_variant(
            req_id, stub
        )
        if existing_variant is None:
            stub.CreateFeatureVariant(serialized)
        return serialized.feature_variant.variant, existing_variant

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value


class BaseStream(ABC):
    def __init__(
        self,
        name: str,
        value_type: str,
        entity: str,
        owner: str,
        offline_provider: str,
        description: str,
        variant: str,
        tags: Union[list, None] = None,
        properties: Union[dict, None] = None,
        status: str = "NO_STATUS",
        error: Optional[str] = None,
    ):
        self.name = name
        self.value_type = value_type
        self.entity = entity
        self.owner = owner
        self.offline_provider = offline_provider
        self.description = description
        self.variant = variant
        self.tags = [] if tags is None else tags
        self.properties = {} if properties is None else properties
        self.status = status
        self.error = error

    def __post_init__(self):
        col_types = [member.value for member in ScalarType]
        if self.value_type not in col_types:
            raise ValueError(
                f"Invalid feature type ({self.value_type}) must be one of: {col_types}"
            )

    @staticmethod
    @abstractmethod
    def get_resource_type() -> ResourceType:
        raise NotImplementedError

    @abstractmethod
    def get(self, stub) -> "Stream":
        raise NotImplementedError

    @abstractmethod
    def _create(self, req_id, stub) -> Tuple[None, None]:
        raise NotImplementedError

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    def _create_local(self, db) -> None:
        pass

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value

    def __eq__(self, other):
        for attribute in vars(self):
            if getattr(self, attribute) != getattr(other, attribute):
                return False
        return True


@typechecked
class StreamFeature(BaseStream):
    inference_store: str = ""

    def __init__(self, inference_store: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inference_store = inference_store

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.FEATURE_VARIANT

    def get(self, stub) -> "StreamFeature":
        name_variant = pb.NameVariant(name=self.name, variant=self.variant)
        stream_feature = next(stub.GetFeatureVariants(iter([name_variant])))

        return StreamFeature(
            name=stream_feature.name,
            variant=stream_feature.variant,
            value_type=stream_feature.type,
            entity=stream_feature.entity,
            owner=stream_feature.owner,
            inference_store=stream_feature.provider,
            offline_provider=stream_feature.stream.offline_provider,
            description=stream_feature.description,
            tags=list(stream_feature.tags.tag),
            properties={k: v for k, v in stream_feature.properties.property.items()},
            status=stream_feature.status.Status._enum_type.values[
                stream_feature.status.status
            ].name,
            error=stream_feature.status.error_message,
        )

    def _create(self, req_id, stub) -> Tuple[None, None]:
        serialized = pb.FeatureVariant(
            name=self.name,
            variant=self.variant,
            type=self.value_type,
            entity=self.entity,
            owner=self.owner,
            description=self.description,
            provider=self.inference_store,
            stream=pb.Stream(
                offline_provider=self.offline_provider,
            ),
            mode=ComputationMode.STREAMING.proto(),
            tags=pb.Tags(tag=self.tags),
            properties=Properties(self.properties).serialized,
        )
        stub.CreateFeatureVariant(serialized)
        return None, None


@typechecked
class StreamLabel(BaseStream):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.LABEL_VARIANT

    def get(self, stub) -> "StreamLabel":
        name_variant = pb.NameVariant(name=self.name, variant=self.variant)
        stream_label = next(stub.GetLabelVariants(iter([name_variant])))

        return StreamLabel(
            name=stream_label.name,
            variant=stream_label.variant,
            value_type=stream_label.type,
            entity=stream_label.entity,
            owner=stream_label.owner,
            offline_provider=stream_label.stream.offline_provider,
            description=stream_label.description,
            tags=list(stream_label.tags.tag),
            properties={k: v for k, v in stream_label.properties.property.items()},
            status=stream_label.status.Status._enum_type.values[
                stream_label.status.status
            ].name,
            error=stream_label.status.error_message,
        )

    def _create(self, req_id, stub) -> Tuple[None, None]:
        serialized = pb.LabelVariant(
            name=self.name,
            variant=self.variant,
            type=self.value_type,
            entity=self.entity,
            owner=self.owner,
            description=self.description,
            provider=self.offline_provider,
            stream=pb.Stream(
                offline_provider=self.offline_provider,
            ),
            tags=pb.Tags(tag=self.tags),
            properties=Properties(self.properties).serialized,
        )
        stub.CreateLabelVariant(serialized)
        return None, None


@typechecked
@dataclass
class OnDemandFeatureVariant(ResourceVariant):
    owner: str
    variant: str
    tags: List[str] = field(default_factory=list)
    properties: dict = field(default_factory=dict)
    name: str = ""
    description: str = ""
    status: str = "READY"
    error: Optional[str] = None
    additional_parameters: Optional[Additional_Parameters] = None
    server_status: Optional[ServerStatus] = None

    def __call__(self, fn):
        if self.description == "" and fn.__doc__ is not None:
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__

        self.query = dill.dumps(fn.__code__)
        feature_text = dill.source.getsource(fn)
        self.additional_parameters = OndemandFeatureParameters(definition=feature_text)
        fn.name_variant = self.name_variant
        fn.query = self.query
        return fn

    def name_variant(self):
        return (self.name, self.variant)

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.ONDEMAND_FEATURE

    def _get_and_set_equivalent_variant(self, req_id, stub):
        serialized = pb.FeatureVariantRequest(
            feature_variant=pb.FeatureVariant(
                name=self.name,
                variant=self.variant,
                owner=self.owner,
                description=self.description,
                function=pb.PythonFunction(query=self.query),
                mode=ComputationMode.CLIENT_COMPUTED.proto(),
                tags=pb.Tags(tag=self.tags),
                properties=Properties(self.properties).serialized,
                status=pb.ResourceStatus(status=pb.ResourceStatus.READY),
                additional_parameters=self.additional_parameters.proto(),
            ),
            request_id="",
        )

        return (
            serialized,
            _get_and_set_equivalent_variant(
                req_id, serialized, "feature_variant", stub
            ),
            "feature_variant",
        )

    def _create(self, req_id, stub) -> Tuple[Optional[str], Optional[str]]:
        serialized, existing_variant, _ = self._get_and_set_equivalent_variant(
            req_id, stub
        )
        if existing_variant is None:
            stub.CreateFeatureVariant(serialized)
        return serialized.feature_variant.variant, existing_variant

    def get(self, stub) -> "OnDemandFeatureVariant":
        name_variant = pb.NameVariantRequest(
            name_variant=pb.NameVariant(name=self.name, variant=self.variant)
        )
        ondemand_feature = next(stub.GetFeatureVariants(iter([name_variant])))
        additional_Parameters = self._get_additional_parameters(ondemand_feature)

        return OnDemandFeatureVariant(
            name=ondemand_feature.name,
            variant=ondemand_feature.variant,
            owner=ondemand_feature.owner,
            description=ondemand_feature.description,
            tags=list(ondemand_feature.tags.tag),
            properties={k: v for k, v in ondemand_feature.properties.property.items()},
            status=ondemand_feature.status.Status._enum_type.values[
                ondemand_feature.status.status
            ].name,
            error=ondemand_feature.status.error_message,
            additional_parameters=additional_Parameters,
        )

    def _get_additional_parameters(self, feature):
        return OndemandFeatureParameters(definition="() => FUNCTION")

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value


@typechecked
@dataclass
class Label:
    name: str
    default_variant: str
    variants: List[str]

    def to_dictionary(self):
        return {
            "name": self.name,
            "default_variant": self.default_variant,
            "variants": self.variants,
        }


@typechecked
@dataclass
class LabelVariant(ResourceVariant):
    name: str
    source: Any
    value_type: Union[VectorType, ScalarType, str]
    entity: str
    owner: str
    description: str
    location: ResourceLocation
    variant: str
    tags: Optional[list] = None
    properties: Optional[dict] = None
    provider: Optional[str] = None
    created: str = None
    status: str = "NO_STATUS"
    error: Optional[str] = None
    server_status: Optional[ServerStatus] = None
    resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None

    def __post_init__(self):
        if isinstance(self.value_type, str):
            self.value_type = ScalarType(self.value_type)

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.LABEL_VARIANT

    def name_variant(self) -> NameVariant:
        return (self.name, self.variant)

    def get(self, stub) -> "LabelVariant":
        return LabelVariant.get_by_name_variant(stub, self.name, self.variant)

    @staticmethod
    def get_by_name_variant(stub, name, variant):
        name_variant = pb.NameVariantRequest(
            name_variant=pb.NameVariant(name=name, variant=variant)
        )
        label = next(stub.GetLabelVariants(iter([name_variant])))

        return LabelVariant(
            name=label.name,
            variant=label.variant,
            source=(label.source.name, label.source.variant),
            value_type=type_from_proto(label.type),
            entity=label.entity,
            owner=label.owner,
            provider=label.provider,
            location=ResourceColumnMapping("", "", ""),
            description=label.description,
            tags=list(label.tags.tag),
            properties={k: v for k, v in label.properties.property.items()},
            status=label.status.Status._enum_type.values[label.status.status].name,
            server_status=ServerStatus.from_proto(label.status),
            error=label.status.error_message,
        )

    def _get_and_set_equivalent_variant(self, req_id, stub):
        if hasattr(self.source, "name_variant"):
            self.source = self.source.name_variant()
        serialized = pb.LabelVariantRequest(
            label_variant=pb.LabelVariant(
                name=self.name,
                variant=self.variant,
                source=pb.NameVariant(
                    name=self.source[0],
                    variant=self.source[1],
                ),
                provider=self.provider,
                type=self.value_type.to_proto(),
                entity=self.entity,
                owner=self.owner,
                description=self.description,
                columns=self.location.proto(),
                tags=pb.Tags(tag=self.tags),
                properties=Properties(self.properties).serialized,
                status=pb.ResourceStatus(status=pb.ResourceStatus.NO_STATUS),
                resource_snowflake_config=(
                    self.resource_snowflake_config.to_proto()
                    if self.resource_snowflake_config
                    else None
                ),
            ),
            request_id="",
        )

        return (
            serialized,
            _get_and_set_equivalent_variant(req_id, serialized, "label_variant", stub),
            "label_variant",
        )

    def _create(self, req_id, stub) -> Tuple[Optional[str], Optional[str]]:
        serialized, existing_variant, _ = self._get_and_set_equivalent_variant(
            req_id, stub
        )
        if existing_variant is None:
            stub.CreateLabelVariant(serialized)
        return serialized.label_variant.variant, existing_variant

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value


@typechecked
@dataclass
class EntityReference:
    name: str
    obj: Union[Entity, None]

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.GET

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.ENTITY

    def _get(self, stub):
        entityList = stub.GetEntities(
            iter([pb.NameRequest(name=pb.Name(name=self.name))])
        )
        try:
            for entity in entityList:
                self.obj = entity
        except grpc._channel._MultiThreadedRendezvous:
            raise ValueError(f"Entity {self.name} not found.")


@typechecked
@dataclass
class ProviderReference:
    name: str
    provider_type: str
    obj: Union[Provider, None]

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.GET

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.PROVIDER

    def _get(self, stub):
        providerList = stub.GetProviders(
            iter([pb.NameRequest(name=pb.Name(name=self.name))])
        )
        try:
            for provider in providerList:
                self.obj = provider
        except grpc._channel._MultiThreadedRendezvous:
            raise ValueError(
                f"Provider {self.name} of type {self.provider_type} not found."
            )


@typechecked
@dataclass
class SourceReference:
    name: str
    variant: str
    obj: Union[SourceVariant, None]

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.GET

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.SOURCE_VARIANT

    def name_variant(self) -> NameVariant:
        return (self.name, self.variant)

    def _get(self, stub):
        sourceList = stub.GetSourceVariants(
            iter(
                [
                    pb.NameVariantRequest(
                        name_variant=pb.NameVariant(
                            name=self.name, variant=self.variant
                        )
                    )
                ]
            )
        )
        try:
            for source in sourceList:
                self.obj = source
        except grpc._channel._MultiThreadedRendezvous:
            raise ValueError(f"Source {self.name}, variant {self.variant} not found.")


@typechecked
@dataclass
class TrainingSet:
    name: str
    default_variant: str
    variants: List[str]

    def to_dictionary(self):
        return {
            "name": self.name,
            "default_variant": self.default_variant,
            "variants": self.variants,
        }


@typechecked
@dataclass
class TrainingSetVariant(ResourceVariant):
    name: str
    owner: str
    label: Any
    features: List[Any]
    description: str
    variant: str
    feature_lags: list = field(default_factory=list)
    tags: list = field(default_factory=list)
    properties: dict = field(default_factory=dict)
    created: str = None
    schedule: str = ""
    schedule_obj: Schedule = None
    provider: str = ""
    status: str = "NO_STATUS"
    error: Optional[str] = None
    server_status: Optional[ServerStatus] = None
    resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None

    def update_schedule(self, schedule) -> None:
        self.schedule_obj = Schedule(
            name=self.name,
            variant=self.variant,
            resource_type=6,
            schedule_string=schedule,
        )
        self.schedule = schedule

    def __post_init__(self):
        from featureform import LabelColumnResource, FeatureColumnResource

        if not isinstance(self.label, LabelColumnResource) and not valid_name_variant(
            self.label
        ):
            raise ValueError("Label must be set")
        if len(self.features) == 0:
            raise ValueError("A training-set must have at least one feature")
        for feature in self.features:
            if not isinstance(
                feature, FeatureColumnResource
            ) and not valid_name_variant(feature):
                raise ValueError("Invalid Feature")

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.TRAININGSET_VARIANT

    def name_variant(self) -> NameVariant:
        return (self.name, self.variant)

    def get(self, stub):
        return TrainingSetVariant.get_by_name_variant(stub, self.name, self.variant)

    @staticmethod
    def get_by_name_variant(stub, name, variant):
        name_variant = pb.NameVariantRequest(
            name_variant=pb.NameVariant(name=name, variant=variant)
        )
        ts = next(stub.GetTrainingSetVariants(iter([name_variant])))

        return TrainingSetVariant(
            created=None,
            name=ts.name,
            variant=ts.variant,
            owner=ts.owner,
            description=ts.description,
            status=ts.status.Status._enum_type.values[ts.status.status].name,
            label=(ts.label.name, ts.label.variant),
            features=[(f.name, f.variant) for f in ts.features],
            feature_lags=[],
            provider=ts.provider,
            tags=list(ts.tags.tag),
            properties={k: v for k, v in ts.properties.property.items()},
            error=ts.status.error_message,
            server_status=ServerStatus.from_proto(ts.status),
        )

    def _get_and_set_equivalent_variant(self, req_id, stub):
        feature_lags = []
        for lag in self.feature_lags:
            lag_duration = Duration()
            _ = lag_duration.FromTimedelta(lag["lag"])
            feature_lag = pb.FeatureLag(
                feature=lag["feature"],
                variant=lag["variant"],
                name=lag["name"],
                lag=lag_duration,
            )
            feature_lags.append(feature_lag)

        for i, f in enumerate(self.features):
            if hasattr(f, "name_variant"):
                self.features[i] = f.name_variant()

        if hasattr(self.label, "name_variant"):
            self.label = self.label.name_variant()

        serialized = pb.TrainingSetVariantRequest(
            training_set_variant=pb.TrainingSetVariant(
                created=None,
                name=self.name,
                variant=self.variant,
                description=self.description,
                schedule=self.schedule,
                owner=self.owner,
                features=[
                    pb.NameVariant(name=v[0], variant=v[1]) for v in self.features
                ],
                label=pb.NameVariant(name=self.label[0], variant=self.label[1]),
                feature_lags=feature_lags,
                tags=pb.Tags(tag=self.tags),
                properties=Properties(self.properties).serialized,
                status=pb.ResourceStatus(status=pb.ResourceStatus.NO_STATUS),
                provider=self.provider,
                resource_snowflake_config=(
                    self.resource_snowflake_config.to_proto()
                    if self.resource_snowflake_config
                    else None
                ),
            ),
            request_id="",
        )
        return (
            serialized,
            _get_and_set_equivalent_variant(
                req_id, serialized, "training_set_variant", stub
            ),
            "training_set_variant",
        )

    def _create(self, req_id, stub) -> Tuple[Optional[str], Optional[str]]:
        serialized, existing_variant, _ = self._get_and_set_equivalent_variant(
            req_id, stub
        )
        if existing_variant is None:
            stub.CreateTrainingSetVariant(serialized)
        return serialized.training_set_variant.variant, existing_variant

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value


@typechecked
@dataclass
class Model:
    name: str
    description: str = ""
    tags: list = field(default_factory=list)
    properties: dict = field(default_factory=dict)

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def get_resource_type() -> ResourceType:
        return ResourceType.MODEL

    def _create(self, req_id, stub) -> Tuple[None, None]:
        properties = pb.Properties(property=self.properties)
        serialized = pb.ModelRequest(
            model=pb.Model(
                name=self.name,
                tags=pb.Tags(tag=self.tags),
                properties=Properties(self.properties).serialized,
            ),
            request_id="",
        )

        stub.CreateModel(serialized)
        return None, None

    def to_dictionary(self):
        return {
            "name": self.name,
            "description": self.description,
            "tags": self.tags,
            "properties": self.properties,
        }


Resource = Union[
    PrimaryData,
    Provider,
    Entity,
    User,
    FeatureVariant,
    LabelVariant,
    TrainingSetVariant,
    SourceVariant,
    Schedule,
    ProviderReference,
    SourceReference,
    EntityReference,
    Model,
    OnDemandFeatureVariant,
    StreamFeature,
    StreamLabel,
]


class ResourceRedefinedError(Exception):
    @typechecked
    def __init__(self, resource: Resource):
        variantStr = (
            f" variant {resource.variant}" if hasattr(resource, "variant") else ""
        )
        resourceId = f"{resource.name}{variantStr}"
        super().__init__(
            f"{resource.get_resource_type()} resource {resourceId} defined in multiple places"
        )


class ResourceState:
    def __init__(self):
        self.__state = {}

    def reset(self):
        self.__state = {}

    @typechecked
    def add(self, resource: Resource) -> None:
        if hasattr(resource, "variant"):
            key = (
                resource.operation_type().name,
                resource.get_resource_type(),
                resource.name,
                resource.variant,
            )
        else:
            key = (
                resource.operation_type().name,
                resource.get_resource_type().to_string(),
                resource.name,
            )
        if key in self.__state:
            if resource == self.__state[key]:
                print(
                    f"Resource {resource.get_resource_type().to_string()} already registered."
                )
                return
            raise ResourceRedefinedError(resource)
        self.__state[key] = resource
        if hasattr(resource, "schedule_obj") and resource.schedule_obj != None:
            my_schedule = resource.schedule_obj
            key = (my_schedule.get_resource_type(), my_schedule.name)
            self.__state[key] = my_schedule

    def is_empty(self) -> bool:
        return len(self.__state) == 0

    def sorted_list(self) -> List[Resource]:
        resource_order = {
            ResourceType.USER: 0,
            ResourceType.PROVIDER: 1,
            ResourceType.SOURCE_VARIANT: 2,
            ResourceType.ENTITY: 3,
            ResourceType.FEATURE_VARIANT: 4,
            ResourceType.ONDEMAND_FEATURE: 5,
            ResourceType.LABEL_VARIANT: 6,
            ResourceType.TRAININGSET_VARIANT: 7,
            ResourceType.SCHEDULE: 8,
            ResourceType.MODEL: 9,
        }

        def to_sort_key(res):
            resource_num = resource_order[res.get_resource_type()]
            return resource_num

        return sorted(self.__state.values(), key=to_sort_key)

    def create_all_dryrun(self) -> None:
        for resource in self.sorted_list():
            if resource.operation_type() is OperationType.GET:
                print(
                    "Getting", resource.get_resource_type().to_string(), resource.name
                )
            if resource.operation_type() is OperationType.CREATE:
                print(
                    "Creating", resource.get_resource_type().to_string(), resource.name
                )

    def run_all(self, stub, client_objs_for_resource: dict = None) -> None:
        if not feature_flag.is_enabled("FF_GET_EQUIVALENT_VARIANTS", True):
            print("Runs are not supported when env:FF_GET_EQUIVALENT_VARIANTS is false")
            return
        req_id = uuid.uuid4()
        resources = []
        for resource in self.sorted_list():
            if (
                resource.get_resource_type() == ResourceType.PROVIDER
                and resource.name == "local-mode"
            ):
                continue
            try:
                resource_variant = getattr(resource, "variant", "")
                rv_for_print = f" {resource_variant}" if resource_variant else ""
                if resource.operation_type() is OperationType.CREATE:
                    if isinstance(resource, ResourceVariant):
                        (
                            serialized,
                            equiv_variant,
                            var_type,
                        ) = resource._get_and_set_equivalent_variant(req_id, stub)
                        if equiv_variant is None:
                            print(
                                f"{resource.name}{rv_for_print} has not been applied. Aborting the run."
                            )
                            return
                        client_obj = client_objs_for_resource.get(
                            resource.to_key(), None
                        )
                        resource.variant = equiv_variant
                        if client_obj is not None:
                            client_obj.variant = equiv_variant
                        resources.append((resource, serialized, var_type))

            except grpc.RpcError as e:
                raise e
        proto_resources = []
        for resource, serialized, var_type in resources:
            resource_variant = getattr(resource, "variant", "")
            rv_for_print = f" {resource_variant}" if resource_variant else ""
            print(
                f"Running {resource.get_resource_type().to_string()} {resource.name}{rv_for_print}"
            )
            res_pb = pb.ResourceVariant(**{var_type: getattr(serialized, var_type)})
            proto_resources.append(res_pb)
        req = pb.RunRequest(
            request_id=str(req_id),
            variants=proto_resources,
        )
        stub.Run(req)

    def build_dashboard_url(self, host, resource_type, name, variant=""):
        scheme = "https"
        if "localhost" in host:
            scheme = "http"
            host = "localhost"

        resource_type_to_resource_url = {
            ResourceType.FEATURE_VARIANT: "features",
            ResourceType.SOURCE_VARIANT: "sources",
            ResourceType.LABEL_VARIANT: "labels",
            ResourceType.TRAININGSET_VARIANT: "training-sets",
            ResourceType.PROVIDER: "providers",
            ResourceType.ONDEMAND_FEATURE: "features",
            ResourceType.TRANSFORMATION: "sources",
            ResourceType.ENTITY: "entities",
            ResourceType.MODEL: "models",
            ResourceType.USER: "users",
        }
        resource_url = resource_type_to_resource_url[resource_type]
        path = f"{resource_url}/{name}"
        if variant:
            query = urlencode({"variant": variant})
            dashboard_url = urlunparse((scheme, host, path, "", query, ""))
        else:
            dashboard_url = urlunparse((scheme, host, path, "", "", ""))

        return dashboard_url

    def create_all(
        self, stub, asynchronous, host, client_objs_for_resource: dict = None
    ) -> None:
        check_up_to_date(False, "register")
        req_id = uuid.uuid4()
        for resource in self.sorted_list():
            if (
                resource.get_resource_type() == ResourceType.PROVIDER
                and resource.name == "local-mode"
            ):
                continue
            try:
                resource_variant = getattr(resource, "variant", "")
                rv_for_print = f"{resource_variant}" if resource_variant else ""
                if resource.operation_type() is OperationType.GET:
                    print(
                        f"Getting {resource.get_resource_type().to_string()} {resource.name} {rv_for_print}"
                    )
                    resource._get(stub)
                if resource.operation_type() is OperationType.CREATE:
                    if resource.name != "default_user":
                        print(
                            f"Creating {resource.get_resource_type().to_string()} {resource.name} {rv_for_print}"
                        )
                    created_variant, existing_variant = resource._create(req_id, stub)
                    if asynchronous:
                        variant_used = (
                            existing_variant if existing_variant else created_variant
                        )
                        url = self.build_dashboard_url(
                            host,
                            resource.get_resource_type(),
                            resource.name,
                            variant_used,
                        )
                        print(url)
                        print("")

                    if isinstance(resource, ResourceVariant):
                        # look up the client object with the original resource
                        client_obj = client_objs_for_resource.get(
                            resource.to_key(), None
                        )
                        resource.variant = created_variant
                        if client_obj is not None:
                            client_obj.variant = created_variant

            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                    print(f"{resource.name} {rv_for_print} already exists.")
                    continue

                raise e


## Executor Providers
@typechecked
@dataclass
class DatabricksCredentials:
    """

    Credentials for a Databricks cluster.

    **Example**
    ```
    databricks = ff.DatabricksCredentials(
        username="<my_username>",
        password="<my_password>",
        host="<databricks_hostname>",
        token="<databricks_token>",
        cluster_id="<databricks_cluster>",
    )

    spark = ff.register_spark(
        name="spark",
        executor=databricks,
        ...
    )
    ```

    Args:
        username (str): Username for a Databricks cluster.
        password (str): Password for a Databricks cluster.
        host (str): The hostname of a Databricks cluster.
        token (str): The token for a Databricks cluster.
        cluster_id (str): ID of an existing Databricks cluster.
    """

    username: str = ""
    password: str = ""
    host: str = ""
    token: str = ""
    cluster_id: str = ""

    def __post_init__(self):
        host_token_provided = (
            self.username == ""
            and self.password == ""
            and self.host != ""
            and self.token != ""
        )
        username_password_provided = (
            self.username != ""
            and self.password != ""
            and self.host == ""
            and self.token == ""
        )

        if (
            not host_token_provided
            and not username_password_provided
            or host_token_provided
            and username_password_provided
        ):
            raise Exception(
                "The DatabricksCredentials requires only one credentials set ('username' and 'password' or 'host' and 'token' set.)"
            )

        if not self.cluster_id:
            raise Exception("Cluster_id of existing cluster must be provided")

        if not self._validate_cluster_id():
            raise ValueError(
                f"Invalid cluster_id: expected id in the format 'xxxx-xxxxxx-xxxxxxxx' but received '{self.cluster_id}'"
            )

        if self.host and not self._validate_token():
            raise ValueError(
                f"Invalid token: expected token in the format 'dapi' + 32 alphanumeric characters (optionally ending with '-' and 1 alphanumeric character) but received '{self.token}'"
            )

    def _validate_cluster_id(self):
        cluster_id_regex = r"^\w{4}-\w{6}-\w{8}$"
        return re.match(cluster_id_regex, self.cluster_id)

    def _validate_token(self):
        token_regex = r"^dapi[a-zA-Z0-9]{32}(-[a-zA-Z0-9])?$"
        return re.match(token_regex, self.token)

    def type(self):
        return "DATABRICKS"

    def config(self):
        return {
            "Username": self.username,
            "Password": self.password,
            "Host": self.host,
            "Token": self.token,
            "Cluster": self.cluster_id,
        }


@typechecked
@dataclass
class EMRCredentials:
    def __init__(
        self,
        emr_cluster_id: str,
        emr_cluster_region: str,
        credentials: Union[AWSStaticCredentials, AWSAssumeRoleCredentials],
    ):
        """

        Credentials for an EMR cluster.

        **Example**
        ```
        emr = ff.EMRCredentials(
            emr_cluster_id="<cluster_id>",
            emr_cluster_region="<cluster_region>",
            credentials="<AWS_Credentials>",
        )

        spark = ff.register_spark(
            name="spark",
            executor=emr,
            ...
        )
        ```

        Args:
            emr_cluster_id (str): ID of an existing EMR cluster.
            emr_cluster_region (str): Region of an existing EMR cluster.
            credentials (Union[AWSStaticCredentials, AWSAssumeRoleCredentials]): Credentials for an AWS account with access to the cluster
        """
        self.emr_cluster_id = emr_cluster_id
        self.emr_cluster_region = emr_cluster_region
        self.credentials = credentials

    def type(self):
        return "EMR"

    def config(self):
        return {
            "ClusterName": self.emr_cluster_id,
            "ClusterRegion": self.emr_cluster_region,
            "Credentials": self.credentials.config(),
        }


@typechecked
@dataclass
class SparkCredentials:
    def __init__(
        self,
        master: str,
        deploy_mode: str,
        python_version: str,
        core_site_path: str = "",
        yarn_site_path: str = "",
    ):
        """

        Credentials for a Generic Spark Cluster

        **Example**
        ```
        spark_credentials = ff.SparkCredentials(
            master="yarn",
            deploy_mode="cluster",
            python_version="3.7.12",
            core_site_path="core-site.xml",
            yarn_site_path="yarn-site.xml"
        )

        spark = ff.register_spark(
            name="spark",
            executor=spark_credentials,
            ...
        )
        ```

        Args:
            master (str): The hostname of the Spark cluster. (The same that would be passed to `spark-submit`).
            deploy_mode (str): The deploy mode of the Spark cluster. (The same that would be passed to `spark-submit`).
            python_version (str): The Python version running on the cluster. Supports 3.7-3.11
            core_site_path (str): The path to the core-site.xml file. (For Yarn clusters only)
            yarn_site_path (str): The path to the yarn-site.xml file. (For Yarn clusters only)
        """
        self.master = master.lower()
        self.deploy_mode = deploy_mode.lower()
        self.core_site_path = core_site_path
        self.yarn_site_path = yarn_site_path

        if self.deploy_mode != "cluster" and self.deploy_mode != "client":
            raise Exception(
                f"Spark does not support '{self.deploy_mode}' deploy mode. It only supports 'cluster' and 'client'."
            )

        self.python_version = self._verify_python_version(
            self.deploy_mode, python_version
        )

        self._verify_yarn_config()

    def _verify_python_version(self, deploy_mode, version):
        if deploy_mode == "cluster" and version == "":
            client_python_version = sys.version_info
            client_major = str(client_python_version.major)
            client_minor = str(client_python_version.minor)

            if client_major != MAJOR_VERSION:
                client_major = "3"
            if client_minor not in MINOR_VERSIONS:
                client_minor = "7"

            version = f"{client_major}.{client_minor}"

        if version.count(".") == 2:
            major, minor, _ = version.split(".")
        elif version.count(".") == 1:
            major, minor = version.split(".")
        else:
            raise Exception(
                "Please specify your Python version on the Spark cluster. Accepted formats: Major.Minor or Major.Minor.Patch; ex. '3.7' or '3.7.16"
            )

        if major != MAJOR_VERSION or minor not in MINOR_VERSIONS:
            raise Exception(
                f"The Python version {version} is not supported. Currently, supported versions are 3.7-3.10."
            )

        """
        The Python versions on the Docker image are 3.7.16, 3.8.16, 3.9.16, 3.10.10, and 3.11.2.
        This conditional statement sets the patch number based on the minor version. 
        """
        if minor == "10":
            patch = "10"
        elif minor == "11":
            patch = "2"
        else:
            patch = "16"

        return f"{major}.{minor}.{patch}"

    def _verify_yarn_config(self):
        if self.master == "yarn" and (
            self.core_site_path == "" or self.yarn_site_path == ""
        ):
            raise Exception(
                "Yarn requires core-site.xml and yarn-site.xml files."
                "Please copy these files from your Spark instance to local, then provide the local path in "
                "core_site_path and yarn_site_path. "
            )

    def type(self):
        return "SPARK"

    def config(self):
        core_site = (
            "" if self.core_site_path == "" else open(self.core_site_path, "r").read()
        )
        yarn_site = (
            "" if self.yarn_site_path == "" else open(self.yarn_site_path, "r").read()
        )

        return {
            "Master": self.master,
            "DeployMode": self.deploy_mode,
            "PythonVersion": self.python_version,
            "CoreSite": core_site,
            "YarnSite": yarn_site,
        }


# Looks to see if there is an existing resource variant that matches on a resources key fields
# and sets the serialized to it.
#
# i.e. for a source variant, looks for a source variant with the same name and definition
def _get_and_set_equivalent_variant(
    req_id, resource_variant_proto, variant_field, stub
) -> Optional[str]:
    if feature_flag.is_enabled("FF_GET_EQUIVALENT_VARIANTS", True):
        res_pb = pb.ResourceVariant(
            **{variant_field: getattr(resource_variant_proto, variant_field)}
        )
        logger.info("Starting to get equivalent")
        # Get equivalent from stub
        equivalent = stub.GetEquivalent(
            pb.GetEquivalentRequest(
                request_id=resource_variant_proto.request_id,
                variant=res_pb,
            )
        )
        rv_proto = getattr(resource_variant_proto, variant_field)
        logger.info("Finished get equivalent")

        # grpc call returns the default ResourceVariant proto when equivalent doesn't exist which explains the below check
        if equivalent != pb.ResourceVariant():
            variant_value = getattr(getattr(equivalent, variant_field), "variant")
            print(
                f"Looks like an equivalent {variant_field.replace('_', ' ')} already exists, going to use its variant: ",
                variant_value,
            )
            # TODO add confirmation from user before using equivalent variant
            rv_proto.variant = variant_value
            return variant_value
    return None

@typechecked
@dataclass
class TrainingSetFeatures:
    training_set_name: str
    training_set_variant: str
    feature_name: str
    feature_variant: str

    def to_dictionary(self):
        return {
            "training_set_name": self.training_set_name,
            "training_set_variant": self.training_set_variant,
            "feature_name": self.feature_name,
            "feature_variant": self.feature_variant,
        }


ExecutorCredentials = Union[EMRCredentials, DatabricksCredentials, SparkCredentials]


def read_file(file_path: str) -> str:
    file = ""
    if file_path != "":
        try:
            with open(file_path, "r") as f:
                file = f.read()
        except Exception as e:
            raise Exception(f"Error reading file {file_path}: {e}")

    if file == "":
        raise Exception(f"File {file_path} is empty. File cannot be empty")

    return file
