# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import json
import os
import re
import sys
from abc import ABC
from typing import Any, Dict
from typing import List, Tuple, Union, Optional

import dill
import grpc
from dataclasses import field
from google.protobuf.duration_pb2 import Duration
from google.rpc import error_details_pb2

from . import feature_flag
from .types import VectorType, type_from_proto
from .enums import *
from .exceptions import *
from .version import check_up_to_date

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

    def type(self) -> str:
        return "schedule"

    def _create(self, stub) -> None:
        serialized = pb.SetScheduleChangeRequest(
            resource=pb.ResourceId(
                pb.NameVariant(name=self.name, variant=self.variant),
                resource_type=self.resource_type,
            ),
            schedule=self.schedule_string,
        )
        stub.RequestScheduleChange(serialized)


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
class AWSCredentials:
    def __init__(
        self,
        access_key: str,
        secret_key: str,
    ):
        """

        Credentials for an AWS.

        **Example**
        ```
        aws_credentials = ff.AWSCredentials(
            access_key="<AWS_ACCESS_KEY>",
            secret_key="<AWS_SECRET_KEY>"
        )
        ```

        Args:
            access_key (str): AWS Access Key.
            secret_key (str): AWS Secret Key.
        """
        if access_key == "":
            raise Exception("'AWSCredentials' access_key cannot be empty")

        if secret_key == "":
            raise Exception("'AWSCredentials' secret_key cannot be empty")

        self.access_key = access_key
        self.secret_key = secret_key

    def type(self):
        return "AWS_CREDENTIALS"

    def config(self):
        return {
            "AWSAccessKeyId": self.access_key,
            "AWSSecretKey": self.secret_key,
        }


@typechecked
@dataclass
class GCPCredentials:
    def __init__(
        self,
        project_id: str,
        credentials_path: str,
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

        if credentials_path == "":
            raise Exception("'GCPCredentials' credentials_path cannot be empty")

        if not os.path.isfile(credentials_path):
            raise Exception(
                f"'GCPCredentials' credentials_path '{credentials_path}' file not found"
            )

        self.project_id = project_id
        with open(credentials_path) as f:
            self.credentials = json.load(f)

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
    def __init__(
        self,
        bucket_path: str,
        bucket_region: str,
        credentials: AWSCredentials,
        path: str = "",
    ):
        bucket_path_ends_with_slash = len(bucket_path) != 0 and bucket_path[-1] == "/"

        if bucket_path_ends_with_slash:
            raise Exception("The 'bucket_path' cannot end with '/'.")

        self.bucket_path = bucket_path
        self.bucket_region = bucket_region
        self.credentials = credentials
        self.path = path

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
    def __init__(self, host: str, port: str, path: str, username: str):
        bucket_path_ends_with_slash = len(path) != 0 and path[-1] == "/"

        if bucket_path_ends_with_slash:
            raise Exception("The 'bucket_path' cannot end with '/'.")

        self.path = path
        self.host = host
        self.port = port
        self.username = username

    def software(self) -> str:
        return "HDFS"

    def type(self) -> str:
        return "HDFS"

    def serialize(self) -> bytes:
        config = {
            "Host": self.host,
            "Port": self.port,
            "Path": self.path,
            "Username": self.username,
        }
        return bytes(json.dumps(config), "utf-8")

    def config(self):
        return {
            "Host": self.host,
            "Port": self.port,
            "Path": self.path,
            "Username": self.username,
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


@typechecked
@dataclass
class DynamodbConfig:
    region: str
    access_key: str
    secret_key: str
    should_import_from_s3: bool

    def software(self) -> str:
        return "dynamodb"

    def type(self) -> str:
        return "DYNAMODB_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "Region": self.region,
            "AccessKey": self.access_key,
            "SecretKey": self.secret_key,
            "ImportFromS3": self.should_import_from_s3,
        }
        return bytes(json.dumps(config), "utf-8")


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

    def __post_init__(self):
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
        }
        return bytes(json.dumps(config), "utf-8")


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


@typechecked
@dataclass
class SparkConfig:
    executor_type: str
    executor_config: dict
    store_type: str
    store_config: dict

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
        return bytes(json.dumps(config), "utf-8")


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
    team: str
    config: Config
    function: str
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
    def type() -> str:
        return "provider"

    def get(self, stub) -> "Provider":
        name = pb.Name(name=self.name)
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

    def _create(self, stub) -> None:
        serialized = pb.Provider(
            name=self.name,
            description=self.description,
            type=self.config.type(),
            software=self.config.software(),
            team=self.team,
            serialized_config=self.config.serialize(),
            tags=pb.Tags(tag=self.tags),
            properties=Properties(self.properties).serialized,
        )
        stub.CreateProvider(serialized)

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

    def type(self) -> str:
        return "user"

    def _create(self, stub) -> None:
        serialized = pb.User(
            name=self.name,
            tags=pb.Tags(tag=self.tags),
            properties=Properties(self.properties).serialized,
        )
        stub.CreateUser(serialized)

    def to_dictionary(self):
        return {
            "name": self.name,
            "status": self.status,
            "tags": self.tags,
            "properties": self.properties,
        }


@typechecked
@dataclass
class SQLTable:
    name: str


@typechecked
@dataclass
class Directory:
    path: str


Location = Union[SQLTable, Directory]


class ResourceVariant(ABC):
    name: str
    variant: str
    server_status: ServerStatus

    @staticmethod
    def type():
        raise NotImplementedError

    def name_variant(self):
        return self.name, self.variant

    def to_key(self) -> Tuple[str, str, str]:
        return self.type(), self.name, self.variant


@typechecked
@dataclass
class PrimaryData:
    location: Location

    def kwargs(self):
        return {
            "primaryData": pb.PrimaryData(
                table=pb.PrimarySQLTable(
                    name=self.location.name,
                ),
            ),
        }

    def name(self):
        return self.location.name

    def path(self):
        return self.location.path


class Transformation:
    pass


@typechecked
@dataclass
class SQLTransformation(Transformation):
    query: str
    args: Optional[K8sArgs] = None
    func_params_to_inputs: Dict[str, Any] = field(default_factory=dict)

    _sql_placeholder_regex: str = field(
        default=r"\{\{\s*\w+\s*\}\}", init=False, repr=False
    )

    def __post_init__(self):
        self._validate_inputs_to_func_params(self.func_params_to_inputs)

    def type(self) -> str:
        """Return the type of the SQL transformation."""
        return SourceType.SQL_TRANSFORMATION.value

    def kwargs(self) -> Dict[str, pb.Transformation]:
        """Construct kwargs for the SQL transformation."""
        input_to_name_variant = self._resolve_input_variants()

        # Find and replace placeholders in the query with source name variants
        final_query = self.query
        for placeholder in self._get_placeholders():
            clean_placeholder = placeholder.strip(" {}")
            name_variant = input_to_name_variant[clean_placeholder]
            replacement = "{{ " + f"{name_variant[0]}.{name_variant[1]}" + " }}"
            final_query = final_query.replace(placeholder, replacement)

        transformation = pb.Transformation(
            SQLTransformation=pb.SQLTransformation(query=final_query)
        )

        if self.args is not None:
            transformation = self.args.apply(transformation)

        return {"transformation": transformation}

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


@typechecked
@dataclass
class DFTransformation(Transformation):
    query: bytes
    inputs: list
    args: K8sArgs = None
    source_text: str = ""

    def type(self):
        return SourceType.DF_TRANSFORMATION.value

    def kwargs(self):
        for i, inp in enumerate(self.inputs):
            if hasattr(inp, "name_variant"):  # TODO shouldn't have to have this check
                self.inputs[i] = inp.name_variant()

        name_variants = []
        for inp in self.inputs:
            name_variants.append(pb.NameVariant(name=inp[0], variant=inp[1]))

        transformation = pb.Transformation(
            DFTransformation=pb.DFTransformation(
                query=self.query,
                inputs=name_variants,
                source_text=self.source_text,
            )
        )

        if self.args is not None:
            transformation = self.args.apply(transformation)

        return {"transformation": transformation}


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
    def type() -> str:
        return "source"

    def get(self, stub):
        name_variant = pb.NameVariant(name=self.name, variant=self.variant)
        source = next(stub.GetSourceVariants(iter([name_variant])))
        definition = self._get_source_definition(source)

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
        )

    def _get_source_definition(self, source):
        if source.primaryData.table.name:
            return PrimaryData(SQLTable(source.primaryData.table.name))
        elif source.transformation:
            return self._get_transformation_definition(source)
        else:
            raise Exception(f"Invalid source type {source}")

    def _get_transformation_definition(self, source):
        if source.transformation.DFTransformation.query != bytes():
            transformation = source.transformation.DFTransformation
            return DFTransformation(
                query=transformation.query,
                inputs=[(input.name, input.variant) for input in transformation.inputs],
                source_text=transformation.source_text,
            )
        elif source.transformation.SQLTransformation.query != "":
            return SQLTransformation(source.transformation.SQLTransformation.query)
        else:
            raise Exception(f"Invalid transformation type {source}")

    def _create(self, stub) -> Optional[str]:
        defArgs = self.definition.kwargs()

        serialized = pb.SourceVariant(
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
            **defArgs,
        )
        existing_variant = _get_and_set_equivalent_variant(
            serialized, "source_variant", stub
        )
        if not existing_variant:
            stub.CreateSourceVariant(serialized)
        return serialized.variant

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
    def type() -> str:
        return "entity"

    def _create(self, stub) -> None:
        serialized = pb.Entity(
            name=self.name,
            description=self.description,
            tags=pb.Tags(tag=self.tags),
            properties=Properties(self.properties).serialized,
        )
        stub.CreateEntity(serialized)

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
    tags: list = None
    properties: dict = None
    schedule: str = ""
    schedule_obj: Schedule = None
    status: str = "NO_STATUS"
    error: Optional[str] = None
    additional_parameters: Optional[Additional_Parameters] = None
    server_status: Optional[ServerStatus] = None

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
    def type() -> str:
        return "feature"

    def get(self, stub) -> "FeatureVariant":
        name_variant = pb.NameVariant(name=self.name, variant=self.variant)
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

    def _create(self, stub) -> Optional[str]:
        if hasattr(self.source, "name_variant"):
            self.source = self.source.name_variant()

        serialized = pb.FeatureVariant(
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
        )
        existing_variant = _get_and_set_equivalent_variant(
            serialized, "feature_variant", stub
        )
        if not existing_variant:
            stub.CreateFeatureVariant(serialized)
        return serialized.variant

    def get_status(self):
        return ResourceStatus(self.status)

    def is_ready(self):
        return self.status == ResourceStatus.READY.value


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
    def type() -> str:
        return "ondemand_feature"

    def _create(self, stub) -> Optional[str]:
        serialized = pb.FeatureVariant(
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
        )
        existing_variant = _get_and_set_equivalent_variant(
            serialized, "feature_variant", stub
        )
        if not existing_variant:
            stub.CreateFeatureVariant(serialized)
        return serialized.variant

    def get(self, stub) -> "OnDemandFeatureVariant":
        name_variant = pb.NameVariant(name=self.name, variant=self.variant)
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
    tags: list
    properties: dict
    location: ResourceLocation
    variant: str
    provider: Optional[str] = None
    created: str = None
    status: str = "NO_STATUS"
    error: Optional[str] = None
    server_status: Optional[ServerStatus] = None

    def __post_init__(self):
        if isinstance(self.value_type, str):
            self.value_type = ScalarType(self.value_type)

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def type() -> str:
        return "label"

    def get(self, stub) -> "LabelVariant":
        name_variant = pb.NameVariant(name=self.name, variant=self.variant)
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

    def _create(self, stub) -> Optional[str]:
        if hasattr(self.source, "name_variant"):
            self.source = self.source.name_variant()
        serialized = pb.LabelVariant(
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
            columns=self.location.proto(),
            tags=pb.Tags(tag=self.tags),
            properties=Properties(self.properties).serialized,
            status=pb.ResourceStatus(status=pb.ResourceStatus.NO_STATUS),
        )
        existing_variant = _get_and_set_equivalent_variant(
            serialized, "label_variant", stub
        )
        if not existing_variant:
            stub.CreateLabelVariant(serialized)
        return serialized.variant

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
    def type() -> str:
        return "entity"

    def _get(self, stub):
        entityList = stub.GetEntities(iter([pb.Name(name=self.name)]))
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
    def type() -> str:
        return "provider"

    def _get(self, stub):
        providerList = stub.GetProviders(iter([pb.Name(name=self.name)]))
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
    def type() -> str:
        return "source"

    def _get(self, stub):
        sourceList = stub.GetSourceVariants(
            iter([pb.NameVariant(name=self.name, variant=self.variant)])
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
            raise ValueError("A training-set must have atleast one feature")
        for feature in self.features:
            if not isinstance(
                feature, FeatureColumnResource
            ) and not valid_name_variant(feature):
                raise ValueError("Invalid Feature")

    @staticmethod
    def operation_type() -> OperationType:
        return OperationType.CREATE

    @staticmethod
    def type() -> str:
        return "training-set"

    def get(self, stub):
        name_variant = pb.NameVariant(name=self.name, variant=self.variant)
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

    def _create(self, stub) -> Optional[str]:
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

        serialized = pb.TrainingSetVariant(
            created=None,
            name=self.name,
            variant=self.variant,
            description=self.description,
            schedule=self.schedule,
            owner=self.owner,
            features=[pb.NameVariant(name=v[0], variant=v[1]) for v in self.features],
            label=pb.NameVariant(name=self.label[0], variant=self.label[1]),
            feature_lags=feature_lags,
            tags=pb.Tags(tag=self.tags),
            properties=Properties(self.properties).serialized,
            status=pb.ResourceStatus(status=pb.ResourceStatus.NO_STATUS),
        )
        existing_variant = _get_and_set_equivalent_variant(
            serialized, "training_set_variant", stub
        )
        if not existing_variant:
            stub.CreateTrainingSetVariant(serialized)
        return serialized.variant

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

    def type(self) -> str:
        return "model"

    def _create(self, stub) -> None:
        properties = pb.Properties(property=self.properties)
        serialized = pb.Model(
            name=self.name,
            tags=pb.Tags(tag=self.tags),
            properties=Properties(self.properties).serialized,
        )
        stub.CreateModel(serialized)

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
]


class ResourceRedefinedError(Exception):
    @typechecked
    def __init__(self, resource: Resource):
        variantStr = (
            f" variant {resource.variant}" if hasattr(resource, "variant") else ""
        )
        resourceId = f"{resource.name}{variantStr}"
        super().__init__(
            f"{resource.type()} resource {resourceId} defined in multiple places"
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
                resource.type(),
                resource.name,
                resource.variant,
            )
        else:
            key = (resource.operation_type().name, resource.type(), resource.name)
        if key in self.__state:
            if resource == self.__state[key]:
                print(f"Resource {resource.type()} already registered.")
                return
            raise ResourceRedefinedError(resource)
        self.__state[key] = resource
        if hasattr(resource, "schedule_obj") and resource.schedule_obj != None:
            my_schedule = resource.schedule_obj
            key = (my_schedule.type(), my_schedule.name)
            self.__state[key] = my_schedule

    def is_empty(self) -> bool:
        return len(self.__state) == 0

    def sorted_list(self) -> List[Resource]:
        resource_order = {
            "user": 0,
            "provider": 1,
            "source": 2,
            "entity": 3,
            "feature": 4,
            "ondemand_feature": 5,
            "label": 6,
            "training-set": 7,
            "schedule": 8,
            "model": 9,
        }

        def to_sort_key(res):
            resource_num = resource_order[res.type()]
            return resource_num

        return sorted(self.__state.values(), key=to_sort_key)

    def create_all_dryrun(self) -> None:
        for resource in self.sorted_list():
            if resource.operation_type() is OperationType.GET:
                print("Getting", resource.type(), resource.name)
            if resource.operation_type() is OperationType.CREATE:
                print("Creating", resource.type(), resource.name)

    def create_all(self, stub, client_objs_for_resource: dict = None) -> None:
        check_up_to_date(False, "register")
        for resource in self.sorted_list():
            if resource.type() == "provider" and resource.name == "local-mode":
                continue
            try:
                resource_variant = getattr(resource, "variant", "")
                rv_for_print = f" {resource_variant}" if resource_variant else ""
                if resource.operation_type() is OperationType.GET:
                    print(f"Getting {resource.type()} {resource.name}{rv_for_print}")
                    resource._get(stub)
                if resource.operation_type() is OperationType.CREATE:
                    if resource.name != "default_user":
                        print(
                            f"Creating {resource.type()} {resource.name}{rv_for_print}"
                        )

                    created_variant = resource._create(stub)

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
                    print(f"{resource.name}{rv_for_print} already exists.")
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
        self, emr_cluster_id: str, emr_cluster_region: str, credentials: AWSCredentials
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
            credentials (AWSCredentials): Credentials for an AWS account with access to the cluster
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
    resource_variant_proto, variant_field, stub
) -> Optional[str]:
    if feature_flag.is_enabled("FF_GET_EQUIVALENT_VARIANTS", True):
        # Get equivalent from stub
        equivalent = stub.GetEquivalent(
            pb.ResourceVariant(**{variant_field: resource_variant_proto})
        )

        # grpc call returns the default ResourceVariant proto when equivalent doesn't exist which explains the below check
        if equivalent != pb.ResourceVariant():
            variant_value = getattr(getattr(equivalent, variant_field), "variant")
            print(
                f"Looks like an equivalent {variant_field.replace('_', ' ')} already exists, going to use its variant: ",
                variant_value,
            )
            # TODO add confirmation from user before using equivalent variant
            resource_variant_proto.variant = variant_value
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
