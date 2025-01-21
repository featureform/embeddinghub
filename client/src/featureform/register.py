#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import ast
import inspect
import os
import warnings
from abc import ABC
from collections.abc import Iterable
from datetime import timedelta
from typing import Callable, Dict, List, Optional, Tuple, Union

import dill

import pandas as pd
from dataclasses import dataclass, field
from typeguard import typechecked

from . import feature_flag
from .exceptions import InvalidSQLQuery
from .get import *
from .grpc_client import GrpcClient
from .list import *
from .logging import setup_logging
from .parse import *
from .proto import metadata_pb2_grpc as ff_grpc
from .resources import *
from .search import search
from .status_display import display_statuses
from .tls import insecure_channel, secure_channel
from .types import pd_to_ff_datatype, VectorType
from .variant_names_generator import get_current_timestamp_variant
from .variant_names_generator import get_random_name
from .enums import ResourceType

NameVariant = Tuple[str, str]

s3_config = S3StoreConfig("", "", AWSStaticCredentials("id", "secret"))
NON_INFERENCE_STORES = [s3_config.type()]
ONE_DAY_TARGET_LAG = "1 days"

DEFAULT_OWNER = "default_owner"


def set_tags_properties(tags: Optional[List[str]], properties: Optional[dict]):
    """
    Helper function to set default values for tags and properties
    """
    if tags is None:
        tags = []
    if properties is None:
        properties = {}
    return tags, properties


class EntityRegistrar:
    def __init__(self, registrar, entity):
        self.__registrar = registrar
        self.__entity = entity

    def name(self) -> str:
        return self.__entity.name


class UserRegistrar:
    def __init__(self, registrar, user):
        self.__registrar = registrar
        self.__user = user

    def name(self) -> str:
        return self.__user.name

    def make_default_owner(self):
        self.__registrar.set_default_owner(self.name())


class OfflineProvider:
    def __init__(self, registrar, provider):
        self.__registrar = registrar
        self.__provider = provider

    def name(self) -> str:
        return self.__provider.name

    def __eq__(self, __value: object) -> bool:
        assert isinstance(__value, OfflineProvider)
        return self.__provider == __value.__provider


class OfflineSQLProvider(OfflineProvider):
    def __init__(self, registrar, provider):
        super().__init__(registrar, provider)
        self.__registrar = registrar
        self.__provider = provider

    def register_table(
        self,
        name: str,
        table: str,
        variant: str = "",
        timestamp_column: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        schema: str = "",
        database: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a SQL table as a primary data source.

        **Example**

        ```
        postgres = client.get_provider("my_postgres")
        table =  postgres.register_table(
            name="transactions",
            variant="july_2023",
            table="transactions_table",
        ):
        ```

        Args:
            name (str): Name of table to be registered
            variant (str): Name of variant to be registered
            table (str): Name of SQL table
            owner (Union[str, UserRegistrar]): Owner
            timestamp_column (str): Optional parameter that can be used for incremental reads
            description (str): Description of table to be registered

        Returns:
            source (ColumnSourceRegistrar): source
        """

        location = SQLTable(schema=schema, database=database, name=table)

        return self.__registrar.register_primary_data(
            name=name,
            variant=variant,
            location=location,
            owner=owner,
            provider=self.name(),
            description=description,
            tags=tags,
            properties=properties,
            timestamp_column=timestamp_column,
        )

    def config(self):
        return self.__provider

    def sql_transformation(
        self,
        owner: Union[str, UserRegistrar] = "",
        variant: str = "",
        name: str = "",
        schedule: str = "",
        description: str = "",
        inputs: list = None,
        tags: List[str] = None,
        properties: dict = None,
        resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None,
    ):
        """
        Register a SQL transformation source.

        The name of the function is the name of the resulting source.

        Sources for the transformation can be specified by adding the Name and Variant in brackets '{{ name.variant }}'.
        The correct source is substituted when the query is run.

        **Examples**:

        ``` py
        postgres = client.get_provider("my_postgres")
        @postgres.sql_transformation(variant="quickstart")
        def average_user_transaction():
            return "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{transactions.v1}} GROUP BY user_id"
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            schedule (str): The frequency at which the transformation is run as a cron expression
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            inputs (list): A list of Source NameVariant Tuples to input into the transformation


        Returns:
            source (ColumnSourceRegistrar): Source
        """
        tags, properties = set_tags_properties(tags, properties)
        if (
            resource_snowflake_config is not None
            and self.__provider.config.type() != "SNOWFLAKE_OFFLINE"
        ):
            raise ValueError(
                "Dynamic tables are only supported for Snowflake offline providers"
            )
        return self.__registrar.sql_transformation(
            name=name,
            variant=variant,
            owner=owner,
            schedule=schedule,
            provider=self.name(),
            description=description,
            inputs=inputs,
            tags=tags,
            properties=properties,
            resource_snowflake_config=resource_snowflake_config,
        )

    def register_training_set(
        self,
        name: str,
        variant: str = "",
        features: Optional[List] = None,
        label: Optional[NameVariant] = None,
        resources: Optional[List] = None,
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        schedule: str = "",
        tags: Optional[List[str]] = None,
        properties: Optional[Dict] = None,
    ):
        return self.__registrar.register_training_set(
            name=name,
            variant=variant,
            features=features if features is not None else [],
            label=label if label is not None else ("", ""),
            resources=resources if resources is not None else [],
            owner=owner,
            description=description,
            schedule=schedule,
            tags=tags if tags is not None else [],
            properties=properties if properties is not None else {},
            provider=self.name(),
        )

    def __eq__(self, __value: object) -> bool:
        assert isinstance(__value, OfflineSQLProvider)
        return (
            self.__provider == __value.__provider
            and self.__registrar == __value.__registrar
        )


class OfflineSparkProvider(OfflineProvider):
    def __init__(self, registrar, provider):
        super().__init__(registrar, provider)
        self.__registrar = registrar
        self.__provider = provider

    def register_iceberg_table(
        self,
        name: str,
        database: str,
        table: str,
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        tags: Optional[List[str]] = None,
        properties: Optional[dict] = None,
    ):
        """
        Register an Iceberg table as a primary data source.

        **Examples**

        ```
        spark = client.get_provider("my_spark")
        transactions = spark.register_iceberg_table(
            name="transactions",
            database="fraud",
            table="transactions",
            description="A dataset of fraudulent transactions"
        )
        ```

        Args:
            name (str): Name of table to be registered
            database (str): Name of database
            table (str): Name of table
            variant (str): Name of variant to be registered
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of table to be registered
            tags (List[str]): Tags
            properties (dict): Properties
        Returns:
            source (ColumnSourceRegistrar): source
        """
        tags, properties = set_tags_properties(tags, properties)

        return self.__registrar.register_primary_data(
            name=name,
            variant=variant,
            location=GlueCatalogTable(
                database, table, table_format=TableFormat.ICEBERG
            ),
            owner=owner,
            provider=self.name(),
            description=description,
            tags=tags,
            properties=properties,
        )

    def register_delta_table(
        self,
        name: str,
        database: str,
        table: str,
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        tags: Optional[List[str]] = None,
        properties: Optional[dict] = None,
    ):
        """
        Register a Delta Lake table as a primary data source.

        **Examples**

        ```
        spark = client.get_provider("my_spark")
        transactions = spark.register_delta_table(
            name="transactions",
            database="fraud",
            table="transactions",
            description="A dataset of fraudulent transactions"
        )
        ```

        Args:
            name (str): Name of table to be registered
            database (str): Name of database
            table (str): Name of table
            variant (str): Name of variant to be registered
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of table to be registered
            tags (List[str]): Tags
            properties (dict): Properties
        Returns:
            source (ColumnSourceRegistrar): source
        """
        tags, properties = set_tags_properties(tags, properties)

        return self.__registrar.register_primary_data(
            name=name,
            variant=variant,
            location=GlueCatalogTable(database, table, table_format=TableFormat.DELTA),
            owner=owner,
            provider=self.name(),
            description=description,
            tags=tags,
            properties=properties,
        )

    def register_file(
        self,
        name: str,
        file_path: str,
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a Spark data source as a primary data source.

        **Examples**

        ```
        spark = client.get_provider("my_spark")
        transactions = spark.register_file(
            name="transactions",
            variant="quickstart",
            description="A dataset of fraudulent transactions",
            file_path="s3://featureform-spark/featureform/transactions.parquet"
        )
        ```

        Args:
            name (str): Name of table to be registered
            variant (str): Name of variant to be registered
            file_path (str): The URI of the file. Must be the full path
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of table to be registered

        Returns:
            source (ColumnSourceRegistrar): source
        """
        FilePrefix.validate(self.__provider.config.store_type, file_path)

        return self.__registrar.register_primary_data(
            name=name,
            variant=variant,
            location=FileStore(file_path),
            owner=owner,
            provider=self.name(),
            description=description,
            tags=tags,
            properties=properties,
        )

    def register_parquet_file(
        self,
        name: str,
        file_path: str,
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
    ):
        if self.__provider.config.executor_type != "EMR" and file_path.startswith(
            FilePrefix.S3.value
        ):
            file_path = file_path.replace(FilePrefix.S3.value, FilePrefix.S3A.value)
        return self.register_file(name, file_path, variant, owner, description)

    def sql_transformation(
        self,
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        name: str = "",
        schedule: str = "",
        inputs: list = None,
        description: str = "",
        tags: List[str] = [],
        properties: dict = {},
        max_job_duration: timedelta = timedelta(hours=48),
        spark_params: Optional[Dict[str, str]] = None,
        write_options: Optional[Dict[str, str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
    ):
        """
        Register a SQL transformation source. The spark.sql_transformation decorator takes the returned string in the
        following function and executes it as a SQL Query.

        The name of the function is the name of the resulting source.

        Sources for the transformation can be specified by adding the Name and Variant in brackets '{{ name.variant }}'.
        The correct source is substituted when the query is run.

        **Examples**:
        ``` py
        @spark.sql_transformation(variant="quickstart")
        def average_user_transaction():
            return "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{transactions.v1}} GROUP BY user_id"
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            inputs (list[Tuple(str, str)]): A list of Source NameVariant Tuples to input into the transformation
            max_job_duration (timedelta): Maximum duration FeatureForm will wait for the job to complete; default is 48 hours; jobs that exceed this duration will be canceled


        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.sql_transformation(
            name=name,
            variant=variant,
            owner=owner,
            schedule=schedule,
            provider=self.name(),
            description=description,
            inputs=inputs,
            tags=tags,
            properties=properties,
            max_job_duration=max_job_duration,
            spark_params=spark_params,
            write_options=write_options,
            table_properties=table_properties,
        )

    def df_transformation(
        self,
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        name: str = "",
        description: str = "",
        inputs: list = [],
        tags: List[str] = [],
        properties: dict = {},
        max_job_duration: timedelta = timedelta(hours=48),
        spark_params: Optional[Dict[str, str]] = None,
        write_options: Optional[Dict[str, str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
    ):
        """
        Register a Dataframe transformation source. The spark.df_transformation decorator takes the contents
        of the following function and executes the code it contains at serving time.

        The name of the function is used as the name of the source when being registered.

        The specified inputs are loaded into dataframes that can be accessed using the function parameters.

        **Examples**:
        ``` py
        @spark.df_transformation(inputs=[("source", "one")])        # Sources are added as inputs
        def average_user_transaction(df):                           # Sources can be manipulated by adding them as params
            return df
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            inputs (list[Tuple(str, str)]): A list of Source NameVariant Tuples to input into the transformation
            max_job_duration (timedelta): Maximum duration FeatureForm will wait for the job to complete; default is 48 hours; jobs that exceed this duration will be canceled

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.df_transformation(
            name=name,
            variant=variant,
            owner=owner,
            provider=self.name(),
            description=description,
            inputs=inputs,
            tags=tags,
            properties=properties,
            max_job_duration=max_job_duration,
            spark_params=spark_params or {},
            write_options=write_options or {},
            table_properties=table_properties or {},
        )

    def register_training_set(
        self,
        name: str,
        variant: str = "",
        features: list = [],
        label: NameVariant = ("", ""),
        resources: list = [],
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        schedule: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a training set on the Spark provider.

        **Example**:
        ```
        spark.register_training_set(
            name="my_training_set",
            label=("label", "v1"),
            features=[("feature1", "v1"), ("feature2", "v1")],
        )
        ```

        Args:
            name (str): Name of training set to be registered
            variant (str): Name of variant to be registered
            label (NameVariant): Label of training set
            features (List[NameVariant]): Features of training set
            resources (List[Resource]): A list of previously registered resources
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of training set to be registered
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")
            tags (List[str]): Optional grouping mechanism for resources
            properties (dict): Optional grouping mechanism for resources

        Returns:
            resource (ResourceRegistrar): resource
        """
        return self.__registrar.register_training_set(
            name=name,
            variant=variant,
            features=features,
            label=label,
            resources=resources,
            owner=owner,
            description=description,
            schedule=schedule,
            tags=tags,
            properties=properties,
            provider=self.name(),
        )

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, OfflineSparkProvider):
            return False
        return (
            self.__provider == __value.__provider
            and self.__registrar == __value.__registrar
        )


class OfflineK8sProvider(OfflineProvider):
    def __init__(self, registrar, provider):
        super().__init__(registrar, provider)
        self.__registrar = registrar
        self.__provider = provider

    def register_file(
        self,
        name: str,
        path: str,
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a Kubernetes Runner data source as a primary data source.

        **Examples**

        ```
        k8s = client.get_provider("my_k8s")
        transactions = k8s.register_file(
            name="transactions",
            variant="quickstart",
            description="A dataset of fraudulent transactions",
            file_path="s3://featureform-spark/featureform/transactions.parquet"
        )
        ```

        Args:
            name (str): Name of table to be registered
            variant (str): Name of variant to be registered
            path (str): The path to blob store file
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of table to be registered

        Returns:
            source (ColumnSourceRegistrar): source
        """
        FilePrefix.validate(self.__provider.config.store_type, path)

        return self.__registrar.register_primary_data(
            name=name,
            variant=variant,
            location=SQLTable(path),
            owner=owner,
            provider=self.name(),
            description=description,
            tags=tags,
            properties=properties,
        )

    def sql_transformation(
        self,
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        name: str = "",
        schedule: str = "",
        inputs: list = None,
        description: str = "",
        docker_image: str = "",
        resource_specs: Union[K8sResourceSpecs, None] = None,
        tags: List[str] = [],
        properties: dict = {},
    ):
        """
        Register a SQL transformation source. The k8s.sql_transformation decorator takes the returned string in the
        following function and executes it as a SQL Query.

        The name of the function is the name of the resulting source.

        Sources for the transformation can be specified by adding the Name and Variant in brackets '{{ name.variant }}'.
        The correct source is substituted when the query is run.

        **Examples**:
        ``` py
        @k8s.sql_transformation(variant="quickstart")
        def average_user_transaction():
            return "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{transactions.v1}} GROUP BY user_id"
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            inputs (list): A list of Source NameVariant Tuples to input into the transformation
            description (str): Description of primary data to be registered
            docker_image (str): A custom Docker image to run the transformation
            resource_specs (K8sResourceSpecs): Custom resource requests and limits


        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.sql_transformation(
            name=name,
            variant=variant,
            owner=owner,
            schedule=schedule,
            provider=self.name(),
            description=description,
            inputs=inputs,
            args=K8sArgs(docker_image=docker_image, specs=resource_specs),
            tags=tags,
            properties=properties,
        )

    def df_transformation(
        self,
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        name: str = "",
        description: str = "",
        inputs: list = [],
        docker_image: str = "",
        resource_specs: Union[K8sResourceSpecs, None] = None,
        tags: List[str] = [],
        properties: dict = {},
    ):
        """
        Register a Dataframe transformation source. The k8s.df_transformation decorator takes the contents
        of the following function and executes the code it contains at serving time.

        The name of the function is used as the name of the source when being registered.

        The specified inputs are loaded into dataframes that can be accessed using the function parameters.

        **Examples**:
        ``` py
        @k8s.df_transformation(inputs=[("source", "one")])        # Sources are added as inputs
        def average_user_transaction(df):                         # Sources can be manipulated by adding them as params
            return df
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            inputs (list[Tuple(str, str)]): A list of Source NameVariant Tuples to input into the transformation
            docker_image (str): A custom Docker image to run the transformation
            resource_specs (K8sResourceSpecs): Custom resource requests and limits

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.df_transformation(
            name=name,
            variant=variant,
            owner=owner,
            provider=self.name(),
            description=description,
            inputs=inputs,
            args=K8sArgs(docker_image=docker_image, specs=resource_specs),
            tags=tags,
            properties=properties,
        )


class OnlineProvider:
    def __init__(self, registrar, provider):
        self.__registrar = registrar
        self.__provider = provider

    def name(self) -> str:
        return self.__provider.name

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, OnlineProvider):
            return False
        return (
            self.__provider == __value.__provider
            and self.__registrar == __value.__registrar
        )


class FileStoreProvider:
    def __init__(self, registrar, provider, config, store_type):
        self.__registrar = registrar
        self.__provider = provider
        self.__config = config.config()
        self.__store_type = store_type

    def name(self) -> str:
        return self.__provider.name

    def store_type(self) -> str:
        return self.__store_type

    def config(self):
        return self.__config

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, FileStoreProvider):
            return False
        return (
            self.__provider == __value.__provider
            and self.__registrar == __value.__registrar
            and self.__config == __value.__config
            and self.__store_type == __value.__store_type
        )


class SourceRegistrar:
    def __init__(self, registrar, source):
        self.__registrar = registrar
        self.__source = source

    def id(self) -> NameVariant:
        return self.__source.name, self.__source.variant

    def name_variant(self) -> NameVariant:
        return self.id()

    def registrar(self):
        return self.__registrar

    def source(self):
        return self.__source

    def __eq__(self, other):
        return self.__source == other.__source and self.__registrar == other.__registrar

    def get_resource_type(self) -> ResourceType:
        return self.__source.get_resource_type()


class ColumnMapping(dict):
    name: str
    column: str
    resource_type: ResourceType
    tags: List[str]
    properties: dict
    variant: str = ""
    resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None


class SubscriptableTransformation:
    """
    SubscriptableTransformation creates a wrapped decorator that's callable and subscriptable,
    which allows for the following syntax:

    ``` py
    @local.transformation(variant="quickstart")
    def average_user_transaction():
        return "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{transactions.v1}} GROUP BY user_id"

    feature = ff.Feature(average_user_transaction[["user_id", "avg_transaction_amt"]])
    ```

    Given the function type does not implement __getitem__ we need to wrap it in a class that
    enables this behavior while still maintaining the original function signature and behavior.
    """

    def __init__(
        self,
        fn,
        registrar,
        provider,
        decorator_register_resources_method,
        decorator_name_variant_method,
        transformation,
    ):
        # if not self.__has_return_statement(fn):
        #     raise Exception(
        #         "Transformation function seems to be missing a return statement"
        #     )

        self.fn = fn
        self.registrar = registrar
        self.provider = provider
        # Previously, the descriptor protocol was used to apply methods from the decorator classes
        # to instances of SubscriptableTransformation such that a user could call `fn.name_variant()`
        # and receive a tuple of (name, variant) where name was the name of the wrapped function and
        # variant was either the value passed to the decorator or the default value. This was achieved
        # via the following syntax: `self.name_variant = decorator_name_variant_method.__get__(self)`
        # For as-of-yet unknown reasons, this behavior was not working as expected in Python 3.11.2,
        # so the code has been reverted to the original syntax, which simply passes a reference to
        # the decorator methods to the SubscriptableTransformation class.
        self.register_resources = decorator_register_resources_method
        self.name_variant = decorator_name_variant_method
        self.transformation = transformation

    def name_variant(self):
        return self.transformation.name_variant()

    def get_resource_type(self):
        return self.transformation.get_resource_type()

    def __getitem__(self, columns: List[str]):
        col_len = len(columns)
        if col_len < 2:
            raise Exception(
                f"Expected 2 columns, but found {col_len}. Missing entity and/or source columns"
            )
        elif col_len > 3:
            raise Exception(
                f"Found unrecognized columns {', '.join(columns[3:])}. Expected 2 required columns and an optional 3rd timestamp column"
            )
        return (self.registrar, self.transformation, columns)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    @staticmethod
    def __has_return_statement(fn):
        """
        Parses the function’s source code into an abstract syntax tree
        and then walks through the tree to check for any Return nodes.
        Not full-proof but will at least catch cases on the client.
        """
        tree = ast.parse(inspect.getsource(fn))
        for node in ast.walk(tree):
            if isinstance(node, ast.Return):
                return True
        return False


@dataclass
class SQLTransformationDecorator:
    registrar: "Registrar"
    owner: str
    provider: str
    tags: List[str]
    properties: dict
    run: str = ""
    variant: str = ""
    name: str = ""
    schedule: str = ""
    inputs: list = field(default_factory=list)
    description: str = ""
    args: Union[K8sArgs, None] = None
    query: str = field(default_factory=str, init=False)
    partition_options: Optional[PartitionType] = None
    func_params_to_inputs: dict = field(default_factory=dict, init=False)
    max_job_duration: timedelta = timedelta(hours=48)
    spark_flags: SparkFlags = field(default_factory=lambda: EmptySparkFlags)
    resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None

    def __post_init__(self):
        if self.inputs is None:
            self.inputs = []

    def __call__(self, fn: Callable[[], str]):
        if self.description == "" and fn.__doc__ is not None:
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__

        func_params = inspect.signature(fn).parameters

        if len(func_params) > 0:
            if len(func_params) > len(self.inputs):
                raise ValueError(
                    f"Transformation function has more parameters than inputs. \n"
                    f"Make sure each function parameter has a corresponding input in the decorator."
                )

            if len(func_params) < len(self.inputs):
                raise ValueError(
                    f"Too many inputs for transformation function. Expected {len(func_params)} inputs, but found {len(self.inputs)}.\n"
                )

            if not isinstance(self.inputs, list):
                raise ValueError("Dataframe transformation inputs must be a list")

            self.func_params_to_inputs = {
                param: inp for param, inp in zip(func_params, self.inputs)
            }

            self.__set_query(fn(*self.inputs))
        else:
            self.__set_query(fn())

        self.registrar.map_client_object_to_resource(self, self.to_source())
        self.registrar.add_resource(self.to_source())
        return SubscriptableTransformation(
            fn,
            self.registrar,
            self.provider,
            self.register_resources,
            self.name_variant,
            self,
        )

    @typechecked
    def __set_query(self, query: str):
        if query == "":
            raise ValueError("Query cannot be an empty string")

        self._assert_query_contains_at_least_one_source(query)
        if len(self.inputs) > 0:
            # if inputs are specified, then the query will be resolved at the time of creation (when #kwargs is called)
            self.query = query
        else:
            self.query = add_variant_to_name(query, self.run)

    def to_source(self) -> SourceVariant:
        return SourceVariant(
            created=None,
            name=self.name,
            variant=self.variant,
            definition=SQLTransformation(
                query=self.query,
                args=self.args,
                func_params_to_inputs=self.func_params_to_inputs,
                partition_options=self.partition_options,
                spark_flags=self.spark_flags,
                resource_snowflake_config=self.resource_snowflake_config,
            ),
            owner=self.owner,
            schedule=self.schedule,
            provider=self.provider,
            description=self.description,
            tags=self.tags,
            properties=self.properties,
            max_job_duration=self.max_job_duration,
        )

    def name_variant(self):
        return (self.name, self.variant)

    def get_resource_type(self):
        return ResourceType.SOURCE_VARIANT

    def register_resources(
        self,
        entity: Union[str, EntityRegistrar],
        entity_column: str,
        owner: Union[str, UserRegistrar] = "",
        inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
        features: List[ColumnMapping] = None,
        labels: List[ColumnMapping] = None,
        timestamp_column: str = "",
        description: str = "",
        schedule: str = "",
    ):
        return self.registrar.register_column_resources(
            source=self,
            entity=entity,
            entity_column=entity_column,
            owner=owner,
            inference_store=inference_store,
            features=features,
            labels=labels,
            timestamp_column=timestamp_column,
            description=description,
            schedule=schedule,
            client_object=self,
        )

    @staticmethod
    def _assert_query_contains_at_least_one_source(query):
        # Checks to verify that the query contains a FROM {{ name.variant }}

        # the pattern pulls the string within the double curly braces
        pattern = r"\{\{\s*(.*?)\s*\}\}"
        matches = re.findall(pattern, query)
        if len(matches) == 0:
            raise InvalidSQLQuery(query, "No source specified.")

        for m in matches:
            name, variant = get_name_variant(query, m)
            if name == "":
                raise InvalidSQLQuery(query, "Source name is empty.")

            # Check for invalid characters in the source name and variant
            if name.startswith(" ") or name.endswith(" "):
                raise InvalidSQLQuery(
                    query, "Source name cannot start or end with a space."
                )
            if variant.startswith(" ") or variant.endswith(" "):
                raise InvalidSQLQuery(
                    query, "Source variant cannot start or end with a space."
                )

            if name.startswith("_") or name.endswith("_"):
                raise InvalidSQLQuery(
                    query, "Source name cannot start or end with an underscore."
                )
            if variant.startswith("_") or variant.endswith("_"):
                raise InvalidSQLQuery(
                    query, "Source variant cannot start or end with an underscore."
                )

            if "__" in name or "__" in variant:
                raise InvalidSQLQuery(
                    query,
                    "Source name and variant cannot contain consecutive underscores.",
                )


def get_name_variant(query, source_str):
    # Based on the source string, split the name and variant
    name_variant = source_str.split(".")
    if len(name_variant) > 2:
        raise InvalidSQLQuery(
            query, "Source name and variant cannot contain more than one period."
        )
    elif len(name_variant) == 2:
        name = name_variant[0]
        variant = name_variant[1]
    else:
        name = name_variant[0]
        variant = ""

    return name, variant


@dataclass
class DFTransformationDecorator:
    registrar: "Registrar"
    owner: str
    provider: str
    tags: List[str]
    properties: dict
    variant: str = ""
    name: str = ""
    description: str = ""
    partition_options: Optional[PartitionType] = None
    inputs: list = field(default_factory=list)
    args: Union[K8sArgs, None] = None
    source_text: str = ""
    canonical_func_text: str = ""
    max_job_duration: timedelta = timedelta(hours=48)

    def __call__(self, fn):
        if self.description == "" and fn.__doc__ is not None:
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__

        func_params = inspect.signature(fn).parameters
        if len(func_params) > len(self.inputs):
            raise ValueError(
                f"Transformation function has more parameters than inputs. \n"
                f"Make sure each function parameter has a corresponding input in the decorator."
            )

        if not isinstance(self.inputs, list):
            raise ValueError("Dataframe transformation inputs must be a list")

        # check that input isn't self referencing
        for nv in self.inputs:
            if isinstance(
                nv, tuple
            ):  # TODO all that should be called here is name_variant()
                n, v = nv
            else:
                n, v = nv.name_variant()
            if self.name is n and self.variant is v:
                raise ValueError(
                    f"Transformation cannot be input for itself: {self.name} {self.variant}"
                )
        self.query = dill.dumps(fn.__code__)
        self.source_text = dill.source.getsource(fn)
        self.canonical_func_text = canonicalize_function_definition(fn)
        self.registrar.map_client_object_to_resource(self, self.to_source())
        self.registrar.add_resource(self.to_source())
        return SubscriptableTransformation(
            fn,
            self.registrar,
            self.provider,
            self.register_resources,
            self.name_variant,
            self,
        )

    def to_source(self) -> SourceVariant:
        return SourceVariant(
            created=None,
            name=self.name,
            variant=self.variant,
            definition=DFTransformation(
                query=self.query,
                inputs=self.inputs,
                args=self.args,
                source_text=self.source_text,
                canonical_func_text=self.canonical_func_text,
            ),
            owner=self.owner,
            provider=self.provider,
            description=self.description,
            tags=self.tags,
            properties=self.properties,
            max_job_duration=self.max_job_duration,
        )

    def name_variant(self):
        return (self.name, self.variant)

    def register_resources(
        self,
        entity: Union[str, EntityRegistrar],
        entity_column: str,
        owner: Union[str, UserRegistrar] = "",
        inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
        features: List[ColumnMapping] = None,
        labels: List[ColumnMapping] = None,
        timestamp_column: str = "",
        description: str = "",
    ):
        return self.registrar.register_column_resources(
            source=self,
            entity=entity,
            entity_column=entity_column,
            owner=owner,
            inference_store=inference_store,
            features=features,
            labels=labels,
            timestamp_column=timestamp_column,
            description=description,
            client_object=self,
        )


class ColumnSourceRegistrar(SourceRegistrar):
    def __getitem__(self, columns: List[str]):
        col_len = len(columns)
        if col_len < 2:
            raise Exception(
                f"Expected 2 columns, but found {col_len}. Missing entity and/or source columns"
            )
        elif col_len > 3:
            raise Exception(
                f"Found unrecognized columns {', '.join(columns[3:])}. Expected 2 required columns and an optional 3rd timestamp column"
            )
        return (self.registrar(), self, columns)

    def register_resources(
        self,
        entity: Union[str, EntityRegistrar],
        entity_column: str,
        owner: Union[str, UserRegistrar] = "",
        inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
        features: List[ColumnMapping] = None,
        labels: List[ColumnMapping] = None,
        timestamp_column: str = "",
        description: str = "",
        schedule: str = "",
    ):
        """
        Registers a features and/or labels that can be used in training sets or served.

        **Examples**:
        ``` py
        average_user_transaction.register_resources(
            entity=user,
            entity_column="CustomerID",
            inference_store=local,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
            ],
        )
        ```

        Args:
            entity (Union[str, EntityRegistrar]): The name to reference the entity by when serving features
            entity_column (str): The name of the column in the source to be used as the entity
            owner (Union[str, UserRegistrar]): The owner of the resource(s)
            inference_store (Union[str, OnlineProvider, FileStoreProvider]): Where to store the materialized feature for serving. (Use the local provider in Localmode)
            features (List[ColumnMapping]): A list of column mappings to define the features
            labels (List[ColumnMapping]): A list of column mappings to define the labels
            timestamp_column: (str): The name of an optional timestamp column in the dataset. Will be used to match the features and labels with point-in-time correctness

        Returns:
            registrar (ResourceRegister): Registrar
        """
        return self.registrar().register_column_resources(
            source=self,
            entity=entity,
            entity_column=entity_column,
            owner=owner,
            inference_store=inference_store,
            features=features,
            labels=labels,
            timestamp_column=timestamp_column,
            description=description,
            schedule=schedule,
            client_object=self,
        )


class ResourceRegistrar:
    def __init__(self, registrar, features, labels):
        self.__registrar = registrar
        self.__features = features
        self.__labels = labels

    def create_training_set(
        self,
        name: str,
        variant: str = "",
        label: NameVariant = None,
        schedule: str = "",
        features: List[NameVariant] = None,
        resources: List = None,
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
    ):
        if len(self.__labels) == 0:
            raise ValueError("A label must be included in a training set")
        if len(self.__features) == 0:
            raise ValueError("A feature must be included in a training set")
        if len(self.__labels) > 1 and label == None:
            raise ValueError("Only one label may be specified in a TrainingSet.")
        if features is not None:
            featureSet = set(
                [(feature["name"], feature["variant"]) for feature in self.__features]
            )
            for feature in features:
                if feature not in featureSet:
                    raise ValueError(f"Feature {feature} not found.")
        else:
            features = [
                (feature["name"], feature["variant"]) for feature in self.__features
            ]
        if label is None:
            label = (self.__labels[0]["name"], self.__labels[0]["variant"])
        else:
            labelSet = set(
                [(label["name"], label["variant"]) for label in self.__labels]
            )
            if label not in labelSet:
                raise ValueError(f"Label {label} not found.")
        return self.__registrar.register_training_set(
            name=name,
            variant=variant,
            label=label,
            features=features,
            resources=resources,
            owner=owner,
            schedule=schedule,
            description=description,
        )

    def features(self):
        return self.__features

    def label(self):
        if isinstance(self.__labels, list):
            if len(self.__labels) > 1:
                raise ValueError(
                    "A resource used has multiple labels. A training set can only have one label"
                )
            elif len(self.__labels) == 1:
                self.__labels = (self.__labels[0]["name"], self.__labels[0]["variant"])
            else:
                self.__labels = ()
        return self.__labels


class ModelRegistrar:
    def __init__(self, registrar, model):
        self.__registrar = registrar
        self.__model = model

    def name(self) -> str:
        return self.__model.name


class ColumnResource(ABC):
    """
    Base class for all column resources. This class is not meant to be instantiated directly.
    In the original syntax, features and labels were registered using the `register_resources`
    method on the sources (e.g. SQL/DF transformation or tables sources); however, in the new
    Class API syntax, features and labels can now be declared as class attributes on an entity
    class. This means that all possible params for either resource must be passed into this base
    class prior to calling `register_column_resources` on the registrar.
    """

    def __init__(
        self,
        transformation_args: tuple,
        type: Union[ScalarType, str],
        resource_type: ResourceType,
        entity: Union[Entity, str],
        owner: Union[str, UserRegistrar],
        timestamp_column: str,
        description: str,
        schedule: str,
        tags: List[str],
        properties: Dict[str, str],
        inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
        name: str = "",
        variant: str = "",
        resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None,
    ):
        registrar, source_name_variant, columns = transformation_args
        self.type = type if isinstance(type, str) else type.value
        self.registrar = registrar
        self.source = source_name_variant
        self.entity_column = columns[0]
        self.source_column = columns[1]
        self.resource_type = resource_type
        self.entity = entity
        self.name = name
        self.owner = owner
        self.inference_store = inference_store
        if not timestamp_column and len(columns) == 3:
            self.timestamp_column = columns[2]
        elif timestamp_column and len(columns) == 3:
            raise Exception("Timestamp column specified twice.")
        else:
            self.timestamp_column = timestamp_column
        self.description = description
        self.schedule = schedule
        tags, properties = set_tags_properties(tags, properties)
        self.tags = tags
        self.properties = properties
        self.variant = variant
        self.resource_snowflake_config = resource_snowflake_config

    def register(self):
        features, labels = self.get_resources_by_type(self.resource_type)

        self.registrar.register_column_resources(
            source=self.source,
            entity=self.entity,
            entity_column=self.entity_column,
            owner=self.owner,
            inference_store=self.inference_store,
            features=features,
            labels=labels,
            timestamp_column=self.timestamp_column,
            schedule=self.schedule,
            client_object=self,
        )

    def get_resources_by_type(
        self, resource_type: ResourceType
    ) -> Tuple[List[ColumnMapping], List[ColumnMapping]]:
        resources = [
            {
                "name": self.name,
                "variant": self.variant,
                "column": self.source_column,
                "type": self.type,
                "description": self.description,
                "tags": self.tags,
                "properties": self.properties,
                "resource_snowflake_config": self.resource_snowflake_config,
            }
        ]

        if resource_type == ResourceType.FEATURE_VARIANT:
            features = resources
            labels = []
        elif resource_type == ResourceType.LABEL_VARIANT:
            features = []
            labels = resources
        else:
            raise ValueError(
                f"Resource type {self.resource_type.to_string()} not supported"
            )
        return (features, labels)

    def name_variant(self) -> NameVariant:
        if self.name is None:
            raise ValueError("Resource name not set")
        if self.variant is None:
            raise ValueError("Resource variant not set")
        return (self.name, self.variant)

    def get_resource_type(self) -> ResourceType:
        return self.resource_type


class Variants:
    def __init__(self, resources: Dict[str, ColumnResource]):
        self.resources = resources
        self.validate_variant_names()

    def validate_variant_names(self):
        for variant_key, resource in self.resources.items():
            if resource.variant == "":
                resource.variant = variant_key
            if resource.variant != variant_key:
                raise ValueError(
                    f"Variant name {variant_key} does not match resource variant name {resource.variant}"
                )

    def register(self):
        for resource in self.resources.values():
            resource.register()


class FeatureColumnResource(ColumnResource):
    def __init__(
        self,
        transformation_args: tuple,
        type: Union[ScalarType, str],
        entity: Union[Entity, str] = "",
        name: str = "",
        variant: str = "",
        owner: str = "",
        inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
        timestamp_column: str = "",
        description: str = "",
        schedule: str = "",
        tags: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
        resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None,
    ):
        """
        Feature registration object.

        **Example**
        ```
        @ff.entity
        class Customer:
        # Register a column from a transformation as a feature
            transaction_amount = ff.Feature(
                fare_per_family_member[["CustomerID", "Amount", "Transaction Time"]],
                variant="quickstart",
                type=ff.Float64,
                inference_store=redis,
            )
        ```

        Args:
            transformation_args (tuple): A transformation or source function and the columns name in the format: <transformation_function>[[<entity_column>, <value_column>, <timestamp_column (optional)>]].
            variant (str): An optional variant name for the feature.
            type (Union[ScalarType, str]): The type of the value in for the feature.
            inference_store (Union[str, OnlineProvider, FileStoreProvider]): Where to store for online serving.
        """
        super().__init__(
            transformation_args=transformation_args,
            type=type,
            resource_type=ResourceType.FEATURE_VARIANT,
            entity=entity,
            name=name,
            variant=variant,
            owner=owner,
            inference_store=inference_store,
            timestamp_column=timestamp_column,
            description=description,
            schedule=schedule,
            tags=tags,
            properties=properties,
            resource_snowflake_config=set_resource_snowflake_config_defaults(
                resource_snowflake_config
            ),
        )


class MultiFeatureColumnResource(Iterable):
    def __iter__(self):
        return iter(self.features)

    def __init__(
        self,
        dataset: SourceVariant,
        df: pd.DataFrame,
        entity_column: Union[Entity, str],
        variant: str = "",
        owner: str = "",
        inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
        timestamp_column: str = "",
        include_columns: List[str] = None,
        exclude_columns: List[str] = None,
        description: str = "",
        schedule: str = "",
        tags: List[str] = None,
        properties: Dict[str, str] = None,
        # TODO: (Erik) determine if we need to add a snowflake_dynamic_table_config here
    ):
        """
        Registering multiple features from the same table. The name of each feature is the name of the column in the table.

        **Example**
        ```
        # Register a file or table from an offline provider as a dataset

        client = ff.Client()
        df = client.dataframe(dataset)

        @ff.entity
        class Customer:
        # Register multiple columns from a dataset as features
            transaction_features = ff.MultiFeature(
                dataset,
                df,
                variant="quickstart",
                inference_store=redis,
                entity_column="CustomerID",
                timestamp_column="Timestamp",
                exclude_columns=["TransactionID", "IsFraud"],
                inference_store=redis,
            )
        ```

        Args:
            dataset (SourceVariant): The dataset to register features from
            df (pd.DataFrame): The client.dataframe to register features from
            include_columns (List[str]): List of columns to be registered as features
            exclude_columns (List[str]): List of columns to be excluded from registration
            entity_column (Union[Entity, str]): The name of the column in the source to be used as the entity
            variant (str): An optional variant name for the feature.
            inference_store (Union[str, OnlineProvider, FileStoreProvider]): Where to store for online serving.
        """
        self.tags = tags or []
        self.properties = properties or {}
        self.owner = owner
        self.description = description
        self.schedule = schedule
        self.features = []

        include_columns = include_columns or []
        exclude_columns = exclude_columns or []

        register_columns = self._get_feature_columns(
            df, include_columns, exclude_columns, entity_column, timestamp_column
        )
        self._create_feature_columns(
            df,
            dataset,
            register_columns,
            entity_column,
            timestamp_column,
            inference_store,
            variant,
        )

    def _create_feature_columns(
        self,
        df,
        dataset,
        register_columns,
        entity_column,
        timestamp_column,
        inference_store,
        variant,
    ):
        df_has_quotes = self._check_df_column_format(df)
        for column_name in register_columns:
            transformation_args = (
                dataset[[entity_column, column_name, timestamp_column]]
                if timestamp_column != ""
                else dataset[[entity_column, column_name]]
            )
            feature = FeatureColumnResource(
                transformation_args=transformation_args,
                name=column_name,
                variant=variant,
                type=pd_to_ff_datatype[
                    df[self._modify_column_name(column_name, df_has_quotes)].dtype
                ],
                inference_store=inference_store,
            )
            self.features.append(feature)

        return self.features

    def _get_feature_columns(
        self, df, include_columns, exclude_columns, entity_column, timestamp_column
    ):
        all_columns_set = set([self._clean_name(col) for col in df.columns])
        include_columns_set = set(include_columns)
        exclude_columns_set = set(exclude_columns)
        exclude_columns_set.add(entity_column)

        if timestamp_column != "":
            exclude_columns_set.add(timestamp_column)

        if not include_columns_set.issubset(all_columns_set):
            raise ValueError(
                f"{all_columns_set - include_columns_set} columns are not in the dataframe"
            )

        if not exclude_columns_set.issubset(all_columns_set):
            raise ValueError(
                f"Exclude columns: {exclude_columns_set - all_columns_set} columns are not in the dataframe"
            )

        if not include_columns_set.isdisjoint(exclude_columns_set):
            raise ValueError(
                f"{include_columns_set.intersection(exclude_columns_set)} cannot be in the include and exclude lists"
            )

        if len(include_columns_set) > 0:
            return list(include_columns_set - exclude_columns_set)
        else:
            return list(all_columns_set - exclude_columns_set)

    @staticmethod
    def _check_df_column_format(df):
        df_has_quotes = False
        for column_name in df.columns:
            if '"' in column_name:
                df_has_quotes = True
            return df_has_quotes

    # TODO: Verify if you can have empty strings as column names (Add unit test for it)
    @staticmethod
    def _clean_name(string_name):
        return string_name.replace('"', "")

    def _modify_column_name(self, string_name, df_has_quotes):
        if df_has_quotes:
            return '"' + self._clean_name(string_name) + '"'
        return self._clean_name(string_name)


class LabelColumnResource(ColumnResource):
    def __init__(
        self,
        transformation_args: tuple,
        type: Union[ScalarType, str],
        entity: Union[Entity, str] = "",
        name: str = "",
        variant: str = "",
        owner: str = "",
        timestamp_column: str = "",
        description: str = "",
        schedule: str = "",
        tags: List[str] = [],
        properties: Dict[str, str] = {},
        resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None,
    ):
        """
        Label registration object.

        **Example**
        ```
        @ff.entity
        class Customer:
        # Register a column from a transformation as a label
            transaction_amount = ff.Label(
                fare_per_family_member[["CustomerID", "Amount", "Transaction Time"]],
                variant="quickstart",
                type=ff.Float64
            )
        ```

        Args:
            transformation_args (tuple): A transformation or source function and the columns name in the format: <transformation_function>[[<entity_column>, <value_column>, <timestamp_column (optional)>]]
            variant (str): An optional variant name for the label.
            type (Union[ScalarType, str]): The type of the value in for the label.
        """
        super().__init__(
            transformation_args=transformation_args,
            type=type,
            resource_type=ResourceType.LABEL_VARIANT,
            entity=entity,
            name=name,
            variant=variant,
            owner=owner,
            timestamp_column=timestamp_column,
            description=description,
            schedule=schedule,
            tags=tags,
            properties=properties,
            resource_snowflake_config=resource_snowflake_config,
        )


class EmbeddingColumnResource(ColumnResource):
    def __init__(
        self,
        transformation_args: tuple,
        dims: int,
        vector_db: Union[str, OnlineProvider, FileStoreProvider],
        entity: Union[Entity, str] = "",
        name: str = "",
        variant="",
        owner: str = "",
        timestamp_column: str = "",
        description: str = "",
        schedule: str = "",
        tags: List[str] = [],
        properties: Dict[str, str] = {},
    ):
        super().__init__(
            transformation_args=transformation_args,
            type=ScalarType.FLOAT32,
            resource_type=ResourceType.FEATURE_VARIANT,
            entity=entity,
            variant=variant,
            owner=owner,
            inference_store=vector_db,
            timestamp_column=timestamp_column,
            description=description,
            schedule=schedule,
            tags=tags,
            properties=properties,
        )
        if dims < 1:
            raise ValueError("Vector dimensions must be a positive integer")
        self.dims = dims

    def get_resources_by_type(
        self, resource_type: ResourceType
    ) -> Tuple[List[ColumnMapping], List[ColumnMapping]]:
        features, labels = super().get_resources_by_type(resource_type)
        features[0]["dims"] = self.dims
        features[0]["is_embedding"] = True
        return (features, labels)


class FeatureStreamResource:
    def __init__(
        self,
        type: Union["ScalarType", str],
        offline_store: Union[str, "OfflineProvider"],
        inference_store: Union[str, "OnlineProvider"],
        variant: str = "",
        owner: str = "",
        description: str = "",
        tags: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name = ""
        self.entity = ""
        self.type = type if isinstance(type, str) else type.value
        self.offline_provider = offline_store
        self.inference_store = inference_store
        self.variant = variant
        self.owner = owner
        self.description = description
        self.tags = tags or []
        self.properties = properties or {}

    def register(self) -> None:
        register_feature_stream(
            name=self.name,
            entity=self.entity,
            type=self.type,
            offline_provider=self.offline_provider,
            inference_store=self.inference_store,
            variant=self.variant,
            owner=self.owner,
            description=self.description,
            tags=self.tags,
            properties=self.properties,
        )


class LabelStreamResource:
    def __init__(
        self,
        type: Union["ScalarType", str],
        offline_store: Union[str, "OfflineProvider"],
        variant: str = "",
        owner: str = "",
        description: str = "",
        tags: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name = ""
        self.entity = ""
        self.type = type if isinstance(type, str) else type.value
        self.offline_provider = offline_store
        self.variant = variant
        self.owner = owner
        self.description = description
        self.tags = tags or []
        self.properties = properties or {}

    def register(self) -> None:
        register_label_stream(
            name=self.name,
            entity=self.entity,
            type=self.type,
            offline_provider=self.offline_provider,
            variant=self.variant,
            owner=self.owner,
            description=self.description,
            tags=self.tags,
            properties=self.properties,
        )


class Registrar:
    """These functions are used to register new resources and retrieving existing resources.
    Retrieved resources can be used to register additional resources.

    ``` py title="definitions.py"
    import featureform as ff

    # e.g. registering a new provider
    redis = ff.register_redis(
        name="redis-quickstart",
        host="quickstart-redis",  # The internal dns name for redis
        port=6379,
        description="A Redis deployment we created for the Featureform quickstart"
    )
    ```
    """

    def __init__(self):
        self.__state = ResourceState()
        self.__resources = []
        self.__default_owner = ""
        self.__variant_prefix = ""
        if feature_flag.is_enabled("FF_GET_EQUIVALENT_VARIANTS", True):
            self.__run = get_current_timestamp_variant(self.__variant_prefix)
        else:
            self.__run = get_random_name()

        """
        maps client objects (feature object, label object, source decorators) to their resource in the event we want 
        to update the client object after the resource was created
        
        Introduced for timestamp variants where updates during a resource create ensures that the client object
        has the correct variant when being used as a dependency other resources
        """
        self.__client_obj_to_resource_map = {}

    def add_resource(self, resource):
        self.__resources.append(resource)

    def map_client_object_to_resource(
        self, client_obj, resource_variant: ResourceVariant
    ):
        self.__client_obj_to_resource_map[resource_variant.to_key()] = client_obj

    def get_client_objects_for_resource(self):
        return self.__client_obj_to_resource_map

    def get_resources(self):
        return self.__resources

    def register_user(
        self, name: str, tags: List[str] = [], properties: dict = {}
    ) -> UserRegistrar:
        """Register a user.

        Args:
            name (str): User to be registered.

        Returns:
            UserRegistrar: User
        """
        user = User(name=name, tags=tags, properties=properties)
        self.__resources.append(user)
        return UserRegistrar(self, user)

    def set_default_owner(self, user: str):
        """Set default owner.

        Args:
            user (str): User to be set as default owner of resources.
        """
        self.__default_owner = user

    def default_owner(self) -> str:
        return self.__default_owner

    def must_get_default_owner(self) -> str:
        owner = self.default_owner()
        if owner == "":  # if no owner is specified, set to the default
            owner = DEFAULT_OWNER
        return owner

    def set_variant_prefix(self, variant_prefix: str = ""):
        """Set variant prefix.

        Args:
            variant_prefix (str): variant prefix to be set.
        """
        self.__variant_prefix = variant_prefix
        self.set_run()

    def set_run(self, run: str = ""):
        """

        **Example 1**: Using set_run() without arguments will generate a random run name.
        ``` py
        import featureform as ff
        ff.set_run()

        postgres.register_table(
            name="transactions",
            table="transactions_table",
        )

        # Applying will register the source as name=transactions, variant=<randomly-generated>

        ```

        **Example 2**: Using set_run() with arguments will set the variant to the provided name.
        ``` py
        import featureform as ff
        ff.set_run("last_30_days")

        postgres.register_table(
            name="transactions",
            table="transactions_table",
        )

        # Applying will register the source as name=transactions, variant=last_30_days
        ```

        **Example 3**: Generated and set variant names can be used together
        ``` py
        import featureform as ff
        ff.set_run()

        file = spark.register_file(
            name="transactions",
            path="my/transactions.parquet",
            variant="last_30_days"
        )

        @spark.df_transformation(inputs=[file]):
        def customer_count(transactions):
            return transactions.groupBy("CustomerID").count()


        # Applying without a variant for the dataframe transformation will result in
        # the transactions source having a variant of last_30_days and the transformation
        # having a randomly generated variant
        ```

        **Example 4**: This also works within SQL Transformations
        ``` py
        import featureform as ff
        ff.set_run("last_30_days")

        @postgres.sql_transformation():
        def my_transformation():
            return "SELECT CustomerID, Amount FROM {{ transactions }}"

        # The variant will be autofilled so the SQL query is returned as:
        # "SELECT CustomerID, Amount FROM {{ transactions.last_30_days }}"
        ```

        Args:
            run (str): Name of a run to be set.
        """
        if run == "":
            if feature_flag.is_enabled("FF_GET_EQUIVALENT_VARIANTS", True):
                self.__run = get_current_timestamp_variant(self.__variant_prefix)
            else:
                self.__run = get_random_name()
        else:
            self.__run = run

    def get_run(self) -> str:
        """
        Get the current run name.

        **Examples**:
        ``` py
        import featureform as ff

        client = ff.Client()
        f = client.features(("avg_transaction_amount", ff.get_run()), {"user": "123"})

        ```

        Returns:
            run: The name of the current run
        """
        return self.__run

    def get_source(self, name, variant, local=False):
        """
        get_source() can be used to get a reference to an already registered primary source or transformation.
        The returned object can be used to register features and labels or be extended off of to create additional
        transformations.

        **Examples**:

        Registering a transformation from an existing source.
        ``` py
        spark = ff.get_spark("prod-spark")
        transactions = ff.get_source("transactions","kaggle")

        @spark.df_transformation(inputs=[transactions]):
        def customer_count(transactions):
            return transactions.groupBy("CustomerID").count()
        ```

        Registering a feature from an existing source.
        ``` py
        transactions = ff.get_source("transactions","kaggle")

        transactions.register_resources(
            entity=user,
            entity_column="customerid",
            labels=[
                {"name": "fraudulent", "variant": "quickstart", "column": "isfraud", "type": "bool"},
            ],
        )
        ```

        Args:
            name (str): Name of source to be retrieved
            variant (str): Name of variant of source to be retrieved
            local (bool): If localmode is being used

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        if local:
            raise Exception(
                "Localmode is not supported; please try featureform <= 1.12.0"
            )
        else:
            mock_definition = PrimaryData(location=SQLTable(name=""))
            mock_source = SourceVariant(
                created=None,
                name=name,
                variant=variant,
                definition=mock_definition,
                owner="",
                provider="",
                description="",
                tags=[],
                properties={},
            )
            return ColumnSourceRegistrar(self, mock_source)

    def get_redis(self, name):
        """Get a Redis provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        redis = ff.get_redis("redis-quickstart")

        average_user_transaction.register_resources(
            entity=user,
            entity_column="user_id",
            inference_store=redis,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
            ],
        )
        ```

        Args:
            name (str): Name of Redis provider to be retrieved

        Returns:
            redis (OnlineProvider): Provider
        """
        warnings.warn(
            "Use client.get_redis instead",
            DeprecationWarning,
        )
        mock_config = RedisConfig(host="", port=123, password="", db=123)
        mock_provider = Provider(
            name=name, function="ONLINE", description="", team="", config=mock_config
        )
        return OnlineProvider(self, mock_provider)

    def get_dynamodb(self, name: str):
        """Get a DynamoDB provider. The returned object can be used as an inference store in feature registration.

        **Examples**:
        ``` py
        dynamodb = ff.get_dynamodb("dynamodb-quickstart")

        @ff.entity
        class User:
            avg_transactions = ff.Feature(
                average_user_transaction[["user_id", "avg_transaction_amt"]],
                type=ff.Float32,
                inference_store=dynamodb,
            )
        ```

        Args:
            name (str): Name of DynamoDB provider to be retrieved

        Returns:
            dynamodb (OnlineProvider): Provider
        """
        warnings.warn(
            "Use client.get_dynamodb instead",
            DeprecationWarning,
        )
        mock_config = DynamodbConfig(
            region="", access_key="", secret_key="", should_import_from_s3=False
        )
        mock_provider = Provider(
            name=name, function="ONLINE", description="", team="", config=mock_config
        )
        return OnlineProvider(self, mock_provider)

    def get_mongodb(self, name: str):
        """Get a MongoDB provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        mongodb = ff.get_mongodb("mongodb-quickstart")

        average_user_transaction.register_resources(
            entity=user,
            entity_column="user_id",
            inference_store=mongodb,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
            ],
        )
        ```

        Args:
            name (str): Name of MongoDB provider to be retrieved

        Returns:
            mongodb (OnlineProvider): Provider
        """
        warnings.warn(
            "Use client.get_mongodb instead",
            DeprecationWarning,
        )
        mock_config = MongoDBConfig(
            username="", password="", host="", port="", database="", throughput=1
        )
        mock_provider = Provider(
            name=name, function="ONLINE", description="", team="", config=mock_config
        )
        return OnlineProvider(self, mock_provider)

    def get_blob_store(self, name):
        """Get an Azure Blob provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        azure_blob = ff.get_blob_store("azure-blob-quickstart")

        average_user_transaction.register_resources(
            entity=user,
            entity_column="user_id",
            inference_store=azure_blob,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
            ],
        )
        ```

        Args:
            name (str): Name of Azure blob provider to be retrieved

        Returns:
            azure_blob (FileStoreProvider): Provider
        """
        warnings.warn(
            "Use client.get_blob_store instead",
            DeprecationWarning,
        )
        fake_azure_config = AzureFileStoreConfig(
            account_name="", account_key="", container_name="", root_path=""
        )
        fake_config = OnlineBlobConfig(
            store_type="AZURE", store_config=fake_azure_config.config()
        )
        mock_provider = Provider(
            name=name, function="ONLINE", description="", team="", config=fake_config
        )
        return FileStoreProvider(self, mock_provider, fake_config, "AZURE")

    def get_postgres(self, name):
        """Get a Postgres provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        postgres = ff.get_postgres("postgres-quickstart")
        transactions = postgres.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```

        Args:
            name (str): Name of Postgres provider to be retrieved

        Returns:
            postgres (OfflineSQLProvider): Provider
        """
        warnings.warn(
            "Use client.get_postgres instead",
            DeprecationWarning,
        )
        mock_config = PostgresConfig(
            host="",
            port="",
            database="",
            user="",
            password="",
            sslmode="",
        )
        mock_provider = Provider(
            name=name, function="OFFLINE", description="", team="", config=mock_config
        )
        return OfflineSQLProvider(self, mock_provider)

    def get_clickhouse(self, name):
        """Get a ClickHouse provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        clickhouse = ff.get_clickhouse("clickhouse-quickstart")
        transactions = clickhouse.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in ClickHouse
        )
        ```

        Args:
            name (str): Name of ClickHouse provider to be retrieved

        Returns:
            clickhouse (OfflineSQLProvider): Provider
        """
        warnings.warn(
            "Use client.get_clickhouse instead",
            DeprecationWarning,
        )
        mock_config = ClickHouseConfig(
            host="",
            port=9000,
            database="",
            user="",
            password="",
            ssl=False,
        )
        mock_provider = Provider(
            name=name, function="OFFLINE", description="", team="", config=mock_config
        )
        return OfflineSQLProvider(self, mock_provider)

    def get_snowflake(self, name):
        """Get a Snowflake provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        snowflake = ff.get_snowflake("snowflake-quickstart")
        transactions = snowflake.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```

        Args:
            name (str): Name of Snowflake provider to be retrieved

        Returns:
            snowflake (OfflineSQLProvider): Provider
        """
        warnings.warn(
            "Use client.get_snowflake instead",
            DeprecationWarning,
        )
        mock_config = SnowflakeConfig(
            account="ff_fake",
            database="ff_fake",
            organization="ff_fake",
            username="ff_fake",
            password="ff_fake",
            schema="ff_fake",
        )
        mock_provider = Provider(
            name=name, function="OFFLINE", description="", team="", config=mock_config
        )
        return OfflineSQLProvider(self, mock_provider)

    def get_snowflake_legacy(self, name: str):
        """Get a Snowflake provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        snowflake = ff.get_snowflake_legacy("snowflake-quickstart")
        transactions = snowflake.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```

        Args:
            name (str): Name of Snowflake provider to be retrieved

        Returns:
            snowflake_legacy (OfflineSQLProvider): Provider
        """
        warnings.warn(
            "Use client.get_snowflake_legacy instead",
            DeprecationWarning,
        )
        mock_config = SnowflakeConfig(
            account_locator="ff_fake",
            database="ff_fake",
            username="ff_fake",
            password="ff_fake",
            schema="ff_fake",
            warehouse="ff_fake",
            role="ff_fake",
        )
        mock_provider = Provider(
            name=name, function="OFFLINE", description="", team="", config=mock_config
        )
        return OfflineSQLProvider(self, mock_provider)

    def get_redshift(self, name):
        """Get a Redshift provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        redshift = ff.get_redshift("redshift-quickstart")
        transactions = redshift.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```

        Args:
            name (str): Name of Redshift provider to be retrieved

        Returns:
            redshift (OfflineSQLProvider): Provider
        """
        warnings.warn(
            "Use client.get_redshift instead",
            DeprecationWarning,
        )
        mock_config = RedshiftConfig(
            host="", port="5439", database="", user="", password="", sslmode=""
        )
        mock_provider = Provider(
            name=name, function="OFFLINE", description="", team="", config=mock_config
        )
        return OfflineSQLProvider(self, mock_provider)

    def get_bigquery(self, name):
        """Get a BigQuery provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        bigquery = ff.get_bigquery("bigquery-quickstart")
        transactions = bigquery.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in BigQuery
        )
        ```

        Args:
            name (str): Name of BigQuery provider to be retrieved

        Returns:
            bigquery (OfflineSQLProvider): Provider
        """
        warnings.warn(
            "Use client.get_bigquery instead",
            DeprecationWarning,
        )
        mock_config = BigQueryConfig(
            project_id="mock_project",
            dataset_id="mock_dataset",
            credentials=GCPCredentials(
                project_id="mock_project",
                credentials_path="client/tests/test_files/bigquery_dummy_credentials.json",
            ),
        )
        mock_provider = Provider(
            name=name, function="OFFLINE", description="", team="", config=mock_config
        )
        return OfflineSQLProvider(self, mock_provider)

    def get_spark(self, name):
        """Get a Spark provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        spark = ff.get_spark("spark-quickstart")
        transactions = spark.register_file(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            file_path="s3://bucket/path/to/file/transactions.parquet",  # This is the path to file
        )
        ```

        Args:
            name (str): Name of Spark provider to be retrieved

        Returns:
            spark (OfflineSQLProvider): Provider
        """
        warnings.warn(
            "Use client.get_spark instead",
            DeprecationWarning,
        )
        mock_config = SparkConfig(
            executor_type="", executor_config={}, store_type="", store_config={}
        )
        mock_provider = Provider(
            name=name, function="OFFLINE", description="", team="", config=mock_config
        )
        return OfflineSparkProvider(self, mock_provider)

    def get_kubernetes(self, name):
        """
        Get a k8s provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py

        k8s = ff.get_kubernetes("k8s-azure-quickstart")
        transactions = k8s.register_file(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            path="path/to/blob",
        )
        ```

        Args:
            name (str): Name of k8s provider to be retrieved

        Returns:
            k8s (OfflineK8sProvider): Provider
        """
        warnings.warn(
            "Use client.get_kubernetes instead",
            DeprecationWarning,
        )
        mock_config = K8sConfig(store_type="", store_config={})
        mock_provider = Provider(
            name=name, function="OFFLINE", description="", team="", config=mock_config
        )
        return OfflineK8sProvider(self, mock_provider)

    def get_s3(self, name):
        """
        Get a S3 provider. The returned object can be used with other providers such as Spark and Databricks.

        **Examples**:

        ``` py

        s3 = ff.get_s3("s3-quickstart")
        spark = ff.register_spark(
            name=f"spark-emr-s3",
            description="A Spark deployment we created for the Featureform quickstart",
            team="featureform-team",
            executor=emr,
            filestore=s3,
        )
        ```

        Args:
            name (str): Name of S3 to be retrieved

        Returns:
            s3 (FileStore): Provider
        """
        warnings.warn(
            "Use client.get_s3 instead",
            DeprecationWarning,
        )
        provider = Provider(
            name=name,
            function="OFFLINE",
            description="description",
            team="team",
            config=s3_config,
        )
        return FileStoreProvider(
            registrar=self,
            provider=provider,
            config=s3_config,
            store_type=s3_config.type(),
        )

    def get_gcs(self, name):
        warnings.warn(
            "Use client.get_gcs instead",
            DeprecationWarning,
        )
        filePath = "provider/connection/mock_credentials.json"
        fake_creds = GCPCredentials(project_id="id", credentials_path=filePath)
        mock_config = GCSFileStoreConfig(
            bucket_name="", bucket_path="", credentials=fake_creds
        )
        mock_provider = Provider(
            name=name, function="OFFLINE", description="", team="", config=mock_config
        )
        return OfflineK8sProvider(self, mock_provider)

    def _create_mock_creds_file(self, filename, json_data):
        with open(filename, "w") as f:
            json.dumps(json_data, f)

    def get_entity(self, name: str):
        """Get an entity. The returned object can be used to register additional resources.

        **Examples**:

        ``` py
        entity = get_entity("user")
        transactions.register_resources(
            entity=entity,
            entity_column="customerid",
            labels=[
                {"name": "fraudulent", "variant": "quickstart", "column": "isfraud", "type": "bool"},
            ],
        )
        ```

        Args:
            name (str): Name of entity to be retrieved
        Returns:
            entity (EntityRegistrar): Entity
        """
        fakeEntity = Entity(
            name=name, description="", status="", tags=[], properties={}
        )
        return EntityRegistrar(self, fakeEntity)

    def register_redis(
        self,
        name: str,
        host: str,
        port: int = 6379,
        db: int = 0,
        password: str = "",
        description: str = "",
        team: str = "",
        tags: Optional[List[str]] = None,
        properties: Optional[dict] = None,
    ):
        """Register a Redis provider.

        **Examples**:
        ```
        redis = ff.register_redis(
            name="redis-quickstart",
            host="quickstart-redis",
            port=6379,
            password="password",
            description="A Redis deployment we created for the Featureform quickstart"
        )
        ```

        Args:
            name (str): (Immutable) Name of Redis provider to be registered
            host (str): (Immutable) Hostname for Redis
            db (str): (Immutable) Redis database number
            port (int): (Mutable) Redis port
            password (str): (Mutable) Redis password
            description (str): (Mutable) Description of Redis provider to be registered
            team (str): (Mutable) Name of team
            tags (Optional[List[str]]): (Mutable) Optional grouping mechanism for resources
            properties (Optional[dict]): (Mutable) Optional grouping mechanism for resources

        Returns:
            redis (OnlineProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = RedisConfig(host=host, port=port, password=password, db=db)
        provider = Provider(
            name=name,
            function="ONLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_pinecone(
        self,
        name: str,
        project_id: str,
        environment: str,
        api_key: str,
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a Pinecone provider.

        **Examples**:
        ```
        pinecone = ff.register_pinecone(
            name="pinecone-quickstart",
            project_id="2g13ek7",
            environment="us-west4-gcp-free",
            api_key="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        )
        ```

        Args:
            name (str): (Immutable) Name of Pinecone provider to be registered
            project_id (str): (Immutable) Pinecone project id
            environment (str): (Immutable) Pinecone environment
            api_key (str): (Mutable) Pinecone api key
            description (str): (Mutable) Description of Pinecone provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            pinecone (OnlineProvider): Provider
        """

        tags, properties = set_tags_properties(tags, properties)
        config = PineconeConfig(
            project_id=project_id, environment=environment, api_key=api_key
        )
        provider = Provider(
            name=name,
            function="ONLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_weaviate(
        self,
        name: str,
        url: str,
        api_key: str,
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a Weaviate provider.

        **Examples**:
        ```
        weaviate = ff.register_weaviate(
            name="weaviate-quickstart",
            url="https://<CLUSTER NAME>.weaviate.network",
            api_key="<API KEY>"
            description="A Weaviate project for using embeddings in Featureform"
        )
        ```

        Args:
            name (str): (Immutable) Name of Weaviate provider to be registered
            url (str): (Immutable) Endpoint of Weaviate cluster, either in the cloud or via another deployment operation
            api_key (str): (Mutable) Weaviate api key
            description (str): (Mutable) Description of Weaviate provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            weaviate (OnlineProvider): Provider
        """
        config = WeaviateConfig(url=url, api_key=api_key)
        provider = Provider(
            name=name,
            function="ONLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_blob_store(
        self,
        name: str,
        account_name: str,
        account_key: str,
        container_name: str,
        root_path: str,
        description: str = "",
        team: str = "",
        tags=None,
        properties=None,
    ):
        """Register an Azure Blob Store provider.

        Azure Blob Storage can be used as the storage component for Spark or the Featureform Pandas Runner.

        **Examples**:
        ```
        blob = ff.register_blob_store(
            name="azure-quickstart",
            container_name="my_company_container"
            root_path="custom/path/in/container"
            account_name=<azure_account_name>
            account_key=<azure_account_key>
            description="An azure blob store provider to store offline and inference data"
        )
        ```

        Args:
            name (str): (Immutable) Name of Azure blob store to be registered
            container_name (str): (Immutable) Azure container name
            root_path (str): (Immutable) A custom path in container to store data
            account_name (str): (Immutable) Azure account name
            account_key (str):  (Mutable) Secret azure account key
            description (str): (Mutable) Description of Azure Blob provider to be registered
            team (str): (Mutable) The name of the team registering the filestore
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            blob (StorageProvider): Provider
                has all the functionality of OnlineProvider
        """

        tags, properties = set_tags_properties(tags, properties)

        container_name = container_name.replace("abfss://", "")
        if "/" in container_name:
            raise ValueError(
                "container_name cannot contain '/'. container_name should be the name of the Azure Blobstore container only."
            )

        azure_config = AzureFileStoreConfig(
            account_name=account_name,
            account_key=account_key,
            container_name=container_name,
            root_path=root_path,
        )
        config = OnlineBlobConfig(
            store_type="AZURE", store_config=azure_config.config()
        )

        provider = Provider(
            name=name,
            function="ONLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return FileStoreProvider(self, provider, azure_config, "AZURE")

    def register_s3(
        self,
        name: str,
        credentials: Union[AWSStaticCredentials, AWSAssumeRoleCredentials],
        bucket_region: str,
        bucket_name: str,
        path: str = "",
        description: str = "",
        team: str = "",
        tags: Optional[List[str]] = None,
        properties: Optional[dict] = None,
    ):
        """Register a S3 store provider.

        This has the functionality of an offline store and can be used as a parameter
        to a k8s or spark provider

        **Examples**:
        ```
        s3 = ff.register_s3(
            name="s3-quickstart",
            credentials=aws_creds,
            bucket_name="bucket_name",
            bucket_region=<bucket_region>,
            path="path/to/store/featureform_files/in/",
            description="An s3 store provider to store offline"
        )
        ```

        Args:
            name (str): (Immutable) Name of S3 store to be registered
            bucket_name (str): (Immutable) AWS Bucket Name
            bucket_region (str): (Immutable) AWS region the bucket is located in
            path (str): (Immutable) The path used to store featureform files in
            credentials (Union[AWSStaticCredentials, AWSAssumeRoleCredentials]): (Mutable) AWS credentials to access the bucket
            description (str): (Mutable) Description of S3 provider to be registered
            team (str): (Mutable) The name of the team registering the filestore
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            s3 (FileStoreProvider): Provider
                has all the functionality of OfflineProvider
        """
        tags, properties = set_tags_properties(tags, properties)

        if bucket_name == "":
            raise ValueError("bucket_name is required and cannot be empty string")

        # TODO: add verification into S3StoreConfig
        bucket_name = bucket_name.replace("s3://", "").replace("s3a://", "")

        if "/" in bucket_name:
            raise ValueError(
                "bucket_name cannot contain '/'. bucket_name should be the name of the AWS S3 bucket only."
            )

        s3_config = S3StoreConfig(
            bucket_path=bucket_name,
            bucket_region=bucket_region,
            credentials=credentials,
            path=path,
        )

        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=s3_config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return FileStoreProvider(self, provider, s3_config, s3_config.type())

    def register_gcs(
        self,
        name: str,
        bucket_name: str,
        root_path: str,
        credentials: GCPCredentials,
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a GCS store provider.

        **Examples**:
        ```
        gcs = ff.register_gcs(
            name="gcs-quickstart",
            credentials=ff.GCPCredentials(...),
            bucket_name="bucket_name",
            root_path="featureform/path/",
            description="An gcs store provider to store offline"
        )
        ```

        Args:
            name (str): (Immutable) Name of GCS store to be registered
            bucket_name (str): (Immutable) The bucket name
            root_path (str): (Immutable) Custom path to be used by featureform
            credentials (GCPCredentials): (Mutable) GCP credentials to access the bucket
            description (str): (Mutable) Description of GCS provider to be registered
            team (str): (Mutable) The name of the team registering the filestore
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            gcs (FileStoreProvider): Provider
                has all the functionality of OfflineProvider
        """
        tags, properties = set_tags_properties(tags, properties)

        if bucket_name == "":
            raise ValueError("bucket_name is required and cannot be empty string")

        bucket_name = bucket_name.replace("gs://", "")
        if "/" in bucket_name:
            raise ValueError(
                "bucket_name cannot contain '/'. bucket_name should be the name of the GCS bucket only."
            )

        gcs_config = GCSFileStoreConfig(
            bucket_name=bucket_name, bucket_path=root_path, credentials=credentials
        )
        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=gcs_config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return FileStoreProvider(self, provider, gcs_config, gcs_config.type())

    def register_hdfs(
        self,
        name: str,
        host: str,
        port: str,
        path: str = "",
        hdfs_site_file: str = "",
        core_site_file: str = "",
        credentials: Union[BasicCredentials, KerberosCredentials] = None,
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a HDFS store provider with support for Kerberos if needed.

        This has the functionality of an offline store and can be used as a parameter
        to a k8s or spark provider

        **Examples**:
        ```
        hdfs = ff.register_hdfs(
            name="hdfs-quickstart",
            host="<host>",
            port="<port>",
            path="<path>",
            credentials=<ff.BasicCredentials or ff.KerberosCredentials>,
            description="An hdfs store provider to store offline"
        )
        ```
        Args:
            name (str): Name of HDFS store to be registered
            host (str): The hostname for HDFS
            port (str): The IPC port for the Namenode for HDFS. (Typically 8020 or 9000)
            path (str): A storage path within HDFS
            core_site_file (str): A path to a core_site config file
            hdfs_site_file (str): A path to a hdfs_site config file
            credentials (Union[BasicCredentials, KerberosCredentials]): Credentials to access HDFS
            description (str): Description of HDFS provider to be registered
            team (str): The name of the team registering HDFS
        Returns:
            hdfs (FileStoreProvider): Provider
        """

        hdfs_config = HDFSConfig(
            host=host,
            port=port,
            path=path,
            credentials=credentials,
            hdfs_site_file=hdfs_site_file,
            core_site_file=core_site_file,
        )

        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=hdfs_config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return FileStoreProvider(self, provider, hdfs_config, hdfs_config.type())

    # TODO: Set Deprecation Warning For Credentials Path
    def register_firestore(
        self,
        name: str,
        collection: str,
        project_id: str,
        credentials: GCPCredentials,
        credentials_path: str = "",
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a Firestore provider.

        **Examples**:
        ```
        firestore = ff.register_firestore(
            name="firestore-quickstart",
            description="A Firestore deployment we created for the Featureform quickstart",
            project_id="quickstart-project",
            collection="quickstart-collection",
            credentials=ff.GCPCredentials(...)
        )
        ```

        Args:
            name (str): (Immutable) Name of Firestore provider to be registered
            project_id (str): (Immutable) The Project name in GCP
            collection (str): (Immutable) The Collection name in Firestore under the given project ID
            credentials (GCPCredentials): (Mutable) GCP credentials to access Firestore
            description (str): (Mutable) Description of Firestore provider to be registered
            team (str): (Mutable) The name of the team registering the filestore
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            firestore (OfflineSQLProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = FirestoreConfig(
            collection=collection,
            project_id=project_id,
            credentials=credentials,
        )
        provider = Provider(
            name=name,
            function="ONLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    # TODO: Check these fields
    def register_cassandra(
        self,
        name: str,
        host: str,
        port: int,
        username: str,
        password: str,
        keyspace: str,
        consistency: str = "THREE",
        replication: int = 3,
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a Cassandra provider.

        **Examples**:
        ```
        cassandra = ff.register_cassandra(
                name = "cassandra",
                description = "Example inference store",
                team = "Featureform",
                host = "0.0.0.0",
                port = 9042,
                username = "cassandra",
                password = "cassandra",
                consistency = "THREE",
                replication = 3
            )
        ```

        Args:
            name (str): (Immutable) Name of Cassandra provider to be registered
            host (str): (Immutable) DNS name of Cassandra
            port (str): (Mutable) Port
            username (str): (Mutable) Username
            password (str): (Mutable) Password
            consistency (str): (Mutable) Consistency
            replication (int): (Mutable) Replication
            description (str): (Mutable) Description of Cassandra provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            cassandra (OnlineProvider): Provider
        """
        config = CassandraConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            keyspace=keyspace,
            consistency=consistency,
            replication=replication,
        )
        provider = Provider(
            name=name,
            function="ONLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_dynamodb(
        self,
        name: str,
        credentials: Union[AWSStaticCredentials, AWSAssumeRoleCredentials],
        region: str,
        should_import_from_s3: bool = False,
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a DynamoDB provider.

        **Examples**:
        ```
        dynamodb = ff.register_dynamodb(
            name="dynamodb-quickstart",
            description="A Dynamodb deployment we created for the Featureform quickstart",
            credentials=aws_creds,
            region="us-east-1"
        )
        ```

        Args:
            name (str): (Immutable) Name of DynamoDB provider to be registered
            region (str): (Immutable) Region to create dynamo tables
            credentials (Union[AWSStaticCredentials, AWSAssumeRoleCredentials]): (Mutable) AWS credentials with permissions to create DynamoDB tables
            should_import_from_s3 (bool): (Mutable) Determines whether feature materialization will occur via a direct import of data from S3 to new table (see [docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.HowItWorks.html) for details)
            description (str): (Mutable) Description of DynamoDB provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            dynamodb (OnlineProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = DynamodbConfig(
            credentials=credentials,
            region=region,
            should_import_from_s3=should_import_from_s3,
        )
        provider = Provider(
            name=name,
            function="ONLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_mongodb(
        self,
        name: str,
        username: str,
        password: str,
        database: str,
        host: str,
        port: str,
        throughput: int = 1000,
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a MongoDB provider.

        **Examples**:
        ```
        mongodb = ff.register_mongodb(
            name="mongodb-quickstart",
            description="A MongoDB deployment",
            username="my_username",
            password="myPassword",
            database="featureform_database"
            host="my-mongodb.host.com",
            port="10225",
            throughput=10000
        )
        ```

        Args:
            name (str): (Immutable) Name of MongoDB provider to be registered
            database (str): (Immutable) MongoDB database
            host (str): (Immutable) MongoDB hostname
            port (str): (Immutable) MongoDB port
            username (str): (Mutable) MongoDB username
            password (str): (Mutable) MongoDB password
            throughput (int): (Mutable) The maximum RU limit for autoscaling in CosmosDB
            description (str): (Mutable) Description of MongoDB provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            mongodb (OnlineProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = MongoDBConfig(
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            throughput=throughput,
        )
        provider = Provider(
            name=name,
            function="ONLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_snowflake_legacy(
        self,
        name: str,
        username: str,
        password: str,
        account_locator: str,
        database: str,
        schema: str = "PUBLIC",
        description: str = "",
        team: str = "",
        warehouse: str = "",
        role: str = "",
        tags: List[str] = [],
        properties: dict = {},
        catalog: Optional[SnowflakeCatalog] = None,
        session_params: Optional[Dict[str, str]] = None,
    ):
        """Register a Snowflake provider using legacy credentials.

        **Examples**:
        ```
        snowflake = ff.register_snowflake_legacy(
            name="snowflake-quickstart",
            username="snowflake",
            password="password",
            account_locator="account-locator",
            database="snowflake",
            schema="PUBLIC",
            description="A Snowflake deployment we created for the Featureform quickstart"
            warehouse="featureform_warehouse",
            catalog=ff.SnowflakeCatalog(
                external_volume="sf_ext_volume",
                base_location="iceberg_tables",
                table_config=ff.SnowflakeDynamicTableConfig(
                    target_lag='1 days',
                    refresh_mode=ff.RefreshMode.INCREMENTAL,
                    initialize=ff.Initialize.ON_CREATE,
                ),
            )
            session_params={ "query_tag": "featureform" }
        )
        ```

        Args:
            name (str): (Immutable) Name of Snowflake provider to be registered
            account_locator (str): (Immutable) Account Locator
            schema (str): (Immutable) Schema
            database (str): (Immutable) Database
            username (str): (Mutable) Username
            password (str): (Mutable) Password
            warehouse (str): (Mutable) Specifies the virtual warehouse to use by default for queries, loading, etc.
            role (str): (Mutable) Specifies the role to use by default for accessing Snowflake objects in the client session
            description (str): (Mutable) Description of Snowflake provider to be registered
            team (str): (Mutable) Name of team
            catalog (SnowflakeCatalog): Catalog configuration and default dynamic table configuration
            session_params (Dict[str, str]): (Mutable) Session parameters to set for the Snowflake session
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            snowflake (OfflineSQLProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = SnowflakeConfig(
            account_locator=account_locator,
            database=database,
            username=username,
            password=password,
            schema=schema,
            warehouse=warehouse,
            role=role,
            catalog=catalog,
            session_params=session_params,
        )
        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

    # TODO: Recheck mutable fields
    def register_snowflake(
        self,
        name: str,
        username: str,
        password: str,
        account: str,
        organization: str,
        database: str,
        schema: str = "PUBLIC",
        description: str = "",
        team: str = "",
        warehouse: str = "",
        role: str = "",
        tags: List[str] = [],
        properties: dict = {},
        catalog: Optional[SnowflakeCatalog] = None,
        session_params: Optional[Dict[str, str]] = None,
    ):
        """Register a Snowflake provider.

        **Examples**:
        ```
        snowflake = ff.register_snowflake(
            name="snowflake-quickstart",
            username="snowflake",
            password="password", #pragma: allowlist secret
            account="account",
            organization="organization",
            database="snowflake",
            schema="PUBLIC",
            description="A Snowflake deployment we created for the Featureform quickstart"
            warehouse="featureform_warehouse",
            catalog=ff.SnowflakeCatalog(
                external_volume="sf_ext_volume",
                base_location="iceberg_tables",
                table_config=ff.SnowflakeDynamicTableConfig(
                    target_lag='1 days',
                    refresh_mode=ff.RefreshMode.INCREMENTAL,
                    initialize=ff.Initialize.ON_CREATE,
                ),
            )
            session_params={ "query_tag": "featureform" }
        )
        ```

        Args:
            name (str): (Immutable) Name of Snowflake provider to be registered
            account (str): (Immutable) Account
            organization (str): (Immutable) Organization
            database (str): (Immutable) Database
            schema (str): (Immutable) Schema
            username (str): (Mutable) Username
            password (str): (Mutable) Password
            warehouse (str): (Mutable) Specifies the virtual warehouse to use by default for queries, loading, etc.
            role (str): (Mutable) Specifies the role to use by default for accessing Snowflake objects in the client session
            description (str): (Mutable) Description of Snowflake provider to be registered
            team (str): (Mutable) Name of team
            catalog (SnowflakeCatalog): Catalog configuration and default dynamic table configuration
            session_params (Dict[str, str]): (Mutable) Session parameters to set for the Snowflake session
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            snowflake (OfflineSQLProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = SnowflakeConfig(
            account=account,
            database=database,
            organization=organization,
            username=username,
            password=password,
            schema=schema,
            warehouse=warehouse,
            role=role,
            catalog=catalog,
            session_params=session_params,
        )
        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

    def register_postgres(
        self,
        name: str,
        host: str,
        user: str,
        password: str,
        database: str,
        port: str = "5432",
        description: str = "",
        team: str = "",
        sslmode: str = "disable",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a Postgres provider.

        **Examples**:
        ```
        postgres = ff.register_postgres(
            name="postgres-quickstart",
            description="A Postgres deployment we created for the Featureform quickstart",
            host="quickstart-postgres",  # The internal dns name for postgres
            port="5432",
            user="postgres",
            password="password", #pragma: allowlist secret
            database="postgres"
        )
        ```

        Args:
            name (str): (Immutable) Name of Postgres provider to be registered
            host (str): (Immutable) Hostname for Postgres
            database (str): (Immutable) Database
            port (str): (Mutable) Port
            user (str): (Mutable) User
            password (str): (Mutable) Password
            sslmode (str): (Mutable) SSL mode
            description (str): (Mutable) Description of Postgres provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            postgres (OfflineSQLProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = PostgresConfig(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode=sslmode,
        )
        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=config,
            tags=tags or [],
            properties=properties or {},
        )

        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

    def register_clickhouse(
        self,
        name: str,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 9000,
        description: str = "",
        team: str = "",
        ssl: bool = False,
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a ClickHouse provider.

        **Examples**:
        ```
        clickhouse = ff.register_clickhouse(
            name="clickhouse-quickstart",
            description="A ClickHouse deployment we created for the Featureform quickstart",
            host="quickstart-clickhouse",  # The internal dns name for clickhouse
            port=9000,
            user="default",
            password="", #pragma: allowlist secret
            database="default"
        )
        ```

        Args:
            name (str): (Immutable) Name of ClickHouse provider to be registered
            host (str): (Immutable) Hostname for ClickHouse
            database (str): (Immutable) ClickHouse database
            port (int): (Mutable) Port
            ssl (bool): (Mutable) Enable SSL
            user (str): (Mutable) User
            password (str): (Mutable) ClickHouse password
            description (str): (Mutable) Description of ClickHouse provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            clickhouse (OfflineSQLProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = ClickHouseConfig(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            ssl=ssl,
        )
        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

    def register_redshift(
        self,
        name: str,
        host: str,
        port: str,
        user: str,
        password: str,
        database: str,
        description: str = "",
        team: str = "",
        sslmode: str = "disable",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a Redshift provider.

        **Examples**:
        ```
        redshift = ff.register_redshift(
            name="redshift-quickstart",
            description="A Redshift deployment we created for the Featureform quickstart",
            host="quickstart-redshift",  # The internal dns name for redshift
            port="5432",
            user="redshift",
            password="password", #pragma: allowlist secret
            database="dev"
        )
        ```

        Args:
            name (str): (Immutable) Name of Redshift provider to be registered
            host (str): (Immutable) Hostname for Redshift
            database (str): (Immutable) Redshift database
            port (str): (Mutable) Port
            user (str): (Mutable) User
            password (str): (Mutable) Redshift password
            sslmode (str): (Mutable) SSL mode
            description (str): (Mutable) Description of Redshift provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            redshift (OfflineSQLProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = RedshiftConfig(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode=sslmode,
        )
        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

    # TODO: Add deprecated warning for credentials_path
    def register_bigquery(
        self,
        name: str,
        project_id: str,
        dataset_id: str,
        credentials: GCPCredentials,
        credentials_path: str = "",
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a BigQuery provider.

        **Examples**:
        ```
        bigquery = ff.register_bigquery(
            name="bigquery-quickstart",
            description="A BigQuery deployment we created for the Featureform quickstart",
            project_id="quickstart-project",
            dataset_id="quickstart-dataset",
            credentials=GCPCredentials(...)
        )
        ```

        Args:
            name (str): (Immutable) Name of BigQuery provider to be registered
            project_id (str): (Immutable) The Project name in GCP
            dataset_id (str): (Immutable) The Dataset name in GCP under the Project Id
            credentials (GCPCredentials): (Mutable) GCP credentials to access BigQuery
            description (str): (Mutable) Description of BigQuery provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            bigquery (OfflineSQLProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)

        config = BigQueryConfig(
            project_id=project_id,
            dataset_id=dataset_id,
            credentials=credentials,
        )
        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

    def register_spark(
        self,
        name: str,
        executor: ExecutorCredentials,
        filestore: FileStoreProvider,
        catalog: Optional[Catalog] = None,
        description: str = "",
        team: str = "",
        tags: List[str] = [],
        properties: dict = {},
        spark_params: Optional[Dict[str, str]] = None,
        write_options: Optional[Dict[str, str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
    ):
        """Register a Spark on Executor provider.

        **Examples**:
        ```
        spark = ff.register_spark(
            name="spark-quickstart",
            description="A Spark deployment we created for the Featureform quickstart",
            team="featureform-team",
            executor=databricks,
            filestore=azure_blob_store
        )
        ```

        Args:
            name (str): (Immutable) Name of Spark provider to be registered
            executor (ExecutorCredentials): (Mutable) An Executor Provider used for the compute power
            filestore (FileStoreProvider): (Mutable) A FileStoreProvider used for storage of data
            catalog (Optional[Catalog]): (Mutable) A Catalog Provider used for metadata storage
            description (str): (Mutable) Description of Spark provider to be registered
            team (str): (Mutable) Name of team
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources

        Returns:
            spark (OfflineSparkProvider): Provider
        """
        tags, properties = set_tags_properties(tags, properties)
        config = SparkConfig(
            executor_type=executor.type(),
            executor_config=executor.config(),
            store_type=filestore.store_type(),
            store_config=filestore.config(),
            catalog=catalog.config() if catalog is not None else None,
        )

        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OfflineSparkProvider(self, provider)

    # TODO: Change things to either filestore or store
    def register_k8s(
        self,
        name: str,
        store: FileStoreProvider,
        description: str = "",
        team: str = "",
        docker_image: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """
        Register an offline store provider to run on Featureform's own k8s deployment.
        **Examples**:
        ```
        spark = ff.register_k8s(
            name="k8s",
            store=AzureBlobStore(),
            docker_image="my-repo/image:version"
        )
        ```

        Args:
            name (str): (Immutable) Name of provider
            store (FileStoreProvider): (Mutable) Reference to registered file store provider
            docker_image (str): (Mutable) A custom docker image using the base image featureformcom/k8s_runner
            description (str): (Mutable) Description of primary data to be registered
            team (str): (Mutable) A string parameter describing the team that owns the provider
            tags (List[str]): (Mutable) Optional grouping mechanism for resources
            properties (dict): (Mutable) Optional grouping mechanism for resources
        """

        tags, properties = set_tags_properties(tags, properties)
        config = K8sConfig(
            store_type=store.store_type(),
            store_config=store.config(),
            docker_image=docker_image,
        )

        provider = Provider(
            name=name,
            function="OFFLINE",
            description=description,
            team=team,
            config=config,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(provider)
        return OfflineK8sProvider(self, provider)

    def register_primary_data(
        self,
        name: str,
        location: Location,
        provider: Union[str, OfflineProvider],
        tags: List[str],
        properties: dict,
        variant: str = "",
        timestamp_column: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
    ):
        """Register a primary data source.

        Args:
            name (str): Name of source
            variant (str): Name of variant
            location (Location): Location of primary data
            provider (Union[str, OfflineProvider]): Provider
            timestamp_column (str): Optionally include timestamp column for append-only tables.
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if variant == "":
            variant = self.__run
        if not isinstance(provider, str):
            provider = provider.name()
        source = SourceVariant(
            created=None,
            name=name,
            variant=variant,
            definition=PrimaryData(
                location=location, timestamp_column=timestamp_column
            ),
            owner=owner,
            provider=provider,
            description=description,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(source)
        column_source_registrar = ColumnSourceRegistrar(self, source)
        self.map_client_object_to_resource(column_source_registrar, source)
        return column_source_registrar

    def register_sql_transformation(
        self,
        name: str,
        query: str,
        provider: Union[str, OfflineProvider],
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        schedule: str = "",
        args: K8sArgs = None,
        inputs: Union[List[NameVariant], List[str], List[ColumnSourceRegistrar]] = None,
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a SQL transformation source.

        Args:
            name (str): Name of source
            variant (str): Name of variant
            query (str): SQL query
            provider (Union[str, OfflineProvider]): Provider
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")
            args (K8sArgs): Additional transformation arguments
            tags (List[str]): Optional grouping mechanism for resources
            properties (dict): Optional grouping mechanism for resources

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if variant == "":
            variant = self.__run
        if not isinstance(provider, str):
            provider = provider.name()
        source = SourceVariant(
            created=None,
            name=name,
            variant=variant,
            definition=SQLTransformation(query, args),
            owner=owner,
            schedule=schedule,
            provider=provider,
            description=description,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(source)
        return ColumnSourceRegistrar(self, source)

    def sql_transformation(
        self,
        provider: Union[str, OfflineProvider],
        variant: str = "",
        name: str = "",
        schedule: str = "",
        owner: Union[str, UserRegistrar] = "",
        inputs: Union[List[NameVariant], List[str], List[ColumnSourceRegistrar]] = None,
        description: str = "",
        args: K8sArgs = None,
        tags: List[str] = [],
        properties: dict = {},
        max_job_duration: timedelta = timedelta(hours=48),
        spark_params: Optional[Dict[str, str]] = None,
        write_options: Optional[Dict[str, str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
        resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None,
    ):
        """SQL transformation decorator.

        Args:
            variant (str): Name of variant
            provider (Union[str, OfflineProvider]): Provider
            name (str): Name of source
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")
            owner (Union[str, UserRegistrar]): Owner
            inputs (list): Inputs to transformation
            description (str): Description of SQL transformation
            args (K8sArgs): Additional transformation arguments
            tags (List[str]): Optional grouping mechanism for resources
            properties (dict): Optional grouping mechanism for resources
            max_job_duration (timedelta): Maximum duration FeatureForm will wait for the job to complete; default is 48 hours; jobs that exceed this duration will be canceled

        Returns:
            decorator (SQLTransformationDecorator): decorator
        """
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if variant == "":
            variant = self.__run
        if not isinstance(provider, str):
            provider = provider.name()
        decorator = SQLTransformationDecorator(
            registrar=self,
            name=name,
            run=self.__run,
            variant=variant,
            provider=provider,
            schedule=schedule,
            owner=owner,
            description=description,
            inputs=inputs,
            args=args,
            tags=tags,
            properties=properties,
            max_job_duration=max_job_duration,
            spark_flags=SparkFlags(
                spark_params=spark_params or {},
                write_options=write_options or {},
                table_properties=table_properties or {},
            ),
            resource_snowflake_config=resource_snowflake_config,
        )
        return decorator

    def register_df_transformation(
        self,
        name: str,
        query: str,
        provider: Union[str, OfflineProvider],
        variant: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        inputs: list = [],
        schedule: str = "",
        args: K8sArgs = None,
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register a Dataframe transformation source.

        Args:
            name (str): Name of source
            variant (str): Name of variant
            query (str): SQL query
            provider (Union[str, OfflineProvider]): Provider
            name (str): Name of source
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of SQL transformation
            inputs (list): Inputs to transformation
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")
            args (K8sArgs): Additional transformation arguments
            tags (List[str]): Optional grouping mechanism for resources
            properties (dict): Optional grouping mechanism for resources

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if variant == "":
            variant = self.__run
        if not isinstance(provider, str):
            provider = provider.name()
        source = SourceVariant(
            created=None,
            name=name,
            variant=variant,
            definition=DFTransformation(query, inputs, args),
            owner=owner,
            schedule=schedule,
            provider=provider,
            description=description,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(source)
        return ColumnSourceRegistrar(self, source)

    def df_transformation(
        self,
        provider: Union[str, OfflineProvider],
        tags: List[str],
        properties: dict,
        variant: str = "",
        name: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        inputs: Union[List[NameVariant], List[str], List[ColumnSourceRegistrar]] = [],
        args: K8sArgs = None,
        max_job_duration: timedelta = timedelta(hours=48),
        spark_params: Optional[Dict[str, str]] = None,
        write_options: Optional[Dict[str, str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
    ):
        """Dataframe transformation decorator.

        Args:
            variant (str): Name of variant
            provider (Union[str, OfflineProvider]): Provider
            name (str): Name of source
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of SQL transformation
            inputs (list): Inputs to transformation
            args (K8sArgs): Additional transformation arguments
            tags (List[str]): Optional grouping mechanism for resources
            properties (dict): Optional grouping mechanism for resources
            max_job_duration (timedelta): Maximum duration FeatureForm will wait for the job to complete; default is 48 hours; jobs that exceed this duration will be canceled

        Returns:
            decorator (DFTransformationDecorator): decorator
        """

        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if variant == "":
            variant = self.__run
        if not isinstance(provider, str):
            provider = provider.name()
        if not isinstance(inputs, list):
            raise ValueError("Dataframe transformation inputs must be a list")
        for i, nv in enumerate(inputs):
            if isinstance(nv, str):  # TODO remove this functionality
                inputs[i] = (nv, self.__run)
            elif isinstance(nv, tuple):
                try:
                    self._verify_tuple(nv)
                except TypeError as e:
                    transformation_message = f"'{name}:{variant}'"
                    if name == "":
                        transformation_message = f"with '{variant}' variant"

                    raise TypeError(
                        f"DF transformation {transformation_message} requires correct inputs "
                        f" '{nv}' is not a valid tuple: {e}"
                    )
                if inputs[i][1] == "":
                    inputs[i] = (inputs[i][0], self.__run)

        decorator = DFTransformationDecorator(
            registrar=self,
            name=name,
            variant=variant,
            provider=provider,
            owner=owner,
            description=description,
            inputs=inputs,
            args=args,
            tags=tags,
            properties=properties,
            max_job_duration=max_job_duration,
        )
        return decorator

    def _verify_tuple(self, nv_tuple):
        if not isinstance(nv_tuple, tuple):
            raise TypeError(f"not a tuple; received: '{type(nv_tuple).__name__}' type")

        if len(nv_tuple) != 2:
            raise TypeError(
                "Tuple must be of length 2, got length {}".format(len(nv_tuple))
            )
        if len(nv_tuple) == 2:
            not_string_tuples = not (
                isinstance(nv_tuple[0], str) and isinstance(nv_tuple[1], str)
            )
            if not_string_tuples:
                first_position_type = type(nv_tuple[0]).__name__
                second_position_type = type(nv_tuple[1]).__name__
                raise TypeError(
                    f"Tuple must be of type (str, str); got ({first_position_type}, {second_position_type})"
                )

    def ondemand_feature(
        self,
        fn=None,
        *,
        tags: List[str] = [],
        properties: dict = {},
        variant: str = "",
        name: str = "",
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
    ):
        """On Demand Feature decorator.

        **Examples**
        ```python
        import featureform as ff

        @ff.ondemand_feature()
        def avg_user_transactions(client, params, entities):
            pass
        ```

        Args:
            variant (str): Name of variant
            name (str): Name of source
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of on demand feature
            tags (List[str]): Optional grouping mechanism for resources
            properties (dict): Optional grouping mechanism for resources

        Returns:
            decorator (OnDemandFeature): decorator

        """

        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if variant == "":
            variant = self.__run
        decorator = OnDemandFeatureVariant(
            name=name,
            variant=variant,
            owner=owner,
            description=description,
            tags=tags or [],
            properties=properties or {},
        )

        self.__resources.append(decorator)

        if fn is None:
            return decorator
        else:
            return decorator(fn)

    def state(self):
        for resource in self.__resources:
            try:
                self.__state.add(resource)

            except ResourceRedefinedError:
                raise
            except Exception as e:
                resource_variant = (
                    f" ({resource.variant})" if hasattr(resource, "variant") else ""
                )
                raise Exception(
                    f"Could not add apply {resource.name}{resource_variant}: {e}"
                )
        self.__resources = []
        return self.__state

    def clear_state(self):
        self.__state = ResourceState()
        self.__client_obj_to_resource_map = {}
        self.__resources = []

    def get_state(self):
        """
        Get the state of the resources to be registered.

        Returns:
            resources (List[str]): List of resources to be registered ex. "{type} - {name} ({variant})"
        """
        if len(self.__resources) == 0:
            return "No resources to be registered"

        resources = [["Type", "Name", "Variant"]]
        for resource in self.__resources:
            if hasattr(resource, "variant"):
                resources.append(
                    [resource.__class__.__name__, resource.name, resource.variant]
                )
            else:
                resources.append([resource.__class__.__name__, resource.name, ""])

        print("Resources to be registered:")
        self.__print_state(resources)

    def __print_state(self, data):
        # Calculate the maximum width for each column
        max_widths = [max(len(str(item)) for item in col) for col in zip(*data)]

        # Format the table headers
        headers = " | ".join(
            f"{header:{width}}" for header, width in zip(data[0], max_widths)
        )

        # Generate the separator line
        separator = "-" * len(headers)

        # Format the table rows
        rows = [
            f" | ".join(f"{data[i][j]:{max_widths[j]}}" for j in range(len(data[i])))
            for i in range(1, len(data))
        ]

        # Combine the headers, separator, and rows
        table = headers + "\n" + separator + "\n" + "\n".join(rows)

        print(table)

    def register_entity(
        self,
        name: str,
        description: str = "",
        tags: List[str] = [],
        properties: dict = {},
    ):
        """Register an entity.

        **Examples**:
        ``` py
            user = ff.register_entity("user")
        ```

        Args:
            name (str): Name of entity to be registered
            description (str): Description of entity to be registered
            tags (List[str]): Optional grouping mechanism for resources
            properties (dict): Optional grouping mechanism for resources

        Returns:
            entity (EntityRegistrar): Entity
        """
        entity = Entity(
            name=name,
            description=description,
            status="",
            tags=tags,
            properties=properties,
        )
        self.__resources.append(entity)
        return EntityRegistrar(self, entity)

    def register_column_resources(
        self,
        source: Union[
            NameVariant,
            SourceRegistrar,
            SQLTransformationDecorator,
            DFTransformationDecorator,
        ],
        entity: Union[str, EntityRegistrar],
        entity_column: str,
        owner: Union[str, UserRegistrar] = "",
        inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
        features: List[ColumnMapping] = None,
        labels: List[ColumnMapping] = None,
        timestamp_column: str = "",
        description: str = "",
        schedule: str = "",
        client_object=None,
    ):
        """Create features and labels from a source. Used in the register_resources function.

        Args:
            source (Union[NameVariant, SourceRegistrar, SQLTransformationDecorator]): Source of features, labels, entity
            entity (Union[str, EntityRegistrar]): Entity
            entity_column (str): Column of entity in source
            owner (Union[str, UserRegistrar]): Owner
            inference_store (Union[str, OnlineProvider]): Online provider
            features (List[ColumnMapping]): List of ColumnMapping objects (dictionaries containing the keys: name, variant, column, resource_type)
            labels (List[ColumnMapping]): List of ColumnMapping objects (dictionaries containing the keys: name, variant, column, resource_type)
            description (str): Description
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")

        Returns:
            resource (ResourceRegistrar): resource
        """

        if (
            type(inference_store) == FileStoreProvider
            and inference_store.store_type() in NON_INFERENCE_STORES
        ):
            raise Exception(
                f"cannot use '{inference_store.store_type()}' as an inference store."
            )

        if features is None:
            features = []
        if labels is None:
            labels = []
        if len(features) == 0 and len(labels) == 0:
            raise ValueError("No features or labels set")
        if isinstance(source, tuple) and source[1] == "":
            source = source[0], self.__run
        if not isinstance(entity, str):
            entity = entity.name()
        if not isinstance(inference_store, str):
            inference_store = inference_store.name()
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        feature_resources = []
        label_resources = []
        for feature in features:
            variant = feature.get("variant", "")
            if variant == "":
                variant = self.__run
            if not ScalarType.has_value(feature["type"]) and not isinstance(
                feature["type"], ScalarType
            ):
                raise ValueError(
                    f"Invalid type for feature {feature['name']} ({variant}). Must be a ScalarType or one of {ScalarType.get_values()}"
                )
            if isinstance(feature["type"], ScalarType):
                feature["type"] = feature["type"].value
            desc = feature.get("description", "")
            feature_tags = feature.get("tags", [])
            feature_properties = feature.get("properties", {})
            additional_Parameters = self._get_additional_parameters(ondemand_feature)
            is_embedding = feature.get("is_embedding", False)
            dims = feature.get("dims", 0)
            value_type = ScalarType(feature["type"])
            if dims > 0:
                value_type = VectorType(value_type, dims, is_embedding)
            resource = FeatureVariant(
                created=None,
                name=feature["name"],
                variant=variant,
                source=source,
                value_type=value_type,
                entity=entity,
                owner=owner,
                provider=inference_store,
                description=desc,
                schedule=schedule,
                location=ResourceColumnMapping(
                    entity=entity_column,
                    value=feature["column"],
                    timestamp=timestamp_column,
                ),
                tags=feature_tags,
                properties=feature_properties,
                additional_parameters=additional_Parameters,
                resource_snowflake_config=feature.get("resource_snowflake_config"),
            )
            self.__resources.append(resource)
            self.map_client_object_to_resource(client_object, resource)
            feature_resources.append(resource)

        for label in labels:
            variant = label.get("variant", "")
            if variant == "":
                variant = self.__run
            if not ScalarType.has_value(label["type"]) and not isinstance(
                label["type"], ScalarType
            ):
                raise ValueError(
                    f"Invalid type for label {label['name']} ({variant}). Must be a ScalarType or one of {ScalarType.get_values()}"
                )
            if isinstance(label["type"], ScalarType):
                label["type"] = label["type"].value
            desc = label.get("description", "")
            label_tags = label.get("tags", [])
            label_properties = label.get("properties", {})
            resource = LabelVariant(
                name=label["name"],
                variant=variant,
                source=source,
                value_type=label["type"],
                entity=entity,
                owner=owner,
                provider=inference_store,
                description=desc,
                location=ResourceColumnMapping(
                    entity=entity_column,
                    value=label["column"],
                    timestamp=timestamp_column,
                ),
                tags=label_tags,
                properties=label_properties,
            )
            self.__resources.append(resource)
            self.map_client_object_to_resource(client_object, resource)
            label_resources.append(resource)
        return ResourceRegistrar(self, features, labels)

    def _get_additional_parameters(self, feature):
        return OndemandFeatureParameters(definition="() => REGISTER")

    def register_feature_stream(
        self,
        name: str,
        entity: Union[str, EntityRegistrar],
        type: Union[ScalarType, str],
        offline_provider: Union[str, OfflineProvider],
        inference_store: Union[str, OnlineProvider],
        variant: str = "",
        owner: str = "",
        description: str = "",
        tags: Union[List[str], None] = None,
        properties: Union[Dict[str, str], None] = None,
    ):
        if not isinstance(entity, str):
            entity = entity.name()
        if not isinstance(inference_store, str):
            inference_store = inference_store.name()
        if not isinstance(offline_provider, str):
            offline_provider = offline_provider.name()
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if not isinstance(type, str):
            type = type.value
        if variant == "":
            variant = self.__run

        stream_feature = StreamFeature(
            name=name,
            entity=entity,
            value_type=type,
            offline_provider=offline_provider,
            inference_store=inference_store,
            variant=variant,
            owner=owner,
            description=description,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(stream_feature)
        return ResourceRegistrar(self, [stream_feature], [])

    def register_label_stream(
        self,
        name: str,
        entity: Union[str, EntityRegistrar],
        type: Union[ScalarType, str],
        offline_provider: Union[str, OfflineProvider],
        variant: str = "",
        owner: str = "",
        description: str = "",
        tags: Union[List[str], None] = None,
        properties: Union[Dict[str, str], None] = None,
    ):
        if not isinstance(entity, str):
            entity = entity.name()
            if not isinstance(offline_provider, str):
                offline_provider = offline_provider.name()
            if not isinstance(owner, str):
                owner = owner.name()
            if owner == "":
                owner = self.must_get_default_owner()
            if not isinstance(type, str):
                type = type.value
            if variant == "":
                variant = self.__run

        stream_label = StreamLabel(
            name=name,
            entity=entity,
            value_type=type,
            offline_provider=offline_provider,
            variant=variant,
            owner=owner,
            description=description,
            tags=tags,
            properties=properties,
        )
        self.__resources.append(stream_label)
        return ResourceRegistrar(self, [], [stream_label])

    def __register_stream(self, stream: BaseStream):
        self.__resources.append(stream)
        return ResourceRegistrar(self, [stream], [])

    def __get_feature_nv(self, features, run):
        feature_nv_list = []
        feature_lags = []
        for feature in features:
            if isinstance(feature, str):
                feature_nv_list.append((feature, run))
            elif isinstance(feature, dict):
                lag = feature.get("lag")
                if "variant" not in feature:
                    feature["variant"] = run
                if lag:
                    required_lag_keys = set(["lag", "feature", "variant"])
                    received_lag_keys = set(feature.keys())
                    if (
                        required_lag_keys.intersection(received_lag_keys)
                        != required_lag_keys
                    ):
                        raise ValueError(
                            f"feature lags require 'lag', 'feature', 'variant' fields. Received: {feature.keys()}"
                        )

                    if not isinstance(lag, timedelta):
                        raise ValueError(
                            f"the lag, '{lag}', needs to be of type 'datetime.timedelta'. Received: {type(lag)}."
                        )

                    feature_name_variant = (feature["feature"], feature["variant"])
                    if feature_name_variant not in feature_nv_list:
                        feature_nv_list.append(feature_name_variant)

                    lag_name = f"{feature['feature']}_{feature['variant']}_lag_{lag}"
                    sanitized_lag_name = (
                        lag_name.replace(" ", "").replace(",", "_").replace(":", "_")
                    )
                    feature["name"] = feature.get("name", sanitized_lag_name)

                    feature_lags.append(feature)
                else:
                    feature_nv = (feature["name"], feature["variant"])
                    feature_nv_list.append(feature_nv)
            elif isinstance(feature, list):
                feature_nv, feature_lags_list = self.__get_feature_nv(feature, run)
                if len(feature_nv) != 0:
                    feature_nv_list.extend(feature_nv)

                if len(feature_lags_list) != 0:
                    feature_lags.extend(feature_lags_list)
            else:
                feature_nv_list.append(feature)

        return feature_nv_list, feature_lags

    def register_training_set(
        self,
        name: str,
        variant: str = "",
        features: Union[
            list, List[FeatureColumnResource], MultiFeatureColumnResource
        ] = [],
        label: Union[NameVariant, LabelColumnResource] = ("", ""),
        resources: list = [],
        owner: Union[str, UserRegistrar] = "",
        description: str = "",
        schedule: str = "",
        tags: List[str] = [],
        properties: dict = {},
        provider: str = "",
        resource_snowflake_config: Optional[ResourceSnowflakeConfig] = None,
    ):
        """Register a training set.

        **Example**:
        ```
        ff.register_training_set(
            name="my_training_set",
            label=("label", "v1"),
            features=[("feature1", "v1"), ("feature2", "v1")],
        )
        ```

        Args:
            name (str): Name of training set to be registered
            variant (str): Name of variant to be registered
            label (NameVariant): Label of training set
            features (List[NameVariant]): Features of training set
            resources (List[Resource]): A list of previously registered resources
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of training set to be registered
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")
            tags (List[str]): Optional grouping mechanism for resources
            properties (dict): Optional grouping mechanism for resources

        Returns:
            resource (ResourceRegistrar): resource
        """
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if variant == "":
            variant = self.__run

        if not isinstance(features, (list, MultiFeatureColumnResource)):
            raise ValueError(
                f"Invalid features type: {type(features)} "
                "Features must be entered as a list of name-variant tuples (e.g. [('feature1', 'quickstart'), ('feature2', 'quickstart')]) or a list of FeatureColumnResource instances."
            )
        if not isinstance(label, (tuple, str, LabelColumnResource)):
            raise ValueError(
                f"Invalid label type: {type(label)} "
                "Label must be entered as a name-variant tuple (e.g. ('fraudulent', 'quickstart')), a resource name, or an instance of LabelColumnResource."
            )

        for resource in resources:
            features += resource.features()
            resource_label = resource.label()
            # label == () if it is NOT manually entered
            if label == ("", ""):
                label = resource_label
            # Elif: If label was updated to store resource_label it will not check the following elif
            elif resource_label != ():
                raise ValueError("A training set can only have one label")

        features, feature_lags = self.__get_feature_nv(features, self.__run)
        if label == ():
            raise ValueError("Label must be set")
        if features == []:
            raise ValueError("A training-set must have at least one feature")
        if isinstance(label, str):
            label = (label, self.__run)
        if not isinstance(label, LabelColumnResource) and label[1] == "":
            label = (label[0], self.__run)

        processed_features = []
        for feature in features:
            if isinstance(feature, tuple) and feature[1] == "":
                feature = (feature[0], self.__run)
            processed_features.append(feature)
        resource = TrainingSetVariant(
            created=None,
            name=name,
            variant=variant,
            description=description,
            owner=owner,
            schedule=schedule,
            label=label,
            features=processed_features,
            feature_lags=feature_lags,
            tags=tags,
            properties=properties,
            provider=provider,
            resource_snowflake_config=set_resource_snowflake_config_defaults(
                resource_snowflake_config
            ),
        )
        self.map_client_object_to_resource(resource, resource)
        self.__resources.append(resource)
        return resource

    def register_model(
        self, name: str, tags: List[str] = [], properties: dict = {}
    ) -> Model:
        """Register a model.

        Args:
            name (str): Model to be registered
            tags (List[str]): Optional grouping mechanism for resources
            properties (dict): Optional grouping mechanism for resources

        Returns:
            ModelRegistrar: Model
        """
        model = Model(name, description="", tags=tags, properties=properties)
        self.__resources.append(model)
        return model


def set_resource_snowflake_config_defaults(
    resource_snowflake_config: Union[ResourceSnowflakeConfig, None],
) -> ResourceSnowflakeConfig:
    # Features and trainging sets cannot use the default target lag of DOWNSTREAM because we
    # need to ensure that if users serve features or create training sets, they should be up
    # to date within the target lag period. If no configuration is provided, we set the target
    # lag to ONE_DAY_TARGET_LAG.
    if not resource_snowflake_config:
        resource_snowflake_config = ResourceSnowflakeConfig(
            dynamic_table_config=SnowflakeDynamicTableConfig(
                target_lag=ONE_DAY_TARGET_LAG,
                refresh_mode=RefreshMode.AUTO,
                initialize=Initialize.ON_CREATE,
            )
        )
    # If the user has provided the warehouse but not any dynamic table config, we set the
    # SnowflakeDynamicTableConfig to the default values (i.e. ONE_DAY_TARGET_LAG).
    elif not resource_snowflake_config.dynamic_table_config:
        resource_snowflake_config.dynamic_table_config = SnowflakeDynamicTableConfig(
            target_lag=ONE_DAY_TARGET_LAG,
            refresh_mode=RefreshMode.AUTO,
            initialize=Initialize.ON_CREATE,
        )

    return resource_snowflake_config


class ResourceClient:
    """
    The resource client is used to retrieve information on specific resources
    (entities, providers, features, labels, training sets, models, users).

    Args:
        host (str): The hostname of the Featureform instance.
        insecure (bool): True if connecting to an insecure Featureform endpoint. False if using a self-signed or public TLS certificate
        cert_path (str): The path to a public certificate if using a self-signed certificate.

    **Using the Resource Client:**
    ``` py title="definitions.py"
    import featureform as ff
    from featureform import ResourceClient

    rc = ResourceClient("localhost:8000")

    # example query:
    redis = client.get_provider("redis-quickstart")
    ```
    """

    def __init__(
        self,
        host=None,
        local=False,
        insecure=False,
        cert_path=None,
        dry_run=False,
        debug=False,
    ):
        if local:
            raise Exception(
                "Local mode is not supported in this version. Use featureform <= 1.12.0 for localmode"
            )

        # This line ensures that the warning is only raised if ResourceClient is instantiated directly
        # TODO: Remove this check once ServingClient is deprecated
        is_instantiated_directed = inspect.stack()[1].function != "__init__"
        if is_instantiated_directed:
            warnings.warn(
                "ResourceClient is deprecated and will be removed in future versions; use Client instead.",
                PendingDeprecationWarning,
            )
        self._dry_run = dry_run
        self._stub = None
        self.local = local

        if dry_run:
            return

        setup_logging(debug)

        host = host or os.getenv("FEATUREFORM_HOST")
        if host is None:
            raise RuntimeError(
                "If not in local mode then `host` must be passed or the environment"
                " variable FEATUREFORM_HOST must be set."
            )
        if insecure:
            channel = insecure_channel(host)
        else:
            channel = secure_channel(host, cert_path)
        self._stub = GrpcClient(ff_grpc.ApiStub(channel), debug=debug)
        self._host = host
        self._cert_path = cert_path or os.getenv("FEATUREFORM_CERT")
        self._insecure = insecure
        self.logger = logging.getLogger(__name__)

    def apply(self, asynchronous=False, verbose=False):
        """
        Apply all definitions, creating and retrieving all specified resources.

        ```python
        import featureform as ff
        client = ff.Client()

        ff.register_postgres(
            host="localhost",
            port=5432,
        )

        client.apply()
        ```

        Args:
            asynchronous (bool): If True, apply will return immediately and not wait for resources to be created. If False, apply will wait for resources to be created and print out the status of each resource.

        """

        try:
            resource_state = state()
            if resource_state.is_empty():
                print("No resources to apply")
                return

            print(f"Applying Run: {get_run()}")

            if self._dry_run:
                print(resource_state.sorted_list())
                return

            resource_state.create_all(
                self._stub,
                asynchronous,
                self._host,
                global_registrar.get_client_objects_for_resource(),
            )

            print()

            if not asynchronous and self._stub:
                resources = resource_state.sorted_list()
                display_statuses(self._stub, resources, self._host, verbose=verbose)
        finally:
            if feature_flag.is_enabled("FF_GET_EQUIVALENT_VARIANTS", True):
                set_run("")
            clear_state()

    DeletableResource = Union[
        FeatureColumnResource,
        SubscriptableTransformation,
        LabelColumnResource,
        TrainingSetVariant,
        ColumnSourceRegistrar,
        OnlineProvider,
        OfflineProvider,
    ]

    def delete(
        self,
        source: Union[DeletableResource, str],  #  TODO: get this typing correct
        variant: Optional[str] = None,
        resource_type: Optional[ResourceType] = None,
    ):
        """
        Delete a resource by name and variant or by resource object.

        There are three ways to delete a resource:
        1. Using a resource object (Feature, Label, Source, etc.)
        2. Using a provider object (OnlineProvider, OfflineProvider)
        3. Using a string name with required parameters

        Examples:
            ```python
            import featureform as ff
            client = ff.Client()

            # Delete using string name (requires variant and resource_type)
            client.delete("transactions", "kaggle", ff.ResourceType.SOURCE)

            # Delete using resource object
            feature = client.get_feature("my_feature", "v1")
            client.delete(feature)

            # Delete provider (no variant needed)
            client.delete("redis_provider", resource_type=ff.ResourceType.PROVIDER)
            ```

        Args:
            source: Either a resource object (Feature, Label, Source, etc.) or the name of the resource as a string.
                If a string is provided, resource_type is required and variant is required for non-provider resources.
            variant: Variant of the resource to delete. Required if source is a string and resource_type is not PROVIDER.
            resource_type: Type of resource to delete. Required if source is a string.

        Raises:
            ValueError: If source is a string and:
                - resource_type is not provided
                - variant is not provided (for non-provider resources)
        """
        # Prepare the request based on the input type
        request = self._create_delete_request(source, variant, resource_type)

        # Send the request to delete the resource
        self._stub.MarkForDeletion(request)
        print("Deleting resource async")

    def _create_delete_request(
        self,
        source: Union[DeletableResource, str],
        variant: Optional[str] = None,
        resource_type: Optional[ResourceType] = None,
    ) -> metadata_pb2.MarkForDeletionRequest:
        if isinstance(source, str):
            if resource_type is None:
                raise ValueError(
                    "resource_type must be specified if source is a string"
                )

            if resource_type is ResourceType.PROVIDER:
                return metadata_pb2.MarkForDeletionRequest(
                    resource_id=metadata_pb2.ResourceID(
                        resource=metadata_pb2.NameVariant(name=source),
                        resource_type=resource_type.to_proto(),
                    )
                )

            # All other string resources require a variant
            if not variant:
                raise ValueError("variant must be specified for non-provider resources")

            return metadata_pb2.MarkForDeletionRequest(
                resource_id=metadata_pb2.ResourceID(
                    resource=metadata_pb2.NameVariant(name=source, variant=variant),
                    resource_type=resource_type.to_proto(),
                )
            )

        if isinstance(source, (OfflineProvider, OnlineProvider)):
            return metadata_pb2.MarkForDeletionRequest(
                resource_id=metadata_pb2.ResourceID(
                    resource=metadata_pb2.NameVariant(name=source.name()),
                    resource_type=ResourceType.PROVIDER.to_proto(),
                )
            )

        name, variant = source.name_variant()
        return metadata_pb2.MarkForDeletionRequest(
            resource_id=metadata_pb2.ResourceID(
                resource=metadata_pb2.NameVariant(name=name, variant=variant),
                resource_type=source.get_resource_type().to_proto(),
            )
        )

    def run(self):
        """
        Run tasks for all definitions, creating and retrieving all specified resources.

        ```python
        import featureform as ff
        client = ff.Client()

        ff.register_postgres(
            host="localhost",
            port=5432,
        )

        client.run()
        ```
        """

        try:
            resource_state = state()
            if resource_state.is_empty():
                print("No resources to run")
                return

            if self._dry_run:
                print(resource_state.sorted_list())
                return

            resource_state.run_all(
                self._stub, global_registrar.get_client_objects_for_resource()
            )

        finally:
            clear_state()

    def get_user(self, name, local=False):
        """Get a user. Prints out name of user, and all resources associated with the user.

        **Examples:**

        ``` py title="Input"
        featureformer = rc.get_user("featureformer")
        ```

        ``` json title="Output"
        // get_user prints out formatted information on user
        USER NAME:                     featureformer
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        avg_transactions               quickstart                     feature
        fraudulent                     quickstart                     label
        fraud_training                 quickstart                     training set
        transactions                   kaggle                         source
        average_user_transaction       quickstart                     source
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(featureformer)
        ```

        ``` json title="Output"
        // get_user returns the User object

        name: "featureformer"
        features {
        name: "avg_transactions"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        sources {
        name: "transactions"
        variant: "kaggle"
        }
        sources {
        name: "average_user_transaction"
        variant: "quickstart"
        }
        ```

        Args:
            name (str): Name of user to be retrieved

        Returns:
            user (User): User
        """
        return get_user_info(self._stub, name)

    def get_entity(self, name, local=False):
        """Get an entity. Prints out information on entity, and all resources associated with the entity.

        **Examples:**

        ``` py title="Input"
        entity = rc.get_entity("user")
        ```

        ``` json title="Output"
        // get_entity prints out formatted information on entity

        ENTITY NAME:                   user
        STATUS:                        NO_STATUS
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        avg_transactions               quickstart                     feature
        fraudulent                     quickstart                     label
        fraud_training                 quickstart                     training set
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(postgres)
        ```

        ``` json title="Output"
        // get_entity returns the Entity object

        name: "user"
        features {
            name: "avg_transactions"
            variant: "quickstart"
        }
        labels {
            name: "fraudulent"
            variant: "quickstart"
        }
        trainingsets {
            name: "fraud_training"
            variant: "quickstart"
        }
        ```
        """
        return get_entity_info(self._stub, name)

    def get_model(self, name, local=False) -> Model:
        """Get a model. Prints out information on model, and all resources associated with the model.

        Args:
            name (str): Name of model to be retrieved

        Returns:
            model (Model): Model
        """
        model = None
        model_proto = get_resource_info(self._stub, "model", name)
        if model_proto is not None:
            model = Model(model_proto.name, description="", tags=[], properties={})

        return model

    def get_provider(self, name, local=False):
        """Get a provider. Prints out information on provider, and all resources associated with the provider.

        **Examples:**

        ``` py title="Input"
        postgres = client.get_provider("postgres-quickstart")
        ```

        ``` json title="Output"
        // get_provider prints out formatted information on provider

        NAME:                          postgres-quickstart
        DESCRIPTION:                   A Postgres deployment we created for the Featureform quickstart
        TYPE:                          POSTGRES_OFFLINE
        SOFTWARE:                      postgres
        STATUS:                        NO_STATUS
        -----------------------------------------------
        SOURCES:
        NAME                           VARIANT
        transactions                   kaggle
        average_user_transaction       quickstart
        -----------------------------------------------
        FEATURES:
        NAME                           VARIANT
        -----------------------------------------------
        LABELS:
        NAME                           VARIANT
        fraudulent                     quickstart
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        fraud_training                 quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(postgres)
        ```

        ``` json title="Output"
        // get_provider returns the Provider object

        name: "postgres-quickstart"
        description: "A Postgres deployment we created for the Featureform quickstart"
        type: "POSTGRES_OFFLINE"
        software: "postgres"
        serialized_config: "{\"Host\": \"quickstart-postgres\",
                            \"Port\": \"5432\",
                            \"Username\": \"postgres\",
                            \"Password\": \"password\",
                            \"Database\": \"postgres\"}"
        sources {
        name: "transactions"
        variant: "kaggle"
        }
        sources {
        name: "average_user_transaction"
        variant: "quickstart"
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        ```

        Args:
            name (str): Name of provider to be retrieved

        Returns:
            provider (Provider): Provider
        """
        return get_provider_info(self._stub, name)

    def get_feature(self, name, variant):
        return FeatureVariant.get_by_name_variant(self._stub, name, variant)

    def print_feature(self, name, variant=None, local=False):
        """Get a feature. Prints out information on feature, and all variants associated with the feature. If variant is included, print information on that specific variant and all resources associated with it.

        **Examples:**

        ``` py title="Input"
        avg_transactions = rc.get_feature("avg_transactions")
        ```

        ``` json title="Output"
        // get_feature prints out formatted information on feature

        NAME:                          avg_transactions
        STATUS:                        NO_STATUS
        -----------------------------------------------
        VARIANTS:
        quickstart                     default
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(avg_transactions)
        ```

        ``` json title="Output"
        // get_feature returns the Feature object

        name: "avg_transactions"
        default_variant: "quickstart"
        variants: "quickstart"
        ```

        ``` py title="Input"
        avg_transactions_variant = ff.get_feature("avg_transactions", "quickstart")
        ```

        ``` json title="Output"
        // get_feature with variant provided prints out formatted information on feature variant

        NAME:                          avg_transactions
        VARIANT:                       quickstart
        TYPE:                          float32
        ENTITY:                        user
        OWNER:                         featureformer
        PROVIDER:                      redis-quickstart
        STATUS:                        NO_STATUS
        -----------------------------------------------
        SOURCE:
        NAME                           VARIANT
        average_user_transaction       quickstart
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        fraud_training                 quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(avg_transactions_variant)
        ```

        ``` json title="Output"
        // get_feature returns the FeatureVariant object

        name: "avg_transactions"
        variant: "quickstart"
        source {
        name: "average_user_transaction"
        variant: "quickstart"
        }
        type: "float32"
        entity: "user"
        created {
        seconds: 1658168552
        nanos: 142461900
        }
        owner: "featureformer"
        provider: "redis-quickstart"
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        columns {
        entity: "user_id"
        value: "avg_transaction_amt"
        }
        ```

        Args:
            name (str): Name of feature to be retrieved
            variant (str): Name of variant of feature

        Returns:
            feature (Union[Feature, FeatureVariant]): Feature or FeatureVariant
        """
        if not variant:
            return get_resource_info(self._stub, "feature", name)
        return get_feature_variant_info(self._stub, name, variant)

    def _get_all_and_latest_variants(
        self, resource_name, resource_type, get_latest_variant=False
    ):
        if isinstance(resource_name, str):
            res_name = resource_name
            if resource_type is None:
                raise ValueError(
                    "A resource type param must be provided if the resource name param is of type string."
                )
            res_type = resource_type
        elif isinstance(
            resource_name,
            (
                FeatureColumnResource,
                LabelColumnResource,
                TrainingSetVariant,
                ColumnSourceRegistrar,
                SubscriptableTransformation,
            ),
        ):
            res_name = resource_name.name_variant()[0]
            res_type = resource_name.get_resource_type()
        elif isinstance(resource_name, Variants):
            if resource_type is None:
                raise ValueError(
                    f"{resource_name} is of type Variants. Please provide a resource type."
                )
            for _, res in resource_name.resources.items():
                res_name = res.name
                break
            res_type = resource_type
        else:
            raise ValueError(
                f"Expected input: string, Feature, Label, Source or Training Set object, actual input: {resource_name}."
            )

        search_name = metadata_pb2.NameRequest(name=metadata_pb2.Name(name=res_name))

        stub_get_functions = {
            ResourceType.FEATURE_VARIANT: self._stub.GetFeatures,
            ResourceType.ONDEMAND_FEATURE: self._stub.GetFeatures,
            ResourceType.LABEL_VARIANT: self._stub.GetLabels,
            ResourceType.SOURCE_VARIANT: self._stub.GetSources,
            ResourceType.TRAININGSET_VARIANT: self._stub.GetTrainingSets,
        }

        try:
            get_func = stub_get_functions[res_type]
        except KeyError:
            raise ValueError(
                f"Resource type {res_type.to_string()} doesnt have variants."
            )
        # Only the variants of the first resource are needed
        for x in get_func(iter([search_name])):
            return x.default_variant if get_latest_variant else x.variants

    def get_variants(self, resource_name, resource_type=None):
        """
        Get all variants of a resource.

        Args:
            resource_name (Union[str, FeatureColumnResource, LabelColumnResource, TrainingSetVariant, ColumnSourceRegistrar, Variants]): Name of resource or Resource object
            resource_type (ResourceType): Type of resource

        Returns:
            variants (List[str]): List of variants of the resource
        """
        return self._get_all_and_latest_variants(
            resource_name, resource_type, get_latest_variant=False
        )

    def latest_variant(self, resource_name, resource_type=None):
        """
        Get the most recent variant of a resource.

        Args:
            resource_name (Union[str, FeatureColumnResource, LabelColumnResource, TrainingSetVariant, ColumnSourceRegistrar, Variants]): Name of resource or Resource object
            resource_type (ResourceType): Type of resource

        Returns:
            variants (str): Latest variant of the resource
        """
        return self._get_all_and_latest_variants(
            resource_name, resource_type, get_latest_variant=True
        )

    def get_label(self, name, variant):
        return LabelVariant.get_by_name_variant(self._stub, name, variant)

    def print_label(self, name, variant=None, local=False):
        """Get a label. Prints out information on label, and all variants associated with the label. If variant is included, print information on that specific variant and all resources associated with it.

        **Examples:**

        ``` py title="Input"
        fraudulent = rc.get_label("fraudulent")
        ```

        ``` json title="Output"
        // get_label prints out formatted information on label

        NAME:                          fraudulent
        STATUS:                        NO_STATUS
        -----------------------------------------------
        VARIANTS:
        quickstart                     default
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(fraudulent)
        ```

        ``` json title="Output"
        // get_label returns the Label object

        name: "fraudulent"
        default_variant: "quickstart"
        variants: "quickstart"
        ```

        ``` py title="Input"
        fraudulent_variant = ff.get_label("fraudulent", "quickstart")
        ```

        ``` json title="Output"
        // get_label with variant provided prints out formatted information on label variant

        NAME:                          fraudulent
        VARIANT:                       quickstart
        TYPE:                          bool
        ENTITY:                        user
        OWNER:                         featureformer
        PROVIDER:                      postgres-quickstart
        STATUS:                        NO_STATUS
        -----------------------------------------------
        SOURCE:
        NAME                           VARIANT
        transactions                   kaggle
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        fraud_training                 quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(fraudulent_variant)
        ```

        ``` json title="Output"
        // get_label returns the LabelVariant object

        name: "fraudulent"
        variant: "quickstart"
        type: "bool"
        source {
        name: "transactions"
        variant: "kaggle"
        }
        entity: "user"
        created {
        seconds: 1658168552
        nanos: 154924300
        }
        owner: "featureformer"
        provider: "postgres-quickstart"
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        columns {
        entity: "customerid"
        value: "isfraud"
        }
        ```

        Args:
            name (str): Name of label to be retrieved
            variant (str): Name of variant of label

        Returns:
            label (Union[label, LabelVariant]): Label or LabelVariant
        """
        if not variant:
            return get_resource_info(self._stub, "label", name)
        return get_label_variant_info(self._stub, name, variant)

    def get_training_set(self, name, variant):
        return TrainingSetVariant.get_by_name_variant(self._stub, name, variant)

    def print_training_set(self, name, variant=None, local=False):
        """Get a training set. Prints out information on training set, and all variants associated with the training set. If variant is included, print information on that specific variant and all resources associated with it.

        **Examples:**

        ``` py title="Input"
        fraud_training = rc.get_training_set("fraud_training")
        ```

        ``` json title="Output"
        // get_training_set prints out formatted information on training set

        NAME:                          fraud_training
        STATUS:                        NO_STATUS
        -----------------------------------------------
        VARIANTS:
        quickstart                     default
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(fraud_training)
        ```

        ``` json title="Output"
        // get_training_set returns the TrainingSet object

        name: "fraud_training"
        default_variant: "quickstart"
        variants: "quickstart"
        ```

        ``` py title="Input"
        fraudulent_variant = ff.get_training set("fraudulent", "quickstart")
        ```

        ``` json title="Output"
        // get_training_set with variant provided prints out formatted information on training set variant

        NAME:                          fraud_training
        VARIANT:                       quickstart
        OWNER:                         featureformer
        PROVIDER:                      postgres-quickstart
        STATUS:                        NO_STATUS
        -----------------------------------------------
        LABEL:
        NAME                           VARIANT
        fraudulent                     quickstart
        -----------------------------------------------
        FEATURES:
        NAME                           VARIANT
        avg_transactions               quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(fraudulent_variant)
        ```

        ``` json title="Output"
        // get_training_set returns the TrainingSetVariant object

        name: "fraud_training"
        variant: "quickstart"
        owner: "featureformer"
        created {
        seconds: 1658168552
        nanos: 157934800
        }
        provider: "postgres-quickstart"
        features {
        name: "avg_transactions"
        variant: "quickstart"
        }
        label {
        name: "fraudulent"
        variant: "quickstart"
        }
        ```

        Args:
            name (str): Name of training set to be retrieved
            variant (str): Name of variant of training set

        Returns:
            training_set (Union[TrainingSet, TrainingSetVariant]): TrainingSet or TrainingSetVariant
        """
        if not variant:
            return get_resource_info(self._stub, "training-set", name)
        return get_training_set_variant_info(self._stub, name, variant)

    def get_source(self, name, variant):
        source_variant = SourceVariant.get_by_name_variant(self._stub, name, variant)
        return ColumnSourceRegistrar(global_registrar, source_variant)

    def print_source(self, name, variant=None, local=False):
        """Get a source. Prints out information on source, and all variants associated with the source. If variant is included, print information on that specific variant and all resources associated with it.

        **Examples:**

        ``` py title="Input"
        transactions = rc.get_transactions("transactions")
        ```

        ``` json title="Output"
        // get_source prints out formatted information on source

        NAME:                          transactions
        STATUS:                        NO_STATUS
        -----------------------------------------------
        VARIANTS:
        kaggle                         default
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(transactions)
        ```

        ``` json title="Output"
        // get_source returns the Source object

        name: "transactions"
        default_variant: "kaggle"
        variants: "kaggle"
        ```

        ``` py title="Input"
        transactions_variant = rc.get_source("transactions", "kaggle")
        ```

        ``` json title="Output"
        // get_source with variant provided prints out formatted information on source variant

        NAME:                          transactions
        VARIANT:                       kaggle
        OWNER:                         featureformer
        DESCRIPTION:                   Fraud Dataset From Kaggle
        PROVIDER:                      postgres-quickstart
        STATUS:                        NO_STATUS
        -----------------------------------------------
        DEFINITION:
        TRANSFORMATION

        -----------------------------------------------
        SOURCES
        NAME                           VARIANT
        -----------------------------------------------
        PRIMARY DATA
        Transactions
        FEATURES:
        NAME                           VARIANT
        -----------------------------------------------
        LABELS:
        NAME                           VARIANT
        fraudulent                     quickstart
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        fraud_training                 quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(transactions_variant)
        ```

        ``` json title="Output"
        // get_source returns the SourceVariant object

        name: "transactions"
        variant: "kaggle"
        owner: "featureformer"
        description: "Fraud Dataset From Kaggle"
        provider: "postgres-quickstart"
        created {
        seconds: 1658168552
        nanos: 128768000
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        primaryData {
        table {
            name: "Transactions"
        }
        }
        ```

        Args:
            name (str): Name of source to be retrieved
            variant (str): Name of variant of source

        Returns:
            source (Union[Source, SourceVariant]): Source or SourceVariant
        """
        if not variant:
            return get_resource_info(self._stub, "source", name)
        return get_source_variant_info(self._stub, name, variant)

    def list_features(self, local=False):
        """List all features.

        **Examples:**
        ``` py title="Input"
        features_list = rc.list_features()
        ```

        ``` json title="Output"
        // list_features prints out formatted information on all features

        NAME                           VARIANT                        STATUS
        user_age                       quickstart (default)           READY
        avg_transactions               quickstart (default)           READY
        avg_transactions               production                     CREATED
        ```

        ``` py title="Input"
        print(features_list)
        ```

        ``` json title="Output"
        // list_features returns a list of Feature objects

        [name: "user_age"
        default_variant: "quickstart"
        variants: "quickstart"
        , name: "avg_transactions"
        default_variant: "quickstart"
        variants: "quickstart"
        variants: "production"
        ]
        ```

        Returns:
            features (List[Feature]): List of Feature Objects
        """
        return list_name_variant_status(self._stub, "feature")

    def list_labels(self, local=False):
        """List all labels.

        **Examples:**
        ``` py title="Input"
        features_list = rc.list_labels()
        ```

        ``` json title="Output"
        // list_labels prints out formatted information on all labels

        NAME                           VARIANT                        STATUS
        user_age                       quickstart (default)           READY
        avg_transactions               quickstart (default)           READY
        avg_transactions               production                     CREATED
        ```

        ``` py title="Input"
        print(label_list)
        ```

        ``` json title="Output"
        // list_features returns a list of Feature objects

        [name: "user_age"
        default_variant: "quickstart"
        variants: "quickstart"
        , name: "avg_transactions"
        default_variant: "quickstart"
        variants: "quickstart"
        variants: "production"
        ]
        ```

        Returns:
            labels (List[Label]): List of Label Objects
        """
        return list_name_variant_status(self._stub, "label")

    def list_users(self, local=False):
        """List all users. Prints a list of all users.

        **Examples:**
        ``` py title="Input"
        users_list = rc.list_users()
        ```

        ``` json title="Output"
        // list_users prints out formatted information on all users

        NAME                           STATUS
        featureformer                  NO_STATUS
        featureformers_friend          CREATED
        ```

        ``` py title="Input"
        print(features_list)
        ```

        ``` json title="Output"
        // list_features returns a list of Feature objects

        [name: "featureformer"
        features {
        name: "avg_transactions"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        sources {
        name: "transactions"
        variant: "kaggle"
        }
        sources {
        name: "average_user_transaction"
        variant: "quickstart"
        },
        name: "featureformers_friend"
        features {
        name: "user_age"
        variant: "production"
        }
        sources {
        name: "user_profiles"
        variant: "production"
        }
        ]
        ```

        Returns:
            users (List[User]): List of User Objects
        """
        return list_name_status(self._stub, "user")

    def list_entities(self, local=False):
        """List all entities. Prints a list of all entities.

        **Examples:**
        ``` py title="Input"
        entities = rc.list_entities()
        ```

        ``` json title="Output"
        // list_entities prints out formatted information on all entities

        NAME                           STATUS
        user                           CREATED
        transaction                    CREATED
        ```

        ``` py title="Input"
        print(features_list)
        ```

        ``` json title="Output"
        // list_entities returns a list of Entity objects

        [name: "user"
        features {
        name: "avg_transactions"
        variant: "quickstart"
        }
        features {
        name: "avg_transactions"
        variant: "production"
        }
        features {
        name: "user_age"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        ,
        name: "transaction"
        features {
        name: "amount_spent"
        variant: "production"
        }
        ]
        ```

        Returns:
            entities (List[Entity]): List of Entity Objects
        """
        return list_name_status(self._stub, "entity")

    def list_sources(self, local=False):
        """List all sources. Prints a list of all sources.

        **Examples:**
        ``` py title="Input"
        sources_list = rc.list_sources()
        ```

        ``` json title="Output"
        // list_sources prints out formatted information on all sources

        NAME                           VARIANT                        STATUS                         DESCRIPTION
        average_user_transaction       quickstart (default)           NO_STATUS                      the average transaction amount for a user
        transactions                   kaggle (default)               NO_STATUS                      Fraud Dataset From Kaggle
        ```

        ``` py title="Input"
        print(sources_list)
        ```

        ``` json title="Output"
        // list_sources returns a list of Source objects

        [name: "average_user_transaction"
        default_variant: "quickstart"
        variants: "quickstart"
        , name: "transactions"
        default_variant: "kaggle"
        variants: "kaggle"
        ]
        ```

        Returns:
            sources (List[Source]): List of Source Objects
        """
        return list_name_variant_status_desc(self._stub, "source")

    def list_training_sets(self, local=False):
        """List all training sets. Prints a list of all training sets.

        **Examples:**
        ``` py title="Input"
        training_sets_list = rc.list_training_sets()
        ```

        ``` json title="Output"
        // list_training_sets prints out formatted information on all training sets

        NAME                           VARIANT                        STATUS                         DESCRIPTION
        fraud_training                 quickstart (default)           READY                          Training set for fraud detection.
        fraud_training                 v2                             CREATED                        Improved training set for fraud detection.
        recommender                    v1 (default)                   CREATED                        Training set for recommender system.
        ```

        ``` py title="Input"
        print(training_sets_list)
        ```

        ``` json title="Output"
        // list_training_sets returns a list of TrainingSet objects

        [name: "fraud_training"
        default_variant: "quickstart"
        variants: "quickstart", "v2",
        name: "recommender"
        default_variant: "v1"
        variants: "v1"
        ]
        ```

        Returns:
            training_sets (List[TrainingSet]): List of TrainingSet Objects
        """
        return list_name_variant_status_desc(self._stub, "training-set")

    def list_models(self, local=False) -> List[Model]:
        """List all models. Prints a list of all models.

        Returns:
            models (List[Model]): List of Model Objects
        """
        model_protos = list_name(self._stub, "model")
        # TODO: apply values from proto
        models = [Model(proto.name, tags=[], properties={}) for proto in model_protos]

        return models

    def list_providers(self, local=False):
        """List all providers. Prints a list of all providers.

        **Examples:**
        ``` py title="Input"
        providers_list = rc.list_providers()
        ```

        ``` json title="Output"
        // list_providers prints out formatted information on all providers

        NAME                           STATUS                         DESCRIPTION
        redis-quickstart               CREATED                      A Redis deployment we created for the Featureform quickstart
        postgres-quickstart            CREATED                      A Postgres deployment we created for the Featureform quickst
        ```

        ``` py title="Input"
        print(providers_list)
        ```

        ``` json title="Output"
        // list_providers returns a list of Providers objects

        [name: "redis-quickstart"
        description: "A Redis deployment we created for the Featureform quickstart"
        type: "REDIS_ONLINE"
        software: "redis"
        serialized_config: "{\"Addr\": \"quickstart-redis:6379\", \"Password\": \"\", \"DB\": 0}"
        features {
        name: "avg_transactions"
        variant: "quickstart"
        }
        features {
        name: "avg_transactions"
        variant: "production"
        }
        features {
        name: "user_age"
        variant: "quickstart"
        }
        , name: "postgres-quickstart"
        description: "A Postgres deployment we created for the Featureform quickstart"
        type: "POSTGRES_OFFLINE"
        software: "postgres"
        serialized_config: "{\"Host\": \"quickstart-postgres\", \"Port\": \"5432\", \"Username\": \"postgres\", \"Password\": \"password\", \"Database\": \"postgres\"}"
        sources {
        name: "transactions"
        variant: "kaggle"
        }
        sources {
        name: "average_user_transaction"
        variant: "quickstart"
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        ]
        ```

        Returns:
            providers (List[Provider]): List of Provider Objects
        """
        return list_name_status_desc(self._stub, "provider")

    def search(self, raw_query, local=False):
        """Search for registered resources. Prints a list of results.

        **Examples:**
        ``` py title="Input"
        providers_list = rc.search("transact")
        ```

        ``` json title="Output"
        // search prints out formatted information on all matches

        NAME                           VARIANT            TYPE
        avg_transactions               default            Source
        ```
        """
        if type(raw_query) != str or len(raw_query) == 0:
            raise Exception("query must be string and cannot be empty")
        processed_query = raw_query.translate({ord(i): None for i in ".,-@!*#"})
        return search(processed_query, self._host)


class EmbeddingColumnResource(ColumnResource):
    def __init__(
        self,
        transformation_args: tuple,
        dims: int,
        vector_db: Union[str, OnlineProvider, FileStoreProvider],
        entity: Union[Entity, str] = "",
        name="",
        variant="",
        owner: str = "",
        timestamp_column: str = "",
        description: str = "",
        schedule: str = "",
        tags: List[str] = [],
        properties: Dict[str, str] = {},
    ):
        """
        Embedding Feature registration object.

        **Example**
        ```
        @ff.entity
        class Speaker:
        # Register a column from a transformation as a label
            transaction_amount = ff.Embedding(
                vectorize_comments[["PK", "Vector"]],
                dims=384,
                vector_db=pinecone,
                description="Embeddings created from speakers' comments in episodes",
                variant="v1"
            )
        ```

        Args:
            transformation_args (tuple): A transformation or source function and the columns name in the format: <transformation_function>[[<entity_column>, <value_column>]]
            dims (int): Dimensionality of the embedding.
            vector_db (Union[str, OnlineProvider]): The name of the vector database to store the embeddings in.
            variant (str): An optional variant name for the feature.
            description (str): An optional description for the feature.
        """
        super().__init__(
            transformation_args=transformation_args,
            type=ScalarType.FLOAT32,
            resource_type=ResourceType.FEATURE_VARIANT,
            entity=entity,
            name=name,
            variant=variant,
            owner=owner,
            inference_store=vector_db,
            timestamp_column=timestamp_column,
            description=description,
            schedule=schedule,
            tags=tags,
            properties=properties,
        )
        if dims < 1:
            raise ValueError("Vector dimensions must be a positive integer")
        self.dims = dims

    def get_resources_by_type(
        self, resource_type: ResourceType
    ) -> Tuple[List[ColumnMapping], List[ColumnMapping]]:
        features, labels = super().get_resources_by_type(resource_type)
        features[0]["dims"] = self.dims
        features[0]["is_embedding"] = True
        return (features, labels)


def entity(cls):
    """
    Class decorator for registering entities and their associated features and labels.

    **Examples**
    ```python
    @ff.entity
    class User:
        avg_transactions = ff.Feature()
        fraudulent = ff.Label()
    ```

    Returns:
        entity (class): Decorated class
    """
    # 1. Use the lowercase name of the class as the entity name
    entity = register_entity(cls.__name__.lower())
    # 2. Given the Feature/Label/Variant class constructors are evaluated
    #    before the entity decorator, apply the entity name to their
    #    respective name dictionaries prior to registration
    for attr_name in cls.__dict__:
        if isinstance(
            cls.__dict__[attr_name],
            (FeatureColumnResource, LabelColumnResource, EmbeddingColumnResource),
        ):
            resource = cls.__dict__[attr_name]
            resource.name = attr_name if resource.name == "" else resource.name
            resource.entity = entity
            resource.register()
        elif isinstance(cls.__dict__[attr_name], Variants):
            variants = cls.__dict__[attr_name]
            for variant_key, resource in variants.resources.items():
                resource.name = attr_name if resource.name == "" else resource.name
                resource.entity = entity
                resource.register()
        elif isinstance(cls.__dict__[attr_name], MultiFeatureColumnResource):
            multi_feature_resources = cls.__dict__[attr_name].features
            for resource in multi_feature_resources:
                setattr(entity, resource.name, resource)
                resource.entity = entity
                resource.register()
    return cls


global_registrar = Registrar()
state = global_registrar.state
clear_state = global_registrar.clear_state
get_state = global_registrar.get_state
set_run = global_registrar.set_run
set_variant_prefix = global_registrar.set_variant_prefix
get_run = global_registrar.get_run
register_user = global_registrar.register_user
register_redis = global_registrar.register_redis
register_pinecone = global_registrar.register_pinecone
register_weaviate = global_registrar.register_weaviate
register_blob_store = global_registrar.register_blob_store
register_bigquery = global_registrar.register_bigquery
register_clickhouse = global_registrar.register_clickhouse
register_firestore = global_registrar.register_firestore
register_cassandra = global_registrar.register_cassandra
register_dynamodb = global_registrar.register_dynamodb
register_mongodb = global_registrar.register_mongodb
register_snowflake = global_registrar.register_snowflake
register_snowflake_legacy = global_registrar.register_snowflake_legacy
register_postgres = global_registrar.register_postgres
register_redshift = global_registrar.register_redshift
register_spark = global_registrar.register_spark
register_k8s = global_registrar.register_k8s
register_s3 = global_registrar.register_s3
register_hdfs = global_registrar.register_hdfs
register_gcs = global_registrar.register_gcs
register_entity = global_registrar.register_entity
register_column_resources = global_registrar.register_column_resources
register_feature_stream = global_registrar.register_feature_stream
register_label_stream = global_registrar.register_label_stream
register_training_set = global_registrar.register_training_set
register_model = global_registrar.register_model
sql_transformation = global_registrar.sql_transformation
register_sql_transformation = global_registrar.register_sql_transformation
get_entity = global_registrar.get_entity
get_source = global_registrar.get_source
get_redis = global_registrar.get_redis
get_postgres = global_registrar.get_postgres
get_dynamodb = global_registrar.get_dynamodb
get_mongodb = global_registrar.get_mongodb
get_snowflake = global_registrar.get_snowflake
get_snowflake_legacy = global_registrar.get_snowflake_legacy
get_redshift = global_registrar.get_redshift
get_bigquery = global_registrar.get_bigquery
get_clickhouse = global_registrar.get_clickhouse
get_spark = global_registrar.get_spark
get_kubernetes = global_registrar.get_kubernetes
get_blob_store = global_registrar.get_blob_store
get_s3 = global_registrar.get_s3
get_gcs = global_registrar.get_gcs
ondemand_feature = global_registrar.ondemand_feature
ResourceStatus = ResourceStatus

Nil = ScalarType.NIL
String = ScalarType.STRING
Int = ScalarType.INT
Int32 = ScalarType.INT32
Int64 = ScalarType.INT64
Float32 = ScalarType.FLOAT32
Float64 = ScalarType.FLOAT64
Bool = ScalarType.BOOL
DateTime = ScalarType.DATETIME
