#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import argparse
import base64
from decimal import Decimal
import datetime
import io
import json
import os
import sys
import types
import uuid
import logging
from enum import Enum
from pathlib import Path
from typing import List
from urllib.parse import urlparse
from abc import ABC, abstractmethod
import time
import importlib.util

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.config import Config
import dill
from pyspark.sql import Window
import pyspark.sql.functions as F
from azure.storage.blob import BlobServiceClient
from google.cloud import storage
from google.oauth2 import service_account
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import asc, col, when
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

FILESTORES = ["local", "s3", "azure_blob_store", "google_cloud_storage", "hdfs"]


class OutputFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"


class Headers(str, Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"


class JobType(str, Enum):
    TRANSFORMATION = "Transformation"
    MATERIALIZATION = "Materialization"
    TRAINING_SET = "Training Set"
    BATCH_FEATURES = "Batch Features"


if os.getenv("FEATUREFORM_LOCAL_MODE"):
    real_path = os.path.realpath(__file__)
    dir_path = os.path.dirname(real_path)
    LOCAL_DATA_PATH = f"{dir_path}/.featureform/data"
else:
    LOCAL_DATA_PATH = "dbfs:/transformation"


def main(args):
    job_type = ""
    if hasattr(args, "job_type"):
        job_type = args.job_type
    print(f"Starting execution of {args.transformation_type}\nJob Type: {job_type}")
    file_path = set_gcp_credential_file_path(
        args.store_type, args.spark_config, args.credential
    )

    output_location = ""
    try:
        if job_type == "Materialization" and args.direct_copy_use_iceberg:
            print(f"executing direct materialization")
            execute_materialization(
                credentials=args.credential,
                spark_configs=args.spark_config,
                sources=args.sources,
                target=args.direct_copy_target,
                table_name=args.direct_copy_table_name,
                feature_name=args.direct_copy_feature_name,
                variant_name=args.direct_copy_feature_variant,
                entity_column=args.direct_copy_entity_column,
                value_column=args.direct_copy_value_column,
                timestamp_column=args.direct_copy_timestamp_column,
            )
            return
        elif args.transformation_type == "sql":
            print(f'executing sql query: "{args.sql_query}"')
            output_location = execute_sql_query(
                job_type=args.job_type,
                output=args.output,
                sql_query=args.sql_query,
                spark_configs=args.spark_config,
                sources=args.sources,
                output_format=args.output_format,
                headers=args.headers,
                credentials=args.credential,
                is_update=args.is_update,
            )
        elif args.transformation_type == "df":
            output_location = execute_df_job(
                output=args.output,
                code=args.code,
                store_type=args.store_type,
                spark_configs=args.spark_config,
                headers=args.headers,
                credentials=args.credential,
                sources=args.sources,
                is_update=args.is_update,
            )

        print(
            f"Finished execution of {args.transformation_type}. Please check {output_location} for output file."
        )
    except Exception as e:
        error_message = f"the {args.transformation_type} job failed. Error: {e}"
        print(error_message)
        raise Exception(error_message)
    finally:
        delete_file(file_path)

    return output_location


class TableFormatClient(ABC):
    def __init__(self, spark, catalog, database):
        self._spark = spark
        self.default_catalog = catalog
        self.default_database = database

    def _format_table_identifier(self, catalog, database, table):
        catalog = catalog or self.default_catalog
        database = database or self.default_database
        table = table.lower()
        return f"{catalog}.{database}.{table}"

    def read_table(self, table, catalog=None, database=None):
        ident = self._format_table_identifier(database, catalog, table)
        print(f"Reading {self.table_format()} table {ident}")
        return self._read_table(ident)

    def table_exists(self, table, catalog=None, database=None):
        ident = self._format_table_identifier(database, catalog, table)
        print(f"Checking {self.table_format()} table {ident} exists")
        return self._spark.catalog.tableExists(ident)

    def create_table(self, table, df, catalog=None, database=None):
        ident = self._format_table_identifier(database, catalog, table)
        print(f"Creating {self.table_format()} table {ident}")
        self._create_table(ident, df)

    def delete_table(self, table, catalog=None, database=None):
        ident = self._format_table_identifier(database, catalog, table)
        print(f"Deleting {self.table_format()} table {ident}")
        self._delete_table(ident)

    # TODO
    # @abstractmethod
    # def _read_table_since(self, identifier, timestamp):
    #     pass

    @abstractmethod
    def _read_table(self, identifier):
        pass

    @abstractmethod
    def _create_table(self, identifier, df):
        pass

    @abstractmethod
    def _delete_table(self, identifier):
        pass

    @abstractmethod
    def table_format(self):
        pass


class IcebergClient(TableFormatClient):
    def __init__(self, spark, catalog, database):
        super().__init__(spark, catalog, database)

    def _read_table(self, identifier):
        print(f"Reading iceberg table {identifier}")
        return self._spark.read.format("org.apache.iceberg.spark.source.IcebergSource").load(identifier)

    def _create_table(self, identifier, df):
        df.writeTo(identifier).createOrReplace()

    def _delete_table(self, identifier):
        self._spark.sql(f"DROP TABLE {identifier} PURGE")

    def table_format(self):
        return "iceberg"


# Note that the Glue database requires a location to be set or CreatTable fails with:
# Can not create path with empty string.
class DeltaClient(TableFormatClient):
    def __init__(self, spark, database):
        # Delta uses the default catalog in our implementation.
        super().__init__(spark, "", database)

    def _read_table(self, identifier):
        return self._spark.read.format("delta").load(identifier)

    def _create_table(self, identifier, df):
        df.writeTo(identifier).using("delta").createOrReplace()

    def _delete_table(self, identifier):
        self._spark.sql(f"DROP TABLE {identifier} PURGE")

    def _format_table_identifier(self, catalog, database, table):
        # delta doesn't use the catalog
        database = database or self.default_database
        table = table.lower()
        return f"{database}.{table}"

    def table_format(self):
        return "delta"


# Only import these modules if they exist. Some people may not be
# using our Kafka provider and therefore not have it installed on
# their Spark cluster.
kafka_modules = ["kafka", "aws_msk_iam_sasl_signer"]
has_kafka_modules = True
for kafka_module in kafka_modules:
    if not importlib.util.find_spec(kafka_module):
        has_kafka_modules = False


if not has_kafka_modules:
    print("Kafka modules not installed, Kafka support not initialized")
else:
    from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
    from kafka.admin import NewTopic
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    # Used to get MSK auth tokens to access IAM auth-ed MSK clusters.
    class MSKTokenProvider:
        """
        Class to provide the MSK IAM authentication token using AWS credentials.
        """

        def __init__(self, region):
            self.region = region

        def token(self):
            """
            Generate the MSK IAM authentication token.
            :return: Token string.
            """
            token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
            return token

    class KafkaClient:
        def __init__(
            self,
            kafka_bootstrap_servers,
            kafka_topic,
            num_partitions=1,
            replication_factor=1,
            region="us-east-1",
        ):
            """
            Initializes the KafkaClient with Kafka bootstrap servers, topic, and IAM credentials.
            :param kafka_bootstrap_servers: Kafka bootstrap servers as a comma-separated string.
            :param kafka_topic: The Kafka topic to read from/write to.
            :param num_partitions: Number of partitions for the new topic.
            :param replication_factor: Replication factor for the new topic.
            """
            self.kafka_bootstrap_servers = kafka_bootstrap_servers
            self.kafka_topic = kafka_topic
            self.region = region
            self.num_partitions = num_partitions
            self.replication_factor = replication_factor
            self.admin_client = None
            self.token_provider = MSKTokenProvider(region)

        def __enter__(self):
            """
            Initializes the KafkaAdminClient in a context manager.
            :return: The KafkaClient instance.
            """
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_bootstrap_servers,
                security_protocol="SASL_SSL",
                sasl_mechanism="OAUTHBEARER",
                sasl_oauth_token_provider=self.token_provider,
            )
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            """
            Closes the KafkaAdminClient when exiting the context manager.
            """
            if self.admin_client:
                self.admin_client.close()

        def create_topic_if_not_exists(self):
            """
            Checks if a Kafka topic exists and creates it if it doesn't, using IAM authentication.
            :return: None
            """
            try:
                topics = self.admin_client.list_topics()
                if self.kafka_topic not in topics:
                    # Create the topic if it doesn't exist
                    topic = NewTopic(
                        name=self.kafka_topic,
                        num_partitions=self.num_partitions,
                        replication_factor=self.replication_factor,
                    )
                    self.admin_client.create_topics([topic])
                    print(f"Topic {self.kafka_topic} created.")
                else:
                    print(f"Topic {self.kafka_topic} already exists.")
            except Exception as e:
                print(f"Error creating topic: {e}")
                raise

        def create(self, key_value_data):
            """
            Writes key-value data to the Kafka topic using IAM authentication.
            :param key_value_data: List of tuples containing key-value pairs (key, value).
            :return: None
            """
            # Ensure the topic exists
            self.create_topic_if_not_exists()

            producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                security_protocol="SASL_SSL",
                sasl_mechanism="OAUTHBEARER",
                sasl_oauth_token_provider=self.token_provider,
            )

            for key, value in key_value_data:
                producer.send(self.kafka_topic, key=key, value=value)
                producer.flush()

            producer.close()

        def exists(self):
            """
            Checks if a Kafka topic exists using Kafka AdminClient with IAM authentication.
            :return: Boolean indicating if the topic exists.
            """
            try:
                topics = self.admin_client.list_topics()
                return self.kafka_topic in topics
            except Exception as e:
                print(f"Error checking topic existence: {e}")
                return False

        def delete(self):
            """
            Deletes a Kafka topic using Kafka AdminClient with IAM authentication.
            :return: None
            """
            try:
                self.admin_client.delete_topics([self.kafka_topic])
                print(f"Topic {self.kafka_topic} deleted.")
            except Exception as e:
                print(f"Error deleting topic: {e}")
                raise

        def read(self):
            """
            Reads messages from the Kafka topic using IAM authentication.
            :return: List of tuples containing key-value pairs (key, value).
            """
            consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                key_deserializer=lambda k: k.decode("utf-8"),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                security_protocol="SASL_SSL",
                sasl_mechanism="OAUTHBEARER",
                sasl_oauth_token_provider=self.token_provider,
            )

            key_value_data = []
            for message in consumer:
                print(message.key, message.value)

            consumer.close()
            return key_value_data

        def write(self, key_value_data):
            """
            Writes key-value data to the Kafka topic using IAM authentication.
            :param key_value_data: List of tuples containing key-value pairs (key, value).
            :return: None
            """
            self.create(key_value_data)


# Used to only pass properties that can be serialized across Spark
# executors.
def write_partition_closure(region_name, access_key, secret_key, table_name):
    def write_partition(partition):
        # These all have to be recreated on each executor since it can't be serialized
        # and passed around.
        config = Config(
            retries={
                "max_attempts": 100,  # Maximum number of retry attempts
                "mode": "adaptive",  # Exponential backoff
            }
        )
        # Initialize the DynamoDB resource with the retry configuration
        dynamodb = boto3.resource(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=config,  # Apply the retry config here
        )
        # Initialize the DynamoDB client with the retry configuration
        dynamodb_client = boto3.client(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=config,  # Apply the retry config here
        )
        dynamodb_table = dynamodb.Table(table_name)

        def write_batch(table, items):
            """
            Helper function to perform batch writes and handle unprocessed items.
            """
            while items:
                response = dynamodb_client.batch_write_item(
                    RequestItems={table.name: items}
                )

                # Check if there are unprocessed items
                unprocessed_items = response.get("UnprocessedItems", {})

                if unprocessed_items:
                    # Retry unprocessed items after a delay
                    items = unprocessed_items[table.name]
                    time.sleep(1)  # Simple backoff before retrying
                else:
                    break  # Exit loop when all items are processed

        batch_size = 25  # DynamoDB batch write limit
        items_to_write = []
        # Dynamo needs values in the:
        # {'S': <value>} type format. This serializer does that conversion for us.
        serializer = TypeSerializer()
        for row in partition:
            # Boto3 doesn't support floats
            row_dict = {k: Decimal(str(v)) if isinstance(v, float) else v for k, v in row.asDict().items()}
            item = {k: serializer.serialize(v) for k, v in row_dict.items()}
            items_to_write.append({"PutRequest": {"Item": item}})
            # If we've reached the batch size limit, write the batch
            if len(items_to_write) == batch_size:
                write_batch(dynamodb_table, items_to_write)
                items_to_write = []  # Reset the batch
        # Write any remaining items that didn't reach the batch size
        if items_to_write:
            write_batch(dynamodb_table, items_to_write)

    return write_partition


class DynamoDBClient:
    # TODO access key and secret key not always necessary.
    def __init__(self, region_name: str, access_key: str, secret_key: str):
        self.region_name = region_name
        self.access_key = access_key
        self.secret_key = secret_key

        self.config = Config(
            retries={
                "max_attempts": 100,  # Maximum number of retry attempts
                "mode": "adaptive",  # Exponential backoff
            }
        )

        # Initialize the DynamoDB resource with the retry configuration
        self.dynamodb = boto3.resource(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=self.config,  # Apply the retry config here
        )

        # Initialize the DynamoDB client with the retry configuration
        self.dynamodb_client = boto3.client(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=self.config,  # Apply the retry config here
        )

        self.logger = logging.getLogger(__name__)

    # Make entity_column type configurable
    def create_table(self, table_name: str, entity_column: str):
        """
        Creates a DynamoDB table with auto-scaling (on-demand) capacity mode.
        The entity_column will always be used as the partition key and will always be a string.

        :param table_name: Name of the table to create.
        :param entity_column: Column name that will be used as the partition key (hash key) - always a string.
        """
        attribute_definitions = [
            {
                "AttributeName": entity_column,
                "AttributeType": "S",  # Always a string for the partition key
            }
        ]

        key_schema = [
            {"AttributeName": entity_column, "KeyType": "HASH"}  # Partition key
        ]
        print(f"Creating table {table_name} with entity {entity_column}.")
        table = self.dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode="PAY_PER_REQUEST",  # On-demand billing mode
        )
        table.wait_until_exists()
        self.logger.info(f"Table {table_name} created successfully with auto-scaling.")

    def delete_table(self, table_name: str):
        """
        Deletes a DynamoDB table.

        :param table_name: Name of the table to delete.
        """
        try:
            table = self.dynamodb.Table(table_name)
            table.delete()
            table.wait_until_not_exists()
            self.logger.info(f"Table {table_name} deleted successfully.")
        except Exception as e:
            self.logger.error(f"Failed to delete table: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a DynamoDB table exists.

        :param table_name: Name of the table to check.
        :return: True if the table exists, False otherwise.
        """
        try:
            table = self.dynamodb.Table(table_name)
            table.load()  # Loads the table details, raises an exception if it doesn't exist
            return True
        except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
            return False
        except Exception as e:
            self.logger.error(f"Error checking if table {table_name} exists: {e}")
            return False

    def _estimate_row_size(self, schema: StructType):
        """
        Estimate the size of a row based on the schema of the DataFrame.
        """
        # Map each data type to an approximate size in bytes
        data_type_sizes = {
            "IntegerType": 4,  # 4 bytes for an integer
            "LongType": 8,  # 8 bytes for a long
            "DoubleType": 8,  # 8 bytes for a double
            "FloatType": 4,  # 4 bytes for a float
            "BooleanType": 1,  # 1 byte for a boolean
            "TimestampType": 8,  # 8 bytes for a timestamp
            "StringType": 50,  # Assume average 50 bytes per string (adjustable)
        }

        # Start with a size of 0 bytes
        total_row_size = 0

        # Iterate through each field in the schema
        for field in schema.fields:
            data_type_name = type(field.dataType).__name__
            if data_type_name in data_type_sizes:
                total_row_size += data_type_sizes[data_type_name]
            else:
                # For unsupported or complex types, assume an estimated size (adjust as needed)
                self.logger.warning(
                    f"Warning: {data_type_name} not in predefined sizes, assuming 100 bytes"
                )
                total_row_size += (
                    100  # Assign a default size for complex or unknown types
                )

        return total_row_size

    def _calculate_num_partitions(self, df: DataFrame, target_partition_size_mb=128):
        """
        Calculate the number of partitions based on the estimated size of each row and the total size of the DataFrame.
        """
        # Get the schema of the DataFrame
        schema = df.schema

        # Estimate the row size
        row_size_in_bytes = self._estimate_row_size(schema)

        # Count the number of rows (can be expensive, consider optimizing or using an approximation)
        num_rows = df.count()

        # Calculate the total size of the DataFrame in MB
        total_size_in_bytes = num_rows * row_size_in_bytes
        total_size_in_mb = total_size_in_bytes / (1024 * 1024)  # Convert to MB

        # Calculate the number of partitions based on the total size and the target partition size
        num_partitions = max(1, int(total_size_in_mb / target_partition_size_mb))

        self.logger.info(f"Estimated DataFrame size: {total_size_in_mb} MB")
        self.logger.info(
            f"Repartitioning DataFrame into {num_partitions} partitions based on target partition size of {target_partition_size_mb} MB"
        )

        return num_partitions

    def write_df(self, table_name: str, df):
        """
        Writes a Spark DataFrame to DynamoDB using foreachPartition.

        :param table_name: The name of the DynamoDB table.
        :param df: The Spark DataFrame to write to DynamoDB.
        """

        write_partition_fn = write_partition_closure(
            self.region_name, self.access_key, self.secret_key, table_name
        )
        # Estimate the number of partitions based on DataFrame size
        num_partitions = self._calculate_num_partitions(df)

        self.logger.info(f"Repartitioning to {num_partitions} before writing.")
        # Repartition the DataFrame based on the calculated number of partitions
        df = df.repartition(num_partitions)

        try:
            df.foreachPartition(write_partition_fn)
            self.logger.info(
                f"DataFrame written to DynamoDB table {table_name} successfully."
            )
        except Exception as e:
            self.logger.error(f"Failed to write DataFrame to table {table_name}: {e}")
            raise


class LatestFeaturesTransform:
    def __init__(self, entity_col, value_cols, timestamp_col):
        self.entity_col = entity_col
        self.value_cols = value_cols
        self.timestamp_col = timestamp_col

    def apply(self, df):
        window_spec = Window.partitionBy(self.entity_col).orderBy(
            F.desc(self.timestamp_col)
        )
        df_with_row_num = df.withColumn("row_num", F.row_number().over(window_spec))
        result_df = df_with_row_num.filter(F.col("row_num") == 1).drop("row_num")
        select_columns = [self.entity_col] + self.value_cols + [self.timestamp_col]
        return result_df.select(*select_columns)


class IcebergMaterializationTable:
    def __init__(self, iceberg, table_name):
        self._iceberg = iceberg
        self._spark = iceberg._spark
        self._identifier = iceberg._format_table_identifier(None, None, table_name)

    # Consider putting this directly into Iceberg and Delta clients.
    def merge_in(self, df, entity_col, value_col, timestamp_col=None):
        # Other columns will get added automatically with the MERGE in Iceberg due to
        # automatic schema evolution
        # TODO make ts a TIMESTAMP
        print(f"identifier: {self._identifier}")
        # Even if timestamp col is None, other features may have timestamps so we still
        # include it.
        self._spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS {self._identifier} (
            entity STRING,
            ts STRING
        ) USING iceberg
        """
        )

        field_type_map = {
            field.name: field.dataType.simpleString().upper()
            for field in df.schema.fields
        }
        if value_col in field_type_map:
            feature_value_type = field_type_map[value_col]
        else:
            raise ValueError(f"Column '{value_col}' not found in the DataFrame")

        alter_table_query = f"""
        ALTER TABLE {self._identifier}
        ADD COLUMNS ({value_col} {feature_value_type})
        """
        # Try to execute the ALTER TABLE command and catch any errors
        try:
            self._spark.sql(alter_table_query)
            print(f"Column '{value_col}' added successfully.")
        except Exception as e:
            # No better way to check this error
            if "already exists" in str(e):
                print(f"Column '{value_col}' already exists in the table, skipping.")
            else:
                raise e  # Re-raise if it's a different error

        aliased_df = df.withColumnRenamed(entity_col, "entity")
        if timestamp_col is not None:
            aliased_df = aliased_df.withColumnRenamed(timestamp_col, "ts")
        aliased_df.createOrReplaceTempView("incoming_data")

        # Dynamically generate the SQL for the update clause based on df columns. This is so
        # we don't override other feature columns that may already exist in the table.
        val_cols = [value_col]
        if timestamp_col is not None:
            val_cols.append("ts")
        update_clause = ", ".join([f"t.{col} = d.{col}" for col in val_cols])

        # Insert only necessary columns in a new row
        insert_cols = ["entity", value_col]
        if timestamp_col is not None:
            insert_cols.append("ts")
        insert_val_clause = ",".join(["d." + col for col in insert_cols])
        insert_source_clause = ",".join(insert_cols)
        insert_clause = f"INSERT ({insert_source_clause}) VALUES ({insert_val_clause})"

        merge_query = f"""
        MERGE INTO {self._identifier} t
        USING incoming_data d
        ON t.entity = d.entity
        WHEN MATCHED THEN
        UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
        {insert_clause}
        """
        print(merge_query)
        self._spark.sql(merge_query)


class DynamoMaterializationTable:
    def __init__(self, dynamo, table_name, feature_name):
        self.dynamo = dynamo
        self.table_name = table_name
        self.feature_name = feature_name

    def merge_in(self, df, entity_col, value_col):
        if not self.dynamo.table_exists(self.table_name):
            raise Exception(f"Dynamo table {self.table_name} does not exist")
        dynamo_df = (
            df.select(entity_col, value_col)
            .withColumn(entity_col, F.col(entity_col).cast("string"))
            .withColumnRenamed(entity_col, self.feature_name)
            .withColumnRenamed(value_col, "FeatureValue")
        )
        self.dynamo.write_df(self.table_name, dynamo_df)


class MaterializeToIcebergAndDynamoV1Operation:
    def __init__(self, dynamo, iceberg, feature_name, feature_variant, table_name):
        self._dynamo = dynamo
        self._iceberg = iceberg
        self.feature_name = feature_name
        self.feature_variant = feature_variant
        self.table_name = table_name

    def run(self, df, entity_col, value_col, timestamp_col=None):
        latest_df = df
        if timestamp_col is not None:
            latest_df = LatestFeaturesTransform(
                entity_col, [value_col], timestamp_col
            ).apply(df)
        # This matches existing schemas
        aliased_df = latest_df.withColumnRenamed(value_col, "FeatureValue")
        value_col = "FeatureValue"
        iceberg_table = IcebergMaterializationTable(self._iceberg, self.table_name)
        iceberg_table.merge_in(aliased_df, entity_col, value_col, timestamp_col)
        dynamo_table = DynamoMaterializationTable(
            self._dynamo, self.table_name, self.feature_name
        )
        dynamo_table.merge_in(aliased_df, entity_col, value_col)


def execute_materialization(
    credentials,
    spark_configs,
    sources,
    target,
    table_name,
    feature_name,
    variant_name,
    entity_column,
    value_column,
    timestamp_column,
):
    try:
        spark = SparkSession.builder.appName("Execute SQL Query")
        set_spark_configs(spark, spark_configs)
        spark = spark.getOrCreate()
        if len(sources) > 1:
            raise Exception(f"Cannot materialize from more than one source: {sources}")
        source = sources[0]
        source_df = get_source_df(source, credentials, False, spark)
        if target != "dynamo":
            raise Exception(f"Cannot directly materialize into {target}")
        access = credentials.get("dynamo_aws_access_key_id")
        secret = credentials.get("dynamo_aws_secret_access_key")
        region = credentials.get("dynamo_aws_region")
        dynamo = DynamoDBClient(region, access, secret)
        iceberg = IcebergClient(spark, "ff_catalog", "featureform")
        op = MaterializeToIcebergAndDynamoV1Operation(
            dynamo,
            iceberg,
            feature_name,
            variant_name,
            table_name,
        )
        op.run(source_df, entity_column, value_column, timestamp_column)
    except Exception as e:
        print(e)
        raise e


def execute_sql_query(
    job_type,
    output,
    sql_query,
    spark_configs,
    sources,
    output_format,
    headers,
    credentials,
    is_update=False,
):
    # Executes the SQL Queries:
    # Parameters:
    #     job_type: string ("Transformation", "Materialization", "Training Set")
    #     output: dict (s3 path or AWS Glue table name)
    #     sql_query: string (eg. "SELECT * FROM source_0)
    #     spark_configs: dict (eg. {"fs.azure.account.key.account_name.dfs.core.windows.net": "aksdfkai=="})
    #     sources: List(dict) containing the location of sources, their provider type and possible information about the file/directory
    # Return:
    #     output_uri_with_timestamp: string (output s3 path)
    try:
        spark = SparkSession.builder.appName("Execute SQL Query")
        set_spark_configs(spark, spark_configs)
        spark = spark.getOrCreate()

        if job_type in list(JobType):
            for i, source in enumerate(sources):
                source_df = get_source_df(source, credentials, is_update, spark)
                source_df.createOrReplaceTempView(f"source_{i}")
        else:
            raise Exception(
                f"the '{job_type}' is not supported. Supported types: 'Transformation', 'Materialization', 'Training Set'"
            )

        print("Executing SQL query")
        output_dataframe = spark.sql(sql_query)
        _validate_output_df(output_dataframe)
        print("Successfully executed SQL query")
        output_location = output.get("outputLocation")
        output_location_type = output.get("locationType")
        # TODO: rename this variable to something more accurate once locations have been
        # implemented for DF transformations
        output_uri_with_timestamp = ""
        print(f"Writing output to {output_location} of type {output_location_type}")
        if output_location_type == "filestore":
            dt = datetime.datetime.now()
            safe_datetime = dt.strftime("%Y-%m-%d-%H-%M-%S-%f")

            # remove the '/' at the end of output_uri in order to avoid double slashes in the output file path.
            output_uri_with_timestamp = f"{output_location.rstrip('/')}/{safe_datetime}"

            if output_format == OutputFormat.PARQUET:
                if headers == Headers.EXCLUDE:
                    raise Exception(
                        f"the output format '{output_format}' does not support excluding headers. Supported types: 'csv'"
                    )
                output_dataframe.write.option("header", "true").mode(
                    "overwrite"
                ).parquet(output_uri_with_timestamp)
            elif output_format == OutputFormat.CSV:
                if headers == Headers.EXCLUDE:
                    output_dataframe.write.mode("overwrite").csv(
                        output_uri_with_timestamp
                    )
                else:
                    output_dataframe.write.option("header", "true").mode(
                        "overwrite"
                    ).csv(output_uri_with_timestamp)
                print(f"Successfully wrote CSV output {output_uri_with_timestamp}")
        elif output_location_type == "catalog":
            table_format = output.get("tableFormat")

            if table_format == "iceberg":
                glue_table = "ff_catalog." + output_location
                print("Writing to iceberg table: ", glue_table)
                final_write_obj = output_dataframe.writeTo(glue_table)

                # TODO: (Erik) determine how to arrive at the path for this write option
                # final_write_obj.option("write.object-storage.enabled", "true").option(
                #     "write.data.path", ""
                # )

                print("creating/replacing table")
                final_write_obj.createOrReplace()
            elif table_format == "delta":
                output_location = output_location.replace("-", "_")
                print("Writing to delta table: ", output_location)
                final_write_obj = output_dataframe.writeTo(output_location).using(
                    "delta"
                )

                final_write_obj.append()
                final_write_obj.createOrReplace()

                print(f"Delta table {output_location} created successfully")
            else:
                raise Exception(
                    f"the table format '{table_format}' is not supported. Supported types: Apache Iceberg and Delta Lake"
                )
        else:
            raise Exception(
                f"the output format '{output_format}' is not supported. Supported types: 'parquet', 'csv'"
            )
        print("Successfully completed SQL job")
        return output_uri_with_timestamp
    except Exception as e:
        print(e)
        raise e


def get_source_df(source, credentials, is_update, spark):
    location = source.get("location")
    location_type = source.get("locationType")
    print(f"Reading source: {location} of type {location_type}")
    if location_type == "catalog":
        table_format = source.get("tableFormat")
        if table_format == "iceberg":
            table = "ff_catalog." + location
            role_arn = source.get("awsAssumeRoleArn")
            spark_reader = spark.read.format("org.apache.iceberg.spark.source.IcebergSource")
            has_new_data = True
            if role_arn is not None:
                spark.conf.set("spark.hadoop.fs.s3a.assumed.role.arn", role_arn)
                source_df = spark_reader.load(table)
                spark.conf.unset("spark.hadoop.fs.s3a.assumed.role.arn")
            else:
                source_df = spark_reader.load(table)
            if not has_new_data:
                source_df = source_df.limit(0)
            return source_df
        elif table_format == "delta":
            print(f"Reading Delta table: {location}")
            if source.get("isIncremental") and is_update:
                source_df = get_incremental_delta_records(spark, location)
            else:
                source_df = spark.read.format("delta").table(location)
            return source_df
        else:
            raise Exception(f"Unsupported table format: {table_format} for {location}")
    elif location_type == "sql" and source.get("provider") == "SNOWFLAKE_OFFLINE":
        print(f"Reading Snowflake table: {location}")
        options = {
            "sfURL": credentials.get("sfURL"),
            "sfUser": credentials.get("sfUser"),
            "sfPassword": credentials.get("sfPassword"),
            "sfWarehouse": credentials.get("sfWarehouse"),
            "sfDatabase": source.get("database"),
            "sfSchema": source.get("schema"),
            "dbtable": location,
        }
        source_df = spark.read.format("snowflake").options(**options).load()

        timestamp_column = source.get("timestampColumnName")
        return source_df
    elif location_type == "sql" and source.get("provider") == "BIGQUERY_OFFLINE":
        print(f"Reading Bigquery table: {location}")
        # Validate required credentials
        required_creds = ["bqProjectId", "bqDatasetId", "bqCreds"]
        missing_creds = [cred for cred in required_creds if not credentials.get(cred)]
        if missing_creds:
            raise ValueError(f"Missing required BigQuery credentials: {', '.join(missing_creds)}")

        projId = credentials.get("bqProjectId")
        datasetId = credentials.get("bqDatasetId")

        # Validate table identifier format
        if not location or not location.strip():
            raise ValueError("BigQuery table name cannot be empty")

        options = {
            "credentials": credentials.get("bqCreds"),
            "parentProject": projId,
            "viewsEnabled": "true",
            "table": f"{projId}.{datasetId}.{location}",
        }
        try:
            source_df = spark.read.format("bigquery").options(**options).load()
        except Exception as e:
            raise Exception(f"Failed to read from BigQuery table: {str(e)}")
        timestamp_column = source.get("timestampColumnName")
        return source_df
    elif location_type == "filestore":
        file_extension = Path(location).suffix
        is_directory = file_extension == ""

        if file_extension == ".csv":
            print(f"Reading CSV file: {location}")
            source_df = (
                spark.read.option("header", "true")
                .option("ignoreCorruptFiles", "true")
                .option("recursiveFileLookup", "true")
                .csv(location)
            )
            return source_df
        elif file_extension == ".parquet" or is_directory:
            print(f"Reading Parquet file: {location}")
            source_df = (
                spark.read.option("header", "true")
                .option("ignoreCorruptFiles", "true")
                .option("recursiveFileLookup", "true")
                .parquet(location)
            )
            return source_df
        else:
            raise Exception(
                f"the file type {file_extension} for '{source}' file is not supported."
            )
    else:
        raise Exception(
            f"the location type '{location_type}' for '{location}' with  provider '{source.get('provider')}' is not supported."
        )


def partition_delta_by_timestamp(df, output_location, column):
    df = df.withColumn("date", F.date_format(F.col(column), "yyyy-MM-dd"))

    # Write the table with partitioning
    return (
        df.sortWithinPartitions(column)
        .writeTo(output_location)
        .using("delta")
        .partitionedBy("date")
    )


def get_incremental_delta_records(spark, table, last_run_timestamp):
    # 1. Check to ensure the table has delta.enableChangeDataFeed; if it doesn't
    # we cannot proceed given we won't be able to use the read options that make this
    # operation efficient
    rows = spark.sql(
        f"SHOW TBLPROPERTIES {table}('delta.enableChangeDataFeed')"
    ).collect()

    if rows[0].value != "true":
        raise Exception(
            "{table} does not have property delta.enableChangeDataFeed enabled; cannot read incremental records"
        )

    # 2. Create a temporary view from the describe query given it cannot be used as a subquery;
    # this will allow use to check if the table has been overwritten since the last run and subsequently
    # read the records that were appended since the last run
    table_history = spark.sql(f"DESCRIBE HISTORY {table}")
    table_history.createOrReplaceTempView("table_history")

    overwrite_check = f"""
    SELECT
        COUNT(*) AS cnt
    FROM table_history
    WHERE
        timestamp > TO_TIMESTAMP({last_run_timestamp})
        AND operation IN ('WRITE', 'CREATE OR REPLACE', 'REPLACE')
        AND (operationParameters['mode'] = 'Overwrite' OR operationParameters['mode'] IS NULL)
    """

    rows = spark.sql(overwrite_check).collect()

    if rows[0].cnt != 0:
        raise Exception(f"{table} as been overwritten since last run")

    has_new_data = f"""
    SELECT
        version
    FROM table_history
    WHERE
        timestamp > TO_TIMESTAMP({last_run_timestamp})
        AND isBlindAppend
    """

    rows = spark.sql(has_new_data).collect()

    if len(rows) == 0:
        raise Exception(f"No new snapshots for {table}")

    last_run_version = rows[0].version

    if not isinstance(last_run_version, int):
        raise Exception(
            f"Expected last_run_version to be int; found {type(last_run_version)} instead"
        )

    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", last_run_version)
        .table(table)
    )


def execute_df_job(
    output,
    code,
    store_type,
    spark_configs,
    headers,
    credentials,
    sources,
    partition_options=None,
    output_format=OutputFormat.PARQUET,
    is_update=False,
):
    # Executes the DF transformation:
    # Parameters:
    #     output_uri: string (s3 paths)
    #     code: code (python code)
    #     sources: List(dict) containing the location of sources, their provider type and possible information about the file/directory
    # Return:
    #     output_uri_with_timestamp: string (output s3 path)
    spark = SparkSession.builder.appName("Dataframe Transformation")
    set_spark_configs(spark, spark_configs)
    spark = spark.getOrCreate()

    print(f"reading {len(sources)} source files")
    func_parameters: List[DataFrame] = []
    for source in sources:
        source_df = get_source_df(source, credentials, is_update, spark)
        func_parameters.append(source_df)

    try:
        code = get_code_from_file(code, store_type, credentials)
        func = types.FunctionType(code, globals(), "df_transformation")
        output_dataframe = func(*func_parameters)
        _validate_output_df(output_dataframe)

        dt = datetime.datetime.now()
        safe_datetime = dt.strftime("%Y-%m-%d-%H-%M-%S-%f")

        output_location = output.get("outputLocation")
        output_location_type = output.get("locationType")
        print(f"Writing output to {output_location} of type {output_location_type}")

        # remove the '/' at the end of output_uri in order to avoid double slashes in the output file path.
        output_uri_with_timestamp = f"{output_location.rstrip('/')}/{safe_datetime}"

        if output_location_type == "filestore":
            dt = datetime.datetime.now()
            safe_datetime = dt.strftime("%Y-%m-%d-%H-%M-%S-%f")

            # remove the '/' at the end of output_uri in order to avoid double slashes in the output file path.
            output_uri_with_timestamp = f"{output_location.rstrip('/')}/{safe_datetime}"
            print(f"Writing output to {output_uri_with_timestamp}", flush=True)

            if output_format == OutputFormat.PARQUET:
                if headers == Headers.EXCLUDE:
                    raise Exception(
                        f"the output format '{output_format}' does not support excluding headers. Supported types: 'csv'"
                    )
                output_dataframe.write.mode("overwrite").option(
                    "header", "true"
                ).parquet(output_uri_with_timestamp)
                print(
                    f"Successfully wrote Parquet output {output_uri_with_timestamp}",
                    flush=True,
                )
            elif output_format == OutputFormat.CSV:
                if headers == Headers.EXCLUDE:
                    output_dataframe.write.mode("overwrite").csv(
                        output_uri_with_timestamp
                    )
                else:
                    output_dataframe.write.option("header", "true").mode(
                        "overwrite"
                    ).csv(output_uri_with_timestamp)
                print(
                    f"Successfully wrote CSV output {output_uri_with_timestamp}",
                    flush=True,
                )
        elif output_location_type == "catalog":
            table_format = output.get("tableFormat")

            if table_format == "iceberg":
                glue_table = "ff_catalog." + output_location
                print("Writing to iceberg table: ", glue_table)
                final_write_obj = output_dataframe.writeTo(glue_table)

                # TODO: (Erik) determine how to arrive at the path for this write option
                # final_write_obj.option("write.object-storage.enabled", "true").option(
                #     "write.data.path", ""
                # )

                final_write_obj.createOrReplace()
            elif table_format == "delta":
                output_location = output_location.replace("-", "_")
                print("Writing to delta table: ", output_location)
                print("Partition options: ", partition_options)
                final_write_obj = output_dataframe.writeTo(output_location).using(
                    "delta"
                )

                final_write_obj.createOrReplace()

                print(f"Delta table {output_location} created successfully")
            else:
                raise Exception(
                    f"the table format '{table_format}' is not supported. Supported types: Apache Iceberg and Delta Lake"
                )
        else:
            raise Exception(
                f"the output format '{output_format}' is not supported. Supported types: 'parquet', 'csv'"
            )

        return output_uri_with_timestamp
    except (IOError, OSError) as e:
        print(f"Issue with execution of the transformation: {e}")
        raise e
    except Exception as e:
        error = check_dill_exception(e)
        raise error


def _validate_output_df(output_df):
    if output_df is None:
        raise Exception("the transformation code returned None.")
    if type(output_df).__name__ != "DataFrame":
        raise TypeError(
            f"Expected output to be of type 'DataFrame', "
            f"got '{type(output_df).__name__}' instead.\n"
            f"Please make sure that the transformation code returns a dataframe."
        )


def get_code_from_file(
    file_path,
    store_type=None,
    credentials=None,
):
    # Reads the code from a pkl file into a python code object.
    # Then this object will be used to execute the transformation.

    # Parameters:
    #     file_path: string (path to file)
    #     store_type: string ("s3" | "azure_blob_store" | "google_cloud_storage")
    #     credentials: dict
    # Return:
    #     code: code object that could be executed

    print(f"Retrieving transformation code from {file_path} in {store_type}.")

    if store_type == "s3":
        # S3 paths are the following path: 's3a://{bucket}/key/to/file'.
        # the split below separates the bucket name and the key that is
        # used to read the object in the bucket.

        s3_object = get_s3_object(file_path, credentials)

        with io.BytesIO() as f:
            s3_object.download_fileobj(f)

            f.seek(0)
            transformation_pkl = f.read()

    elif store_type == "hdfs":
        # S3 paths are the following path: 's3://{bucket}/key/to/file'.
        # the split below separates the bucket name and the key that is
        # used to read the object in the bucket.

        import subprocess

        output = subprocess.check_output(f"hdfs dfs -cat {file_path}", shell=True)
        transformation_pkl = bytes(output)

    elif store_type == "azure_blob_store":
        connection_string = credentials.get("azure_connection_string")
        container = credentials.get("azure_container_name")

        if connection_string is None or container is None:
            raise Exception(
                "both 'azure_connection_string' and 'azure_container_name' need to be passed in as credential"
            )

        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        container_client = blob_service_client.get_container_client(container)

        transformation_path = download_blobs_to_local(
            container_client, file_path, "transformation.pkl"
        )
        with open(transformation_path, "rb") as f:
            transformation_pkl = f.read()

    elif store_type == "google_cloud_storage":
        transformation_path = "transformation.pkl"
        bucket_name = credentials.get("gcp_bucket_name")
        project_id = credentials.get("gcp_project_id")
        gcp_credentials = get_credentials_dict(credentials.get("gcp_credentials"))

        credentials = service_account.Credentials.from_service_account_info(
            gcp_credentials
        )
        client = storage.Client(project=project_id, credentials=credentials)

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.download_to_filename(transformation_path)

        with open(transformation_path, "rb") as f:
            transformation_pkl = f.read()

    else:
        with open(file_path, "rb") as f:
            transformation_pkl = f.read()

    print("Retrieved code.")
    code = dill.loads(transformation_pkl)
    return code


def get_s3_object(file_path, credentials):
    use_service_account = credentials.get("use_service_account", "false") == "true"
    if not use_service_account:
        missing_credentials = [
            key
            for key in [
                "aws_region",
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_bucket_name",
            ]
            if not credentials.get(key)
        ]

        if missing_credentials:
            raise Exception(
                "Missing credential values for {}".format(
                    ", ".join(missing_credentials)
                )
            )

    if use_service_account:
        session = boto3.Session(region_name=credentials.get("aws_region"))
    else:
        session = boto3.Session(
            aws_access_key_id=credentials.get("aws_access_key_id"),
            aws_secret_access_key=credentials.get("aws_secret_access_key"),
            region_name=credentials.get("aws_region"),
        )

    s3 = session.resource("s3")
    s3_object = s3.Object(credentials.get("aws_bucket_name"), file_path)

    return s3_object


def download_blobs_to_local(container_client, blob, local_filename):
    # Downloads a blob to local to be used by pandas.

    # Parameters:
    #     client:         ContainerClient (used to interact with Azure container)
    #     blob:           str (path to blob store)
    #     local_filename: str (path to local file)

    # Output:
    #     full_path:      str (path to local file that will be used to read by pandas)

    if not os.path.isdir(LOCAL_DATA_PATH):
        os.makedirs(LOCAL_DATA_PATH)

    full_path = f"{LOCAL_DATA_PATH}/{local_filename}"
    print(f"downloading {blob} to {full_path}")

    blob_client = container_client.get_blob_client(blob)

    with open(full_path, "wb") as my_blob:
        download_stream = blob_client.download_blob()
        my_blob.write(download_stream.readall())

    return full_path


def set_spark_configs(spark, configs):
    # This method is used to set configs for Spark. It will be mostly
    # used to set access credentials for Spark to the store.

    print("setting spark configs")
    spark.config("spark.sql.parquet.enableVectorizedReader", "false")
    spark.config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    # TODO: research implications/tradeoffs of setting these to CORRECTED, especially
    # in combination with `outputTimestampType` above.
    # See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#configuration
    spark.config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    for key, value in configs.items():
        spark.config(key, value)


def set_gcp_credential_file_path(store_type, spark_args, creds):
    file_path = f"/tmp/{uuid.uuid4()}.json"
    if store_type == "google_cloud_storage":
        base64_creds = creds.get("gcp_credentials", "")
        creds = get_credentials_dict(base64_creds)

        with open(file_path, "w") as f:
            json.dump(creds, f)

        spark_args["google.cloud.auth.service.account.json.keyfile"] = file_path

    return file_path


def get_credentials_dict(base64_creds):
    # Takes a base64 creds and converts it into
    # Python dictionary.
    # Input:
    #   - base64_creds: string
    # Output:
    #   - creds: dict

    base64_bytes = base64_creds.encode("ascii")
    creds_bytes = base64.b64decode(base64_bytes)
    creds_string = creds_bytes.decode("ascii")
    creds = json.loads(creds_string)
    return creds


def delete_file(file_path):
    print(f"deleting {file_path} file")
    if os.path.isfile(file_path):
        os.remove(file_path)
    else:
        print(f"{file_path} does not exist.")


def check_dill_exception(exception):
    error_message = str(exception).lower()
    is_op_code_error = "opcode" in error_message
    is_op_code_expected_arguments_error = (
        "typeerror: code() takes at most" in error_message
    )
    if is_op_code_error or is_op_code_expected_arguments_error:
        version = sys.version_info
        python_version = f"{version.major}.{version.minor}.{version.micro}"
        error_message = f"""This error is most likely caused by different Python versions between the client and Spark provider. Check to see if you are running Python version '{python_version}' on the client."""
        return Exception(error_message)
    return exception


def split_key_value(values):
    arguments = {}
    for value in values:
        # split it into key and value; parse the first value
        value = value.replace('"', "")
        value = value.replace("\\", "")
        key, value = value.split("=", 1)
        # assign into dictionary
        arguments[key] = value
    return arguments


def parse_args(args=None):
    """
    Parses command line arguments for different transformation types and options.

    :param args: List of arguments to be parsed (defaults to None, which uses sys.argv).
    :return: The parsed arguments' namespace.
    """

    parser = argparse.ArgumentParser(description="Process transformation commands.")
    subparsers = parser.add_subparsers(dest="transformation_type", required=True)

    # Setup for SQL parser
    sql_parser = subparsers.add_parser("sql")
    setup_sql_parser(sql_parser)

    # Setup for DataFrame parser
    df_parser = subparsers.add_parser("df")
    setup_df_parser(df_parser)

    arguments = parser.parse_args(args)
    post_process_args(arguments)

    return arguments


def setup_sql_parser(parser):
    """
    Configures the SQL parser with necessary arguments.

    :param parser: The SQL command parser.
    """
    setup_common_parser(parser)
    parser.add_argument("--sql_query", help="SQL query to run on the data source.")

def setup_df_parser(parser):
    """
    Configures the DataFrame parser with necessary arguments.

    :param parser: The DataFrame command parser.
    """
    setup_common_parser(parser)
    parser.add_argument("--code", required=True, help="Path to transformation code file.")

def setup_common_parser(parser):
    # fmt: off
    parser.add_argument("--job_type", choices=list(JobType), help="Type of job being run.")
    parser.add_argument("--output", action=JsonAction, help="Output file location; e.g., s3a://bucket/path")
    parser.add_argument("--sources", nargs="*", action=JsonListAction,
                        help="List of sources in the transformation string.")
    parser.add_argument("--store_type", choices=FILESTORES)
    parser.add_argument("--spark_config", "-sc", action="append", default=[], help="Spark config to set by default.")
    parser.add_argument("--credential", "-c", action="append", default=[], help="Credentials needed for the job.")
    parser.add_argument("--output_format", default=OutputFormat.PARQUET.value, choices=list(OutputFormat),
                        help="Output file format.")
    parser.add_argument("--headers", default=Headers.INCLUDE.value, choices=list(Headers),
                        help="Whether to include/exclude headers in output.")
    parser.add_argument("--submit_params_uri", help="Path to the submit params file.")
    parser.add_argument("--is_update", default=False, action=BoolAction,
                        help="Specifies if this transform has been run successfully before, and that this is an update.")
    parser.add_argument("--direct_copy_use_iceberg", default=False, action=BoolAction, help="Specifies that we should use the new implementation of materialization that uses iceberg tables")
    parser.add_argument("--direct_copy_target", help="The type of provider we're doing a direct copy to. ex. dynamo")
    parser.add_argument("--direct_copy_table_name", help="If doing a direct copy, this is the table it'll be copied to")
    parser.add_argument("--direct_copy_feature_name", help="If doing a direct copy, this is the feature name we are working on")
    parser.add_argument("--direct_copy_feature_variant", help="If doing a direct copy, this is the feature variant we are working on")
    parser.add_argument("--direct_copy_entity_column", help="If doing a direct copy, the name of the entity column in the source dataframe")
    parser.add_argument("--direct_copy_value_column", help="If doing a direct copy, the name of the value column in the source dataframe")
    parser.add_argument("--direct_copy_timestamp_column", help="If doing a direct copy, the name of the timestamp column in the source dataframe. Don't set this if not relevent.")
    # fmt: on


def post_process_args(arguments):
    arguments.spark_config = split_key_value(arguments.spark_config)
    arguments.credential = split_key_value(arguments.credential)

    # Conditional logic for loading SQL query and source list from S3 which is done for submit
    # params that exceed the maximum character limit for the API (e.g. 10,240 characters/bytes)
    if (
        arguments.transformation_type == "sql"
        and arguments.submit_params_uri is not None
    ):
        if arguments.store_type == "s3":
            print(f"Retrieving submit params from {arguments.submit_params_uri}")
            # Assuming get_s3_object and credentials handling are properly defined
            s3_object = get_s3_object(
                arguments.submit_params_uri,
                arguments.credential,
            )
            with io.BytesIO() as f:
                s3_object.download_fileobj(f)
                f.seek(0)
                submit_params = json.load(f)
            arguments.sql_query = submit_params.get("sql_query")
            arguments.sources = [
                json.loads(source) for source in submit_params.get("sources")
            ]
        else:
            raise Exception(
                f"the '{arguments.store_type}' is not supported. Supported types: 's3'"
            )


class BoolAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values == "true":
            setattr(namespace, self.dest, True)
        elif values == "false":
            setattr(namespace, self.dest, False)
        else:
            raise argparse.ArgumentTypeError(f"Invalid bool: {values}")


class JsonAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        # Expecting a single JSON string
        try:
            parsed_value = json.loads(values)
            setattr(namespace, self.dest, parsed_value)
        except json.JSONDecodeError:
            raise argparse.ArgumentTypeError(f"Invalid JSON: {values}")


class JsonListAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        # TODO: handle checking the type of values to better handle different cases
        parsed_values = []
        for value in values:
            try:
                parsed_value = json.loads(value)
                parsed_values.append(parsed_value)
            except json.JSONDecodeError:
                raise argparse.ArgumentTypeError(
                    f"Invalid JSON found in sources: {value}"
                )

        setattr(namespace, self.dest, parsed_values)


if __name__ == "__main__":
    main(parse_args())
