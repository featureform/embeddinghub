#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from dataclasses import dataclass
from enum import Enum
from featureform.proto import metadata_pb2 as pb
from typeguard import typechecked
from os import path
from fnmatch import fnmatch


class ScalarType(str, Enum):
    """
    ScalarType is an enum of all the scalar types supported by Featureform.

    Attributes:
        NIL: An empty string representing no specified type.
        INT: A string representing an integer type.
        INT32: A string representing a 32-bit integer type.
        INT64: A string representing a 64-bit integer type.
        FLOAT32: A string representing a 32-bit float type.
        FLOAT64: A string representing a 64-bit float type.
        STRING: A string representing a string type.
        BOOL: A string representing a boolean type.
        DATETIME: A string representing a datetime type.
    """

    NIL = ""
    INT = "int"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    STRING = "string"
    BOOL = "bool"
    DATETIME = "datetime"

    @classmethod
    def has_value(cls, value):
        try:
            cls(value)
            return True
        except ValueError:
            return False

    @classmethod
    def get_values(cls):
        return [e.value for e in cls]

    def to_proto(self):
        proto_enum = self.to_proto_enum()
        return pb.ValueType(scalar=proto_enum)

    def to_proto_enum(self):
        mapping = {
            ScalarType.NIL: pb.ScalarType.NULL,
            ScalarType.INT: pb.ScalarType.INT,
            ScalarType.INT32: pb.ScalarType.INT32,
            ScalarType.INT64: pb.ScalarType.INT64,
            ScalarType.FLOAT32: pb.ScalarType.FLOAT32,
            ScalarType.FLOAT64: pb.ScalarType.FLOAT64,
            ScalarType.STRING: pb.ScalarType.STRING,
            ScalarType.BOOL: pb.ScalarType.BOOL,
            ScalarType.DATETIME: pb.ScalarType.DATETIME,
        }
        return mapping[self]

    @classmethod
    def from_proto(cls, proto_val):
        mapping = {
            pb.ScalarType.NULL: ScalarType.NIL,
            pb.ScalarType.INT: ScalarType.INT,
            pb.ScalarType.INT32: ScalarType.INT32,
            pb.ScalarType.INT64: ScalarType.INT64,
            pb.ScalarType.FLOAT32: ScalarType.FLOAT32,
            pb.ScalarType.FLOAT64: ScalarType.FLOAT64,
            pb.ScalarType.STRING: ScalarType.STRING,
            pb.ScalarType.BOOL: ScalarType.BOOL,
            pb.ScalarType.DATETIME: ScalarType.DATETIME,
        }
        return mapping[proto_val]


class ResourceStatus(str, Enum):
    """
    ResourceStatus is an enumeration representing the possible states that a
    resource may occupy within an application.

    Each status is represented as a string, which provides a human-readable
    representation for each of the stages in the lifecycle of a resource.

    Attributes:
        NO_STATUS (str): The state of a resource that cannot have another status.
        CREATED (str): The state after a resource has been successfully created.
        PENDING (str): The state indicating that the resource is in the process of being prepared, but is not yet ready.
        READY (str): The state indicating that the resource has been successfully prepared and is now ready for use.
        FAILED (str): The state indicating that an error occurred during the creation or preparation of the resource.
    """

    NO_STATUS = "NO_STATUS"
    CREATED = "CREATED"
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"
    RUNNING = "RUNNING"

    @staticmethod
    def from_proto(proto):
        return proto.Status._enum_type.values[proto.status].name


class ComputationMode(Enum):
    PRECOMPUTED = "PRECOMPUTED"
    CLIENT_COMPUTED = "CLIENT_COMPUTED"
    STREAMING = "STREAMING"

    def __eq__(self, other: str) -> bool:
        return self.value == other

    def proto(self) -> int:
        if self == ComputationMode.PRECOMPUTED:
            return pb.ComputationMode.PRECOMPUTED
        elif self == ComputationMode.CLIENT_COMPUTED:
            return pb.ComputationMode.CLIENT_COMPUTED
        elif self == ComputationMode.STREAMING:
            return pb.ComputationMode.STREAMING
        else:
            raise ValueError(f"Unknown computation mode: {self}")


@typechecked
@dataclass
class OperationType(Enum):
    GET = 0
    CREATE = 1


@typechecked
@dataclass
class SourceType(str, Enum):
    PRIMARY_SOURCE = "PRIMARY"
    DIRECTORY = "DIRECTORY"
    DF_TRANSFORMATION = "DF"
    SQL_TRANSFORMATION = "SQL"


class FilePrefix(Enum):
    S3 = ("s3://", "s3a://")
    S3A = ("s3a://",)
    HDFS = ("hdfs://",)
    GCS = ("gs://",)
    AZURE = ("abfss://",)

    def __init__(self, *valid_prefixes):
        self.prefixes = valid_prefixes

    @property
    def value(self):
        return self.prefixes[0]

    def validate_file_scheme(self, file_path: str) -> (bool, str):
        if not any(file_path.startswith(prefix) for prefix in self.prefixes):
            raise Exception(
                f"File path '{file_path}' must be a full path. Must start with '{self.prefixes}'"
            )

    @staticmethod
    def validate(store_type: str, file_path: str):
        try:
            prefix = FilePrefix[store_type]
            prefix.validate_file_scheme(file_path)
        except KeyError:
            raise Exception(f"Invalid store type: {store_type}")


class FileFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"

    @classmethod
    def is_supported(cls, file_path: str) -> bool:
        file_name = path.basename(file_path)

        for file_format in cls:
            if fnmatch(file_name, f"*.{file_format.value}"):
                return True

        return False

    @classmethod
    def get_format(cls, file_path: str, default: str = "") -> str:
        file_name = path.basename(file_path)

        for file_format in cls:
            if fnmatch(file_name, f"*.{file_format.value}"):
                return file_format.value

        if default != "":
            return default
        raise ValueError(f"File format not supported: {file_name}")

    @classmethod
    def supported_formats(cls) -> str:
        return ", ".join([file_format.value for file_format in cls])


@typechecked
class DataResourceType(Enum):
    # ResourceType is an enumeration representing the possible types of
    # resources that may be registered with Featureform. Each value is based
    # on OfflineResourceType in providers/offline.go

    NO_TYPE = 0
    LABEL = 1
    FEATURE = 2
    TRAINING_SET = 3
    PRIMARY = 4
    TRANSFORMATION = 5
    FEATURE_MATERIALIZATION = 6


class ResourceType(Enum):
    NO_TYPE = None
    USER = pb.ResourceType.USER
    PROVIDER = pb.ResourceType.PROVIDER
    SOURCE_VARIANT = pb.ResourceType.SOURCE_VARIANT
    ENTITY = pb.ResourceType.ENTITY
    FEATURE_VARIANT = pb.ResourceType.FEATURE_VARIANT
    ONDEMAND_FEATURE = pb.ResourceType.FEATURE_VARIANT
    LABEL_VARIANT = pb.ResourceType.LABEL_VARIANT
    TRAININGSET_VARIANT = pb.ResourceType.TRAINING_SET_VARIANT
    SCHEDULE = None
    MODEL = pb.ResourceType.MODEL
    TRANSFORMATION = pb.ResourceType.SOURCE_VARIANT

    def to_proto(self):
        return self.value

    def to_string(self) -> str:
        return self.name.replace("_", " ").title()


@typechecked
@dataclass
class TableFormat(str, Enum):
    ICEBERG = "iceberg"
    DELTA = "delta"

    @classmethod
    def get_format(cls, format: str):
        try:
            return cls(format)
        except ValueError:
            raise ValueError(f"Table format not supported: {format}")

    @classmethod
    def is_supported(cls, table_format: str) -> bool:
        return table_format in [table_format.value for table_format in cls]

    @classmethod
    def supported_formats(cls) -> str:
        return ", ".join([table_format.value for table_format in cls])


class LocationType(str, Enum):
    TABLE = "table"
    FILESTORE = "filestore"
    CATALOG = "catalog"


class RefreshMode(Enum):
    AUTO = pb.RefreshMode.REFRESH_MODE_AUTO
    FULL = pb.RefreshMode.REFRESH_MODE_FULL
    INCREMENTAL = pb.RefreshMode.REFRESH_MODE_INCREMENTAL

    @classmethod
    def from_proto(cls, proto_value):
        try:
            return cls(proto_value)
        except ValueError:
            return None

    def to_proto(self):
        return self.value

    def to_string(self):
        return self.name

    @classmethod
    def from_string(cls, value):
        try:
            return cls[value.upper()]
        except KeyError:
            raise ValueError(f"Refresh Mode value not supported: {value}")
        except AttributeError:
            raise ValueError(f"Refresh Mode value required: received {value}")


class Initialize(Enum):
    ON_CREATE = pb.Initialize.INITIALIZE_ON_CREATE
    ON_SCHEDULE = pb.Initialize.INITIALIZE_ON_SCHEDULE

    @classmethod
    def from_proto(cls, proto_value):
        try:
            return cls(proto_value)
        except ValueError:
            return None

    def to_proto(self):
        return self.value

    def to_string(self):
        return self.name

    @classmethod
    def from_string(cls, value):
        try:
            return cls[value.upper()]
        except KeyError:
            raise ValueError(f"Initialize value not supported: {value}")
        except AttributeError:
            raise ValueError(f"Initialize value required: received {value}")


class SnowflakeSessionParamKey(Enum):
    ABORT_DETACHED_QUERY = "abort_detached_query"
    AUTOCOMMIT = "autocommit"
    BINARY_INPUT_FORMAT = "binary_input_format"
    BINARY_OUTPUT_FORMAT = "binary_output_format"
    DATE_INPUT_FORMAT = "date_input_format"
    DATE_OUTPUT_FORMAT = "date_output_format"
    ERROR_ON_NONDETERMINISTIC_MERGE = "error_on_nondeterministic_merge"
    ERROR_ON_NONDETERMINISTIC_UPDATE = "error_on_nondeterministic_update"
    GEOGRAPHY_OUTPUT_FORMAT = "geography_output_format"
    HYBRID_TABLE_LOCK_TIMEOUT = "hybrid_table_lock_timeout"
    JSON_INDENT = "json_indent"
    LOG_LEVEL = "log_level"
    LOCK_TIMEOUT = "lock_timeout"
    QUERY_TAG = "query_tag"
    ROWS_PER_RESULTSET = "rows_per_resultset"
    S3_STAGE_VPCE_DNS_NAME = "s3_stage_vpce_dns_name"
    SEARCH_PATH = "search_path"
    SIMULATED_DATA_SHARING_CONSUMER = "simulated_data_sharing_consumer"
    STATEMENT_TIMEOUT_IN_SECONDS = "statement_timeout_in_seconds"
    STRICT_JSON_OUTPUT = "strict_json_output"
    TIMESTAMP_DAY_IS_ALWAYS_24H = "timestamp_day_is_always_24h"
    TIMESTAMP_INPUT_FORMAT = "timestamp_input_format"
    TIMESTAMP_LTZ_OUTPUT_FORMAT = "timestamp_ltz_output_format"
    TIMESTAMP_NTZ_OUTPUT_FORMAT = "timestamp_ntz_output_format"
    TIMESTAMP_OUTPUT_FORMAT = "timestamp_output_format"
    TIMESTAMP_TYPE_MAPPING = "timestamp_type_mapping"
    TIMESTAMP_TZ_OUTPUT_FORMAT = "timestamp_tz_output_format"
    TIMEZONE = "timezone"
    TIME_INPUT_FORMAT = "time_input_format"
    TIME_OUTPUT_FORMAT = "time_output_format"
    TRACE_LEVEL = "trace_level"
    TRANSACTION_DEFAULT_ISOLATION_LEVEL = "transaction_default_isolation_level"
    TWO_DIGIT_CENTURY_START = "two_digit_century_start"
    UNSUPPORTED_DDL_ACTION = "unsupported_ddl_action"
    USE_CACHED_RESULT = "use_cached_result"
    WEEK_OF_YEAR_POLICY = "week_of_year_policy"
    WEEK_START = "week_start"

    @classmethod
    def validate_key(cls, key: str) -> bool:
        return key.lower() in cls._value2member_map_
