from dataclasses import dataclass
from enum import Enum
from featureform.proto import metadata_pb2 as pb
from typeguard import typechecked
from os import path
from fnmatch import fnmatch


class ScalarType(Enum):
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

    @staticmethod
    def from_proto(proto):
        return proto.Status._enum_type.values[proto.status].name


class ComputationMode(Enum):
    PRECOMPUTED = "PRECOMPUTED"
    CLIENT_COMPUTED = "CLIENT_COMPUTED"

    def __eq__(self, other: str) -> bool:
        return self.value == other

    def proto(self) -> int:
        if self == ComputationMode.PRECOMPUTED:
            return pb.ComputationMode.PRECOMPUTED
        elif self == ComputationMode.CLIENT_COMPUTED:
            return pb.ComputationMode.CLIENT_COMPUTED


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
@dataclass
class OfflineResourceType(Enum):
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
    """
    ResourceType is an enumeration representing the possible types of
    resources that may be registered with Featureform. Each value corresponds
    to a variant in ResourceType in metadata/proto/metadata.proto.

    Attributes:
        FEATURE (int): A feature.
        LABEL (int): A label.
        TRAINING_SET (int): A training set.
        SOURCE (int): A source.
        FEATURE_VARIANT (int): A feature variant.
        LABEL_VARIANT (int): A label variant.
        TRAINING_SET_VARIANT (int): A training set variant.
        SOURCE_VARIANT (int): A source variant.
        PROVIDER (int): A provider.
        ENTITY (int): The type of a resource representing an entity.
        MODEL (int): A model.
        USER (int): A user.
        TRIGGER (int): A trigger.
    """

    FEATURE = 0
    LABEL = 1
    TRAINING_SET = 2
    SOURCE = 3
    FEATURE_VARIANT = 4
    LABEL_VARIANT = 5
    TRAINING_SET_VARIANT = 6
    SOURCE_VARIANT = 7
    PROVIDER = 8
    ENTITY = 9
    MODEL = 10
    USER = 11
    TRIGGER = 12

    @classmethod
    def has_value(cls, value):
        try:
            cls(value)
            return True
        except ValueError:
            return False

    @classmethod
    def from_proto(cls, proto_val):
        mapping = {
            pb.ResourceType.FEATURE: ResourceType.FEATURE,
            pb.ResourceType.LABEL: ResourceType.LABEL,
            pb.ResourceType.TRAINING_SET: ResourceType.TRAINING_SET,
            pb.ResourceType.SOURCE: ResourceType.SOURCE,
            pb.ResourceType.FEATURE_VARIANT: ResourceType.FEATURE_VARIANT,
            pb.ResourceType.LABEL_VARIANT: ResourceType.LABEL_VARIANT,
            pb.ResourceType.TRAINING_SET_VARIANT: ResourceType.TRAINING_SET_VARIANT,
            pb.ResourceType.SOURCE_VARIANT: ResourceType.SOURCE_VARIANT,
            pb.ResourceType.PROVIDER: ResourceType.PROVIDER,
            pb.ResourceType.ENTITY: ResourceType.ENTITY,
            pb.ResourceType.MODEL: ResourceType.MODEL,
            pb.ResourceType.USER: ResourceType.USER,
            pb.ResourceType.TRIGGER: ResourceType.TRIGGER,
        }
        return mapping[proto_val]

    def to_proto(self):
        mapping = {
            ResourceType.FEATURE: pb.ResourceType.FEATURE,
            ResourceType.LABEL: pb.ResourceType.LABEL,
            ResourceType.TRAINING_SET: pb.ResourceType.TRAINING_SET,
            ResourceType.SOURCE: pb.ResourceType.SOURCE,
            ResourceType.FEATURE_VARIANT: pb.ResourceType.FEATURE_VARIANT,
            ResourceType.LABEL_VARIANT: pb.ResourceType.LABEL_VARIANT,
            ResourceType.TRAINING_SET_VARIANT: pb.ResourceType.TRAINING_SET_VARIANT,
            ResourceType.SOURCE_VARIANT: pb.ResourceType.SOURCE_VARIANT,
            ResourceType.PROVIDER: pb.ResourceType.PROVIDER,
            ResourceType.ENTITY: pb.ResourceType.ENTITY,
            ResourceType.MODEL: pb.ResourceType.MODEL,
            ResourceType.USER: pb.ResourceType.USER,
            ResourceType.TRIGGER: pb.ResourceType.TRIGGER,
        }
        return mapping[self]


class TriggerType(Enum):
    SCHEDULED = 0

    @classmethod
    def has_value(cls, value):
        try:
            cls(value)
            return True
        except ValueError:
            return False
