#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import inspect
import os
import random
import types
import warnings
from collections.abc import Iterator
from typing import List, Optional, Union

import dill
import math
import numpy as np
import pandas as pd
from featureform.proto import serving_pb2, serving_pb2_grpc
from . import GrpcClient, Model, TrainingSetVariant
from .enums import FileFormat, DataResourceType
from .register import FeatureColumnResource
from .tls import insecure_channel, secure_channel

from .train_test_split import TrainTestSplit
from .version import check_up_to_date
from .grpc_client import GrpcClient


def check_feature_type(features):
    checked_features = []
    for feature in features:
        if isinstance(feature, tuple):
            checked_features.append(feature)
        elif isinstance(feature, str):
            # TODO: Need to identify how to pull the run id
            checked_features.append((feature, "default"))
        elif isinstance(feature, FeatureColumnResource):
            checked_features.append(feature.name_variant())
        elif hasattr(feature, "name_variant"):
            checked_features.append(feature.name_variant())
        else:
            raise ValueError(
                f"Invalid feature type {type(feature)}; must be a tuple, string, or FeatureColumnResource"
            )
    return checked_features


class ServingClient:
    """
    The serving client is used to retrieve training sets and features for training and serving purposes.
    **Using the Serving Client:**
    ``` py
    import featureform as ff
    from featureform import Client
    client = Client()
    # example:
    dataset = client.training_set("fraud_training", "quickstart")
    training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
    for feature_batch in training_dataset:
        # Train model
    ```
    """

    def __init__(
        self, host=None, local=False, insecure=False, cert_path=None, debug=False
    ):
        # This line ensures that the warning is only raised if ServingClient is instantiated directly
        # TODO: Remove this check once ServingClient is deprecated
        is_instantiated_directed = inspect.stack()[1].function != "__init__"
        if is_instantiated_directed:
            warnings.warn(
                "ServingClient is deprecated and will be removed in future versions; use Client instead.",
                PendingDeprecationWarning,
            )
        """
        Args:
            host (str): The hostname of the Featureform instance.
            insecure (bool): True if connecting to an insecure Featureform endpoint. False if using a self-signed or public TLS certificate
            cert_path (str): The path to a public certificate if using a self-signed certificate.
        """
        if local:
            raise Exception(
                "Local mode is not supported in this version. Use featureform <= 1.12.0 for localmode"
            )

        self.impl = HostedClientImpl(host, insecure, cert_path, debug=debug)
        self._cert_path = cert_path or os.getenv("FEATUREFORM_CERT")
        self._insecure = insecure

    def training_set(
        self,
        name,
        variant="",
        include_label_timestamp=False,
        model: Union[str, Model] = None,
    ) -> "Dataset":
        """Return an iterator that iterates through the specified training set.

        **Examples**:
        ``` py
            client = ff.Client()
            dataset = client.training_set("fraud_training", "quickstart")
            training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
            for feature_batch in training_dataset:
                # Train model
        ```

        Args:
            name (str): Name of training set to be retrieved
            variant (str): Variant of training set to be retrieved

        Returns:
            training_set (Dataset): A training set iterator
        """
        if isinstance(name, TrainingSetVariant):
            variant = name.variant
            name = name.name
        return self.impl.training_set(name, variant, include_label_timestamp, model)

    def features(
        self, features, entities, model: Union[str, Model] = None, params: list = None
    ):
        """Returns the feature values for the specified entities.

        **Examples**:
        ``` py
            client = ff.Client()
            fpf = client.features([("avg_transactions", "quickstart")], {"user": "C1410926"})
            # Run features through model
        ```

        Args:
            features (list[(str, str)], list[str]): List of Name Variant Tuples
            entities (dict): Dictionary of entity name/value pairs

        Returns:
            features (numpy.Array): An Numpy array of feature values in the order given by the inputs
        """
        features = check_feature_type(features)
        return self.impl.features(features, entities, model, params)

    def close(self):
        """Closes the connection to the Featureform instance."""
        self.impl.close()

    def batch_features(self, features):
        """
        Return an iterator that iterates over each entity and corresponding features in feats.
        **Example:**
        ```py title="definitions.py"
        for feature_values in client.batch_features([("feature1", "variant"), ("feature2", "variant"), ("feature3", "variant")]):
            print(feature_values)
        ```

        Args:
            features (List[NameVariant]): The features to iterate over

        Returns:
            iterator: An iterator of entity and feature values

        """
        if len(features) == 0:
            raise ValueError("No features provided")
        feature_tuples = check_feature_type(features)
        return self.impl.batch_features(feature_tuples)


class HostedClientImpl:
    def __init__(self, host=None, insecure=False, cert_path=None, debug=False):
        host = host or os.getenv("FEATUREFORM_HOST")
        if host is None:
            raise ValueError(
                "The `host` parameter must be passed or the environment"
                " variable FEATUREFORM_HOST must be set."
            )
        check_up_to_date(False, "serving")
        self._channel = self._create_channel(host, insecure, cert_path)
        self._stub = GrpcClient(
            serving_pb2_grpc.FeatureStub(self._channel),
            debug=debug,
            insecure=insecure,
            host=host,
        )

    @staticmethod
    def _create_channel(host, insecure, cert_path):
        if insecure:
            return insecure_channel(host)
        else:
            return secure_channel(host, cert_path)

    def training_set(
        self, name, variation, include_label_timestamp, model: Union[str, Model] = None
    ):
        training_set_stream = TrainingSetStream(self._stub, name, variation, model)
        return Dataset(training_set_stream)

    def features(
        self, features, entities, model: Union[str, Model] = None, params: list = None
    ):
        req = serving_pb2.FeatureServeRequest()
        for name, values in entities.items():
            entity_proto = req.entities.add()
            entity_proto.name = name
            if isinstance(values, list):
                for value in values:  # Assuming 'values' is a list of strings
                    entity_proto.values.append(value)
            elif isinstance(values, str):
                entity_proto.values.append(values)
            else:
                raise ValueError(
                    "Entity values must be either a string or a list of strings"
                )
        for name, variation in features:
            feature_id = req.features.add()
            feature_id.name = name
            feature_id.version = variation
        if model is not None:
            req.model.name = model if isinstance(model, str) else model.name
        resp = self._stub.FeatureServe(req)

        deprecated_values = resp.values
        value_lists = (
            deprecated_values if len(deprecated_values) > 0 else resp.value_lists
        )

        feature_values = []
        for val_list in value_lists:
            entity_values = []
            for val in val_list.values:
                parsed_value = parse_proto_value(val)
                value_type = type(parsed_value)

                # Ondemand features are returned as a byte array
                # which holds the pickled function
                if value_type == bytes:
                    code = dill.loads(bytearray(parsed_value))
                    func = types.FunctionType(code, globals(), "transformation")
                    parsed_value = func(self, params, entities)
                # Vector features are returned as a Vector32 proto due
                # to the inability to use the `repeated` keyword in
                # in a `oneof` field
                elif value_type == serving_pb2.Vector32:
                    parsed_value = parsed_value.value
                entity_values.append(parsed_value)

            # If theres only one entity row, only return that row
            if len(value_lists) == 1:
                return entity_values
            feature_values.append(entity_values)

        # if only one entity is requested, return a flat list
        if len(entities.keys()) == 1 and type(feature_values[0]) is list:
            return [j for sub in feature_values for j in sub]

        return feature_values

    def batch_features(self, features):
        return FeatureSetIterator(self._stub, features)

    def _get_source_as_df(self, name, variant, limit):
        columns = self._get_source_columns(name, variant)
        data = self._get_source_data(name, variant, limit)
        return pd.DataFrame(data=data, columns=columns)

    def _get_source_data(self, name, variant, limit):
        id = serving_pb2.SourceID(name=name, version=variant)
        req = serving_pb2.SourceDataRequest(id=id, limit=limit)
        resp = self._stub.SourceData(req)
        data = []
        for batch in resp:
            for rows in batch.rows:
                row = [getattr(r, r.WhichOneof("value")) for r in rows.rows]
                data.append(row)
        return data

    def _get_source_columns(self, name, variant):
        id = serving_pb2.SourceID(name=name, version=variant)
        req = serving_pb2.SourceDataRequest(id=id)
        resp = self._stub.SourceColumns(req)
        # The Python type of resp.columns is <class 'google._upb._message.RepeatedScalarContainer'>
        # which is not a "recognized" type by pandas internal type check, which resulted in the following error:
        # `Index(...) must be called with a collection of some kind`
        # To avoid this issue, we convert the resp.columns to a Python list
        return list(resp.columns)

    def nearest(self, name, variant, vector, k):
        id = serving_pb2.FeatureID(name=name, version=variant)
        vec = serving_pb2.Vector32(value=vector)
        req = serving_pb2.NearestRequest(id=id, vector=vec, k=k)
        resp = self._stub.Nearest(req)
        return resp.entities

    def location(self, name: str, variant: str, resource_type: DataResourceType) -> str:
        req = serving_pb2.ResourceIdRequest()
        req.name = name
        req.variant = variant
        req.type = resource_type.value

        resp = self._stub.GetResourceLocation(req)

        return resp.location

    def close(self):
        self._channel.close()


class TrainingSetStream(Iterator):
    def __init__(self, stub, name, version, model: Union[str, Model] = None):
        req = serving_pb2.TrainingDataRequest()
        req.id.name = name
        req.id.version = version
        if model is not None:
            req.model.name = model if isinstance(model, str) else model.name
        self.name = name
        self.version = version
        self._stub = stub
        self._req = req
        self._iter = stub.TrainingData(req)
        self._buf = []
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._buf):
            self._buf = next(self._iter).rows
            self._index = 0

        row = Row(self._buf[self._index])
        self._index += 1
        return row

    def restart(self):
        self._iter = self._stub.TrainingData(self._req)


class LocalStream:
    def __init__(self, datalist, include_label_timestamp):
        self._datalist = datalist
        self._iter = iter(datalist)
        self._include_label_timestamp = include_label_timestamp

    def __iter__(self):
        return iter(self._iter)

    def __next__(self):
        return LocalRow(next(self._iter), self._include_label_timestamp)

    def restart(self):
        self._iter = iter(self._datalist)


class Repeat:
    def __init__(self, repeat_num, stream):
        self.repeat_num = repeat_num
        self._stream = stream

    def __iter__(self):
        return self

    def __next__(self):
        try:
            next_val = next(self._stream)
        except StopIteration:
            self.repeat_num -= 1
            if self.repeat_num >= 0:
                self._stream.restart()
                next_val = next(self._stream)
            else:
                raise

        return next_val


class Shuffle:
    def __init__(self, buffer_size, stream):
        self.buffer_size = buffer_size
        self._shuffled_data_list = []
        self._stream = stream
        self.__setup_buffer()

    def __setup_buffer(self):
        try:
            for _ in range(self.buffer_size):
                self._shuffled_data_list.append(next(self._stream))
        except StopIteration:
            pass

    def restart(self):
        self._stream.restart()
        self.__setup_buffer()

    def __iter__(self):
        return self

    def __next__(self):
        if len(self._shuffled_data_list) == 0:
            raise StopIteration
        random_index = random.randrange(len(self._shuffled_data_list))
        next_row = self._shuffled_data_list.pop(random_index)

        try:
            self._shuffled_data_list.append(next(self._stream))
        except StopIteration:
            pass

        return next_row


class Batch:
    def __init__(self, batch_size, stream):
        self.batch_size = batch_size
        self._stream = stream

    def restart(self):
        self._stream.restart()

    def __iter__(self):
        return self

    def __next__(self):
        rows = BatchRow()
        for _ in range(self.batch_size):
            try:
                next_row = next(self._stream)
                rows.append(next_row)
            except StopIteration:
                if len(rows) == 0:
                    raise
                return rows
        return rows


class Dataset:
    def __init__(self, stream, dataframe=None):
        """Repeats the Dataset for the specified number of times

        Args:
            stream (Iterator): An iterable object.

        Returns:
            self (Dataset): Returns a Dataset created from the iterable object.
        """
        self._stream = stream
        self._dataframe = dataframe

    def train_test_split(
        self,
        test_size: float = 0,
        train_size: float = 0,
        shuffle: bool = True,
        random_state: Optional[int] = None,
        batch_size: int = 1,
    ):
        """
        (This functionality is currently only available for Clickhouse).

        Splits an existing training set into training and testing iterators. The split is processed on the underlying
        provider and calculated at serving time.

        **Examples**:

        ``` py
        import featureform as ff
        client = ff.Client()
        train, test = client
            .training_set("fraud_training", "v1")
            .train_test_split(
                test_size=0.7,
                train_size=0.3,
                shuffle=True,
                random_state=None,
                batch_size=5
            )

        for features, label in train:
            print(features)
            print(label)
            clf.partial_fit(features, label)

        for features, label in test:
            print(features)
            print(label)
            clf.score(features, label)


        # TRAIN OUTPUT
        # np.array([
        #   [1, 1, 3],
        #   [5, 1, 2],
        #   [7, 6, 5],
        #   [8, 3, 3],
        #   [5, 2, 2],
        # ])
        # np.array([2, 4, 2, 3, 4])
        # np.array([
        #   [3, 1, 2],
        #   [5, 4, 5],
        # ])
        # np.array([6, 7])

        # TEST OUTPUT
        # np.array([
        #   [5, 1, 3],
        #   [4, 3, 1],
        #   [6, 6, 7],
        # ])
        # np.array([4, 6, 7])

        ```

        Args:
            test_size (float): The ratio of test set size to train set size. Must be a value between 0 and 1. If excluded
                it will be the complement to the train_size. One of test_size or train_size must be specified.
            train_size (float): The ratio of train set size to train set size. Must be a value between 0 and 1. If excluded
                it will be the complement to the test_size. One of test_size or train_size must be specified.
            shuffle (bool): Whether to shuffle the dataset before splitting.
            random_state (Optional[int]): A random state to shuffle the dataset. If None, the dataset will be shuffled
                randomly on every call. If >0, the value will be used a seed to create random shuffle that can be repeated
                if subsequent calls use the same seed.
            batch_size (int): The size of the batch to return from the iterator. Must be greater than 0.

        Returns:
            train (Iterator): An iterator for training values.
            test (Iterator): An iterator for testing values.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be 1 or greater")

        if random_state is not None and random_state < 0:
            raise ValueError("random_state must be 0 or greater")

        test_size, train_size = self.validate_test_size(test_size, train_size)

        name = self._stream.name
        variant = self._stream.version
        stub = self._stream._stub
        model = self._stream.model if hasattr(self._stream, "model") else None

        train, test = TrainTestSplit(
            stub=stub,
            name=name,
            version=variant,
            model=model,
            test_size=test_size,
            train_size=train_size,
            shuffle=shuffle,
            random_state=(0 if random_state is None else random_state),
            batch_size=batch_size,
        ).split()

        return train, test

    @staticmethod
    def validate_test_size(test_size, train_size):
        # Validate ranges
        if not 0 <= test_size <= 1:
            raise ValueError("test_size must be between 0 and 1")
        if not 0 <= train_size <= 1:
            raise ValueError("train_size must be between 0 and 1")

        # If both are specified but don't sum to 1, it's an error
        if test_size != 0 and train_size != 0 and test_size + train_size != 1:
            raise ValueError("test_size + train_size must equal 1 if both are non-zero")

        # Auto-adjust if one is 0
        if test_size == 0 and train_size > 0:
            test_size = 1 - train_size
        elif train_size == 0 and test_size > 0:
            train_size = 1 - test_size

        return test_size, train_size

    def dataframe(self, spark_session=None):
        """Returns the training set as a Pandas DataFrame or Spark DataFrame.
        Args:
        - spark_session: Optional(SparkSession)


        **Examples**:
        ``` py
            client = Client()
            df = client.training_set("fraud_training", "v1").dataframe()
            print(df.head())
            # Output:
            #                feature_avg_transactions_default             label
            # 0                                25.0                       False
            # 1                             27999.0                       False
            # 2                               459.0                       False
            # 3                              2060.0                        True
            # 4                              1762.5                       False
        ```

        Returns:
            df: Union[pd.DataFrame, pyspark.sql.DataFrame] A DataFrame containing the training set.
        """

        if self._dataframe is not None:
            return self._dataframe
        elif spark_session is not None:
            req = serving_pb2.ResourceIdRequest()
            req.name = self._stream.name
            req.variant = self._stream.version
            req.type = DataResourceType.TRAINING_DATA.value
            resp = self._stream._stub.ResourceLocation(req)

            file_format = FileFormat.get_format(resp.location, default="parquet")
            location = self._sanitize_location(resp.location)
            self._dataframe = self._get_spark_dataframe(
                spark_session, file_format, location
            )
            return self._dataframe
        else:
            name = self._stream.name
            variant = self._stream.version
            stub = self._stream._stub
            id = serving_pb2.TrainingDataID(name=name, version=variant)
            req = serving_pb2.TrainingDataRequest(id=id)
            cols = stub.TrainingDataColumns(req)
            data = [r.to_dict(cols.features, cols.label) for r in self._stream]
            self._dataframe = pd.DataFrame(
                data=data, columns=[*cols.features, cols.label]
            )
            self._dataframe.rename(columns={cols.label: "label"}, inplace=True)
            return self._dataframe

    def _sanitize_location(self, location: str) -> str:
        # Returns the location directory rather than a single file if it is part-file.
        # Also converts s3:// to s3a:// if necessary.

        # If the location is a part-file, we want to get the directory instead.
        is_individual_part_file = location.split("/")[-1].startswith("part-")
        if is_individual_part_file:
            location = "/".join(location.split("/")[:-1])

        # If the schema is s3://, we want to convert it to s3a://.
        location = (
            location.replace("s3://", "s3a://")
            if location.startswith("s3://")
            else location
        )

        return location

    def _get_spark_dataframe(self, spark, file_format, location):
        if file_format not in FileFormat.supported_formats():
            raise Exception(
                f"file type '{file_format}' is not supported. Please use 'csv', 'parquet', or 'jsonl'"
            )

        try:
            df = (
                spark.read.option("header", "true")
                .option("recursiveFileLookup", "true")
            )

            if file_format == "jsonl":
                df = df.option("multiline", "true")
                file_format = "json"

            df = (
                df.format(file_format)
                .load(location)
            )

            label_column_name = ""
            for col in df.columns:
                if col.startswith("Label__"):
                    label_column_name = col
                    break

            if label_column_name != "":
                df = df.withColumnRenamed(label_column_name, "Label")

        except Exception as e:
            raise Exception(
                f"please make sure the spark session has ability to read '{location}': {e}"
            )

        return df

    def from_dataframe(dataframe, include_label_timestamp):
        stream = LocalStream(dataframe.values.tolist(), include_label_timestamp)
        return Dataset(stream, dataframe)

    def pandas(self):
        warnings.warn(
            "Dataset.pandas() is deprecated and will be removed in future versions; use Dataset.dataframe() instead.",
            PendingDeprecationWarning,
        )
        if self._dataframe is None:
            _ = self.dataframe()
        return self._dataframe

    def repeat(self, num):
        """Repeats the Dataset for the specified number of times

        **Examples**:
        ``` py
            client = ff.Client()
            dataset = client.training_set("fraud_training", "quickstart")
            training_dataset = dataset.repeat(10) # Repeats data 10 times
            for feature_batch in training_dataset:
                # Train model
        ```

        Args:
            num (int): The number of times the dataset will be repeated

        Returns:
            self (Dataset): Returns the current Dataset
        """
        if num <= 0:
            raise Exception("Must repeat 1 or more times")
        self._stream = Repeat(num, self._stream)
        if self._dataframe is not None:
            temp_df = self._dataframe
            for i in range(num):
                self._dataframe = pd.concat([self._dataframe, temp_df])
        return self

    def shuffle(self, buffer_size):
        """Swaps random rows within the Dataset.

        **Examples**:
        ``` py
            client = ff.Client()
            dataset = client.training_set("fraud_training", "quickstart")
            training_dataset = dataset.shuffle(100) # Swaps 100 Rows
            for feature_batch in training_dataset:
                # Train model
        ```

        Args:
            buffer_size (int): The number of Dataset rows to be randomly swapped

        Returns:
            self (Dataset): Returns the current Dataset
        """
        if buffer_size <= 0:
            raise Exception("Buffer size must be greater than or equal to 1")
        self._stream = Shuffle(buffer_size, self._stream)
        if self._dataframe is not None:
            self._dataframe = self._dataframe.sample(frac=1)
        return self

    def batch(self, batch_size):
        """Creates a batch row in the Dataset.

        **Examples**:
        ``` py
            client = ff.Client()
            dataset = client.training_set("fraud_training", "quickstart")
            training_dataset = dataset.batch(8) # Creates a batch of 8 Datasets for each row
            for feature_batch in training_dataset:
                # Train model
        ```

        Args:
            batch_size (int): The number of items to be added to each batch

        Returns:
            self (Dataset): Returns the current Dataset
        """
        if batch_size <= 0:
            raise Exception("Batch size must be greater than or equal to 1")
        self._stream = Batch(batch_size, self._stream)
        if self._dataframe is not None:
            self._dataframe = np.array_split(
                self._dataframe, math.ceil(len(self._dataframe) // batch_size)
            )
        return self

    def __iter__(self):
        return self

    def __next__(self):
        next_val = next(self._stream)
        return next_val


class Row:
    def __init__(self, proto_row):
        self._types = [proto_type_to_np_type(feature) for feature in proto_row.features]
        self._features = np.array(
            [parse_proto_value(feature) for feature in proto_row.features],
            dtype=get_numpy_array_type(self._types),
        )
        self._label = parse_proto_value(proto_row.label)
        self._row = np.append(self._features, self._label)

    def features(self):
        return np.array([self._row[:-1]])

    def label(self):
        return np.array([self._label])

    def to_numpy(self):
        return self._row

    def to_dict(self, feature_columns: List[str], label_column: str):
        row_dict = dict(zip(feature_columns, self._features))
        row_dict[label_column] = self._label
        return row_dict

    def __repr__(self):
        return "Features: {} , Label: {}".format(self.features(), self.label())


class LocalRow:
    def __init__(self, row_list, include_label_timestamp):
        """
        If include_label_timestamp is true then we want the label to equal to the
        last two columns in the list. Otherwise, just the label will be the last column only.
        """

        self._features = row_list[:-2] if include_label_timestamp else row_list[:-1]
        self._row = row_list
        self._label = row_list[-2:] if include_label_timestamp else row_list[-1]

    def features(self):
        return [self._features]

    def label(self):
        return [self._label]

    def to_numpy(self):
        return np.array(self._row)

    def __repr__(self):
        return "Features: {} , Label: {}".format(self.features(), self.label())


class BatchRow:
    def __init__(self, rows=None):
        self._features = []
        self._labels = []
        if rows is None:
            rows = []
        self._rows = rows
        for row in rows:
            self.append(row)

    def append(self, row):
        self._features.append(row.features()[0])
        self._labels.append(row.label()[0])
        self._rows.append(row)

    def features(self):
        return self._features

    def label(self):
        return self._labels

    def to_list(self):
        return self._rows

    def __len__(self):
        return len(self._rows)


def parse_proto_value(value):
    """parse_proto_value is used to parse the one of Value message"""
    return getattr(value, value.WhichOneof("value"))


def proto_type_to_np_type(value):
    type_mapping = {
        "str_value": str,
        "int_value": np.int32,
        "int32_value": np.int32,
        "int64_value": np.int64,
        "float_value": np.float32,
        "double_value": np.float64,
        "bool_value": bool,
    }
    return type_mapping[value.WhichOneof("value")]


def get_numpy_array_type(types):
    if all(i == types[0] for i in types):
        return types[0]
    else:
        return "O"


class FeatureSetIterator:
    def __init__(self, stub, features):
        req = serving_pb2.BatchFeatureServeRequest()
        for name, variant in features:
            feature_id = req.features.add()
            feature_id.name = name
            feature_id.version = variant
        self._stub = stub
        self._req = req
        self._iter = stub.BatchFeatureServe(req)
        self._buf = []
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._buf):
            self._buf = next(self._iter).rows
            self._index = 0

        row = FeatureSetRow(self._buf[self._index]).to_tuple()
        self._index += 1
        return row

    def restart(self):
        self._iter = self._stub.BatchFeatureServe(self._req)


class FeatureSetRow:
    def __init__(self, proto_row):
        self._features = [parse_proto_value(feature) for feature in proto_row.features]
        self._entity = parse_proto_value(proto_row.entity)
        self._row = [self._entity, self._features]

    def features(self):
        return self._row[1]

    def entity(self):
        return [self._entity]

    def to_numpy(self):
        return np.array(self._row)

    def to_tuple(self):
        return tuple((self._entity, self._features))

    def to_dict(self, feature_columns: List[str], entity_column: str):
        row_dict = dict(zip(feature_columns, self._features))
        row_dict[entity_column] = self._entity
        return row_dict

    def __repr__(self):
        return "Features: {} , Entity: {}".format(self.features(), self.entity())
