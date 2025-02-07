#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import csv
import os
import shutil
import stat
import sys
import time
from tempfile import NamedTemporaryFile
from unittest import TestCase
from unittest import mock

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, "client/src/")
from featureform import ResourceClient, ServingClient
import serving_cases as cases
import featureform as ff
from featureform.serving import check_feature_type, Row, Dataset


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ([("name", "variant")], [("name", "variant")]),
        (["name"], [("name", "default")]),
        (["name1", "name2"], [("name1", "default"), ("name2", "default")]),
        (["name1", ("name2", "variant")], [("name1", "default"), ("name2", "variant")]),
    ],
)
def test_check_feature_type(test_input, expected):
    assert expected == check_feature_type(test_input)


proto_features = [1, 2, 3]
proto_label = 4


@pytest.fixture()
def proto_row():
    class ProtoRow:
        def __init__(self):
            self.features = proto_features
            self.label = proto_label

        def to_numpy(self):
            row = np.array(self.features)
            row = np.append(row, self.label)
            return row

    return ProtoRow()


def side_effect(value):
    if value in proto_features:
        return value
    else:
        return proto_label


def mock_type(value):
    return "int"


@mock.patch(
    "featureform.serving.parse_proto_value", mock.MagicMock(side_effect=side_effect)
)
@mock.patch(
    "featureform.serving.proto_type_to_np_type", mock.MagicMock(side_effect=mock_type)
)
def test_row_to_numpy(proto_row):
    row = Row(proto_row)
    row_np = row.to_numpy()
    proto_row_np = proto_row.to_numpy()

    assert np.array_equal(row_np, proto_row_np)


def replace_nans(row):
    """
    Replaces NaNs in a list with the string 'NaN'. Dealing with NaN's can be a pain in Python so this is a
    helper function to make it easier to test.
    """
    result = []
    for r in row:
        if isinstance(r, float) and np.isnan(r):
            result.append("NaN")
        else:
            result.append(r)
    return result


def clear_and_reset():
    ff.clear_state()
    shutil.rmtree(".featureform", onerror=del_rw)


def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)


def create_temp_file(test_values):
    file = NamedTemporaryFile(delete=False, suffix=".csv")
    with open(file.name, "w") as csvfile:
        writer = csv.writer(csvfile, delimiter=",", quotechar="|")
        writer.writerow(test_values["columns"])
        for row in test_values["values"]:
            writer.writerow(row)
        csvfile.close()

    return file.name


def retry_delete():
    for i in range(0, 100):
        try:
            shutil.rmtree(".featureform", onerror=del_rw)
            print("Table Deleted")
            break
        except Exception as e:
            print(f"Could not delete. Retrying...", e)
            time.sleep(1)


@pytest.mark.parametrize(
    "location, expected_location",
    [
        ("s3://bucket/path/to/file.csv", "s3a://bucket/path/to/file.csv"),
        ("s3a://bucket/path/to/file.csv", "s3a://bucket/path/to/file.csv"),
        (
            "s3://bucket/path/to/directory/part-0000.parquet",
            "s3a://bucket/path/to/directory",
        ),
        ("s3://bucket/path/to/directory", "s3a://bucket/path/to/directory"),
    ],
)
def test_sanitize_location(location, expected_location):
    dataset = Dataset("")
    assert dataset._sanitize_location(location) == expected_location


@pytest.mark.parametrize(
    "location,format",
    [
        ("client/tests/test_files/input_files/input_transactions.csv", "csv"),
        ("client/tests/test_files/input_files/transactions.parquet", "parquet"),
    ],
)
def test_get_spark_dataframe(location, format, spark_session):
    expected_df = (
        spark_session.read.option("header", "true").format(format).load(location)
    )
    dataset = Dataset("")
    actual_df = dataset._get_spark_dataframe(spark_session, format, location)
    assert actual_df.collect() == expected_df.collect()

@pytest.mark.parametrize(
    "location,format",
    [
        ("client/tests/test_files/input_files/transactions.jsonl", "jsonl"),
    ],
)
def test_get_spark_dataframe_jsonl(location, format, spark_session):
    expected_df = (
        spark_session.read.option("header", "true").option("multiline", "true").format(format).load(location)
    )
    dataset = Dataset("")
    actual_df = dataset._get_spark_dataframe(spark_session, format, location)
    assert actual_df.collect() == expected_df.collect()
