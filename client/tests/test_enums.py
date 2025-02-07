#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest
from featureform import FilePrefix
from featureform.enums import FileFormat, RefreshMode, Initialize
from featureform.proto import metadata_pb2 as pb


@pytest.mark.parametrize(
    "store_type, file_path",
    [
        ("AZURE", "abfss://test_bucket/test_file"),
        ("AZURE", "test_bucket/test_file"),
        ("S3", "s3://test_bucket/test_file"),
        ("S3", "test_bucket/test_file"),
        ("HDFS", "hdfs://test_bucket/test_file"),
        ("HDFS", "test_bucket/test_file"),
        ("GCS", "gs://test_bucket/test_file"),
        ("GCS", "test_bucket/test_file"),
    ],
)
def test_validate_file_scheme(store_type, file_path):
    try:
        FilePrefix.validate(store_type, file_path)
    except Exception as e:
        assert (
            str(e)
            == f"File path '{file_path}' must be a full path. Must start with '{FilePrefix[store_type].prefixes}'"
        )


@pytest.mark.parametrize(
    "location, expected_format",
    [
        ("s3://bucket/path/to/file.csv", "csv"),
        ("s3a://bucket/path/to/file.csv", "csv"),
        ("s3://bucket/path/to/directory/part-0000.parquet", "parquet"),
        ("s3://bucket/path/to/directory", "parquet"),
        ("s3://bucket/path/to/file.jsonl", "jsonl"),
    ],
)
def test_get_format(location, expected_format):
    assert FileFormat.get_format(location, "parquet") == expected_format


@pytest.mark.parametrize(
    "input, output",
    [
        (pb.RefreshMode.REFRESH_MODE_AUTO, RefreshMode.AUTO),
        (pb.RefreshMode.REFRESH_MODE_FULL, RefreshMode.FULL),
        (pb.RefreshMode.REFRESH_MODE_INCREMENTAL, RefreshMode.INCREMENTAL),
        (pb.RefreshMode.REFRESH_MODE_UNSPECIFIED, None),
    ],
)
def test_refresh_mode_from_proto(input, output):
    assert RefreshMode.from_proto(input) == output


@pytest.mark.parametrize(
    "input, output",
    [
        (RefreshMode.AUTO, pb.RefreshMode.REFRESH_MODE_AUTO),
        (RefreshMode.FULL, pb.RefreshMode.REFRESH_MODE_FULL),
        (RefreshMode.INCREMENTAL, pb.RefreshMode.REFRESH_MODE_INCREMENTAL),
    ],
)
def test_refresh_mode_to_proto(input, output):
    assert RefreshMode.to_proto(input) == output


@pytest.mark.parametrize(
    "invalid_string_value, expected_message",
    [
        ("invalid_value", "Refresh Mode value not supported"),
        (None, "Refresh Mode value required: received"),
    ],
)
def test_refresh_mode_from_string_invalid_value(invalid_string_value, expected_message):

    with pytest.raises(ValueError, match=expected_message):
        RefreshMode.from_string(invalid_string_value)


@pytest.mark.parametrize(
    "valid_string_value, expected_enum",
    [
        ("AUTO", RefreshMode.AUTO),
        ("full", RefreshMode.FULL),
        ("INCREMENTAL", RefreshMode.INCREMENTAL),
    ],
)
def test_refresh_mode_from_string_valid_value(valid_string_value, expected_enum):

    result = RefreshMode.from_string(valid_string_value)
    assert result == expected_enum, f"Expected {expected_enum}, got {result}"


@pytest.mark.parametrize(
    "input, output",
    [
        (pb.Initialize.INITIALIZE_ON_CREATE, Initialize.ON_CREATE),
        (pb.Initialize.INITIALIZE_ON_SCHEDULE, Initialize.ON_SCHEDULE),
        (pb.Initialize.INITIALIZE_UNSPECIFIED, None),
    ],
)
def test_initialize_from_proto(input, output):
    assert Initialize.from_proto(input) == output


@pytest.mark.parametrize(
    "input, output",
    [
        (Initialize.ON_CREATE, pb.Initialize.INITIALIZE_ON_CREATE),
        (Initialize.ON_SCHEDULE, pb.Initialize.INITIALIZE_ON_SCHEDULE),
    ],
)
def test_initialize_to_proto(input, output):
    assert Initialize.to_proto(input) == output


@pytest.mark.parametrize(
    "invalid_string_value, expected_message",
    [
        ("invalid_value", "Initialize value not supported"),
        (None, "Initialize value required: received"),
    ],
)
def test_initialize_from_string_invalid_value(invalid_string_value, expected_message):

    with pytest.raises(ValueError, match=expected_message):
        Initialize.from_string(invalid_string_value)


@pytest.mark.parametrize(
    "valid_string_value, expected_enum",
    [
        ("ON_CREATE", Initialize.ON_CREATE),
        ("on_create", Initialize.ON_CREATE),
        ("ON_SCHEDULE", Initialize.ON_SCHEDULE),
        ("on_schedule", Initialize.ON_SCHEDULE),
    ],
)
def test_initialize_from_string_valid_value(valid_string_value, expected_enum):

    result = Initialize.from_string(valid_string_value)
    assert result == expected_enum, f"Expected {expected_enum}, got {result}"
