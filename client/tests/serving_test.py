import csv
import os
import shutil
import stat
import sys
import time
from tempfile import NamedTemporaryFile
from unittest import TestCase
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from featureform.local_utils import feature_df_with_entity, label_df_from_csv

sys.path.insert(0, "client/src/")
from featureform import ResourceClient, ServingClient
import serving_cases as cases
import featureform as ff
from featureform.serving import LocalClientImpl, check_feature_type, Row, Dataset


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


class TestIndividualFeatures(TestCase):
    def test_process_feature_no_ts(self):
        for name, case in cases.features_no_ts.items():
            with self.subTest(name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                local_client = LocalClientImpl()
                dataframe_mapping = feature_df_with_entity(file_name, "entity_id", case)
                expected = pd.DataFrame(case["expected"])
                actual = dataframe_mapping
                expected = expected.values.tolist()
                actual = actual.values.tolist()
                local_client.db.close()
                client.impl.db.close()
                assert all(
                    elem in expected for elem in actual
                ), "Expected: {} Got: {}".format(expected, actual)

    def test_process_feature_with_ts(self):
        for name, case in cases.features_with_ts.items():
            with self.subTest(msg=name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                local_client = LocalClientImpl()
                dataframe_mapping = feature_df_with_entity(file_name, "entity_id", case)
                expected = pd.DataFrame(case["expected"])
                actual = dataframe_mapping
                expected = expected.values.tolist()
                actual = actual.values.tolist()
                local_client.db.close()
                client.impl.db.close()
                assert all(
                    elem in expected for elem in actual
                ), "Expected: {} Got: {}".format(expected, actual)

    def test_invalid_entity_col(self):
        case = cases.feature_invalid_entity
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        local_client = LocalClientImpl()
        with pytest.raises(KeyError) as err:
            feature_df_with_entity(file_name, "entity_id", case)
        local_client.db.close()
        client.impl.db.close()
        assert "column does not exist" in str(err.value)

    def test_invalid_value_col(self):
        case = cases.feature_invalid_value
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        local_client = LocalClientImpl()
        with pytest.raises(KeyError) as err:
            feature_df_with_entity(file_name, "entity_id", case)
        local_client.db.close()
        client.impl.db.close()
        assert "column does not exist" in str(err.value)

    def test_invalid_ts_col(self):
        case = cases.feature_invalid_ts
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        local_client = LocalClientImpl()
        with pytest.raises(KeyError) as err:
            feature_df_with_entity(file_name, "entity_id", case)
        local_client.db.close()
        client.impl.db.close()
        assert "column does not exist" in str(err.value)
        retry_delete()


class TestFeaturesE2E(TestCase):
    def test_features(self):
        for name, case in cases.feature_e2e.items():
            with self.subTest(msg=name):
                print("TEST: ", name)
                ff.register_local()
                file_name = create_temp_file(case)
                res = e2e_features(
                    file_name,
                    case["entity"],
                    case["entity_loc"],
                    case["features"],
                    case["value_cols"],
                    case["entities"],
                    case["ts_col"],
                )
                expected = case["expected"]
                assert all(
                    elem in expected for elem in res
                ), "Expected: {} Got: {}".format(expected, res)
            retry_delete()

    def test_timestamp_doesnt_exist(self):
        case = {
            "columns": ["entity", "value"],
            "values": [
                ["a", 1],
                ["b", 2],
                ["c", 3],
            ],
            "value_cols": ["value"],
            "entity": "entity",
            "entity_loc": "entity",
            "features": [("avg_transactions", "v13", "int")],
            "entities": [{"entity": "a"}, {"entity": "b"}, {"entity": "c"}],
            "expected": [[1], [2], [3]],
            "ts_col": "ts",
        }
        file_name = create_temp_file(case)
        ff.register_local()
        with pytest.raises(KeyError) as err:
            e2e_features(
                file_name,
                case["entity"],
                case["entity_loc"],
                case["features"],
                case["value_cols"],
                case["entities"],
                case["ts_col"],
            )
        assert "column does not exist" in str(err.value)

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(tmpdir):
        """Fixture to execute asserts before and after a test is run"""
        # Remove any lingering Databases
        try:
            ff.clear_state()
            shutil.rmtree(".featureform", onerror=del_rw)
        except:
            print("File Already Removed")
        yield
        try:
            shutil.rmtree(".featureform", onerror=del_rw)
        except:
            print("File Already Removed")


class TestIndividualLabels(TestCase):
    def test_individual_labels(self):
        for name, case in cases.labels.items():
            with self.subTest(name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                actual = label_df_from_csv(case, file_name)
                expected = pd.DataFrame(case["expected"]).set_index(case["entity_name"])
                pd.testing.assert_frame_equal(actual, expected)

    def test_invalid_entity(self):
        case = {
            "columns": ["entity", "value", "ts"],
            "values": [],
            "entity_name": "entity",
            "source_entity": "name_dne",
            "source_value": "value",
            "source_timestamp": "ts",
        }
        file_name = create_temp_file(case)
        with pytest.raises(KeyError) as err:
            label_df_from_csv(case, file_name)
        assert "column does not exist" in str(err.value)

    def test_invalid_value(self):
        case = {
            "columns": ["entity", "value", "ts"],
            "values": [],
            "source_entity": "entity",
            "source_value": "value_dne",
            "source_timestamp": "ts",
        }
        file_name = create_temp_file(case)
        with pytest.raises(KeyError) as err:
            label_df_from_csv(case, file_name)
        assert "column does not exist" in str(err.value)

    def test_invalid_ts(self):
        case = {
            "columns": ["entity", "value", "ts"],
            "values": [],
            "source_entity": "entity",
            "source_value": "value",
            "source_timestamp": "ts_dne",
        }
        file_name = create_temp_file(case)
        with pytest.raises(KeyError) as err:
            label_df_from_csv(case, file_name)
        assert "column does not exist" in str(err.value)

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(tmpdir):
        """Fixture to execute asserts before and after a test is run"""
        # Remove any lingering Databases
        try:
            shutil.rmtree(".featureform", onerror=del_rw)
        except:
            print("File Already Removed")
        yield
        try:
            shutil.rmtree(".featureform", onerror=del_rw)
        except:
            print("File Already Removed")


class TestTransformation(TestCase):
    def test_sql(self):
        local = ff.register_local()
        ff.register_user("featureformer").make_default_owner()
        name = "SQL"

        transactions = local.register_file(
            name="transactions",
            variant="SQL",
            description="A dataset of fraudulent transactions",
            path="transactions.csv",
        )

        @local.sql_transformation(variant="quickstart")
        def s_transformation():
            """the average transaction amount for a user"""
            return "SELECT CustomerID as entity, avg(TransactionAmount) as feature_val from {{transactions.SQL}} GROUP BY entity"

        @local.sql_transformation(variant="sql_to_sql")
        def s_transformation1():
            """the customerID for a user"""
            return "SELECT entity, feature_val from {{s_transformation.quickstart}} GROUP BY entity"

        @local.df_transformation(
            variant="quickstart", inputs=[("s_transformation1", "sql_to_sql")]
        )
        def s_average_user_transaction(transactions):
            """the average transaction amount for a user"""
            return transactions.groupby("entity")["feature_val"].mean()

        @local.sql_transformation(variant="df_to_sql")
        def s_transformation2():
            """the customerID for a user"""
            return "SELECT entity, feature_val from {{s_average_user_transaction.quickstart}} GROUP BY entity"

        res = self.sql_run_checks(s_transformation2, name, local)
        np.testing.assert_array_equal(res, np.array([1054.0]))

    def test_simple(self):
        local = ff.register_local()
        ff.register_user("featureformer").make_default_owner()
        name = "Simple"
        case = cases.transform[name]
        self.setup(case, name, local)

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation(df):
            """transformation"""
            return df

        res = self.run_checks(transformation, name, local)
        np.testing.assert_array_equal(res, np.array([1]))

    def test_simple2(self):
        local = ff.register_local()
        ff.register_user("featureformer").make_default_owner()
        name = "Simple2"
        case = cases.transform[name]
        self.setup(case, name, local)

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation(df):
            """transformation"""
            return df

        res = self.run_checks(transformation, name, local)
        np.testing.assert_array_equal(res, np.array([1]))

    def test_groupby(self):
        local = ff.register_local()
        ff.register_user("featureformer").make_default_owner()
        name = "GroupBy"
        case = cases.transform[name]
        self.setup(case, name, local)

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation(df):
            """transformation"""
            return df.groupby("entity")["values"].mean()

        res = self.run_checks(transformation, name, local)
        np.testing.assert_array_equal(res, np.array([5.5]))

    def test_complex_join(self):
        local = ff.register_local()
        ff.register_user("featureformer").make_default_owner()
        name = "Complex"
        case = cases.transform[name]
        self.setup(case, name, local)

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation1(df):
            """transformation"""
            return df.groupby("entity")["values1"].mean()

        @local.df_transformation(variant=name, inputs=[("transactions", name)])
        def transformation2(df):
            """transformation"""
            return df.groupby("entity")["values2"].mean()

        @local.df_transformation(
            variant=name, inputs=[("transformation1", name), ("transformation2", name)]
        )
        def transformation3(df1, df2):
            """transformation"""
            df = df1 + df2
            df = df.reset_index().rename(columns={0: "values"})
            return df

        res = self.run_checks(transformation3, name, local)
        np.testing.assert_array_equal(res, np.array([7.5]))

    def setup(self, case, name, local):
        file = create_temp_file(case)

        local.register_file(
            name="transactions",
            variant=name,
            description="dataset 1",
            path=file,
            owner=name,
        )

    def run_checks(self, transformation, name, local):
        transformation.register_resources(
            entity="user",
            entity_column="entity",
            inference_store=local,
            features=[
                {
                    "name": f"feature-{name}",
                    "variant": name,
                    "column": "values",
                    "type": "float32",
                },
            ],
        )
        client = ff.ResourceClient(local=True)
        client.apply()
        serve = ServingClient(local=True)
        res = serve.features([(f"feature-{name}", name)], {"user": "a"})
        serve.impl.db.close()
        return res

    def sql_run_checks(self, transformation, name, local):
        transformation.register_resources(
            entity="user",
            entity_column="entity",
            inference_store=local,
            features=[
                {
                    "name": f"feature-{name}",
                    "variant": name,
                    "column": "feature_val",
                    "type": "float32",
                },
            ],
        )
        client = ff.ResourceClient(local=True)
        client.apply()
        serve = ServingClient(local=True)
        res = serve.features([(f"feature-{name}", name)], {"user": "C1010876"})
        serve.impl.db.close()
        return res

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(tmpdir):
        """Fixture to execute asserts before and after a test is run"""
        # Remove any lingering Databases
        try:
            ff.clear_state()
            shutil.rmtree(".featureform", onerror=del_rw)
        except:
            print("File Already Removed")
        yield
        try:
            ff.clear_state()
            shutil.rmtree(".featureform", onerror=del_rw)
        except:
            print("File Already Removed")


class TestTrainingSet(TestCase):
    def _register_feature(self, feature, local, case, index, name):
        file = create_temp_file(feature)
        test_file = local.register_file(
            name=f"table-{name}-{index}", variant="v1", description="", path=file
        )
        test_file.register_resources(
            entity=case["entity"],
            entity_column=case["entity_loc"],
            inference_store=local,
            features=[
                {
                    "name": f"feat-{name}-{index}",
                    "variant": "default",
                    "column": "value",
                    "type": "bool",
                },
            ],
            timestamp_column=feature["ts_col"],
        )
        return file

    def _register_label(self, local, case, name):
        label = case["label"]
        file = create_temp_file(label)
        test_file = local.register_file(
            name=f"table-{name}-label", variant="v1", description="", path=file
        )
        test_file.register_resources(
            entity=case["entity"],
            entity_column=case["entity_loc"],
            inference_store=local,
            labels=[
                {
                    "name": f"label-{name}",
                    "variant": "default",
                    "column": "value",
                    "type": "bool",
                },
            ],
            timestamp_column=label["ts_col"],
        )
        return file

    def test_all(self):
        for name, case in cases.training_set.items():
            with self.subTest(msg=name):
                print("TEST: ", name)
                try:
                    clear_and_reset()
                except Exception as e:
                    print(f"Could Not Reset Database: {e}")
                local = ff.register_local()
                ff.register_user("featureformer").make_default_owner()
                feature_list = []
                for i, feature in enumerate(case["features"]):
                    self._register_feature(feature, local, case, i, name)
                    feature_list.append((f"feat-{name}-{i}", "default"))

                self._register_label(local, case, name)

                ff.register_training_set(
                    f"training_set-{name}",
                    "default",
                    label=(f"label-{name}", "default"),
                    features=feature_list,
                )

                client = ff.ResourceClient(local=True)
                client.apply()
                serving = ff.ServingClient(local=True)

                tset = serving.training_set(f"training_set-{name}", "default")
                serving.impl.db.close()
                actual_len = 0
                expected_len = len(case["expected"])

                for i, r in enumerate(tset):
                    actual_len += 1
                    features = [replace_nans(feature) for feature in r.features()]
                    actual = features + r.label()
                    if actual in case["expected"]:
                        case["expected"].remove(actual)
                    else:
                        raise AssertionError(
                            f"{r.features() + r.label()} not in  {case['expected']}"
                        )
                try:
                    clear_and_reset()
                except Exception as e:
                    print(f"Could Not Reset Database: {e}")
                assert actual_len == expected_len


@pytest.fixture
def proto_row():
    class ProtoRow:
        def __init__(self):
            self.features = [1, 2, 3]
            self.label = 4

        def to_numpy(self):
            row = np.array(self.features)
            row = np.append(row, self.label)
            return row

    return ProtoRow()


def test_row_to_numpy(proto_row):
    def side_effect(value):
        if value in proto_row.features:
            return value
        else:
            return proto_row.label

    ff.serving.parse_proto_value = MagicMock(side_effect=side_effect)

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


def e2e_features(
    file, entity_name, entity_loc, name_variant_type, value_cols, entities, ts_col
):
    import uuid

    local = ff.local
    transactions = ff.local.register_file(
        name="transactions",
        variant=str(uuid.uuid4())[:8],
        description="A dataset of fraudulent transactions",
        path=file,
    )
    entity = ff.register_entity(entity_name)
    for i, (name, variant, type) in enumerate(name_variant_type):
        transactions.register_resources(
            entity=entity,
            entity_column=entity_loc,
            inference_store=local,
            features=[
                {
                    "name": name,
                    "variant": variant,
                    "column": value_cols[i],
                    "type": type,
                },
            ],
            timestamp_column=ts_col,
        )
    ResourceClient(local=True).apply()
    client = ServingClient(local=True)
    results = []
    name_variant = [(name, variant) for name, variant, type in name_variant_type]
    for entity in entities:
        results.append(client.features(name_variant, entity))

    return results


def retry_delete():
    for i in range(0, 100):
        try:
            shutil.rmtree(".featureform", onerror=del_rw)
            print("Table Deleted")
            break
        except Exception as e:
            print(f"Could not delete. Retrying...", e)
            time.sleep(1)


def test_read_directory():
    from pandas.testing import assert_frame_equal

    local = LocalClientImpl()
    df = local.read_directory("client/tests/test_files/input_files/readable_directory")
    expected = pd.DataFrame(
        data={"filename": ["c.txt", "b.txt", "a.txt"], "body": ["Sup", "Hi", "Hello"]}
    )
    expected = expected.sort_values(by=expected.columns.tolist()).reset_index(drop=True)
    df = df.sort_values(by=df.columns.tolist()).reset_index(drop=True)
    assert_frame_equal(expected, df)


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
        ("client/tests/test_files/input_files/transactions.csv", "csv"),
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
