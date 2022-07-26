import csv
import shutil
import time
from tempfile import NamedTemporaryFile
from unittest import TestCase
import os, stat
import pandas as pd
import pytest
from featureform import ServingClient, ResourceClient

import serving_cases as cases


class TestIndividualFeatures(TestCase):
    def test_process_feature_no_ts(self):
        for name, case in cases.features_no_ts.items():
            with self.subTest(name):
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                dataframe_mapping = client.process_feature_csv(file_name, case['entity'], case['entity'],
                                                               case['value_col'], [], "test_name_variant", "")
                expected = pd.DataFrame(case['expected'])
                actual = dataframe_mapping[0]
                expected = expected.values.tolist()
                actual = actual.values.tolist()
                client.sqldb.close()
                assert all(elem in expected for elem in actual), \
                    "Expected: {} Got: {}".format(expected, actual)

    def test_process_feature_with_ts(self):
        for name, case in cases.features_with_ts.items():
            with self.subTest(msg=name):
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                dataframe_mapping = client.process_feature_csv(file_name, case['entity'], case['entity'],
                                                               case['value_col'], [], "test_name_variant",
                                                               case['ts_col'])
                expected = pd.DataFrame(case['expected'])
                actual = dataframe_mapping[0]
                expected = expected.values.tolist()
                actual = actual.values.tolist()
                client.sqldb.close()
                assert all(elem in expected for elem in actual), \
                    "Expected: {} Got: {}".format(expected, actual)

    def test_invalid_entity_col(self):
        case = cases.feature_invalid_entity
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_feature_csv(file_name, case['entity'], case['value_col'], case['name'], [],
                                       "test_name_variant", case['ts_col'])
        client.sqldb.close()
        assert "column does not exist" in str(err.value)

    def test_invalid_value_col(self):
        case = cases.feature_invalid_value
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_feature_csv(file_name, case['entity'], case['value_col'], case['name'], [],
                                       "test_name_variant", case['ts_col'])
        client.sqldb.close()
        assert "column does not exist" in str(err.value)

    def test_invalid_ts_col(self):
        case = cases.feature_invalid_ts
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_feature_csv(file_name, case['entity'], case['value_col'], case['name'], [],
                                       "test_name_variant", case['ts_col'])
        client.sqldb.close()
        assert "column does not exist" in str(err.value)


class TestFeaturesE2E(TestCase):
    def test_features(self):
        for name, case in cases.feature_e2e.items():
            with self.subTest(msg=name):
                file_name = create_temp_file(case)
                res = e2e_features(file_name, case['entity'], case['entity_loc'], case['features'], case['value_cols'],
                                   case['entities'], case['ts_col'])
                expected = case['expected']
                assert all(elem in expected for elem in res), \
                    "Expected: {} Got: {}".format(expected, res)
            retry_delete()

    def test_timestamp_doesnt_exist(self):
        case = {
            'columns': ['entity', 'value'],
            'values': [
                ['a', 1],
                ['b', 2],
                ['c', 3],

            ],
            'value_cols': ['value'],
            'entity': 'entity',
            'entity_loc': 'entity',
            'features': [("avg_transactions", "quickstart")],
            'entities': [{"entity": "a"}, {"entity": "b"}, {"entity": "c"}],
            'expected': [[1], [2], [3]],
            'ts_col': "ts"
        }
        file_name = create_temp_file(case)
        with pytest.raises(KeyError) as err:
            e2e_features(file_name, case['entity'], case['entity_loc'], case['features'], case['value_cols'],
                         case['entities'], case['ts_col'])
        assert "column does not exist" in str(err.value)

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(tmpdir):
        """Fixture to execute asserts before and after a test is run"""
        # Remove any lingering Databases
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")
        yield
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")

class TestIndividualLabels(TestCase):
    def test_individual_labels(self):
        for name, case in cases.labels.items():
            with self.subTest(name):
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                actual = client.process_label_csv(file_name, case['entity_name'], case['entity_col'], case['value_col'], case['ts_col'])
                expected = pd.DataFrame(case['expected']).set_index(case['entity_name'])
                pd.testing.assert_frame_equal(actual, expected)

    def test_invalid_entity(self):
        case = {
            'columns': ['entity', 'value', 'ts'],
            'values': [],
            'entity_name': 'entity',
            'entity_col': 'name_dne',
            'value_col': 'value',
            'ts_col': 'ts'
        }
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_label_csv(file_name, case['entity_name'], case['entity_col'], case['value_col'], case['ts_col'])
        assert "column does not exist" in str(err.value)

    def test_invalid_value(self):
        case = {
            'columns': ['entity', 'value', 'ts'],
            'values': [],
            'entity_name': 'entity',
            'entity_col': 'entity',
            'value_col': 'value_dne',
            'ts_col': 'ts'
        }
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_label_csv(file_name, case['entity_name'], case['entity_col'], case['value_col'], case['ts_col'])
        assert "column does not exist" in str(err.value)

    def test_invalid_ts(self):
        case = {
            'columns': ['entity', 'value', 'ts'],
            'values': [],
            'entity_name': 'entity',
            'entity_col': 'entity',
            'value_col': 'value',
            'ts_col': 'ts_dne'
        }
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        with pytest.raises(KeyError) as err:
            client.process_label_csv(file_name, case['entity_name'], case['entity_col'], case['value_col'], case['ts_col'])
        assert "column does not exist" in str(err.value)


def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)


def create_temp_file(test_values):
    file = NamedTemporaryFile(delete=False)
    with open(file.name, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='|')
        writer.writerow(test_values['columns'])
        for row in test_values['values']:
            writer.writerow(row)

    return file.name


def e2e_features(file, entity_name, entity_loc, name_variants, value_cols, entities, ts_col):
    ff = ResourceClient("")
    ff.register_user("featureformer").make_default_owner()
    local = ff.register_local()
    transactions = local.register_file(
        name="transactions",
        variant="quickstart",
        description="A dataset of fraudulent transactions",
        path=file
    )
    entity = ff.register_entity(entity_name)
    for i, variant in enumerate(name_variants):
        transactions.register_resources(
            entity=entity,
            entity_column=entity_loc,
            inference_store=local,
            features=[
                {"name": variant[0], "variant": variant[1], "column": value_cols[i], "type": "float32"},
            ],
            timestamp_column=ts_col
        )
    ff.state().create_all_local()
    client = ServingClient(local=True)
    results = []
    for entity in entities:
        results.append(client.features(name_variants, entity))
    return results


def retry_delete():
    for i in range(0, 100):
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
            print("Table Deleted")
            break
        except Exception:
            print("Could not delete. Retrying...")
            time.sleep(1)

