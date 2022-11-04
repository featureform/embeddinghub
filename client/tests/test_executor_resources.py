
import pytest

from featureform.resources import DatabricksCredentials, EMRCredentials


@pytest.mark.parametrize(
        "username,password,host,token",
    [   
        ("a", "b", "", ""),
        ("", "", "a", "b"),
        pytest.param("a", "b", "a", "b", marks=pytest.mark.xfail),
        pytest.param("", "", "", "", marks=pytest.mark.xfail)
    ]
)
def test_databricks_credentials(username, password, host, token):
    databricks = DatabricksCredentials(username=username, password=password, host=host, token=token)

    expected_config = {
        "username": username,
        "password": password,
        "host": host,
        "token": token
    }

    assert databricks.type() == "databricks"
    assert databricks.config() == expected_config


@pytest.mark.parametrize(
        "aws_access_key_id,aws_secret_access_key,emr_cluster_id,emr_cluster_region",
    [   
        ("a", "b", "c", "d"),
        pytest.param("", "", "a", "b", marks=pytest.mark.xfail),
        pytest.param("", "", "", "", marks=pytest.mark.xfail)
    ]
)
def test_emr_credentials(aws_access_key_id, aws_secret_access_key, emr_cluster_id, emr_cluster_region):
    emr = EMRCredentials(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, emr_cluster_id=emr_cluster_id, emr_cluster_region=emr_cluster_region)

    expected_config = {
        "AWSAccessKeyId": aws_access_key_id,
        "AWSSecretKey": aws_secret_access_key,
        "ClusterName": emr_cluster_id,
        "ClusterRegion": emr_cluster_region
    }

    assert emr.type() == "emr"
    assert emr.config() == expected_config
