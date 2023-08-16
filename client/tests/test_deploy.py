
import pytest
from featureform.deploy import (
    Deployment,
)


@pytest.mark.parametrize(
    "quickstart",
    [
        (True),
        (False),
    ],
)
def test_deployment_class(quickstart):
    deployment = Deployment(quickstart)
    assert deployment is not None
    assert deployment._quickstart == quickstart
    assert deployment.start() is None
    assert deployment.stop() is None
    assert deployment.get_status() is None
    assert deployment.get_config() == []


@pytest.mark.parametrize(
    "deployment, expected_config",
    [
        ("docker_deployment", "docker_deployment_config"),
        ("docker_quickstart_deployment", "docker_quickstart_deployment_config"),
    ],
)
def test_deployment_config(deployment, expected_config, request):
    deployment = request.getfixturevalue(deployment)
    expected_config = request.getfixturevalue(expected_config)
    config = deployment.get_config()

    assert len(config) == len(expected_config)

    for c, expected_c in zip(config, expected_config):
        for name, value in c._asdict().items():
            assert value == getattr(expected_c, name)


@pytest.mark.parametrize(
    "deployment, expected_status",
    [
        ("docker_deployment", "docker_deployment_status"),
        # ("docker_deployment_failure", "docker_deployment_failure_status"),
    ],
)
def test_deployment_status(deployment, expected_status, request):
    deployment = request.getfixturevalue(deployment)
    expected_status = request.getfixturevalue(expected_status)
    status = deployment.get_status()

    assert status == expected_status


@pytest.mark.parametrize(
    "deployment, expected_failure",
    [
        ("docker_deployment", False),
        # ("docker_quickstart_deployment", False),
    ],
)
def test_deployment(deployment, expected_failure, request):
    d = request.getfixturevalue(deployment)
    assert d.start() == (not expected_failure)
    assert d.stop() == (not expected_failure)
