# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
import os.path
import sys

sys.path.insert(0, 'client/src/')
import pytest
from featureform.resources import ResourceRedefinedError, ResourceState, Provider, RedisConfig, CassandraConfig, \
    FirestoreConfig, \
    SnowflakeConfig, PostgresConfig, RedshiftConfig, BigQueryConfig, OnlineBlobConfig, K8sConfig, \
    User, Provider, Entity, Feature, Label, TrainingSet, PrimaryData, SQLTable, \
    Source, ResourceColumnMapping, DynamodbConfig, Schedule

from featureform.proto import metadata_pb2 as pb


@pytest.fixture
def postgres_config():
    return PostgresConfig(
        host="localhost",
        port="123",
        database="db",
        user="user",
        password="p4ssw0rd",
    )


@pytest.fixture
def snowflake_config():
    return SnowflakeConfig(
        account="act",
        database="db",
        organization="org",
        username="user",
        password="pwd",
        schema="schema",
    )


@pytest.fixture
def redis_config():
    return RedisConfig(
        host="localhost",
        port=123,
        password="abc",
        db=3,
    )


@pytest.fixture
def blob_store_config():
    return AzureFileStoreConfig(
        account_name="<account_name>",
        account_key="<account_key>",
        container_name="examplecontainer",
        root_path="example/path",
    )


@pytest.fixture
def online_blob_config(blob_store_config):
    return OnlineBlobConfig(
        store_type="AZURE",
        store_config=blob_store_config.serialize(),
    )


@pytest.fixture
def file_store_provider(blob_store_config):
    return FileStoreProvider(
        regitrar=None,
        provider=None,
        config=blob_store_config,
        store_type="AZURE"
    )


@pytest.fixture
def docker_image():
    return "my-docker-image:latest"

@pytest.fixture
def k8s_store_type():
    return "AZURE"

@pytest.fixture()
def executor_proto(docker_image):
    return pb.ExecutorConfig(
        docker_image=docker_image
    )


@pytest.fixture()
def store_config_proto():
    return pb.AzureBlobStoreConfig(
        account_name="account name",
        account_key="account key",
        container_name="container name",
        container_path="container path"
    )


@pytest.fixture()
def kubernetes_proto(executor_proto, store_config_proto, k8s_store_type):
    return pb.KCFConfig(
        executor_type="K8S",
        executor_config=executor_proto,
        store_type=k8s_store_type,
        store_config=store_config_proto
    )

@pytest.fixture()
def serialized_kubernetes_proto(kubernetes_proto):
    return kubernetes_proto.SerializeToString()


@pytest.fixture
def kubernetes_config(executor_proto, store_config_proto, docker_image, k8s_store_type):
    return K8sConfig(
        store_type=k8s_store_type,
        store=store_config_proto,
        docker_image=docker_image
    )


def test_kubernetes_serialize(kubernetes_config, serialized_kubernetes_proto):
    assert kubernetes_config.serialize() == serialized_kubernetes_proto


@pytest.fixture
def cassandra_config():
    return CassandraConfig(
        host="localhost",
        port="123",
        username="abc",
        password="abc",
        keyspace="",
        consistency="THREE",
        replication=3,
    )


@pytest.fixture
def firesstore_config():
    return FirestoreConfig(
        collection="abc",
        project_id="abc",
        credentials_path="abc",
    )


@pytest.fixture
def dynamodb_config():
    return DynamodbConfig(
        region="abc",
        access_key="abc",
        secret_key="abc"
    )


@pytest.fixture
def redshift_config():
    return RedshiftConfig(
        host="",
        port="5439",
        database="dev",
        user="user",
        password="p4ssw0rd",
    )


@pytest.fixture
def bigquery_config():
    path = os.path.abspath(os.getcwd()) + "/client/tests/test_files/bigquery_dummy_credentials.json"
    return BigQueryConfig(
        project_id="bigquery-project",
        dataset_id="bigquery-dataset",
        credentials_path=path,
    )


def test_bigquery_config(bigquery_config):
    return bigquery_config.serialize()


@pytest.fixture
def postgres_provider(postgres_config):
    return Provider(
        name="postgres",
        description="desc2",
        function="fn2",
        team="team2",
        config=postgres_config,
    )


@pytest.fixture
def snowflake_provider(snowflake_config):
    return Provider(
        name="snowflake",
        description="desc3",
        function="fn3",
        team="team3",
        config=snowflake_config,
    )


@pytest.fixture
def redis_provider(redis_config):
    return Provider(
        name="redis",
        description="desc3",
        function="fn3",
        team="team3",
        config=redis_config,
    )


@pytest.fixture
def redshift_provider(redshift_config):
    return Provider(
        name="redshift",
        description="desc2",
        function="fn2",
        team="team2",
        config=redshift_config,
    )


@pytest.fixture
def bigquery_provider(bigquery_config):
    return Provider(
        name="bigquery",
        description="desc2",
        function="fn2",
        team="team2",
        config=bigquery_config,
    )


def init_feature(input):
    Feature(name="feature",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type=input,
            entity="user",
            owner="Owner",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            provider="redis-name")


@pytest.mark.parametrize("input,fail", [
    ("int", False), ("int32", False), ("int64", False), ("float32", False), ("float64", False), ("string", False),
    ("bool", False), ("datetime", False), ("datetime", False), ("none", True), ("str", True),
])
def test_valid_feature_column_types(input, fail):
    if not fail:
        init_feature(input)
    if fail:
        with pytest.raises(ValueError):
            init_feature(input)


def init_label(input):
    Label(name="feature",
          variant="v1",
          source=("a", "b"),
          description="feature",
          value_type=input,
          entity="user",
          owner="Owner",
          location=ResourceColumnMapping(
              entity="abc",
              value="def",
              timestamp="ts",
          ),
          provider="redis-name")


@pytest.mark.parametrize("input,fail", [
    ("int", False), ("int32", False), ("int64", False), ("float32", False), ("float64", False), ("string", False),
    ("bool", False), ("datetime", False), ("datetime", False), ("none", True), ("str", True),
])
def test_valid_label_column_types(input, fail):
    if not fail:
        init_label(input)
    if fail:
        with pytest.raises(ValueError):
            init_label(input)


@pytest.fixture
def all_resources_set(redis_provider):
    return [
        redis_provider,
        User(name="Featureform"),
        Entity(name="user", description="A user"),
        Source(name="primary",
               variant="abc",
               definition=PrimaryData(location=SQLTable("table")),
               owner="someone",
               description="desc",
               provider="redis-name"),
        Feature(name="feature",
                variant="v1",
                source=("a", "b"),
                description="feature",
                value_type="float32",
                entity="user",
                owner="Owner",
                location=ResourceColumnMapping(
                    entity="abc",
                    value="def",
                    timestamp="ts",
                ),
                provider="redis-name"),
        Label(
            name="label",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            entity="user",
            owner="Owner",
            provider="redis-name"
        ),
        TrainingSet(name="training-set",
                    variant="v1",
                    description="desc",
                    owner="featureform",
                    label=("label", "var"),
                    feature_lags=[],
                    features=[("f1", "var")]),
    ]


@pytest.fixture
def all_resources_strange_order(redis_provider):
    return [
        TrainingSet(name="training-set",
                    variant="v1",
                    description="desc",
                    owner="featureform",
                    feature_lags=[],
                    label=("label", "var"),
                    features=[("f1", "var")]),
        Label(
            name="label",
            variant="v1",
            source=("a", "b"),
            description="feature",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            value_type="float32",
            entity="user",
            owner="Owner",
            provider="redis-name"
        ),
        Feature(name="feature",
                variant="v1",
                source=("a", "b"),
                description="feature",
                value_type="float32",
                entity="user",
                location=ResourceColumnMapping(
                    entity="abc",
                    value="def",
                    timestamp="ts",
                ),
                owner="Owner",
                provider="redis-name"),
        Entity(name="user", description="A user"),
        Source(name="primary",
               variant="abc",
               definition=PrimaryData(location=SQLTable("table")),
               owner="someone",
               description="desc",
               provider="redis-name"),
        redis_provider,
        User(name="Featureform"),
    ]


def test_create_all_provider_types(redis_provider, snowflake_provider,
                                   postgres_provider, redshift_provider, bigquery_provider):
    providers = [
        redis_provider,
        snowflake_provider,
        postgres_provider,
        redshift_provider,
        bigquery_provider,
    ]
    state = ResourceState()
    for provider in providers:
        state.add(provider)


def test_redefine_provider(redis_config, snowflake_config):
    providers = [
        Provider(name="name",
                 description="desc",
                 function="fn",
                 team="team",
                 config=redis_config),
        Provider(name="name",
                 description="desc2",
                 function="fn2",
                 team="team2",
                 config=snowflake_config),
    ]
    state = ResourceState()
    state.add(providers[0])
    with pytest.raises(ResourceRedefinedError):
        state.add(providers[1])


def test_add_all_resource_types(all_resources_strange_order, redis_config):
    state = ResourceState()
    for resource in all_resources_strange_order:
        state.add(resource)
    assert state.sorted_list() == [
        User(name="Featureform"),
        Provider(
            name="redis",
            description="desc3",
            function="fn3",
            team="team3",
            config=redis_config,
        ),
        Source(name="primary",
               variant="abc",
               definition=PrimaryData(location=SQLTable("table")),
               owner="someone",
               description="desc",
               provider="redis-name"),
        Entity(name="user", description="A user"),
        Feature(name="feature",
                variant="v1",
                source=("a", "b"),
                description="feature",
                value_type="float32",
                location=ResourceColumnMapping(
                    entity="abc",
                    value="def",
                    timestamp="ts",
                ),
                entity="user",
                owner="Owner",
                provider="redis-name"),
        Label(
            name="label",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            entity="user",
            owner="Owner",
            provider="redis-name"
        ),
        TrainingSet(name="training-set",
                    variant="v1",
                    description="desc",
                    owner="featureform",
                    label=("label", "var"),
                    features=[("f1", "var")],
                    feature_lags=[]),
    ]


def test_resource_types_differ(all_resources_set):
    types = set()
    for resource in all_resources_set:
        t = resource.type()
        assert t not in types
        types.add(t)


def test_invalid_providers(snowflake_config):
    provider_args = [
        {
            "description": "desc",
            "function": "fn",
            "team": "team",
            "config": snowflake_config
        },
        {
            "name": "name",
            "description": "desc",
            "team": "team",
            "config": snowflake_config
        },
        {
            "name": "name",
            "description": "desc",
            "function": "fn",
            "team": "team"
        },
    ]
    for args in provider_args:
        with pytest.raises(TypeError):
            Provider(**args)


def test_invalid_users():
    user_args = [{}]
    for args in user_args:
        with pytest.raises(TypeError):
            User(**args)


@pytest.mark.parametrize("args", [
    {
        "name": "name",
        "variant": "var",
        "owner": "featureform",
        "description": "abc",
        "label": ("var"),
        "features": [("a", "var")],
    },
    {
        "name": "name",
        "variant": "var",
        "owner": "featureform",
        "label": ("", "var"),
        "description": "abc",
        "features": [("a", "var")],
    },
    {
        "name": "name",
        "variant": "var",
        "owner": "featureform",
        "label": ("a", "var"),
        "features": [],
        "description": "abc",
    },
    {
        "name": "name",
        "variant": "var",
        "owner": "featureform",
        "label": ("a", "var"),
        "features": [("a", "var"), ("var")],
        "description": "abc",
    },
    {
        "name": "name",
        "variant": "var",
        "owner": "featureform",
        "label": ("a", "var"),
        "features": [("a", "var"), ("", "var")],
        "description": "abc",
    },
    {
        "name": "name",
        "variant": "var",
        "owner": "featureform",
        "label": ("a", "var"),
        "features": [("a", "var"), ("b", "")],
        "description": "abc",
    },
])
def test_invalid_training_set(args):
    with pytest.raises((ValueError, TypeError)):
        TrainingSet(**args)


def test_add_all_resources_with_schedule(all_resources_strange_order, redis_config):
    state = ResourceState()
    for resource in all_resources_strange_order:
        if hasattr(resource, 'schedule'):
            resource.update_schedule("* * * * *")
        state.add(resource)
    assert state.sorted_list() == [
        User(name="Featureform"),
        Provider(
            name="redis",
            description="desc3",
            function="fn3",
            team="team3",
            config=redis_config,
        ),
        Source(name="primary",
               variant="abc",
               definition=PrimaryData(location=SQLTable("table")),
               owner="someone",
               description="desc",
               provider="redis-name",
               schedule="* * * * *",
               schedule_obj=Schedule(name="primary", variant="abc", resource_type=7, schedule_string="* * * * *")),
        Entity(name="user", description="A user"),
        Feature(name="feature",
                variant="v1",
                source=("a", "b"),
                description="feature",
                value_type="float32",
                location=ResourceColumnMapping(
                    entity="abc",
                    value="def",
                    timestamp="ts",
                ),
                entity="user",
                owner="Owner",
                provider="redis-name",
                schedule="* * * * *",
                schedule_obj=Schedule(name="feature", variant="v1", resource_type=4, schedule_string="* * * * *")),
        Label(
            name="label",
            variant="v1",
            source=("a", "b"),
            description="feature",
            value_type="float32",
            location=ResourceColumnMapping(
                entity="abc",
                value="def",
                timestamp="ts",
            ),
            entity="user",
            owner="Owner",
            provider="redis-name"
        ),
        TrainingSet(name="training-set",
                    variant="v1",
                    description="desc",
                    owner="featureform",
                    label=("label", "var"),
                    features=[("f1", "var")],
                    feature_lags=[],
                    schedule="* * * * *",
                    schedule_obj=Schedule(name="training-set", variant="v1", resource_type=6,
                                          schedule_string="* * * * *")),
        Schedule(name="feature",
                 variant="v1",
                 resource_type=4,
                 schedule_string="* * * * *"),
        Schedule(name="primary",
                 variant="abc",
                 resource_type=7,
                 schedule_string="* * * * *"),
        Schedule(name="training-set",
                 variant="v1",
                 resource_type=6,
                 schedule_string="* * * * *"),
    ]
