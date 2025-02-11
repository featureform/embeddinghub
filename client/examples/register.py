#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from featureform import ResourceClient

ff = ResourceClient("localhost:8000")

user = ff.register_user("test")
user.make_default_owner()
snowflake = ff.register_snowflake(
    name="snowflake",
    username="username",
    password="password",
    account="account",
    organization="organization",
    database="database",
    schema="schema",
    description="Snowflake",
    team="Featureform Success Team",
)
table = snowflake.register_table(
    name="transaction",
    variant="final",
    table="Transactions",
    description="Transactions file from Kaggle",
)


@snowflake.sql_transformation(
    variant="variant",
)
def transform():
    """Get all transactions over $500"""
    return "SELECT * FROM {{transactions.final}} WHERE amount > 500"


entity = ff.register_entity("user")
redis = ff.register_redis(
    name="redis",
    host="localhost",
    port=1234,
    password="pass",
    db=0,
)

resources = transform.register_resources(
    entity=entity,
    entity_column="abc",
    inference_store=redis,
    features=[
        {"name": "a", "variant": "b", "column": "c", "type": "float32"},
    ],
    labels=[
        {"name": "la", "variant": "lb", "column": "lc", "type": "float32"},
    ],
    timestamp_column="ts",
)

resources.create_training_set(name="ts", variant="v1")
