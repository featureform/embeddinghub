#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
import random

import featureform as ff
from behave import when, then

# comment

@when("I turn on autovariants")
def step_impl(context):
    os.environ["FF_GET_EQUIVALENT_VARIANTS"] = "true"

    context.variant = ff.get_run()


@then("I turn off autovariants")
def step_impl(context):
    os.environ["FF_GET_EQUIVALENT_VARIANTS"] = "false"

    context.variant = ff.get_run()


@when(
    'I register "{transformation_name}" transformation with auto variant of type "{tf_type}"'
)
def step_impl(context, transformation_name, tf_type):
    if tf_type == "sql":

        @context.spark.sql_transformation(
            name=transformation_name,
            inputs=[context.transactions],
        )
        def some_transformation(inp):
            """Unedited transactions"""
            return "select * from {{ inp }}"

    else:

        @context.spark.df_transformation(
            name=transformation_name,
            inputs=[context.transactions],
        )
        def some_transformation(df):
            """Unedited transactions"""
            return df

    context.client.apply(asynchronous=False, verbose=True)
    context.transformation = some_transformation


@then(
    'I should be able to reuse the same variant for the same "{transformation_name}" transformation of type "{tf_type}"'
)
def step_impl(context, transformation_name, tf_type):
    if tf_type == "sql":

        @context.spark.sql_transformation(
            name=transformation_name,
            inputs=[context.transactions],
        )
        def some_transformation(inp):
            """Unedited transactions"""
            return "select * from {{ inp }}"

    else:
        @context.spark.df_transformation(
            name=transformation_name,
            inputs=[context.transactions],
        )
        def some_transformation(df):
            """Unedited transactions"""
            return df

    context.client.apply(asynchronous=False, verbose=True)
    context.new_transformation = some_transformation

    new_nv = context.new_transformation.name_variant()
    old_nv = context.transformation.name_variant()

    assert new_nv == old_nv, f"Expected: {new_nv} Got: {old_nv}"


@then("I should be able to register a new auto variant transformation")
def step_impl(context):
    @context.spark.df_transformation(
        inputs=[context.transactions],
    )
    def some_transformation(df):
        """Unedited transactions"""
        return df

    context.client.apply(asynchronous=False, verbose=True)
    context.new_transformation = some_transformation

    new_nv = context.new_transformation.name_variant()
    old_nv = context.transformation.name_variant()
    assert new_nv == old_nv, f"Expected: {old_nv} Got: {new_nv}"


@then("I can get the transformation as df")
def step_impl(context):
    df = context.client.dataframe(context.new_transformation)
    assert len(df) > 0, f"Expected: > 0 Got: {len(df)}"


@when("I register a transformation with user-provided variant")
def step_impl(context):
    new_random_variant = f"v{str(random.randint(10000, 99999))}"

    @context.spark.df_transformation(
        variant=new_random_variant,
        inputs=[context.transactions],
    )
    def some_transformation(df):
        """Unedited transactions"""
        return df

    context.client.apply(asynchronous=False, verbose=True)
    context.transformation = some_transformation


@then(
    'I should be able to register a modified "{name}" transformation with new auto variant of type "{tf_type}"'
)
def step_impl(context, name, tf_type):
    if tf_type == "sql":

        @context.spark.sql_transformation(
            name=name,
            inputs=[context.transactions],
        )
        def some_transformation(inp):
            """modified transactions"""
            return "select * from {{ inp }} limit 10"

    else:

        @context.spark.df_transformation(
            name=name,
            inputs=[context.transactions],
        )
        def some_transformation(df):
            """modified transactions"""
            from pyspark.sql.functions import lit

            df = df.withColumn("new_column", lit("hi.world"))
            return df

    context.client.apply(asynchronous=False, verbose=True)
    context.new_transformation = some_transformation

    new_nv = context.new_transformation.name_variant()
    old_nv = context.transformation.name_variant()
    assert new_nv[0] == old_nv[0], f"Expected Name:  {old_nv[0]} Got: {new_nv[0]}"
    assert (
        new_nv[1] != old_nv[1]
    ), f"Expected different variant but got the same variant: {old_nv[1]}"


@then("I should be able to register a source with user-defined variant")
def step_impl(context):
    new_random_variant = f"v{str(random.randint(10000, 99999))}"
    context.new_source = context.spark.register_file(
        name="transactions_short",
        variant=new_random_variant,
        file_path="s3://featureform-spark-testing/data/transactions_short.csv",
    )
    context.client.apply(asynchronous=False, verbose=True)


@then("I can get the source as df")
def step_impl(context):
    df = context.client.dataframe(context.new_source)
    assert len(df) > 0, f"Expected: > 0 Got: {len(df)}"
