import os
import random

import uuid
import featureform as ff
from behave import when, then


@when("I turn on autovariants")
def step_impl(context):
    os.environ["FF_GET_EQUIVALENT_VARIANTS"] = "true"

    # Set the a unique variant prefix because we were running
    # into an issue with behave tests running in parallel. This would
    # cause the same variant to be generated for different tests.
    # context.unique_prefix = str(uuid.uuid4())
    # ff.set_variant_prefix(context.unique_prefix)


@then("I turn off autovariants")
def step_impl(context):
    os.environ["FF_GET_EQUIVALENT_VARIANTS"] = "false"


@when("I register a transformation with auto variant")
def step_impl(context):
    @context.spark.df_transformation(
        inputs=[context.transactions],
    )
    def some_transformation(df):
        """Unedited transactions"""
        return df

    context.client.apply(asynchronous=False, verbose=True)
    context.transformation = some_transformation


@then("I should be able to reuse the same variant for the same transformation")
def step_impl(context):
    @context.spark.df_transformation(
        inputs=[context.transactions],
    )
    def some_transformation(df):
        """Unedited transactions"""
        return df

    context.client.apply(asynchronous=False, verbose=True)
    context.new_transformation = some_transformation

    assert (
        context.new_transformation.name_variant()
        == context.transformation.name_variant()
    )


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

    assert (
        context.new_transformation.name_variant()
        != context.transformation.name_variant()
    )


@then("I can get the transformation as df")
def step_impl(context):
    df = context.client.dataframe(context.new_transformation)
    assert len(df) > 0


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


@then("I should be able to register a modified transformation with new auto variant")
def step_impl(context):
    @context.spark.df_transformation(
        inputs=[context.transactions],
    )
    def some_transformation(df):
        """modified transactions"""
        from pyspark.sql.functions import lit

        df = df.withColumn("new_column", lit("hi.world"))
        return df

    context.client.apply(asynchronous=False, verbose=True)
    context.new_transformation = some_transformation

    assert (
        context.new_transformation.name_variant()[0]
        == context.transformation.name_variant()[0]
    )
    assert (
        context.new_transformation.name_variant()[1]
        != context.transformation.name_variant()[1]
    )


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
    assert len(df) > 0
