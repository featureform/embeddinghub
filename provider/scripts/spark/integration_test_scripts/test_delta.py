#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import offline_store_spark_runner as ff_runner


def main(args):
    spark = ff_runner.init_spark_config(args.spark_config)
    data = [(1, "Alice", 29), (2, "Bob", 31), (3, "Cathy", 25)]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, schema=columns)
    table_name = "deltatest"

    client = ff_runner.DeltaClient(spark, "delta_poc")
    try:
        client.delete_table(table_name)
    except:
        pass

    if client.table_exists(table_name):
        raise Exception("Table already exists")
    try:
        client.create_table(table_name, df)
        if not client.table_exists(table_name):
            raise Exception("Created table but not exists")
        got_df = client.read_table(table_name)
        if df.collect() != got_df.collect():
            raise Exception("Dataframe not equal")
    except Exception as e:
        print(e)
    finally:
        client.delete_table(table_name)


if __name__ == "__main__":
    main(ff_runner.parse_args())
