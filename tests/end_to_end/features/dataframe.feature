Feature: Serving Pandas Dataframe with client.dataframe method
  @use_autovariants
  Scenario Outline: Getting the Dataframe of a Transformation
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register an offline provider of type "<offline_provider_type>"
    And I register a dataset located at "<dataset_path>"
    And I register a "<transformation_type>" transformation with "<limit>" rows
    Then I can call client.dataframe on the transformation with "<expected_num_rows>"

    Examples: Spark
      | offline_provider_type |                    dataset_path                       | transformation_type | limit | expected_num_rows |
      |         spark         | s3a://featureform-spark-testing/data/transactions.csv |         df          |   0   |        0          |
      |         spark         | s3a://featureform-spark-testing/data/transactions.csv |         df          |   1   |        1          |
      |         spark         | s3a://featureform-spark-testing/data/transactions.csv |         df          |  none |      10000        |
