Feature: Features with Timestamp
  @use_autovariants
  Scenario Outline: Registering Features with Timestamp Column
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register an offline provider of type "<offline_provider_type>"
    And I register an online provider of type "<online_provider_type>"
    And I register a dataset located at "<dataset_path>"
    And I register a feature on "<feature_column>" with type "<feature_type>" with "<entity_column>", "<timestamp_column>", and "<label_column>"
    Then I can serve the registered feature with the model for "<user>" with "<expected_value>"

    Examples: Postgres with Redis
      | online_provider_type | offline_provider_type | dataset_path |  feature_column   | feature_type | entity_column | timestamp_column | label_column |   user   | expected_value |
      |        redis         |        postgres       | transactions | transactionamount |    float64   |   customerid  |       empty      |   isfraud    | C5841053 |      25.0      |
      |        redis         |        postgres       | transactions | transactionamount |    float64   |   customerid  |     timestamp    |   isfraud    | C5841053 |      25.0      |
    
    Examples: Redis with Spark
      | online_provider_type | offline_provider_type |                     dataset_path                      |  feature_column   | feature_type | entity_column | timestamp_column | label_column |   user   | expected_value |
      |        redis         |         spark         | s3a://featureform-spark-testing/data/transactions.csv | TransactionAmount |    float64   |   CustomerId  |       empty      |   IsFraud    | C5841053 |      25.0      |
      |        redis         |         spark         | s3a://featureform-spark-testing/data/transactions.csv | TransactionAmount |    float64   |   CustomerId  |     Timestamp    |   IsFraud    | C5841053 |      25.0      |
