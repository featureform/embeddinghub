Feature: Model Registration
  @use_autovariants
  Scenario Outline: Registering a model with both non-existing and existing feature and training set
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register an offline provider of type "<offline_provider_type>"
    And I register an online provider of type "<online_provider_type>"
    And I register a dataset located at "<dataset_path>"
    And I register a feature on "<feature_column>" with type "<feature_type>" with "<entity_column>", "<timestamp_column>", and "<label_column>"
    Then I can register a training set based on the feature
    And I cannot serve the non-existing feature with the model
    And I can serve the registered feature with the model for "<user>" with "<expected_value>"
    And I cannot serve the non-existing training set with the model
    And I can serve the registered training set with the model

    Examples:
      | online_provider_type | offline_provider_type |                     dataset_path                      |  feature_column   | feature_type | entity_column | timestamp_column | label_column |   user   | expected_value |
      |        redis         |        postgres       |                     transactions                      | transactionamount |    float64   |   customerid  |       empty      |   isfraud    | C5841053 |      25.0      |
