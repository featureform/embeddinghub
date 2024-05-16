Feature: Get Source

  @use_autovariants
  Scenario Outline: Registering a source and getting the source for next step
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register an offline provider of type "<offline_provider_type>"
    And I register a dataset located at "<dataset_path>"
    And I get the source for the dataset
    Then I can register a transformation based on the source
    And I can serve the transformation

    Examples:
      | offline_provider_type | dataset_path                                          |
      | postgres              | transactions                                          |
      | spark                 | s3a://featureform-spark-testing/data/transactions.csv |
