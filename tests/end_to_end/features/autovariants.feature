Feature: AutoVariants

  @use_autovariants
  Scenario: Same auto variant for same transformation
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register "s3" filestore with bucket "featureform-spark-testing" and root path "data"
    And I register databricks
    And I register transactions_short.csv
    And I register a transformation with auto variant
    Then I should be able to reuse the same variant for the same transformation
    And I can get the transformation as df

  @use_autovariants
  Scenario: User-defined variant and auto variant for same transformation
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register "s3" filestore with bucket "featureform-spark-testing" and root path "data"
    And I register databricks
    And I register transactions_short.csv
    And I register a transformation with user-provided variant
    Then I should be able to register a new auto variant transformation
    And I can get the transformation as df

  @use_autovariants
  Scenario: Different auto variant for different transformation
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register "s3" filestore with bucket "featureform-spark-testing" and root path "data"
    And I register databricks
    When I register transactions_short.csv
    And I register a transformation with auto variant
    Then I should be able to register a modified transformation with new auto variant
    And I can get the transformation as df

  @use_autovariants
  Scenario: Create auto variant dataset and register a new dataset with user-defined variant
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register "s3" filestore with bucket "featureform-spark-testing" and root path "data"
    And I register databricks
    And I register transactions_short.csv
    Then I should be able to register a source with user-defined variant
    And I can get the source as df
