Feature: MultiFeature Registration

  @MultiFeature
  Scenario: Registering all but one feature from one table
    Given Featureform is installed
    When I register postgres
    And I register a table from postgres
    And I create a dataframe from a serving client
    And I register redis
    Then I define a User and register multiple features excluding one
    Then I should be able to serve a batch of features

  @MultiFeature
  Scenario: Registering three features from one large table
    Given Featureform is installed
    When I register postgres
    And I register a table from postgres
    And I create a dataframe from a serving client
    And I register redis
    Then I define a User and register multiple but not all features, with no timestamp column
    Then I should be able to serve a batch of features