Feature: Client API
    Scenario: Data Source Columns (SQL Provider)
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        When I register postgres
        And I generate a random variant name
        And I register a table from postgres
        Then I should get the columns for the data source from "postgres"

    Scenario: Data Source Columns (Spark)
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        And I generate a random variant name
        And I register "s3" filestore with bucket "featureform-spark-testing" and root path "data"
        And I register databricks
        And I register transactions_short.csv
        Then I should get the columns for the data source from "spark"

    Scenario: Reusing Providers
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        And I generate a random variant name
        And I register "s3" filestore with bucket "featureform-spark-testing" and root path "data"
        And I register databricks
        Then I should be able to get s3 provider
        And I should be able to get databricks provider
        And I should be able to register spark provider
        And I should be able to get spark provider
        And I should be able to register transactions_short.csv
        And I should be able to register a transformation with spark provider
        And I should be able to get the data of the transformation

    Scenario: Reusing Sources
        Given Featureform is installed
        And I have a "hosted" "insecure" client for "localhost:7878"
        And I have registered Spark Databricks provider with S3 filestore
        When I register transactions_short.csv
        Then I should be able to get the source for transactions_short.csv
        And I should be able to register a transformation with the source
        And I should be able to get the data of the transformation
