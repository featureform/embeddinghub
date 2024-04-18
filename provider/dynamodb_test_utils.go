package provider

import (
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"os"
	"testing"
)

func GetTestingDynamoDB(t *testing.T) OnlineStore {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	dynamoAccessKey, ok := os.LookupEnv("DYNAMO_ACCESS_KEY")
	if !ok {
		t.Fatalf("missing DYNAMO_ACCESS_KEY variable")
	}
	dynamoSecretKey, ok := os.LookupEnv("DYNAMO_SECRET_KEY")
	if !ok {
		t.Fatalf("missing DYNAMO_SECRET_KEY variable")
	}
	endpoint := os.Getenv("DYNAMO_ENDPOINT")
	dynamoConfig := &pc.DynamodbConfig{
		Region:    "us-east-1",
		AccessKey: dynamoAccessKey,
		SecretKey: dynamoSecretKey,
		Endpoint:  endpoint,
	}

	store, err := GetOnlineStore(pt.DynamoDBOnline, dynamoConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}
	return store
}
