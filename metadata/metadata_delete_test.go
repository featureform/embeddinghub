package metadata

import (
	"context"
	"errors"
	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/stretchr/testify/require"
	"net"
	"testing"

	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/scheduling"
	"github.com/stretchr/testify/assert"
)

func NewMetadataPSQLConfigForTesting() helpers.PSQLConfig {
	config := helpers.PSQLConfig{
		Host:     helpers.GetEnv("POSTGRES_HOST", "localhost"),
		User:     helpers.GetEnv("POSTGRES_USER", "postgres"),
		Password: helpers.GetEnv("POSTGRES_PASSWORD", "password"),
		Port:     helpers.GetEnv("POSTGRES_PORT", "5432"),
		DBName:   helpers.GetEnv("POSTGRES_DB", "postgres"),
		SSLMode:  helpers.GetEnv("POSTGRES_SSL_MODE", "disable"),
	}
	return config
}

func startServPsql(t *testing.T) (*MetadataServer, string) {
	metadataPsqlConfig := NewMetadataPSQLConfigForTesting()
	manager, err := scheduling.NewPSQLTaskMetadataManager(metadataPsqlConfig)
	if err != nil {
		t.Fatalf(err.Error())
	}
	logger := logging.NewTestLogger(t)

	config := &Config{
		Logger:      logger,
		TaskManager: manager,
	}
	serv, err := NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.ServeOnListener(lis); err != nil {
			panic(err)
		}
	}()
	return serv, lis.Addr().String()
}

func TestMetadataDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	redisConfig := pc.RedisConfig{
		Addr:     "0.0.0.0",
		Password: "root",
		DB:       0,
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     "featureformer",
		Password:     "password",
		Organization: "featureform",
		Account:      "featureform-test",
		Database:     "transactions_db",
		Schema:       "fraud",
		Warehouse:    "ff_wh_xs",
		Role:         "sysadmin",
	}
	resources := []ResourceDef{
		UserDef{
			Name:       "Featureform",
			Tags:       Tags{},
			Properties: Properties{},
		},
		UserDef{
			Name:       "Other",
			Tags:       Tags{},
			Properties: Properties{},
		},
		ProviderDef{
			Name:             "mockOnline",
			Description:      "A mock online provider",
			Type:             string(pt.RedisOnline),
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: redisConfig.Serialized(),
			Tags:             Tags{},
			Properties:       Properties{},
		},
		ProviderDef{
			Name:             "mockOfflineToDelete",
			Description:      "A mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: snowflakeConfig.Serialize(),
			Tags:             Tags{},
			Properties:       Properties{},
		},
		ProviderDef{
			Name:             "mockOffline",
			Description:      "A mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: snowflakeConfig.Serialize(),
			Tags:             Tags{},
			Properties:       Properties{},
		},
		EntityDef{
			Name:        "user",
			Description: "A user entity",
			Tags:        Tags{},
			Properties:  Properties{},
		},
		EntityDef{
			Name:        "item",
			Description: "An item entity",
			Tags:        Tags{},
			Properties:  Properties{},
		},
		SourceDef{
			Name:        "primarydata",
			Variant:     "var",
			Description: "A CSV source but different",
			Definition: PrimaryDataSource{
				Location: SQLTable{
					Name: "mockPrimary",
				},
				TimestampColumn: "timestamp",
			},
			Owner:      "Featureform",
			Provider:   "mockOffline",
			Tags:       Tags{},
			Properties: Properties{},
		},
		SourceDef{
			Name:        "tf",
			Variant:     "var",
			Description: "A CSV source",
			Definition: TransformationSource{
				TransformationType: SQLTransformationType{
					Query: "SELECT * FROM dummy",
					Sources: []NameVariant{{
						Name:    "primarydata",
						Variant: "var"},
					},
				},
			},
			Owner:      "Featureform",
			Provider:   "mockOffline",
			Tags:       Tags{},
			Properties: Properties{},
		},
	}

	serv, addr := startServPsql(t)
	cli := client(t, addr)
	err := cli.CreateAll(context.Background(), resources)
	assert.NoError(t, err)

	// set statuses to ready
	_, err = serv.SetResourceStatus(context.Background(), &pb.SetStatusRequest{
		ResourceId: &pb.ResourceID{
			Resource: &pb.NameVariant{
				Name: "mockOnline",
			},
			ResourceType: pb.ResourceType_PROVIDER,
		},
		Status: &pb.ResourceStatus{
			Status: pb.ResourceStatus_READY,
		},
	})
	require.NoError(t, err)

	_, err = serv.SetResourceStatus(context.Background(), &pb.SetStatusRequest{
		ResourceId: &pb.ResourceID{
			Resource: &pb.NameVariant{
				Name: "mockOfflineToDelete",
			},
			ResourceType: pb.ResourceType_PROVIDER,
		},
		Status: &pb.ResourceStatus{
			Status: pb.ResourceStatus_READY,
		},
	})
	require.NoError(t, err)

	_, err = serv.SetResourceStatus(context.Background(), &pb.SetStatusRequest{
		ResourceId: &pb.ResourceID{
			Resource: &pb.NameVariant{
				Name: "mockOffline",
			},
			ResourceType: pb.ResourceType_PROVIDER,
		},
		Status: &pb.ResourceStatus{
			Status: pb.ResourceStatus_READY,
		},
	})
	require.NoError(t, err)

	t.Run("delete provider with no dependencies", func(t *testing.T) {
		// try to delete the online provider
		_, err := serv.MarkForDeletion(context.Background(), &pb.MarkForDeletionRequest{
			ResourceId: &pb.ResourceID{
				Resource: &pb.NameVariant{
					Name: "mockOfflineToDelete",
				},
				ResourceType: pb.ResourceType_PROVIDER,
			},
		})
		assert.NoError(t, err)

		res, err := serv.lookup.Lookup(context.Background(), ResourceID{
			Name: "mockOfflineToDelete",
			Type: PROVIDER,
		})

		assert.Nil(t, res)
		var knfErr *fferr.KeyNotFoundError
		assert.True(t, errors.As(err, &knfErr))
	})

	t.Run("delete provider with dependencies", func(t *testing.T) {
		// try to delete the online provider
		_, err := serv.MarkForDeletion(context.Background(), &pb.MarkForDeletionRequest{
			ResourceId: &pb.ResourceID{
				Resource: &pb.NameVariant{
					Name: "mockOffline",
				},
				ResourceType: pb.ResourceType_PROVIDER,
			},
		})
		require.Error(t, err)
	})

	t.Run("try to delete primary source", func(t *testing.T) {
		_, err := serv.MarkForDeletion(context.Background(), &pb.MarkForDeletionRequest{
			ResourceId: &pb.ResourceID{
				Resource: &pb.NameVariant{
					Name:    "primarydata",
					Variant: "var",
				},
				ResourceType: pb.ResourceType_SOURCE,
			},
		})
		assert.Error(t, err)
	})

	t.Run("delete source that's not ready", func(t *testing.T) {
		_, err := serv.MarkForDeletion(context.Background(), &pb.MarkForDeletionRequest{
			ResourceId: &pb.ResourceID{
				Resource: &pb.NameVariant{
					Name:    "tf",
					Variant: "var",
				},
				ResourceType: pb.ResourceType_SOURCE_VARIANT,
			},
		})

		assert.Error(t, err)
	})
}
