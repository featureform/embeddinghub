package metadata

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
	"github.com/featureform/metadata/common"
	pb "github.com/featureform/metadata/proto"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

type TestResourcesRepository struct {
	repo *sqlResourcesRepository
	db   *postgres.Pool
}

// NewTestResourcesRepository wraps an existing sqlResourcesRepository for testing.
func NewTestResourcesRepository(repo *sqlResourcesRepository, db *postgres.Pool) *TestResourcesRepository {
	return &TestResourcesRepository{
		repo: repo,
		db:   db,
	}
}

// resetDatabase clears all data from tables for a clean test run.
func resetDatabase(db *postgres.Pool) error {
	queries := []string{
		"TRUNCATE TABLE edges CASCADE;",
		"TRUNCATE TABLE ff_task_metadata CASCADE;",
	}
	for _, q := range queries {
		_, err := db.Exec(context.Background(), q)
		if err != nil {
			return fmt.Errorf("failed to reset database: %w", err)
		}
	}
	return nil
}

// Close cleans up the connection pool.
func (tr *TestResourcesRepository) Close() {
	tr.db.Close()
}

// TestMetadataServer wraps the MetadataServer and provides setup/teardown helpers.
type TestMetadataServer struct {
	server *MetadataServer
	client *Client
	TestResourcesRepository
	t         *testing.T
	ctx       context.Context
	logger    logging.Logger
	dbCleanup func()
}

func (ts *TestMetadataServer) SetupTestData(t *testing.T, resources []ResourceDef, setReady bool) {
	ts.ResetDatabase()
	t.Helper()
	ts.SeedResources(ts.ctx, resources)
	resourceIDs := make([]ResourceID, 0, len(resources))
	for _, r := range resources {
		resourceIDs = append(resourceIDs, r.ResourceID())
	}
	if setReady {
		ts.SetResourcesReady(ts.ctx, resourceIDs)
	}
}

func newTestMetadataServer(t *testing.T) *TestMetadataServer {
	t.Helper()

	serv, addr, dbCleanup := startServPsql(t)

	sqlRepo, ok := serv.resourcesRepository.(*sqlResourcesRepository)
	assert.True(t, ok, "resourcesRepository should be of type *sqlResourcesRepository")

	testRepo := NewTestResourcesRepository(sqlRepo, sqlRepo.db)

	ctx, logger := logging.NewTestContextAndLogger(t)
	cli := client(t, ctx, logger, addr)

	return &TestMetadataServer{
		server:                  serv,
		client:                  cli,
		TestResourcesRepository: *testRepo,
		t:                       t,
		ctx:                     ctx,
		logger:                  logger,
		dbCleanup:               dbCleanup,
	}
}

// SeedResources inserts resources via MetadataServer.
func (ts *TestMetadataServer) SeedResources(ctx context.Context, resources []ResourceDef) {
	err := ts.client.CreateAll(ctx, resources)
	require.NoError(ts.t, err, "Failed to seed resources")
}

// set resource to ready
func (ts *TestMetadataServer) SetResourcesReady(ctx context.Context, resourceIDs []ResourceID) {
	for _, id := range resourceIDs {
		if id.Type == PROVIDER {
			_, err := ts.server.SetResourceStatus(ctx, &pb.SetStatusRequest{
				ResourceId: id.Proto(),
				Status: &pb.ResourceStatus{
					Status: pb.ResourceStatus_READY,
				},
			})
			require.NoError(ts.t, err, "Failed to set resource status")
		}

		resource, err := ts.server.lookup.Lookup(ctx, id)
		require.NoError(ts.t, err, "Failed to lookup resource")

		if _, ok := resource.(resourceStatusImplementation); ok {
			taskID, err := resource.(resourceTaskImplementation).TaskIDs()
			require.NoError(ts.t, err, "Failed to get task ID")
			lastTask := taskID[len(taskID)-1]
			run, err := ts.server.taskManager.GetLatestRun(lastTask)
			err = ts.server.taskManager.SetRunStatus(run.ID, lastTask, &pb.ResourceStatus{Status: pb.ResourceStatus_RUNNING})
			require.NoError(ts.t, err, "Failed to set run status", "resource: %v", id)
			err = ts.server.taskManager.SetRunStatus(run.ID, lastTask, &pb.ResourceStatus{Status: pb.ResourceStatus_READY})
			require.NoError(ts.t, err, "Failed to set run status", "resource: %v", id)
		}
	}
}

func (ts *TestMetadataServer) ResetDatabase() {
	err := resetDatabase(ts.db)
	assert.NoError(ts.t, err, "Failed to reset database")
}

// Close shuts down the server.
func (ts *TestMetadataServer) Close() {
	ts.server.Stop()
}

func TestDeleteProvider(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	testServer := newTestMetadataServer(t)
	defer testServer.Close()
	defer testServer.dbCleanup()

	resources := []ResourceDef{
		ProviderDef{Name: "mockOfflineToDelete"},
	}

	ctx := testServer.ctx
	t.Run("Delete existing provider", func(t *testing.T) {
		testServer.SetupTestData(t, resources, true)

		// Attempt to delete the provider
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name: "mockOfflineToDelete",
			Type: common.PROVIDER,
		}, noOpAsyncHandler)
		require.NoError(t, err)

		// Verify the provider is deleted
		res, err := testServer.repo.Lookup(ctx, ResourceID{
			Name: "mockOfflineToDelete",
			Type: PROVIDER,
		}, DeleteLookupOption{DeletedOnly})

		var keyNotFoundErr *fferr.KeyNotFoundError
		require.ErrorAs(t, err, &keyNotFoundErr)
		require.Nil(t, res)

	})

	t.Run("Delete non-existent provider", func(t *testing.T) {
		testServer.SetupTestData(t, resources, true)

		// Attempt to delete a non-existent provider
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name: "nonExistentProvider",
			Type: common.PROVIDER,
		}, noOpAsyncHandler)

		require.Error(t, err) // Expect an error
	})

	t.Run("Delete provider without READY status", func(t *testing.T) {
		testServer.SetupTestData(t, resources, false)
		// Do NOT set status to READY to simulate invalid state

		// Attempt to delete the provider
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name: "mockOfflineToDelete",
			Type: common.PROVIDER,
		}, noOpAsyncHandler)

		require.Error(t, err) // Should fail since it's not READY
	})
}

func TestDeletePrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	ctx := context.Background()

	// Initialize the test server once for all subtests
	testServer := newTestMetadataServer(t)
	defer testServer.Close()

	// Define reusable resources and IDs
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
			Name:             "mockOffline",
			Description:      "A mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: []byte(""),
			Tags:             Tags{},
			Properties:       Properties{},
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
	}

	resourceIds := make([]ResourceID, 0)
	for _, res := range resources {
		resourceIds = append(resourceIds, res.ResourceID())
	}

	t.Run("Delete existing primary", func(t *testing.T) {
		testServer.SetupTestData(t, resources, true)

		// Attempt to delete the primary
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name:    "primarydata",
			Variant: "var",
			Type:    common.SOURCE_VARIANT,
		}, noOpAsyncHandler)
		require.NoError(t, err)

		// Verify the primary is marked for deletion
		res, err := testServer.repo.Lookup(ctx, ResourceID{
			Name:    "primarydata",
			Variant: "var",
			Type:    SOURCE_VARIANT,
		}, DeleteLookupOption{DeletedOnly})

		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, res.ID().Name, "primarydata")
	})

	t.Run("Delete non-existent primary", func(t *testing.T) {
		testServer.SetupTestData(t, resources, true)

		// Attempt to delete a non-existent primary
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name: "nonExistentPrimary",
			Type: common.SOURCE_VARIANT,
		}, noOpAsyncHandler)

		require.Error(t, err) // Expect an error
	})

	t.Run("Delete primary without READY status", func(t *testing.T) {
		testServer.SetupTestData(t, resources, true)
		// Do NOT set status to READY to simulate invalid state

		// Attempt to delete the primary
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name: "mockPrimaryToDelete",
			Type: common.SOURCE_VARIANT,
		}, noOpAsyncHandler)

		require.Error(t, err) // Should fail since it's not READY
	})
}

func TestDeleteDag(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
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
		EntityDef{
			Name:        "user",
			Description: "A user entity",
			Tags:        Tags{},
			Properties:  Properties{},
		},
		ProviderDef{
			Name:             "mockOnline",
			Description:      "A mock online provider",
			Type:             string(pt.RedisOnline),
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: []byte(""),
			Tags:             Tags{},
			Properties:       Properties{},
		},
		ProviderDef{
			Name:             "mockOffline",
			Description:      "A mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: []byte(""),
			Tags:             Tags{},
			Properties:       Properties{},
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
			Name:        "mockSource",
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
		FeatureDef{
			Name:        "feature",
			Variant:     "variant",
			Provider:    "mockOnline",
			Entity:      "user",
			Type:        types.Float32,
			Description: "Feature variant",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Featureform",
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Tags:       Tags{},
			Properties: Properties{},
			Mode:       PRECOMPUTED,
			IsOnDemand: false,
		},
		LabelDef{
			Name:        "label",
			Variant:     "variant",
			Type:        types.Int64,
			Description: "label variant",
			Provider:    "mockOffline",
			Entity:      "user",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Other",
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Tags:       Tags{},
			Properties: Properties{},
		},
		TrainingSetDef{
			Name:        "training-set",
			Variant:     "variant",
			Provider:    "mockOffline",
			Description: "training-set variant",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature", "variant"},
			},
			Owner:      "Other",
			Tags:       Tags{},
			Properties: Properties{},
		},
	}

	// Initialize the test server once for all subtests
	ctx := context.Background()
	testServer := newTestMetadataServer(t)
	defer testServer.Close()

	resourceIds := make([]ResourceID, 0)
	for _, res := range resources {
		resourceIds = append(resourceIds, res.ResourceID())
	}

	testServer.SetupTestData(t, resources, true)

	t.Run("Attempt to delete resource with dependencies", func(t *testing.T) {
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name:    "mockSource",
			Variant: "var",
			Type:    common.SOURCE_VARIANT,
		}, noOpAsyncHandler)
		require.Error(t, err)
	})

	t.Run("Delete training set", func(t *testing.T) {
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name:    "training-set",
			Variant: "variant",
			Type:    common.TRAINING_SET_VARIANT,
		}, noOpAsyncHandler)
		require.NoError(t, err)

		res, err := testServer.repo.Lookup(ctx, ResourceID{
			Name:    "training-set",
			Variant: "variant",
			Type:    TRAINING_SET_VARIANT,
		}, DeleteLookupOption{DeletedOnly})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, res.ID().Name, "training-set")
	})

	t.Run("Delete feature", func(t *testing.T) {
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name:    "feature",
			Variant: "variant",
			Type:    common.FEATURE_VARIANT,
		}, noOpAsyncHandler)
		require.NoError(t, err)

		res, err := testServer.repo.Lookup(ctx, ResourceID{
			Name:    "feature",
			Variant: "variant",
			Type:    FEATURE_VARIANT,
		}, DeleteLookupOption{DeletedOnly})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, res.ID().Name, "feature")
	})

	t.Run("Delete source (should fail since has deps)", func(t *testing.T) {
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name:    "mockSource",
			Variant: "var",
			Type:    common.SOURCE_VARIANT,
		}, noOpAsyncHandler)
		require.Error(t, err)
	})

	t.Run("Delete label", func(t *testing.T) {
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name:    "label",
			Variant: "variant",
			Type:    common.LABEL_VARIANT,
		}, noOpAsyncHandler)
		require.NoError(t, err)

		res, err := testServer.repo.Lookup(ctx, ResourceID{
			Name:    "label",
			Variant: "variant",
			Type:    LABEL_VARIANT,
		}, DeleteLookupOption{DeletedOnly})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, res.ID().Name, "label")
	})

	t.Run("Delete source", func(t *testing.T) {
		err := testServer.repo.MarkForDeletion(ctx, common.ResourceID{
			Name:    "mockSource",
			Variant: "var",
			Type:    common.SOURCE_VARIANT,
		}, noOpAsyncHandler)
		require.NoError(t, err)

		res, err := testServer.repo.Lookup(ctx, ResourceID{
			Name:    "mockSource",
			Variant: "var",
			Type:    SOURCE_VARIANT,
		}, DeleteLookupOption{DeletedOnly})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, res.ID().Name, "mockSource")
	})
}

func noOpAsyncHandler(ctx context.Context, resId ResourceID, logger logging.Logger) error {
	return nil
}

func TestPrune(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
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
		EntityDef{
			Name:        "user",
			Description: "A user entity",
			Tags:        Tags{},
			Properties:  Properties{},
		},
		ProviderDef{
			Name:             "mockOnline",
			Description:      "A mock online provider",
			Type:             string(pt.RedisOnline),
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: []byte(""),
			Tags:             Tags{},
			Properties:       Properties{},
		},
		ProviderDef{
			Name:             "mockOffline",
			Description:      "A mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: []byte(""),
			Tags:             Tags{},
			Properties:       Properties{},
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
			Name:        "mockSource",
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
		FeatureDef{
			Name:        "feature",
			Variant:     "variant",
			Provider:    "mockOnline",
			Entity:      "user",
			Type:        types.Float32,
			Description: "Feature variant",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Featureform",
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Tags:       Tags{},
			Properties: Properties{},
			Mode:       PRECOMPUTED,
			IsOnDemand: false,
		},
		LabelDef{
			Name:        "label",
			Variant:     "variant",
			Type:        types.Int64,
			Description: "label variant",
			Provider:    "mockOffline",
			Entity:      "user",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Other",
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Tags:       Tags{},
			Properties: Properties{},
		},
		TrainingSetDef{
			Name:        "training-set",
			Variant:     "variant",
			Provider:    "mockOffline",
			Description: "training-set variant",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature", "variant"},
			},
			Owner:      "Other",
			Tags:       Tags{},
			Properties: Properties{},
		},
	}

	ctx := context.Background()
	testServer := newTestMetadataServer(t)
	defer func() {
		testServer.ResetDatabase() // Ensure DB is reset after test
		testServer.Close()         // Close the test server
	}()

	resourceIds := make([]ResourceID, 0)
	for _, res := range resources {
		resourceIds = append(resourceIds, res.ResourceID())
	}

	ctx = context.WithValue(ctx, "testServer", testServer) // Attach the test server

	t.Run("Prune", func(t *testing.T) {
		testServer.SetupTestData(t, resources, true)
		markedForDeletion, err := testServer.repo.PruneResource(ctx, common.ResourceID{
			Name:    "training-set",
			Variant: "variant",
			Type:    common.TRAINING_SET_VARIANT,
		}, noOpAsyncHandler)
		require.NoError(t, err)
		require.Len(t, markedForDeletion, 1)
		require.Equal(t, markedForDeletion[0].Name, "training-set")

		//lookup
		res, err := testServer.repo.Lookup(ctx, ResourceID{
			Name:    "training-set",
			Variant: "variant",
			Type:    TRAINING_SET_VARIANT,
		})
		require.Error(t, err)
		require.Nil(t, res)

		markedForDeletion, err = testServer.repo.PruneResource(ctx, common.ResourceID{
			Name:    "mockSource",
			Variant: "var",
			Type:    common.SOURCE_VARIANT,
		}, noOpAsyncHandler)
		require.NoError(t, err)
		require.Len(t, markedForDeletion, 3)
		require.Contains(t, markedForDeletion, common.ResourceID{
			Name:    "mockSource",
			Variant: "var",
			Type:    common.SOURCE_VARIANT,
		})
		require.Contains(t, markedForDeletion, common.ResourceID{
			Name:    "feature",
			Variant: "variant",
			Type:    common.FEATURE_VARIANT,
		})
		require.Contains(t, markedForDeletion, common.ResourceID{
			Name:    "label",
			Variant: "variant",
			Type:    common.LABEL_VARIANT,
		})

		//lookup
		res, err = testServer.repo.Lookup(ctx, ResourceID{
			Name:    "mockSource",
			Variant: "var",
			Type:    SOURCE_VARIANT,
		}, DeleteLookupOption{DeletedOnly})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, res.ID().Name, "mockSource")

		// lookup feature
		res, err = testServer.repo.Lookup(ctx, ResourceID{
			Name:    "feature",
			Variant: "variant",
			Type:    FEATURE_VARIANT,
		}, DeleteLookupOption{DeletedOnly})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, res.ID().Name, "feature")

		// lookup label
		res, err = testServer.repo.Lookup(ctx, ResourceID{
			Name:    "label",
			Variant: "variant",
			Type:    LABEL_VARIANT,
		}, DeleteLookupOption{DeletedOnly})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, res.ID().Name, "label")

		// try and delete again
		markedForDeletion, err = testServer.repo.PruneResource(ctx, common.ResourceID{
			Name:    "training-set",
			Variant: "variant",
			Type:    common.TRAINING_SET_VARIANT,
		}, noOpAsyncHandler)
		require.Error(t, err)
	})

	t.Run("Prune non-existent resource", func(t *testing.T) {
		markedForDeletion, err := testServer.repo.PruneResource(ctx, common.ResourceID{
			Name: "nonExistentResource",
			Type: common.TRAINING_SET_VARIANT,
		}, noOpAsyncHandler)
		require.Error(t, err)
		require.Nil(t, markedForDeletion)
	})
}
