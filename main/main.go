package main

import (
	"fmt"
	"github.com/featureform/api"
	"github.com/featureform/coordinator"
	"github.com/featureform/ffsync"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	dm "github.com/featureform/metadata/dashboard"
	"github.com/featureform/metadata/search"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
	"github.com/featureform/serving"
	ss "github.com/featureform/storage"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"os"
	"time"
)

func main() {
	locker := ffsync.NewMemoryLocker()
	mstorage := ss.NewMemoryStorageImplementation()
	storage := ss.MetadataStorage{
		Locker:  &locker,
		Storage: &mstorage,
	}
	meta := scheduling.NewTaskMetadataManager(storage, ffsync.NewMemoryOrderedIdGenerator())

	local := help.GetEnvBool("FEATUREFORM_LOCAL", true)
	/****************************************** API Server ************************************************************/
	err := godotenv.Load(".env")
	apiPort := help.GetEnv("API_PORT", "7878")
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	servingHost := help.GetEnv("SERVING_HOST", "localhost")
	servingPort := help.GetEnv("SERVING_PORT", "8081")
	apiConn := fmt.Sprintf("0.0.0.0:%s", apiPort)
	metadataConn := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	servingConn := fmt.Sprintf("%s:%s", servingHost, servingPort)
	logger := logging.NewLogger("api")
	go func() {
		err := api.StartHttpsServer(":8443")
		if err != nil && err != http.ErrServerClosed {
			panic(fmt.Sprintf("health check HTTP server failed: %+v", err))
		}
	}()

	/******************************************** Metadata ************************************************************/

	mLogger := logging.NewLogger("metadata")
	addr := help.GetEnv("METADATA_PORT", "8080")
	enableSearch := help.GetEnv("ENABLE_SEARCH", "false")
	config := &metadata.Config{
		Logger:          mLogger,
		Address:         fmt.Sprintf(":%s", addr),
		StorageProvider: &mstorage,
		TaskManager:     meta,
	}
	if enableSearch == "true" {
		logger.Infow("Connecting to search", "host", os.Getenv("MEILISEARCH_HOST"), "port", os.Getenv("MEILISEARCH_PORT"))
		config.SearchParams = &search.MeilisearchParams{
			Port:   help.GetEnv("MEILISEARCH_PORT", "7700"),
			Host:   help.GetEnv("MEILISEARCH_HOST", "localhost"),
			ApiKey: help.GetEnv("MEILISEARCH_APIKEY", ""),
		}
	}

	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}

	/******************************************** Coordinator ************************************************************/

	metadataUrl := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	fmt.Printf("connecting to metadata: %s\n", metadataUrl)

	if err := runner.RegisterFactory(runner.COPY_TO_ONLINE, runner.MaterializedChunkRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Copy to Online' runner factory: %w", err))
	}
	if err := runner.RegisterFactory(runner.MATERIALIZE, runner.MaterializeRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Materialize' runner factory: %w", err))
	}
	if err := runner.RegisterFactory(runner.CREATE_TRANSFORMATION, runner.CreateTransformationRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Create Transformation' runner factory: %w", err))
	}
	if err := runner.RegisterFactory(runner.CREATE_TRAINING_SET, runner.TrainingSetRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Create Training Set' runner factory: %w", err))
	}
	if err := runner.RegisterFactory(runner.S3_IMPORT_DYNAMODB, runner.S3ImportDynamoDBRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register S3 import to DynamoDB runner factory: %v", err))
	}
	cLogger := logging.NewLogger("coordinator")
	defer cLogger.Sync()
	cLogger.Debug("Connected to ETCD")

	client, err := metadata.NewClient(metadataUrl, cLogger)
	if err != nil {
		cLogger.Errorw("Failed to connect: %v", err)
		panic(err)
	}
	cLogger.Debug("Connected to Metadata")
	var spawner coordinator.JobSpawner
	spawner = &coordinator.MemoryJobSpawner{}

	coord, err := coordinator.NewCoordinator(client, cLogger, spawner, &locker)
	if err != nil {
		logger.Errorw("Failed to set up coordinator: %v", err)
		panic(err)
	}
	cLogger.Debug("Begin Job Watch")

	/**************************************** Dashboard Backend *******************************************************/
	dbLogger := zap.NewExample().Sugar()

	dbLogger.Infof("Looking for metadata at: %s\n", metadataUrl)

	metadataServer, err := dm.NewMetadataServer(dbLogger, client, &mstorage)
	if err != nil {
		logger.Panicw("Failed to create server", "error", err)
	}
	metadataHTTPPort := help.GetEnv("METADATA_HTTP_PORT", "3001")
	metadataServingPort := fmt.Sprintf(":%s", metadataHTTPPort)
	dbLogger.Infof("Serving HTTP Metadata on port: %s\n", metadataServingPort)

	/**************************************** Serving *******************************************************/

	sLogger := logging.NewLogger("serving")

	host := help.GetEnv("SERVING_HOST", "0.0.0.0")
	port := help.GetEnv("SERVING_PORT", "8081")
	address := fmt.Sprintf("%s:%s", host, port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		sLogger.Panicw("Failed to listen on port", "Err", err)
	}
	metricsHandler := &metrics.NoOpMetricsHandler{}
	serv, err := serving.NewFeatureServer(client, metricsHandler, sLogger)
	if err != nil {
		sLogger.Panicw("Failed to create training server", "Err", err)
	}
	grpcServer := grpc.NewServer()

	pb.RegisterFeatureServer(grpcServer, serv)
	sLogger.Infow("Server starting", "Port", address)

	/******************************************** Start Servers *******************************************************/

	go func() {
		serv, err := api.NewApiServer(logger, apiConn, metadataConn, servingConn)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(serv.Serve())
	}()

	go func() {
		if err := server.Serve(); err != nil {
			logger.Errorw("Serve failed with error", "Err", err)
		}
	}()

	go func() {
		if err := coord.WatchForNewJobs(); err != nil {
			cLogger.Errorw(err.Error())
			panic(err)
			return
		}
	}()

	go func() {
		metadataServer.Start(metadataServingPort, local)
	}()

	go func() {
		serveErr := grpcServer.Serve(lis)
		if serveErr != nil {
			logger.Errorw("Serve failed with error", "Err", serveErr)
		}
	}()
	for {
		time.Sleep(1 * time.Second)
	}
}
