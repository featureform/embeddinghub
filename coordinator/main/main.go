package main

import (
	"fmt"
	"github.com/featureform/ffsync"
	"time"

	"github.com/featureform/coordinator"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	etcdHost := help.GetEnv("ETCD_HOST", "localhost")
	etcdPort := help.GetEnv("ETCD_PORT", "2379")
	etcdUrl := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	metadataUrl := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	useK8sRunner := help.GetEnv("K8S_RUNNER_ENABLE", "false")
	fmt.Printf("connecting to etcd: %s\n", etcdUrl)
	fmt.Printf("connecting to metadata: %s\n", metadataUrl)
	etcdConfig := clientv3.Config{
		Endpoints:   []string{etcdUrl},
		Username:    help.GetEnv("ETCD_USERNAME", "root"),
		Password:    help.GetEnv("ETCD_PASSWORD", "secretpassword"),
		DialTimeout: time.Second * 1,
	}

	cli, err := clientv3.New(etcdConfig)
	if err != nil {
		panic(err)
	}

	defer func(cli *clientv3.Client) {
		err := cli.Close()
		if err != nil {
			panic(fmt.Errorf("failed to close etcd client: %w", err))
		}
	}(cli)
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
	logger := logging.NewLogger("coordinator")
	defer logger.Sync()
	logger.Debug("Connected to ETCD")
	client, err := metadata.NewClient(metadataUrl, logger)
	if err != nil {
		logger.Errorw("Failed to connect: %v", err)
		panic(err)
	}
	logger.Debug("Connected to Metadata")
	var spawner coordinator.JobSpawner
	if useK8sRunner == "false" {
		spawner = &coordinator.MemoryJobSpawner{}
	} else {
		spawner = &coordinator.KubernetesJobSpawner{EtcdConfig: etcdConfig}
	}

	etcdStorageConfig := help.ETCDConfig{
		Host: etcdHost,
		Port: etcdPort,
	}

	// Add a switch for locker
	locker, err := ffsync.NewETCDLocker(etcdStorageConfig)

	coord, err := coordinator.NewCoordinator(client, logger, spawner, locker)
	if err != nil {
		logger.Errorw("Failed to set up coordinator: %v", err)
		panic(err)
	}
	logger.Debug("Begin Job Watch")
	if err := coord.WatchForNewJobs(); err != nil {
		logger.Errorw(err.Error())
		panic(err)
		return
	}
}
