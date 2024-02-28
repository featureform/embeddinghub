package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	db "github.com/jackc/pgx/v4"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	cfg "github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/kubernetes"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/runner"
	"github.com/featureform/types"
)

func retryWithDelays(name string, retries int, delay time.Duration, idempotentFunction func() error) error {
	var err error
	for i := 0; i < retries; i++ {
		if err = idempotentFunction(); err == nil {
			return nil
		}
		time.Sleep(delay)
	}
	return fferr.NewInternalError(fmt.Errorf("retried %s %d times unsuccessfully: Latest error message: %v", name, retries, err))
}

type Config []byte

func templateReplace(template string, replacements map[string]string, offlineStore provider.OfflineStore) (string, error) {
	formattedString := ""
	numEscapes := strings.Count(template, "{{")
	for i := 0; i < numEscapes; i++ {
		split := strings.SplitN(template, "{{", 2)
		afterSplit := strings.SplitN(split[1], "}}", 2)
		key := strings.TrimSpace(afterSplit[0])
		replacement, has := replacements[key]
		if !has {
			return "", fferr.NewInvalidArgumentError(fmt.Errorf("value %s not found in replacements: %v", key, replacements))
		}

		if offlineStore.Type() == pt.BigQueryOffline {
			bqConfig := pc.BigQueryConfig{}
			bqConfig.Deserialize(offlineStore.Config())
			replacement = fmt.Sprintf("`%s.%s.%s`", bqConfig.ProjectId, bqConfig.DatasetId, replacement)
		} else {
			replacement = sanitize(replacement)
		}
		formattedString += fmt.Sprintf("%s%s", split[0], replacement)
		template = afterSplit[1]
	}
	formattedString += template
	return formattedString, nil
}

func getSourceMapping(template string, replacements map[string]string) ([]provider.SourceMapping, error) {
	sourceMap := []provider.SourceMapping{}
	numEscapes := strings.Count(template, "{{")
	for i := 0; i < numEscapes; i++ {
		split := strings.SplitN(template, "{{", 2)
		afterSplit := strings.SplitN(split[1], "}}", 2)
		key := strings.TrimSpace(afterSplit[0])
		replacement, has := replacements[key]
		if !has {
			return nil, fferr.NewInvalidArgumentError(fmt.Errorf("value %s not found in replacements: %v", key, replacements))
		}
		sourceMap = append(sourceMap, provider.SourceMapping{Template: sanitize(replacement), Source: replacement})
		template = afterSplit[1]
	}
	return sourceMap, nil
}

type Coordinator struct {
	Metadata   *metadata.Client
	Logger     *zap.SugaredLogger
	EtcdClient *clientv3.Client
	KVClient   *clientv3.KV
	Spawner    JobSpawner
	Timeout    int
}

type ETCDConfig struct {
	Endpoints []string
	Username  string
	Password  string
}

func (c *ETCDConfig) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return config, nil
}

func (c *ETCDConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (c *Coordinator) AwaitPendingSource(sourceNameVariant metadata.NameVariant) (*metadata.SourceVariant, error) {
	sourceStatus := metadata.PENDING
	for sourceStatus != metadata.READY {
		source, err := c.Metadata.GetSourceVariant(context.Background(), sourceNameVariant)
		if err != nil {
			return nil, err
		}
		var sourceType fferr.ResourceType
		if source.IsTransformation() {
			sourceType = fferr.TRANSFORMATION
		} else {
			sourceType = fferr.PRIMARY_DATASET
		}
		sourceStatus := source.Status()
		if sourceStatus == metadata.FAILED {
			err := fferr.NewResourceFailedError(sourceNameVariant.Name, sourceNameVariant.Variant, sourceType, fmt.Errorf("required dataset is in a failed state"))
			return nil, err
		}
		if sourceStatus == metadata.READY {
			return source, nil
		}
		time.Sleep(1 * time.Second)
	}
	return c.Metadata.GetSourceVariant(context.Background(), sourceNameVariant)
}

func (c *Coordinator) AwaitPendingFeature(featureNameVariant metadata.NameVariant) (*metadata.FeatureVariant, error) {
	featureStatus := metadata.PENDING
	for featureStatus != metadata.READY {
		feature, err := c.Metadata.GetFeatureVariant(context.Background(), featureNameVariant)
		if err != nil {
			return nil, err
		}
		featureStatus := feature.Status()
		if featureStatus == metadata.FAILED {
			err := fferr.NewResourceFailedError(featureNameVariant.Name, featureNameVariant.Variant, fferr.FEATURE_VARIANT, fmt.Errorf("required feature is in a failed state"))
			return nil, err
		}
		if featureStatus == metadata.READY {
			return feature, nil
		}
		time.Sleep(1 * time.Second)
	}
	return c.Metadata.GetFeatureVariant(context.Background(), featureNameVariant)
}

func (c *Coordinator) AwaitPendingLabel(labelNameVariant metadata.NameVariant) (*metadata.LabelVariant, error) {
	labelStatus := metadata.PENDING
	for labelStatus != metadata.READY {
		label, err := c.Metadata.GetLabelVariant(context.Background(), labelNameVariant)
		if err != nil {
			return nil, err
		}
		labelStatus := label.Status()
		if labelStatus == metadata.FAILED {
			err := fferr.NewResourceFailedError(labelNameVariant.Name, labelNameVariant.Variant, fferr.LABEL_VARIANT, fmt.Errorf("required label is in a failed state"))
			return nil, err
		}
		if labelStatus == metadata.READY {
			return label, nil
		}
		time.Sleep(1 * time.Second)
	}
	return c.Metadata.GetLabelVariant(context.Background(), labelNameVariant)
}

func (c *Coordinator) setPending(resID metadata.ResourceID, currentStatus metadata.ResourceStatus) error {
	if currentStatus == metadata.READY {
		return fferr.NewResourceAlreadyCompleteError(resID.Name, resID.Variant, fferr.ResourceType(resID.Type.String()), nil)
	}
	if currentStatus == metadata.FAILED {
		return fferr.NewResourceAlreadyFailedError(resID.Name, resID.Variant, fferr.ResourceType(resID.Type.String()), nil)
	}
	return c.Metadata.SetStatus(context.Background(), resID, metadata.PENDING, "")
}

type JobSpawner interface {
	GetJobRunner(jobName runner.RunnerName, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error)
}

type KubernetesJobSpawner struct {
	EtcdConfig clientv3.Config
}

type MemoryJobSpawner struct{}

func GetLockKey(jobKey string) string {
	return fmt.Sprintf("LOCK_%s", jobKey)
}

func (k *KubernetesJobSpawner) GetJobRunner(jobName runner.RunnerName, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error) {
	etcdConfig := &ETCDConfig{Endpoints: k.EtcdConfig.Endpoints, Username: k.EtcdConfig.Username, Password: k.EtcdConfig.Password}
	serializedETCD, err := etcdConfig.Serialize()
	if err != nil {
		return nil, err
	}
	pandasImage := cfg.GetPandasRunnerImage()
	workerImage := cfg.GetWorkerImage()
	fmt.Println("GETJOBRUNNERID:", resourceId)
	kubeConfig := kubernetes.KubernetesRunnerConfig{
		EnvVars: map[string]string{
			"NAME":             jobName.String(),
			"CONFIG":           string(config),
			"ETCD_CONFIG":      string(serializedETCD),
			"K8S_RUNNER_IMAGE": pandasImage,
		},
		JobPrefix: "runner",
		Image:     workerImage,
		NumTasks:  1,
		Resource:  resourceId,
	}
	jobRunner, err := kubernetes.NewKubernetesRunner(kubeConfig)
	if err != nil {
		return nil, err
	}
	return jobRunner, nil
}

func (k *MemoryJobSpawner) GetJobRunner(jobName runner.RunnerName, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error) {
	jobRunner, err := runner.Create(jobName, config)
	if err != nil {
		return nil, err
	}
	return jobRunner, nil
}

func NewCoordinator(meta *metadata.Client, logger *zap.SugaredLogger, cli *clientv3.Client, spawner JobSpawner) (*Coordinator, error) {
	logger.Info("Creating new coordinator")
	kvc := clientv3.NewKV(cli)
	return &Coordinator{
		Metadata:   meta,
		Logger:     logger,
		EtcdClient: cli,
		KVClient:   &kvc,
		Spawner:    spawner,
		Timeout:    600,
	}, nil
}

const MAX_ATTEMPTS = 3

func (c *Coordinator) checkError(err error, jobName string) {
	switch err.(type) {
	case *fferr.JobDoesNotExistError:
		c.Logger.Info(err)
	case *fferr.ResourceAlreadyFailedError:
		c.Logger.Infow("resource has failed previously. Ignoring....", "key", jobName)
	case *fferr.ResourceAlreadyCompleteError:
		c.Logger.Infow("resource has already completed. Ignoring....", "key", jobName)
	default:
		c.Logger.Errorw("Error executing job", "job_name", jobName, "error", err)
	}
}

func (c *Coordinator) WatchForNewJobs() error {
	c.Logger.Info("Watching for new jobs")
	getResp, err := (*c.KVClient).Get(context.Background(), "JOB_", clientv3.WithPrefix())
	if err != nil {
		return fferr.NewInternalError(err)
	}
	for _, kv := range getResp.Kvs {
		go func(kv *mvccpb.KeyValue) {
			err := c.ExecuteJob(string(kv.Key))
			if err != nil {
				c.checkError(err, string(kv.Key))
			}
		}(kv)
	}
	for {
		rch := c.EtcdClient.Watch(context.Background(), "JOB_", clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.PUT {
					go func(ev *clientv3.Event) {
						err := c.ExecuteJob(string(ev.Kv.Key))
						if err != nil {
							c.checkError(err, string(ev.Kv.Key))
						}
					}(ev)
				}

			}
		}
	}
}

func (c *Coordinator) WatchForUpdateEvents() error {
	c.Logger.Info("Watching for new update events")
	for {
		rch := c.EtcdClient.Watch(context.Background(), "UPDATE_EVENT_", clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == 0 {
					go func(ev *clientv3.Event) {
						err := c.signalResourceUpdate(string(ev.Kv.Key), string(ev.Kv.Value))
						if err != nil {
							c.Logger.Errorw("Error executing update event catch: Polling search", "error", err)
						}
					}(ev)
				}

			}
		}
	}
	return nil
}

func (c *Coordinator) WatchForScheduleChanges() error {
	c.Logger.Info("Watching for new update events")
	getResp, err := (*c.KVClient).Get(context.Background(), "SCHEDULEJOB_", clientv3.WithPrefix())
	if err != nil {
		return fferr.NewInternalError(err)
	}
	for _, kv := range getResp.Kvs {
		go func(kv *mvccpb.KeyValue) {
			err := c.changeJobSchedule(string(kv.Key), string(kv.Value))
			if err != nil {
				c.Logger.Errorw("Error executing job schedule change: Initial search", "error", err)
			}
		}(kv)
	}
	for {
		rch := c.EtcdClient.Watch(context.Background(), "SCHEDULEJOB_", clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == 0 {
					go func(ev *clientv3.Event) {
						err := c.changeJobSchedule(string(ev.Kv.Key), string(ev.Kv.Value))
						if err != nil {
							c.Logger.Errorw("Error executing job schedule change: Polling search", "error", err)
						}
					}(ev)
				}

			}
		}
	}
	return nil
}

func (c *Coordinator) mapNameVariantsToTables(sources []metadata.NameVariant) (map[string]string, error) {
	sourceMap := make(map[string]string)
	for _, nameVariant := range sources {
		source, err := c.Metadata.GetSourceVariant(context.Background(), nameVariant)
		if err != nil {
			return nil, err
		}
		if source.Status() != metadata.READY {
			return nil, fferr.NewResourceNotReadyError(source.Name(), source.Variant(), "SOURCE_VARIANT", nil)
		}
		providerResourceID := provider.ResourceID{Name: source.Name(), Variant: source.Variant()}
		var tableName string
		sourceProvider, err := source.FetchProvider(c.Metadata, context.Background())
		if err != nil {
			return nil, err
		}

		if (sourceProvider.Type() == "SPARK_OFFLINE" || sourceProvider.Type() == "K8S_OFFLINE") && (source.IsDFTransformation() || source.IsSQLTransformation()) {
			providerResourceID.Type = provider.Transformation
			tableName, err = provider.GetTransformationTableName(providerResourceID)
			if err != nil {
				return nil, err
			}
		} else {
			providerResourceID.Type = provider.Primary
			tableName, err = provider.GetPrimaryTableName(providerResourceID)
			if err != nil {
				return nil, err
			}
		}
		sourceMap[nameVariant.ClientString()] = tableName
	}
	return sourceMap, nil
}

func sanitize(ident string) string {
	return db.Identifier{ident}.Sanitize()
}

func (c *Coordinator) verifyCompletionOfSources(sources []metadata.NameVariant) error {
	allReady := false
	for !allReady {
		sourceVariants, err := c.Metadata.GetSourceVariants(context.Background(), sources)
		if err != nil {
			return err
		}
		total := len(sourceVariants)
		totalReady := 0
		for _, sourceVariant := range sourceVariants {
			if sourceVariant.Status() == metadata.READY {
				totalReady += 1
			}
			if sourceVariant.Status() == metadata.FAILED {
				wrapped := fferr.NewResourceFailedError(sourceVariant.Name(), sourceVariant.Variant(), fferr.SOURCE_VARIANT, fmt.Errorf("required dataset is in a failed state"))
				wrapped.AddDetail("resource_status", sourceVariant.Status().String())
				return wrapped
			}
		}
		allReady = total == totalReady
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (c *Coordinator) runTransformationJob(transformationConfig provider.TransformationConfig, resID metadata.ResourceID, schedule string, sourceProvider *metadata.Provider) error {
	transformation, err := c.Metadata.GetSourceVariant(context.Background(), metadata.NameVariant{Name: resID.Name, Variant: resID.Variant})
	if err != nil {
		return err
	}

	err = c.setPending(resID, transformation.Status())
	if err != nil {
		return err
	}

	createTransformationConfig := runner.CreateTransformationConfig{
		OfflineType:          pt.Type(sourceProvider.Type()),
		OfflineConfig:        sourceProvider.SerializedConfig(),
		TransformationConfig: transformationConfig,
		IsUpdate:             false,
	}
	c.Logger.Debugw("Transformation Serialize Config")
	serialized, err := createTransformationConfig.Serialize()
	if err != nil {
		return err
	}
	c.Logger.Debugw("Transformation Get Job Runner")
	jobRunner, err := c.Spawner.GetJobRunner(runner.CREATE_TRANSFORMATION, serialized, resID)
	if err != nil {
		return err
	}
	c.Logger.Debugw("Transformation Run Job")
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}
	c.Logger.Debugw("Transformation Waiting For Completion")
	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	c.Logger.Debugw("Transformation Setting Status")
	if err := retryWithDelays("set status to ready", 5, time.Millisecond*10, func() error { return c.Metadata.SetStatus(context.Background(), resID, metadata.READY, "") }); err != nil {
		return err
	}
	c.Logger.Debugw("Transformation Complete")
	if schedule != "" {
		scheduleCreateTransformationConfig := runner.CreateTransformationConfig{
			OfflineType:          pt.Type(sourceProvider.Type()),
			OfflineConfig:        sourceProvider.SerializedConfig(),
			TransformationConfig: transformationConfig,
			IsUpdate:             true,
		}
		serializedUpdate, err := scheduleCreateTransformationConfig.Serialize()
		if err != nil {
			return err
		}
		jobRunnerUpdate, err := c.Spawner.GetJobRunner(runner.CREATE_TRANSFORMATION, serializedUpdate, resID)
		if err != nil {
			return err
		}
		cronRunner, isCronRunner := jobRunnerUpdate.(kubernetes.CronRunner)
		if !isCronRunner {
			return fferr.NewInternalError(fmt.Errorf("kubernetes runner does not implement schedule"))
		}
		if err := cronRunner.ScheduleJob(kubernetes.CronSchedule(schedule)); err != nil {
			return err
		}
		if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) runSQLTransformationJob(transformSource *metadata.SourceVariant, resID metadata.ResourceID, offlineStore provider.OfflineStore, schedule string, sourceProvider *metadata.Provider) error {
	c.Logger.Info("Running SQL transformation job on resource: ", resID)
	templateString := transformSource.SQLTransformationQuery()
	sources := transformSource.SQLTransformationSources()

	err := c.verifyCompletionOfSources(sources)
	if err != nil {
		return err
	}

	sourceMap, err := c.mapNameVariantsToTables(sources)
	if err != nil {
		return err
	}
	sourceMapping, err := getSourceMapping(templateString, sourceMap)
	if err != nil {
		return err
	}

	var query string
	query, err = templateReplace(templateString, sourceMap, offlineStore)
	if err != nil {
		return err
	}

	c.Logger.Debugw("Created transformation query", "query", query)
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Transformation}
	transformationConfig := provider.TransformationConfig{
		Type:          provider.SQLTransformation,
		TargetTableID: providerResourceID,
		Query:         query,
		SourceMapping: sourceMapping,
		Args:          transformSource.TransformationArgs(),
	}

	err = c.runTransformationJob(transformationConfig, resID, schedule, sourceProvider)
	if err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) runDFTransformationJob(transformSource *metadata.SourceVariant, resID metadata.ResourceID, offlineStore provider.OfflineStore, schedule string, sourceProvider *metadata.Provider) error {
	c.Logger.Info("Running DF transformation job on resource: ", resID)
	code := transformSource.DFTransformationQuery()
	sources := transformSource.DFTransformationSources()

	err := c.verifyCompletionOfSources(sources)
	if err != nil {
		return err
	}

	sourceMap, err := c.mapNameVariantsToTables(sources)
	if err != nil {
		return err
	}

	sourceMapping, err := getOrderedSourceMappings(sources, sourceMap)
	if err != nil {
		return err
	}

	c.Logger.Debugw("Created transformation query")
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Transformation}
	transformationConfig := provider.TransformationConfig{
		Type:          provider.DFTransformation,
		TargetTableID: providerResourceID,
		Code:          code,
		SourceMapping: sourceMapping,
		Args:          transformSource.TransformationArgs(),
	}

	err = c.runTransformationJob(transformationConfig, resID, schedule, sourceProvider)
	if err != nil {
		return err
	}

	return nil
}

func getOrderedSourceMappings(sources []metadata.NameVariant, sourceMap map[string]string) ([]provider.SourceMapping, error) {
	sourceMapping := make([]provider.SourceMapping, len(sources))
	for i, nv := range sources {
		sourceKey := nv.ClientString()
		tableName, hasKey := sourceMap[sourceKey]
		if !hasKey {
			return nil, fferr.NewInternalError(fmt.Errorf("key %s not in source map", sourceKey))
		}
		sourceMapping[i] = provider.SourceMapping{Template: sourceKey, Source: tableName}
	}
	return sourceMapping, nil
}

func (c *Coordinator) runPrimaryTableJob(source *metadata.SourceVariant, resID metadata.ResourceID, offlineStore provider.OfflineStore, schedule string) error {
	c.Logger.Info("Running primary table job on resource: ", resID)
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Primary}
	if !source.IsPrimaryDataSQLTable() {
		return fferr.NewInvalidArgumentError(fmt.Errorf("%s is not a primary table", source.Name()))
	}
	sourceName := source.PrimaryDataSQLTableName()
	if sourceName == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("source name is not set"))
	}
	if _, err := offlineStore.RegisterPrimaryFromSourceTable(providerResourceID, sourceName); err != nil {
		return err
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) runRegisterSourceJob(resID metadata.ResourceID, schedule string) error {
	c.Logger.Info("Running register source job on resource: ", resID)
	source, err := c.Metadata.GetSourceVariant(context.Background(), metadata.NameVariant{Name: resID.Name, Variant: resID.Variant})
	if err != nil {
		return err
	}
	sourceProvider, err := source.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return err
	}
	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		return err
	}
	sourceStore, err := p.AsOfflineStore()
	if err != nil {
		return err
	}
	defer func(sourceStore provider.OfflineStore) {
		err := sourceStore.Close()
		if err != nil {
			c.Logger.Errorf("could not close offline store: %v", err)
		}
	}(sourceStore)
	if source.IsSQLTransformation() {
		return c.runSQLTransformationJob(source, resID, sourceStore, schedule, sourceProvider)
	} else if source.IsDFTransformation() {
		return c.runDFTransformationJob(source, resID, sourceStore, schedule, sourceProvider)
	} else if source.IsPrimaryDataSQLTable() {
		return c.runPrimaryTableJob(source, resID, sourceStore, schedule)
	} else {
		return fferr.NewInternalError(fmt.Errorf("source type not implemented"))
	}
}

func (c *Coordinator) runLabelRegisterJob(resID metadata.ResourceID, schedule string) error {
	c.Logger.Info("Running label register job: ", resID)
	label, err := c.Metadata.GetLabelVariant(context.Background(), metadata.NameVariant{Name: resID.Name, Variant: resID.Variant})
	if err != nil {
		return err
	}

	err = c.setPending(resID, label.Status())
	if err != nil {
		return err
	}

	sourceNameVariant := label.Source()
	c.Logger.Infow("feature obj", "name", label.Name(), "source", label.Source(), "location", label.Location(), "location_col", label.LocationColumns())

	source, err := c.AwaitPendingSource(sourceNameVariant)
	if err != nil {
		return err
	}
	sourceProvider, err := source.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return err
	}
	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		return err
	}
	sourceStore, err := p.AsOfflineStore()
	if err != nil {
		return err
	}
	defer func(sourceStore provider.OfflineStore) {
		err := sourceStore.Close()
		if err != nil {
			c.Logger.Errorf("could not close offline store: %v", err)
		}
	}(sourceStore)
	var sourceTableName string
	if source.IsSQLTransformation() || source.IsDFTransformation() {
		sourceResourceID := provider.ResourceID{sourceNameVariant.Name, sourceNameVariant.Variant, provider.Transformation}
		sourceTable, err := sourceStore.GetTransformationTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	} else if source.IsPrimaryDataSQLTable() {
		sourceResourceID := provider.ResourceID{sourceNameVariant.Name, sourceNameVariant.Variant, provider.Primary}
		sourceTable, err := sourceStore.GetPrimaryTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	}

	labelID := provider.ResourceID{
		Name:    resID.Name,
		Variant: resID.Variant,
		Type:    provider.Label,
	}
	tmpSchema := label.LocationColumns().(metadata.ResourceVariantColumns)
	schema := provider.ResourceSchema{
		Entity:      tmpSchema.Entity,
		Value:       tmpSchema.Value,
		TS:          tmpSchema.TS,
		SourceTable: sourceTableName,
	}
	c.Logger.Debugw("Creating Label Resource Table", "id", labelID, "schema", schema)
	_, err = sourceStore.RegisterResourceFromSourceTable(labelID, schema)
	if err != nil {
		return err
	}
	c.Logger.Debugw("Resource Table Created", "id", labelID, "schema", schema)

	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) runFeatureMaterializeJob(resID metadata.ResourceID, schedule string) error {
	c.Logger.Info("Running feature materialization job on resource: ", resID)
	feature, err := c.Metadata.GetFeatureVariant(context.Background(), metadata.NameVariant{Name: resID.Name, Variant: resID.Variant})
	if err != nil {
		return err
	}
	c.Logger.Infow("feature variant", "name", feature.Name(), "source", feature.Source(), "location", feature.Location(), "location_col", feature.LocationColumns())
	err = c.setPending(resID, feature.Status())
	if err != nil {
		return err
	}

	sourceNameVariant := feature.Source()
	source, err := c.AwaitPendingSource(sourceNameVariant)
	if err != nil {
		return err
	}
	sourceProvider, err := source.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return err
	}
	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		return err
	}
	sourceStore, err := p.AsOfflineStore()
	if err != nil {
		return err
	}
	defer func(sourceStore provider.OfflineStore) {
		err := sourceStore.Close()
		if err != nil {
			c.Logger.Errorf("could not close offline store: %v", err)
		}
	}(sourceStore)

	var featureProvider *metadata.Provider
	if feature.Provider() != "" {
		featureProvider, err = feature.FetchProvider(c.Metadata, context.Background())
		if err != nil {
			return err
		}
	}

	var vType provider.ValueType
	if feature.IsEmbedding() {
		vType = provider.VectorType{
			ScalarType:  provider.ScalarType(feature.Type()),
			Dimension:   feature.Dimension(),
			IsEmbedding: true,
		}
	} else {
		vType = provider.ScalarType(feature.Type())
	}

	var sourceTableName string
	if source.IsSQLTransformation() || source.IsDFTransformation() {
		sourceResourceID := provider.ResourceID{Name: sourceNameVariant.Name, Variant: sourceNameVariant.Variant, Type: provider.Transformation}
		sourceTable, err := sourceStore.GetTransformationTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	} else if source.IsPrimaryDataSQLTable() {
		sourceResourceID := provider.ResourceID{Name: sourceNameVariant.Name, Variant: sourceNameVariant.Variant, Type: provider.Primary}
		sourceTable, err := sourceStore.GetPrimaryTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	}

	featID := provider.ResourceID{
		Name:    resID.Name,
		Variant: resID.Variant,
		Type:    provider.Feature,
	}
	tmpSchema := feature.LocationColumns().(metadata.ResourceVariantColumns)
	schema := provider.ResourceSchema{
		Entity:      tmpSchema.Entity,
		Value:       tmpSchema.Value,
		TS:          tmpSchema.TS,
		SourceTable: sourceTableName,
	}
	c.Logger.Debugw("Creating Resource Table", "id", featID, "schema", schema)
	_, err = sourceStore.RegisterResourceFromSourceTable(featID, schema)
	if err != nil {
		return err
	}
	c.Logger.Debugw("Resource Table Created", "id", featID, "schema", schema)

	materializedRunnerConfig := runner.MaterializedRunnerConfig{
		OfflineType:   pt.Type(sourceProvider.Type()),
		OfflineConfig: sourceProvider.SerializedConfig(),
		ResourceID:    provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Feature},
		VType:         provider.ValueTypeJSONWrapper{ValueType: vType},
		Cloud:         runner.LocalMaterializeRunner,
		IsUpdate:      false,
	}

	if featureProvider != nil {
		materializedRunnerConfig.OnlineType = pt.Type(featureProvider.Type())
		materializedRunnerConfig.OnlineConfig = featureProvider.SerializedConfig()
	} else {
		materializedRunnerConfig.OnlineType = pt.NONE
	}

	isImportToS3Enabled, err := c.checkS3Import(featureProvider)
	if err != nil {
		return err
	}

	var materializationErr error
	if schedule != "" {
		materializationErr = c.materializeFeatureOnSchedule(resID, materializedRunnerConfig, schedule)
	} else if isImportToS3Enabled {
		materializationErr = c.materializeFeatureViaS3Import(resID, materializedRunnerConfig, sourceStore)
	} else {
		materializationErr = c.materializeFeature(resID, materializedRunnerConfig)
	}
	if materializationErr != nil {
		return materializationErr
	}

	c.Logger.Debugw("Setting status to ready", "id", featID)
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
		return clientv3.ErrNoAvailableEndpoints
	}
	return nil
}

func (c *Coordinator) materializeFeature(id metadata.ResourceID, config runner.MaterializedRunnerConfig) error {
	c.Logger.Infow("Starting Feature Materialization", "id", id)
	serialized, err := config.Serialize()
	if err != nil {
		return err
	}
	jobRunner, err := c.Spawner.GetJobRunner(runner.MATERIALIZE, serialized, id)
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}
	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) materializeFeatureOnSchedule(id metadata.ResourceID, config runner.MaterializedRunnerConfig, schedule string) error {
	c.Logger.Infow("Scheduling Feature Materialization", "id", id)
	config.IsUpdate = true
	serialized, err := config.Serialize()
	if err != nil {
		return err
	}
	jobRunnerUpdate, err := c.Spawner.GetJobRunner(runner.MATERIALIZE, serialized, id)
	if err != nil {
		return err
	}
	cronRunner, isCronRunner := jobRunnerUpdate.(kubernetes.CronRunner)
	if !isCronRunner {
		return err
	}
	if err := cronRunner.ScheduleJob(kubernetes.CronSchedule(schedule)); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) materializeFeatureViaS3Import(id metadata.ResourceID, config runner.MaterializedRunnerConfig, sourceStore provider.OfflineStore) error {
	c.Logger.Infow("Materializing Feature Via S3 Import", "id", id)
	sparkOfflineStore, isSparkOfflineStore := sourceStore.(*provider.SparkOfflineStore)
	if !isSparkOfflineStore {
		return fferr.NewInvalidArgumentError(fmt.Errorf("offline store is not spark offline store"))
	}
	if sparkOfflineStore.Store.FilestoreType() != filestore.S3 {
		return fferr.NewInvalidArgumentError(fmt.Errorf("offline file store must be S3; %s is not supported", sparkOfflineStore.Store.FilestoreType()))
	}
	serialized, err := config.Serialize()
	if err != nil {
		return err
	}
	jobRunner, err := c.Spawner.GetJobRunner(runner.S3_IMPORT_DYNAMODB, serialized, id)
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}
	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	c.Logger.Info("Successfully materialized feature via S3 import to DynamoDB", "id", id)
	return nil
}

func (c *Coordinator) checkS3Import(featureProvider *metadata.Provider) (bool, error) {
	if featureProvider != nil && featureProvider.Type() == string(pt.DynamoDBOnline) {
		c.Logger.Debugw("Feature provider is DynamoDB")
		config := pc.DynamodbConfig{}
		if err := config.Deserialize(featureProvider.SerializedConfig()); err != nil {
			return false, err
		}
		return config.ImportFromS3, nil
	}
	return false, nil
}

func (c *Coordinator) runTrainingSetJob(resID metadata.ResourceID, schedule string) error {
	c.Logger.Info("Running training set job on resource: ", "name", resID.Name, "variant", resID.Variant)
	ts, err := c.Metadata.GetTrainingSetVariant(context.Background(), metadata.NameVariant{Name: resID.Name, Variant: resID.Variant})
	if err != nil {
		return err
	}

	err = c.setPending(resID, ts.Status())
	if err != nil {
		return err
	}

	providerEntry, err := ts.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return err
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return err
	}
	defer func(store provider.OfflineStore) {
		err := store.Close()
		if err != nil {
			c.Logger.Errorf("could not close offline store: %v", err)
		}
	}(store)
	providerResID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.TrainingSet}

	if _, err := store.GetTrainingSet(providerResID); err == nil {
		return err
	}
	features := ts.Features()
	featureList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		featureList[i] = provider.ResourceID{Name: feature.Name, Variant: feature.Variant, Type: provider.Feature}
		featureResource, err := c.Metadata.GetFeatureVariant(context.Background(), feature)
		if err != nil {
			return err
		}
		sourceNameVariant := featureResource.Source()
		_, err = c.AwaitPendingSource(sourceNameVariant)
		if err != nil {
			return err
		}
		_, err = c.AwaitPendingFeature(metadata.NameVariant{Name: feature.Name, Variant: feature.Variant})
		if err != nil {
			return err
		}
	}

	lagFeatures := ts.LagFeatures()
	lagFeaturesList := make([]provider.LagFeatureDef, len(lagFeatures))
	for i, lagFeature := range lagFeatures {
		lagFeaturesList[i] = provider.LagFeatureDef{
			FeatureName:    lagFeature.GetFeature(),
			FeatureVariant: lagFeature.GetVariant(),
			LagName:        lagFeature.GetName(),
			LagDelta:       lagFeature.GetLag().AsDuration(), // see if need to convert it to time.Duration
		}
	}

	label, err := ts.FetchLabel(c.Metadata, context.Background())
	if err != nil {
		return err
	}
	labelSourceNameVariant := label.Source()
	_, err = c.AwaitPendingSource(labelSourceNameVariant)
	if err != nil {
		return err
	}
	label, err = c.AwaitPendingLabel(metadata.NameVariant{Name: label.Name(), Variant: label.Variant()})
	if err != nil {
		return err
	}
	trainingSetDef := provider.TrainingSetDef{
		ID:          providerResID,
		Label:       provider.ResourceID{Name: label.Name(), Variant: label.Variant(), Type: provider.Label},
		Features:    featureList,
		LagFeatures: lagFeaturesList,
	}
	tsRunnerConfig := runner.TrainingSetRunnerConfig{
		OfflineType:   pt.Type(providerEntry.Type()),
		OfflineConfig: providerEntry.SerializedConfig(),
		Def:           trainingSetDef,
		IsUpdate:      false,
	}
	serialized, _ := tsRunnerConfig.Serialize()
	jobRunner, err := c.Spawner.GetJobRunner(runner.CREATE_TRAINING_SET, serialized, resID)
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}
	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
		return err
	}
	if schedule != "" {
		scheduleTrainingSetRunnerConfig := runner.TrainingSetRunnerConfig{
			OfflineType:   pt.Type(providerEntry.Type()),
			OfflineConfig: providerEntry.SerializedConfig(),
			Def:           trainingSetDef,
			IsUpdate:      true,
		}
		serializedUpdate, err := scheduleTrainingSetRunnerConfig.Serialize()
		if err != nil {
			return err
		}
		jobRunnerUpdate, err := c.Spawner.GetJobRunner(runner.CREATE_TRAINING_SET, serializedUpdate, resID)
		if err != nil {
			return err
		}
		cronRunner, isCronRunner := jobRunnerUpdate.(kubernetes.CronRunner)
		if !isCronRunner {
			return fferr.NewInternalError(fmt.Errorf("kubernetes runner does not implement schedule"))
		}
		if err := cronRunner.ScheduleJob(kubernetes.CronSchedule(schedule)); err != nil {
			return err
		}
		if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) getJob(mtx *concurrency.Mutex, key string) (*metadata.CoordinatorJob, error) {
	c.Logger.Debugf("Checking existence of job with key %s\n", key)
	txn := (*c.KVClient).Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpGet(key)).Commit()
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	isOwner := response.Succeeded //response.Succeeded sets true if transaction "if" statement true
	if !isOwner {
		return nil, fferr.NewInternalError(fmt.Errorf("was not owner of lock"))
	}
	responseData := response.Responses[0]
	responseKVs := responseData.GetResponseRange().GetKvs()
	if len(responseKVs) == 0 {
		return nil, fferr.NewJobDoesNotExistError(key, nil)
	}
	responseValue := responseKVs[0].Value //Only single response for single key
	job := &metadata.CoordinatorJob{}
	if err := job.Deserialize(responseValue); err != nil {
		return nil, err
	}
	return job, nil
}

func (c *Coordinator) incrementJobAttempts(mtx *concurrency.Mutex, job *metadata.CoordinatorJob, jobKey string) error {
	job.Attempts += 1
	serializedJob, err := job.Serialize()
	if err != nil {
		return err
	}
	txn := (*c.KVClient).Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpPut(jobKey, string(serializedJob))).Commit()
	if err != nil {
		return fferr.NewInternalError(err)
	}
	isOwner := response.Succeeded //response.Succeeded sets true if transaction "if" statement true
	if !isOwner {
		return fferr.NewInternalError(fmt.Errorf("was not owner of lock"))
	}
	return nil
}

func (c *Coordinator) deleteJob(mtx *concurrency.Mutex, key string) error {
	c.Logger.Info("Deleting job with key: ", key)
	txn := (*c.KVClient).Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return fferr.NewInternalError(err)
	}
	isOwner := response.Succeeded //response.Succeeded sets true if transaction "if" statement true
	if !isOwner {
		return fferr.NewInternalError(fmt.Errorf("was not owner of lock"))
	}
	responseData := response.Responses[0] //OpDelete always returns single response
	numDeleted := responseData.GetResponseDeleteRange().Deleted
	if numDeleted != 1 { //returns 0 if delete key did not exist
		return fferr.NewInternalError(fmt.Errorf("job Already deleted"))
	}
	c.Logger.Info("Successfully deleted job with key: ", key)
	return nil
}

func (c *Coordinator) hasJob(id metadata.ResourceID) (bool, error) {
	getResp, err := (*c.KVClient).Get(context.Background(), metadata.GetJobKey(id), clientv3.WithPrefix())
	if err != nil {
		return false, fferr.NewInternalError(fmt.Errorf("fetch jobs from etcd with prefix %s: %v", metadata.GetJobKey(id), err))
	}
	responseLength := len(getResp.Kvs)
	if responseLength > 0 {
		return true, nil
	}
	return false, nil
}

func (c *Coordinator) createJobLock(jobKey string, s *concurrency.Session) (*concurrency.Mutex, error) {
	mtx := concurrency.NewMutex(s, GetLockKey(jobKey))
	if err := mtx.Lock(context.Background()); err != nil {
		c.Logger.Errorw("could not create job lock restarting.....", "error", err)
		os.Exit(1)
	}
	return mtx, nil
}

func (c *Coordinator) ExecuteJob(jobKey string) error {
	c.Logger.Info("Executing new job with key ", jobKey)
	s, err := concurrency.NewSession(c.EtcdClient, concurrency.WithTTL(10_000_000))
	if err != nil {
		return fferr.NewInternalError(fmt.Errorf("new session: %v", err))
	}
	defer s.Close()
	mtx, err := c.createJobLock(jobKey, s)
	if err != nil {
		return err
	}
	defer func() {
		if err := mtx.Unlock(context.Background()); err != nil {
			c.Logger.Debugw("Error unlocking mutex:", "error", err)
		}
	}()
	job, err := c.getJob(mtx, jobKey)
	if err != nil {
		return err
	}
	c.Logger.Debugf("Job %s is on attempt %d", jobKey, job.Attempts)
	if job.Attempts > MAX_ATTEMPTS {
		if err := c.deleteJob(mtx, jobKey); err != nil {
			c.Logger.Debugw("Error deleting job", "error", err)
			return err
		}
		return fferr.NewInternalError(fmt.Errorf("job failed after %d attempts. Cancelling coordinator flow", MAX_ATTEMPTS))
	}
	if err := c.incrementJobAttempts(mtx, job, jobKey); err != nil {
		return err
	}
	type jobFunction func(metadata.ResourceID, string) error
	fns := map[metadata.ResourceType]jobFunction{
		metadata.TRAINING_SET_VARIANT: c.runTrainingSetJob,
		metadata.FEATURE_VARIANT:      c.runFeatureMaterializeJob,
		metadata.LABEL_VARIANT:        c.runLabelRegisterJob,
		metadata.SOURCE_VARIANT:       c.runRegisterSourceJob,
	}
	jobFunc, has := fns[job.Resource.Type]
	if !has {
		return fferr.NewInvalidResourceTypeError(job.Resource.Name, job.Resource.Variant, fferr.ResourceType(job.Resource.Type.String()), nil)
	}

	if err := jobFunc(job.Resource, job.Schedule); err != nil {
		switch err.(type) {
		case *fferr.ResourceAlreadyFailedError:
			return err
		default:
			//return c.Metadata.SetStatus(context.Background(), job.Resource, metadata.FAILED, fferr.FromErr(err).Error())
			return c.Metadata.SetStatusError(context.Background(), job.Resource, metadata.FAILED, err)
		}
	}
	c.Logger.Info("Successfully executed job with key: ", jobKey)
	if err := c.deleteJob(mtx, jobKey); err != nil {
		c.Logger.Debugw("Error deleting job", "error", err)
		return err
	}
	return nil
}

type ResourceUpdatedEvent struct {
	ResourceID metadata.ResourceID
	Completed  time.Time
}

func (c *ResourceUpdatedEvent) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return config, nil
}

func (c *ResourceUpdatedEvent) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (c *Coordinator) signalResourceUpdate(key string, value string) error {
	c.Logger.Info("Updating metadata with latest resource update status and time", key)
	s, err := concurrency.NewSession(c.EtcdClient, concurrency.WithTTL(1))
	if err != nil {
		return fferr.NewInternalError(fmt.Errorf("create new concurrency session for resource update job: %v", err))
	}
	defer s.Close()
	mtx, err := c.createJobLock(key, s)
	if err != nil {
		return err
	}
	defer func() {
		if err := mtx.Unlock(context.Background()); err != nil {
			c.Logger.Debugw("Error unlocking mutex:", "error", err)
		}
	}()
	resUpdatedEvent := &ResourceUpdatedEvent{}
	if err := resUpdatedEvent.Deserialize(Config(value)); err != nil {
		return err
	}
	if err := c.Metadata.SetStatus(context.Background(), resUpdatedEvent.ResourceID, metadata.READY, ""); err != nil {
		return err
	}
	c.Logger.Info("Succesfully set update status for update job with key: ", key)
	if err := c.deleteJob(mtx, key); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) changeJobSchedule(key string, value string) error {
	c.Logger.Info("Updating schedule of currently made cronjob in kubernetes: ", key)
	s, err := concurrency.NewSession(c.EtcdClient, concurrency.WithTTL(1))
	if err != nil {
		return err
	}
	defer func(s *concurrency.Session) {
		err := s.Close()
		if err != nil {
			c.Logger.Debugw("Error closing scheduling session", "error", err)
		}
	}(s)
	mtx, err := c.createJobLock(key, s)
	if err != nil {
		return err
	}
	defer func() {
		if err := mtx.Unlock(context.Background()); err != nil {
			c.Logger.Debugw("Error unlocking mutex:", "error", err)
		}
	}()
	coordinatorScheduleJob := &metadata.CoordinatorScheduleJob{}
	if err := coordinatorScheduleJob.Deserialize(Config(value)); err != nil {
		return err
	}
	namespace, err := kubernetes.GetCurrentNamespace()
	if err != nil {
		return err
	}
	jobName := kubernetes.CreateJobName(coordinatorScheduleJob.Resource)
	jobClient, err := kubernetes.NewKubernetesJobClient(jobName, namespace)
	if err != nil {
		return err
	}
	cronJob, err := jobClient.GetCronJob()
	if err != nil {
		return err
	}
	cronJob.Spec.Schedule = coordinatorScheduleJob.Schedule
	if _, err := jobClient.UpdateCronJob(cronJob); err != nil {
		return err
	}
	if err := c.Metadata.SetStatus(context.Background(), coordinatorScheduleJob.Resource, metadata.READY, ""); err != nil {
		return err
	}
	c.Logger.Info("Successfully updated schedule for job in kubernetes with key: ", key)
	if err := c.deleteJob(mtx, key); err != nil {
		return err
	}
	return nil
}
