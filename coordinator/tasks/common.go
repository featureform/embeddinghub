package tasks

import (
	"context"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
)

type offlineProviderFetcher interface {
	FetchProvider(*metadata.Client, context.Context) (*metadata.Provider, error)
}

// an adapter to fetch an offline provider for a feature since feature's FetchProvider returns the online provider
type offlineProviderFeatureAdapter struct {
	feature *metadata.FeatureVariant
}

func (f *offlineProviderFeatureAdapter) FetchProvider(client *metadata.Client, ctx context.Context) (*metadata.Provider, error) {
	return f.feature.FetchOfflineStoreProvider(client, ctx)
}

func getOfflineStore(
	ctx context.Context,
	baseTask BaseTask,
	client *metadata.Client,
	pf offlineProviderFetcher,
	logger logging.Logger,
) (provider.OfflineStore, error) {
	logMessage := "Fetching Provider..."
	if err := client.Tasks.AddRunLog(baseTask.taskDef.TaskId, baseTask.taskDef.ID, logMessage); err != nil {
		logger.Warnw("Failed to add run log; continuing.", "error", err)
	}

	logger.Debugf("Fetching provider for task")
	providerEntry, err := pf.FetchProvider(client, ctx)
	if err != nil {
		logger.Errorw("Failed to fetch provider", "error", err)
		return nil, err
	}

	logMessage = "Fetching Offline Store..."
	if err := client.Tasks.AddRunLog(baseTask.taskDef.TaskId, baseTask.taskDef.ID, logMessage); err != nil {
		logger.Warnw("Failed to add run log; continuing", "error", err, "message", logMessage)
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		logger.Errorw("Failed to get provider", "error", err)
		return nil, err
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		logger.Errorw("Retrieved provider is not an offline store", "provider-type", p.Type(), "error", err)
		return nil, err
	}

	if store == nil {
		logger.Errorw("Offline store is nil", "provider-type", p.Type())
		return nil, fferr.NewInternalErrorf("offline store is nil")
	}

	return store, nil
}
