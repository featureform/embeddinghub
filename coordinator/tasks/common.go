package tasks

import (
	"context"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
)

type providerFetcher interface {
	FetchProvider(*metadata.Client, context.Context) (*metadata.Provider, error)
}

func getStore(baseTask BaseTask, client *metadata.Client, pf providerFetcher) (provider.OfflineStore, error) {
	if err := client.Tasks.AddRunLog(baseTask.taskDef.TaskId, baseTask.taskDef.ID, "Fetching Offline Store..."); err != nil {
		return nil, err
	}

	providerEntry, err := pf.FetchProvider(client, context.Background())
	if err != nil {
		return nil, err
	}

	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, err
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		//logger.Errorw(
		//	"Retrieved provider is not an offline store",
		//	"provider-type", p.Type(), "error", err,
		//)
		return nil, err
	}

	return store, nil
}
