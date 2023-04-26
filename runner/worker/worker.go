// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/featureform/coordinator"
	"github.com/featureform/runner"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"os"
	"strconv"
	"time"
)

type Config []byte

func CreateAndRun() error {
	logger := zap.NewExample().Sugar()
	config, ok := os.LookupEnv("CONFIG")

	if !ok {
		return errors.New("CONFIG not set")
	}
	fmt.Printf("Config: %v\n", config)
	name, ok := os.LookupEnv("NAME")

	if !ok {
		return errors.New("NAME not set")
	}
	var etcdConf string
	if !ok {
		return errors.New("ETCD_CONFIG not set")
	}
	jobRunner, err := runner.Create(name, []byte(config))
	if err != nil {
		return err
	}
	logger.Infof("Starting job for resource: %v", jobRunner.Resource())
	if jobRunner.IsUpdateJob() {
		logger.Info("This is an update job")
		etcdConf, ok = os.LookupEnv("ETCD_CONFIG")
		if !ok {
			return errors.New("ETCD_CONFIG not set")
		}
	}
	indexString, hasIndexEnv := os.LookupEnv("JOB_COMPLETION_INDEX")
	indexRunner, isIndexRunner := jobRunner.(runner.IndexRunner)
	if isIndexRunner && !hasIndexEnv {
		return errors.New("index runner needs index set")
	}
	if !isIndexRunner && hasIndexEnv {
		return errors.New("runner is not an index runner")
	}
	if hasIndexEnv && isIndexRunner {
		index, err := strconv.Atoi(indexString)
		if err != nil {
			return errors.New("index not of type int")
		}
		if err := indexRunner.SetIndex(index); err != nil {
			return errors.New("cannot set index")
		}
		jobRunner = indexRunner
	}
	watcher, err := jobRunner.Run()
	if err != nil {
		return err
	}
	if err := watcher.Wait(); err != nil {
		return err
	}
	logger.Infof("Completed job for resource %v", jobRunner.Resource())
	if jobRunner.IsUpdateJob() {
		jobResource := jobRunner.Resource()
		logger.Infof("Logging update success in etcd for job: %v", jobResource)
		etcdConfig := &coordinator.ETCDConfig{}
		err := etcdConfig.Deserialize(coordinator.Config(etcdConf))
		if err != nil {
			return err
		}
		cli, err := clientv3.New(clientv3.Config{Endpoints: etcdConfig.Endpoints, Username: etcdConfig.Username, Password: etcdConfig.Password, DialTimeout: time.Second * 5})
		if err != nil {
			return err
		}
		resourceID := jobRunner.Resource()
		timeCompleted := time.Now()
		updatedEvent := &coordinator.ResourceUpdatedEvent{
			ResourceID: resourceID,
			Completed:  timeCompleted,
		}
		serializedEvent, err := updatedEvent.Serialize()
		if err != nil {
			return err
		}
		eventID := uuid.New().String()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		_, err = cli.Put(ctx, fmt.Sprintf("UPDATE_EVENT_%s__%s__%s__%s", jobResource.Name, jobResource.Variant, jobResource.Type.String(), eventID), string(serializedEvent))
		if err != nil {
			return err
		}
		logger.Infof("Succesfully logged job completion for resource: %v", jobRunner.Resource())
	}
	return nil
}
