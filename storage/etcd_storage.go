// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package storage

import (
	"context"
	"fmt"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/featureform/storage/query"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewETCDStorageImplementation(config helpers.ETCDConfig) (metadataStorageImplementation, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{config.URL()},
		Username:  config.Username,
		Password:  config.Password,
	})
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create etcd client: %w", err))
	}

	return &etcdStorageImplementation{
		client: client,
		ctx:    context.Background(),
	}, nil
}

type etcdStorageImplementation struct {
	client *clientv3.Client
	ctx    context.Context
}

func (etcd *etcdStorageImplementation) Set(key string, value string) error {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key"))
	}

	_, err := etcd.client.Put(etcd.ctx, key, value)
	if err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to set key %s: %w", key, err))
	}

	return nil
}

func (etcd *etcdStorageImplementation) Get(key string, opts ...query.Query) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("cannot get an empty key"))
	}

	resp, err := etcd.client.Get(etcd.ctx, key)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("failed to get key %s: %w", key, err))
	}
	if len(resp.Kvs) == 0 {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	return string(resp.Kvs[0].Value), nil
}

func (etcd *etcdStorageImplementation) List(prefix string, opts ...query.Query) (map[string]string, error) {
	// // todo: commenting until query opts implemented
	// if opts != nil && len(opts) > 0 {
	// 	return nil, fferr.NewInternalErrorf("Etcd storage doesn't support query options")
	// }
	rangeEnd := clientv3.GetPrefixRangeEnd(prefix)
	resp, err := etcd.client.Get(etcd.ctx, prefix, clientv3.WithPrefix(), clientv3.WithRange(rangeEnd))
	if err != nil {
		return nil, fferr.NewInternalErrorf("failed to get keys with prefix %s: %w", prefix, err)
	}

	result := make(map[string]string)
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}

	return result, nil
}

func (etcd *etcdStorageImplementation) ListColumn(prefix string, columns []query.Column, opts ...query.Query) ([]map[string]interface{}, error) {
	return nil, fferr.NewInternalErrorf("Etcd storage doesn't support ListColumn")
}

func (etcd *etcdStorageImplementation) Count(prefix string, opts ...query.Query) (int, error) {
	// todo: commenting until query opts implemented
	// if opts != nil && len(opts) > 0 {
	// 	return 0, fferr.NewInternalErrorf("Etcd storage doesn't support query options")
	// }
	rangeEnd := clientv3.GetPrefixRangeEnd(prefix)
	resp, err := etcd.client.Get(etcd.ctx, prefix, clientv3.WithPrefix(), clientv3.WithRange(rangeEnd))
	if err != nil {
		return 0, fferr.NewInternalErrorf("failed to get keys with prefix %s: %w", prefix, err)
	}
	return len(resp.Kvs), nil
}

func (etcd *etcdStorageImplementation) Delete(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("cannot delete empty key"))
	}

	resp, err := etcd.client.Delete(etcd.ctx, key, clientv3.WithPrevKV())
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("failed to delete key %s: %w", key, err))
	}

	if resp.Deleted == 0 {
		return "", nil
	}

	return string(resp.PrevKvs[0].Value), nil
}

func (etcd *etcdStorageImplementation) Close() {
	etcd.client.Close()
}

func (etcd *etcdStorageImplementation) Type() MetadataStorageType {
	return ETCDMetadataStorage
}
