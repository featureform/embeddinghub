// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package storage

import (
	"fmt"
	"strings"
	"sync"

	"github.com/featureform/fferr"
	"github.com/featureform/storage/query"
)

func NewMemoryStorageImplementation() (memoryStorageImplementation, error) {
	return memoryStorageImplementation{
		storage: &sync.Map{},
	}, nil
}

type memoryStorageImplementation struct {
	storage *sync.Map
}

func (m *memoryStorageImplementation) Set(key string, value string) error {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key"))
	}

	m.storage.Store(key, value)

	return nil
}

func (m *memoryStorageImplementation) Get(key string, opts ...query.Query) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))
	}

	value, ok := m.storage.Load(key)
	if !ok {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	return value.(string), nil
}

func (m *memoryStorageImplementation) List(prefix string, opts ...query.Query) (map[string]string, error) {
	// // todo: commenting until query opts implemented
	// if opts != nil && len(opts) > 0 {
	// 	return nil, fferr.NewInternalErrorf("Memory storage doesn't support query options")
	// }
	result := make(map[string]string)

	m.storage.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(key.(string), prefix) {
			result[key.(string)] = value.(string)
		}
		return true
	})

	return result, nil
}

func (m *memoryStorageImplementation) ListColumn(prefix string, columns []query.Column, opts ...query.Query) ([]map[string]interface{}, error) {
	return nil, fferr.NewInternalErrorf("Memory storage doesn't support ListColumn")
}

func (m *memoryStorageImplementation) Count(prefix string, opts ...query.Query) (int, error) {
	vals, err := m.List(prefix, opts...)
	if err != nil {
		return 0, err
	}
	return len(vals), nil
}

func (m *memoryStorageImplementation) Delete(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))
	}

	value, ok := m.storage.LoadAndDelete(key)
	if !ok {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	return value.(string), nil
}

func (m *memoryStorageImplementation) Close() {
	// Do nothing
}

func (m *memoryStorageImplementation) Type() MetadataStorageType {
	return MemoryMetadataStorage
}
