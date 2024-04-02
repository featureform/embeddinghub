package ffsync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type OrderedId interface {
	Equals(other OrderedId) bool
	Less(other OrderedId) bool
	String() string
	FromString(id string) error
	Value() interface{} // Value returns the underlying value of the ordered ID
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
}

const etcd_id_key = "FFSync/ID" // Key for the etcd ID generator, will add namespace to the end

type Uint64OrderedId uint64

func (id *Uint64OrderedId) Equals(other OrderedId) bool {
	return id.Value() == other.Value()
}

func (id *Uint64OrderedId) Less(other OrderedId) bool {
	if otherID, ok := other.(*Uint64OrderedId); ok {
		return *id < *otherID
	}
	return false
}

func (id *Uint64OrderedId) String() string {
	return fmt.Sprint(id.Value())
}

func (id *Uint64OrderedId) FromString(strID string) error {
	tmp, err := strconv.ParseUint(strID, 10, 64)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	*id = Uint64OrderedId(tmp)
	return nil
}

func (id *Uint64OrderedId) Value() interface{} {
	return uint64(*id)
}

func (id *Uint64OrderedId) UnmarshalJSON(data []byte) error {
	var tmp uint64
	if err := json.Unmarshal(data, &tmp); err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to unmarshal uint64OrderedId: %v", err))
	}

	*id = Uint64OrderedId(tmp)
	return nil
}

func (id *Uint64OrderedId) MarshalJSON() ([]byte, error) {
	return json.Marshal(uint64(*id))
}

type OrderedIdGenerator interface {
	NextId(namespace string) (OrderedId, error)
}

func NewMemoryOrderedIdGenerator() (OrderedIdGenerator, error) {
	return &memoryIdGenerator{
		counters: make(map[string]*Uint64OrderedId),
		mu:       sync.Mutex{},
	}, nil
}

type memoryIdGenerator struct {
	counters map[string]*Uint64OrderedId
	mu       sync.Mutex
}

func (m *memoryIdGenerator) NextId(namespace string) (OrderedId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize the counter if it doesn't exist
	if _, ok := m.counters[namespace]; !ok {
		var initialId Uint64OrderedId = 0
		m.counters[namespace] = &initialId
	}

	// Atomically increment the counter and return the next ID
	nextId := atomic.AddUint64((*uint64)(m.counters[namespace]), 1)
	return (*Uint64OrderedId)(&nextId), nil
}

func NewETCDOrderedIdGenerator() (OrderedIdGenerator, error) {
	etcdHost := helpers.GetEnv("ETCD_HOST", "localhost")
	etcdPort := helpers.GetEnv("ETCD_PORT", "2379")

	etcdHostPort := fmt.Sprintf("%s:%s", etcdHost, etcdPort)

	etcdURL := url.URL{
		Scheme: "http",
		Host:   etcdHostPort,
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcdURL.String()},
	})
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create etcd client: %w", err))
	}

	session, err := concurrency.NewSession(client)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create etcd session: %w", err))
	}

	return &etcdIdGenerator{
		client:  client,
		session: session,
		ctx:     context.Background(),
	}, nil
}

type etcdIdGenerator struct {
	client  *clientv3.Client
	session *concurrency.Session
	ctx     context.Context
}

func (etcd *etcdIdGenerator) NextId(namespace string) (OrderedId, error) {
	if namespace == "" {
		return nil, fferr.NewInternalError(fmt.Errorf("cannot generate ID for empty namespace"))
	}

	// Lock the namespace to prevent concurrent ID generation
	lockKey := createLockKey(etcd_id_key, namespace)
	lockMutex := concurrency.NewMutex(etcd.session, lockKey)
	if err := lockMutex.Lock(etcd.ctx); err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to lock key %s: %w", lockKey, err))
	}
	defer lockMutex.Unlock(etcd.ctx)

	// Get the current ID
	resp, err := etcd.client.Get(etcd.ctx, lockKey)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to get key %s: %w", lockKey, err))
	}

	// Initialize the ID if it doesn't exist
	idNotInitialized := len(resp.Kvs) == 0
	var nextId uint64
	if idNotInitialized {
		nextId = 1
	} else {
		nextIdStr := string(resp.Kvs[0].Value)
		nextId, err := strconv.ParseUint(nextIdStr, 10, 64)
		if err != nil {
			return nil, fferr.NewInternalError(fmt.Errorf("failed to parse ID as uint64: %s: %v", nextIdStr, err))
		}
		nextId++
	}

	// Update the ID
	if _, err := etcd.client.Put(etcd.ctx, lockKey, fmt.Sprint(nextId)); err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to set key %s: %w", lockKey, err))
	}

	return (*Uint64OrderedId)(&nextId), nil
}

func createLockKey(prefix, namespace string) string {
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(prefix, "/"), strings.TrimPrefix(namespace, "/"))
}
