package scheduling

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestMemoryStorageProvider(t *testing.T) {
	testFns := map[string]func(*testing.T){
		"SetStorageProvider":  StorageProviderSet,
		"GetStorageProvider":  StorageProviderGet,
		"ListStorageProvider": StorageProviderList,
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t)
		})
	}
}

func StorageProviderSet(t *testing.T) {
	provider := &MemoryStorageProvider{storage: sync.Map{}, lockedItems: sync.Map{}}
	type TestCase struct {
		key   string
		value string
		err   error
	}
	tests := map[string]TestCase{
		"Simple":     {"setTest/key1", "value1", nil},
		"EmptyKey":   {"", "value1", fmt.Errorf("key is empty")},
		"EmptyValue": {"setTest/key1", "", fmt.Errorf("value is empty for key setTest/key1")},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			lockObject, err := provider.Lock(test.key)
			if test.err != nil && err != nil && err.Error() == test.err.Error() {
				// continue to next test case
				return
			}
			if err != nil {
				t.Errorf("could not lock key: %v", err)
			}
			err = provider.Set(test.key, test.value, lockObject)
			if err != nil && err.Error() != test.err.Error() {
				t.Errorf("Set(%s, %s): expected error %v, got %v", test.key, test.value, test.err, err)
			}
			provider.Unlock(test.key, lockObject)
		})
	}
}

func StorageProviderGet(t *testing.T) {
	provider := &MemoryStorageProvider{storage: sync.Map{}, lockedItems: sync.Map{}}
	type TestCase struct {
		key     string
		prefix  bool
		results map[string]string
		err     error
	}

	tests := map[string]TestCase{
		"Simple":            {"key1", false, map[string]string{"key1": "value1"}, nil},
		"KeyNotFound":       {"key3", false, nil, &KeyNotFoundError{Key: "key3"}},
		"EmptyKey":          {"", false, nil, fmt.Errorf("key is empty")},
		"PrefixNotCalled":   {"prefix/key", false, nil, &KeyNotFoundError{Key: "prefix/key"}},
		"Prefix":            {"prefix/key", true, map[string]string{"prefix/key3": "value3", "prefix/key4": "value4"}, nil},
		"PrefixKeyNotFound": {"prefix/key5", true, nil, &KeyNotFoundError{Key: "prefix/key5"}},
	}

	runTestCase := func(t *testing.T, test TestCase) {
		lockObject1, _ := provider.Lock("key1")
		provider.Set("key1", "value1", lockObject1)

		lockObject2, _ := provider.Lock("key2")
		provider.Set("key2", "value2", lockObject2)

		lockObject3, _ := provider.Lock("prefix/key3")
		provider.Set("prefix/key3", "value3", lockObject3)

		lockObject4, _ := provider.Lock("prefix/key4")
		provider.Set("prefix/key4", "value4", lockObject4)

		results, err := provider.Get(test.key, test.prefix)
		if err != nil && err.Error() != test.err.Error() {
			t.Errorf("Get(%s, %v): expected error %v, got %v", test.key, test.prefix, test.err, err)
		}
		if !compareMaps(results, test.results) {
			t.Errorf("Get(%s, %v): expected results %v, got %v", test.key, test.prefix, test.results, results)
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}
}

func StorageProviderList(t *testing.T) {
	type TestCase struct {
		keys        []string
		prefix      string
		results     []string
		shouldError bool
	}

	tests := map[string]TestCase{
		"Single":      {[]string{"list/key1"}, "list", []string{"list/key1"}, false},
		"Multiple":    {[]string{"list/key1", "list/key2", "list/key3"}, "list", []string{"list/key1", "list/key2", "list/key3"}, false},
		"MixedPrefix": {[]string{"list/key1", "lost/key2", "list/key3"}, "list", []string{"list/key1", "list/key3"}, false},
		"EmptyPrefix": {[]string{"list/key1", "list/key2", "list/key3"}, "", []string{"list/key1", "list/key2", "list/key3"}, false},
	}

	runTestCase := func(t *testing.T, test TestCase) {
		provider := &MemoryStorageProvider{storage: sync.Map{}, lockedItems: sync.Map{}}
		for _, key := range test.keys {
			lockObject, err := provider.Lock(key)
			if err != nil {
				t.Fatalf("could not lock key: %v", err)
			}
			err = provider.Set(key, "value", lockObject)
			if err != nil {
				t.Fatalf("could not set key: %v", err)
			}
		}

		results, err := provider.ListKeys(test.prefix)
		if err != nil {
			t.Fatalf("unable to list keys with prefix (%s): %v", test.prefix, err)
		}
		if len(results) != len(test.results) {
			t.Fatalf("Expected %d results, got %d results\nExpected List: %v, Got List: %v", len(test.results), len(results), test.results, results)
		}
		for !compareStringSlices(results, test.results) {
			t.Fatalf("Expected List: %v, Got List: %v", test.results, results)
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}
}

func compareStringSlices(a, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func compareMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestLockAndUnlock(t *testing.T) {
	provider := &MemoryStorageProvider{storage: sync.Map{}, lockedItems: sync.Map{}}

	key := "/tasks/metadata/task_id=1"

	// Test Lock
	lock, err := provider.Lock(key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item
	diffLock, err := provider.Lock(key)
	if err == nil {
		t.Fatalf("Locking using different id should have failed")
	}

	// Test Unlock with different lock
	err = provider.Unlock(key, diffLock)
	if err == nil {
		t.Fatalf("Unlocking using different id should have failed")
	}

	// Test Unlock with original lock
	err = provider.Unlock(key, lock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestLockAndUnlockWithGoRoutines(t *testing.T) {
	provider := &MemoryStorageProvider{storage: sync.Map{}, lockedItems: sync.Map{}}

	key := "/tasks/metadata/task_id=2"
	lockChannel := make(chan LockObject)
	errChan := make(chan error)

	// Test Lock
	go lockGoRoutine(provider, key, lockChannel, errChan)
	lock := <-lockChannel
	err := <-errChan
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item
	diffLockChannel := make(chan LockObject)
	go lockGoRoutine(provider, key, diffLockChannel, errChan)
	diffLock := <-diffLockChannel
	err = <-errChan
	if err == nil {
		t.Fatalf("Locking using different id should have failed")
	}

	// Test UnLock with different UUID
	go unlockGoRoutine(provider, diffLock, key, errChan)
	err = <-errChan
	if err == nil {
		t.Fatalf("Unlocking using different id should have failed")
	}

	// Test Unlock with original lock
	go unlockGoRoutine(provider, lock, key, errChan)
	err = <-errChan
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestLockTimeUpdates(t *testing.T) {
	provider := NewMemoryStorageProvider()

	key := "/tasks/metadata/task_id=3"
	lock, err := provider.Lock(key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	err = provider.Set(key, "value", lock)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Wait for 2 x ValidTimePeriod
	time.Sleep(2 * ValidTimePeriod)

	// Check if date has been updated
	lockInfo, ok := provider.lockedItems.Load(key)
	if !ok {
		t.Fatalf("Key not found")
	}
	lockDetail := lockInfo.(LockInformation)
	if time.Since(lockDetail.Date) > ValidTimePeriod {
		t.Fatalf("Lock time not updated: current time: %s, lock time: %s", time.Now().String(), lockDetail.Date.String())
	}

	// Unlock the key
	err = provider.Unlock(key, lock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Check if key has been deleted
	_, ok = provider.lockedItems.Load(key)
	if ok {
		t.Fatalf("Key not deleted")
	}
}

func lockGoRoutine(provider *MemoryStorageProvider, key string, lockChannel chan LockObject, errChan chan error) {
	lockObject, err := provider.Lock(key)
	lockChannel <- lockObject
	errChan <- err
}

func unlockGoRoutine(provider *MemoryStorageProvider, lock LockObject, key string, errChan chan error) {
	err := provider.Unlock(key, lock)
	errChan <- err
}
