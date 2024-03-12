package storage_storer

import (
	"fmt"
	"sync"
	"testing"

	"github.com/featureform/fferr"
	"github.com/featureform/locker"
)

type MetadataStorerTest struct {
	t       *testing.T
	storage metadataStorerImplementation
}

func (test *MetadataStorerTest) Run() {
	t := test.t
	storage := test.storage

	testFns := map[string]func(*testing.T, metadataStorerImplementation){
		"SetStorageProvider":  StorerSet,
		"GetStorageProvider":  StorerGet,
		"ListStorageProvider": StorerList,
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t, storage)
		})
	}
}

func StorerSet(t *testing.T, storage metadataStorerImplementation) {
	type TestCase struct {
		key   string
		value string
		err   error
	}
	tests := map[string]TestCase{
		"Simple":   {"setTest/key1", "value1", nil},
		"EmptyKey": {"", "value1", fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := storage.Set(test.key, test.value)
			if err != nil && err.Error() != test.err.Error() {
				t.Errorf("Set(%s, %s): expected error %v, got %v", test.key, test.value, test.err, err)
			}

			// continue to next test case
			if test.key == "" {
				return
			}

			value, err := storage.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}
			if value != test.value {
				t.Fatalf("Delete(%s): expected value %s, got %s", test.key, test.value, value)
			}
		})
	}
}

func StorerGet(t *testing.T, storage metadataStorerImplementation) {
	type TestCase struct {
		key   string
		value string
	}
	tests := map[string]TestCase{
		"Simple": {
			key:   "setTest/key1",
			value: "value1",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := storage.Set(test.key, test.value)
			if err != nil {
				t.Fatalf("Set(%s, %s) failed: %v", test.key, test.value, err)
			}

			value, err := storage.Get(test.key)
			if err != nil {
				t.Errorf("Get(%s): expected no error, got %v", test.key, err)
			}
			if value != test.value {
				t.Errorf("Get(%s): expected value %s, got %s", test.key, test.value, value)
			}

			_, err = storage.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}
		})
	}
}

func StorerList(t *testing.T, storage metadataStorerImplementation) {
	type TestCase struct {
		keys          map[string]string
		prefix        string
		expectedKeys  map[string]string
		expectedError error
	}

	tests := map[string]TestCase{
		"Simple": {
			keys: map[string]string{
				"simple/key1": "v1",
				"simple/key2": "v2",
				"simple/key3": "v3",
			},
			prefix: "simple",
			expectedKeys: map[string]string{
				"simple/key1": "v1",
				"simple/key2": "v2",
				"simple/key3": "v3",
			},
			expectedError: nil,
		},
		"NotAllKeys": {
			keys: map[string]string{
				"u/key1": "v1",
				"u/key2": "v2",
				"u/key3": "v3",
				"v/key4": "v4",
			},
			prefix: "u",
			expectedKeys: map[string]string{
				"u/key1": "v1",
				"u/key2": "v2",
				"u/key3": "v3",
			},
			expectedError: nil,
		},
		"EmptyPrefix": {
			keys: map[string]string{
				"x/key1": "v1",
				"y/key2": "v2",
				"z/key3": "v3",
			},
			prefix: "",
			expectedKeys: map[string]string{
				"x/key1": "v1",
				"y/key2": "v2",
				"z/key3": "v3",
			},
			expectedError: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			for key, value := range test.keys {
				err := storage.Set(key, value)
				if err != nil {
					t.Fatalf("Set(%s, %s) failed: %v", key, value, err)
				}
			}
			defer func() {
				for key, _ := range test.keys {
					_, err := storage.Delete(key)
					if err != nil {
						t.Fatalf("Delete(%s) failed: %v", key, err)
					}
				}
			}()

			keys, err := storage.List(test.prefix)
			if err != nil {
				t.Errorf("List(%s): expected no error, got %v", test.prefix, err)
			}

			if len(keys) != len(test.expectedKeys) {
				t.Fatalf("List(%s): expected %d keys, got %d keys", test.prefix, len(test.expectedKeys), len(keys))
			}

			for key, value := range test.expectedKeys {
				if keys[key] != value {
					t.Fatalf("List(%s): expected key %s to have value %s, got %s", test.prefix, key, value, keys[key])
				}
			}
		})
	}
}

func StorerDelete(t *testing.T, storage metadataStorerImplementation) {
	type TestCase struct {
		setKey        string
		setValue      string
		deleteKey     string
		deleteValue   string
		expectedError error
	}
	tests := map[string]TestCase{
		"Simple": {
			setKey:        "deleteTest/key1",
			setValue:      "value1",
			deleteKey:     "deleteTest/key1",
			deleteValue:   "value1",
			expectedError: nil,
		},
		"EmptyKey": {
			setKey:        "",
			setValue:      "",
			deleteKey:     "",
			deleteValue:   "",
			expectedError: fmt.Errorf("key is empty"),
		},

		"DeleteWrongKey": {
			setKey:        "key1",
			setValue:      "value1",
			deleteKey:     "key2",
			deleteValue:   "",
			expectedError: fmt.Errorf("key '%s' not found", "key2"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := storage.Set(test.setKey, test.setValue)
			if err != nil {
				t.Fatalf("Set(%s, %s) failed: %v", test.setKey, test.setValue, err)
			}

			value, err := storage.Delete(test.deleteKey)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.deleteKey, err)
			}

			if value != test.setValue {
				t.Fatalf("Delete(%s): expected value %s, got %s", test.deleteKey, test.setValue, value)
			}
		})
	}
}

func TestMetadataStorer(t *testing.T) {
	locker := &locker.MemoryLocker{
		LockedItems: sync.Map{},
		Mutex:       &sync.Mutex{},
	}

	storage := &MemoryStorerImplementation{
		Storage: make(map[string]string),
	}

	metadataStorer := MetadataStorer{
		Locker: locker,
		Storer: storage,
	}

	tests := map[string]func(*testing.T, MetadataStorer){
		"TestCreate": testCreate,
		"TestUpdate": testUpdate,
		"TestList":   testList,
		"TestGet":    testGet,
		"TestDelete": testDelete,
	}

	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			fn(t, metadataStorer)
		})
	}
}

func testCreate(t *testing.T, ms MetadataStorer) {
	type TestCase struct {
		key   string
		value string
		err   error
	}
	tests := map[string]TestCase{
		"Simple":   {"createTest/key1", "value1", nil},
		"EmptyKey": {"", "value1", fmt.Errorf("key is empty")},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ms.Create(test.key, test.value)
			if err != nil && err.Error() != test.err.Error() {
				t.Errorf("Create(%s, %s): expected error %v, got %v", test.key, test.value, test.err, err)
			}

			// continue to next test case
			if test.key == "" {
				return
			}

			value, err := ms.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}

			if value != test.value {
				t.Fatalf("Delete(%s): expected value %s, got %s", test.key, test.value, value)
			}
		})
	}
}

func updateFn(currentValue string) (string, fferr.GRPCError) {
	return fmt.Sprintf("%s_updated", currentValue), nil
}

func testUpdate(t *testing.T, ms MetadataStorer) {
	type TestCase struct {
		key          string
		value        string
		updatedValue string
		err          error
	}

	tests := map[string]TestCase{
		"Simple": {
			key:          "updateTest/key1",
			value:        "value1",
			updatedValue: "value1_updated",
			err:          nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ms.Create(test.key, test.value)
			if err != nil {
				t.Fatalf("Create(%s, %s) failed: %v", test.key, test.value, err)
			}

			err = ms.Update(test.key, updateFn)
			if err != nil {
				t.Fatalf("Update(%s) failed: %v", test.key, err)
			}

			value, err := ms.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}

			if value != test.updatedValue {
				t.Fatalf("Update(%s): expected value %s, got %s", test.key, test.updatedValue, value)
			}
		})
	}
}

func testList(t *testing.T, ms MetadataStorer) {
	type TestCase struct {
		keys          map[string]string
		prefix        string
		expectedKeys  map[string]string
		expectedError error
	}

	tests := map[string]TestCase{
		"Simple": {
			keys: map[string]string{
				"simple/key1": "v1",
				"simple/key2": "v2",
				"simple/key3": "v3",
			},
			prefix: "simple",
			expectedKeys: map[string]string{
				"simple/key1": "v1",
				"simple/key2": "v2",
				"simple/key3": "v3",
			},
			expectedError: nil,
		},
		"NotAllKeys": {
			keys: map[string]string{
				"u/key1": "v1",
				"u/key2": "v2",
				"u/key3": "v3",
				"v/key4": "v4",
			},
			prefix: "u",
			expectedKeys: map[string]string{
				"u/key1": "v1",
				"u/key2": "v2",
				"u/key3": "v3",
			},
			expectedError: nil,
		},
		"EmptyPrefix": {
			keys: map[string]string{
				"x/key1": "v1",
				"y/key2": "v2",
				"z/key3": "v3",
			},
			prefix: "",
			expectedKeys: map[string]string{
				"x/key1": "v1",
				"y/key2": "v2",
				"z/key3": "v3",
			},
			expectedError: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			for key, value := range test.keys {
				err := ms.Create(key, value)
				if err != nil {
					t.Fatalf("Create(%s, %s) failed: %v", key, value, err)
				}
			}
			defer func() {
				for key, _ := range test.keys {
					_, err := ms.Delete(key)
					if err != nil {
						t.Fatalf("Delete(%s) failed: %v", key, err)
					}
				}
			}()

			keys, err := ms.List(test.prefix)
			if err != nil {
				t.Errorf("List(%s): expected no error, got %v", test.prefix, err)
			}

			if len(keys) != len(test.expectedKeys) {
				t.Fatalf("List(%s): expected %d keys, got %d keys", test.prefix, len(test.expectedKeys), len(keys))
			}

			for key, value := range test.expectedKeys {
				if keys[key] != value {
					t.Fatalf("List(%s): expected key %s to have value %s, got %s", test.prefix, key, value, keys[key])
				}
			}
		})
	}
}

func testGet(t *testing.T, ms MetadataStorer) {
	type TestCase struct {
		key   string
		value string
	}
	tests := map[string]TestCase{
		"Simple": {
			key:   "setTest/key1",
			value: "value1",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ms.Create(test.key, test.value)
			if err != nil {
				t.Fatalf("Create(%s, %s) failed: %v", test.key, test.value, err)
			}

			value, err := ms.Get(test.key)
			if err != nil {
				t.Errorf("Get(%s): expected no error, got %v", test.key, err)
			}
			if value != test.value {
				t.Errorf("Get(%s): expected value %s, got %s", test.key, test.value, value)
			}

			_, err = ms.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}
		})
	}
}

func testDelete(t *testing.T, ms MetadataStorer) {
	type TestCase struct {
		key   string
		value string
		err   error
	}
	tests := map[string]TestCase{
		"Simple": {"deleteTest/key1", "value1", nil},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ms.Create(test.key, test.value)
			if err != nil {
				t.Fatalf("Create(%s, %s) failed: %v", test.key, test.value, err)
			}

			// continue to next test case
			if test.key == "" {
				return
			}

			value, err := ms.Delete(test.key)
			if err != nil {
				t.Fatalf("Delete(%s) failed: %v", test.key, err)
			}

			if value != test.value {
				t.Fatalf("Delete(%s): expected value %s, got %s", test.key, test.value, value)
			}
		})
	}
}
