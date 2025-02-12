// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/featureform/fferr"

	"github.com/jonboulle/clockwork"
)

type LockerTest struct {
	t          *testing.T
	locker     Locker
	lockerType string
}

func (test *LockerTest) Run(clock clockwork.FakeClock) {
	t := test.t
	locker := test.locker

	testFns := map[string]func(*testing.T, Locker, clockwork.FakeClock){
		"LockAndUnlock":               LockAndUnlock,
		"LockAndUnlockWithGoRoutines": LockAndUnlockWithGoRoutines,
		"StressTestLockAndUnlock":     StressTestLockAndUnlock,
		"TestLockTimeUpdates":         LockTimeUpdates,
		"WaitForLock":                 WaitForLock,
	}

	// TODO: This can be removed?
	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			if name == "TestLockTimeUpdates" && test.lockerType == "etcd" {
				t.Skip("TestLockTimeUpdates is not supported for etcd locker")
			}
			fn(t, locker, clock)
		})
	}
}

func LockAndUnlock(t *testing.T, locker Locker, _ clockwork.FakeClock) {
	key := "/tasks/metadata/task_id=1"

	// Test Lock
	lock, err := locker.Lock(context.Background(), key, false)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Unlock with original lock
	err = locker.Unlock(context.Background(), lock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func LockAndUnlockWithGoRoutines(t *testing.T, locker Locker, _ clockwork.FakeClock) {
	key := "/tasks/metadata/task_id=2"
	lockChannel := make(chan Key)
	errChan := make(chan error)

	// Test Lock
	go lockGoRoutine(locker, key, false, lockChannel, errChan)
	lock := <-lockChannel
	err := <-errChan
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Unlock with original lock
	go unlockGoRoutine(locker, lock, errChan)
	err = <-errChan
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func StressTestLockAndUnlock(t *testing.T, locker Locker, clock clockwork.FakeClock) {
	key := "/tasks/metadata/task_id=6"

	var wg sync.WaitGroup
	// Use a counter to track the number of errors
	errorCount := 0

	// In 1000 threads, lock and unlock the same key
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(t *testing.T, id int) {
			defer wg.Done()
			// So only one thread will be able to lock and unlock the key
			// if multiple threads are able to lock the key, it means
			// there is a race condition. And we are able to detect it because
			// we will fail to unlock the key
			lock, err := locker.Lock(context.Background(), key, false)
			if err != nil {
				return
			}

			clock.Advance(10 * time.Millisecond)

			err = locker.Unlock(context.Background(), lock)
			if err != nil {
				errorCount++
				return
			}
		}(t, i)
	}
	wg.Wait()

	if errorCount > 0 {
		t.Fatalf("race condition detected! %d threads failed to unlock the key", errorCount)
	}
}

func lockGoRoutine(locker Locker, key string, wait bool, lockChannel chan<- Key, errChan chan<- error) {
	lockObject, err := locker.Lock(context.Background(), key, wait)
	lockChannel <- lockObject
	errChan <- err
}

func unlockGoRoutine(locker Locker, lock Key, errChan chan<- error) {
	err := locker.Unlock(context.Background(), lock)
	errChan <- err
}

func LockTimeUpdates(t *testing.T, locker Locker, clock clockwork.FakeClock) {
	key := "/tasks/metadata/task_id=3"
	lock, err := locker.Lock(context.Background(), key, false)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Sleeping and advancing to allow the time extension thread to do work
	clock.Advance(validTimePeriod.Duration() / 2)
	time.Sleep(100 * time.Millisecond)
	clock.Advance(validTimePeriod.Duration() / 2)
	time.Sleep(100 * time.Millisecond)
	clock.Advance(validTimePeriod.Duration() / 2)

	// Lock the key again
	_, err = locker.Lock(context.Background(), key, false)
	if err == nil {
		t.Fatalf("Locking the key should have failed because it is already locked")
	}

	// Release the lock
	err = locker.Unlock(context.Background(), lock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestLockInformation(t *testing.T) {
	type testCase struct {
		name            string
		lockInformation LockInformation
		expectedError   error
	}

	tests := []testCase{
		{
			name: "Valid",
			lockInformation: LockInformation{
				ID:   "id",
				Key:  "key",
				Date: time.Now().UTC(),
			},
			expectedError: nil,
		},
		{
			name: "Missing ID",
			lockInformation: LockInformation{
				Key:  "key",
				Date: time.Now().UTC(),
			},
			expectedError: fferr.NewInvalidArgumentError(fmt.Errorf("lock information is missing ID")),
		},
		{
			name: "Missing Key",
			lockInformation: LockInformation{
				ID:   "id",
				Date: time.Now().UTC(),
			},
			expectedError: fferr.NewInvalidArgumentError(fmt.Errorf("lock information is missing Key")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := test.lockInformation.Marshal()
			if err != nil {
				t.Fatalf("Marshal() failed: %v", err)
			}

			var lockInformation LockInformation
			err = lockInformation.Unmarshal(data)
			if err != nil && err.Error() != test.expectedError.Error() {
				t.Fatalf("Unmarshal() failed: %v", err)
			}
		})
	}
}

func WaitForLock(t *testing.T, locker Locker, clock clockwork.FakeClock) {
	key := "/tasks/metadata/task_id=3"
	lockChannel := make(chan Key, 10)
	errChan := make(chan error, 20)

	done := make(chan struct{})

	// Needed to allow the threads to progress and not get blocked on any Sleeps within the locking routine
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				clock.Advance(5 * time.Second)
			}
		}
	}()

	for i := 0; i < 10; i++ {
		go lockGoRoutine(locker, key, true, lockChannel, errChan)
	}
	for i := 0; i < 10; i++ {
		go unlockGoRoutine(locker, <-lockChannel, errChan)
	}
	done <- struct{}{}
	var firstErr error
	for i := 0; i < 20; i++ {
		// Wait for all unlocks and locks to happen
		if err := <-errChan; err != nil {
			t.Logf("Error in lock/unlock: %s", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		t.Fatalf("Lock failed: %v", firstErr)
	}
}
