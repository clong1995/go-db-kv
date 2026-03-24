package kv

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

func TestStorage(t *testing.T) {
	key := "storage_key"
	value := "storage_value"
	var callCount int32

	fn := func() (string, error) {
		atomic.AddInt32(&callCount, 1)
		return value, nil
	}

	// First call, should execute fn
	gotValue, err := Storage(key, fn)
	if err != nil {
		t.Fatalf("Storage() error = %v", err)
	}
	if !reflect.DeepEqual(gotValue, value) {
		t.Errorf("Storage() gotValue = %v, want %v", gotValue, value)
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("fn should be called once, but was called %d times", callCount)
	}

	// Second call, should not execute fn
	gotValue, err = Storage(key, fn)
	if err != nil {
		t.Fatalf("Storage() error = %v", err)
	}
	if !reflect.DeepEqual(gotValue, value) {
		t.Errorf("Storage() gotValue = %v, want %v", gotValue, value)
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("fn should not be called again, but was called %d times", callCount)
	}
}

func TestStorage_SingleFlight(t *testing.T) {
	key := "storage_key_sf"
	value := "storage_value_sf"
	var callCount int32

	fn := func() (string, error) {
		atomic.AddInt32(&callCount, 1)
		return value, nil
	}

	// Use a WaitGroup to run multiple goroutines concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gotValue, err := Storage(key, fn)
			if err != nil {
				t.Errorf("Storage() error = %v", err)
				return
			}
			if !reflect.DeepEqual(gotValue, value) {
				t.Errorf("Storage() gotValue = %v, want %v", gotValue, value)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("fn should be called only once due to singleflight, but was called %d times", callCount)
	}
}
