package kv

import (
	"reflect"
	"testing"
	"time"
)

func TestSetAndGet(t *testing.T) {
	key := "test_key"
	value := "test_value"

	if err := Set(key, value); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	gotValue, exists, err := Get[string, string](key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !exists {
		t.Fatalf("Get() key not found")
	}
	if !reflect.DeepEqual(gotValue, value) {
		t.Errorf("Get() gotValue = %v, want %v", gotValue, value)
	}
}

func TestSetWithTTL(t *testing.T) {
	key := "test_key_ttl"
	value := "test_value_ttl"
	ttl := int64(1000) // 1 second

	if err := Set(key, value, ttl); err != nil {
		t.Fatalf("Set() with ttl error = %v", err)
	}

	// Check if the key exists immediately
	exists, err := Exists(key)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Fatalf("Exists() key should exist immediately after setting with TTL")
	}

	// Wait for the TTL to expire
	time.Sleep(time.Duration(ttl) * time.Millisecond)

	// Check if the key has expired
	exists, err = Exists(key)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Fatalf("Exists() key should have expired")
	}
}

func TestDelete(t *testing.T) {
	key := "test_key_delete"
	value := "test_value_delete"

	if err := Set(key, value); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	if err := Del(key); err != nil {
		t.Fatalf("Del() error = %v", err)
	}

	exists, err := Exists(key)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Fatalf("Del() key should be deleted")
	}
}

func TestNilValue(t *testing.T) {
	key := "test_key_nil"
	var value *string

	if err := Set(key, value); err != nil {
		t.Fatalf("Set() with nil value error = %v", err)
	}

	gotValue, exists, err := Get[string, *string](key)
	if err != nil {
		t.Fatalf("Get() with nil value error = %v", err)
	}
	if !exists {
		t.Fatalf("Get() with nil value key not found")
	}
	if gotValue != nil {
		t.Errorf("Get() with nil value gotValue = %v, want nil", gotValue)
	}
}
