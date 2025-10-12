package kv

import (
	"testing"
)

func TestSet(t *testing.T) {
	type args[K any, V any] struct {
		key   K
		value V
		ttl   []int64
	}
	type testCase[K any, V any] struct {
		name    string
		args    args[K, V]
		wantErr bool
	}
	tests := []testCase[int64, string]{
		{
			name: "set",
			args: args[int64, string]{
				key:   123,
				value: "abc",
				ttl:   []int64{30000},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Set(tt.args.key, tt.args.value, tt.args.ttl...); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGet(t *testing.T) {
	type args[K any] struct {
		key K
		ttl []int64
	}
	type testCase[K any, V any] struct {
		name       string
		args       args[K]
		wantValue  V
		wantExists bool
		wantErr    bool
	}
	tests := []testCase[int64, string]{
		{
			name: "get",
			args: args[int64]{
				key: 123,
				//ttl: []int64{30000},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotExists, err := Get[int64, string](tt.args.key, tt.args.ttl...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Logf("Get() gotValue = %v", gotValue)
			t.Logf("Get() gotExists = %v", gotExists)

		})
	}
}

func TestDel(t *testing.T) {
	type args[K any] struct {
		key K
	}
	type testCase[K any] struct {
		name    string
		args    args[K]
		wantErr bool
	}
	tests := []testCase[int64]{
		{
			name: "del",
			args: args[int64]{
				key: 123,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Del(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("Del() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExists(t *testing.T) {
	type args[K any] struct {
		key K
		ttl []int64
	}
	type testCase[K any] struct {
		name       string
		args       args[K]
		wantExists bool
		wantErr    bool
	}
	tests := []testCase[int64]{
		{
			name: "exists",
			args: args[int64]{
				key: 123,
				//ttl: []int64{30000},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotExists, err := Exists(tt.args.key, tt.args.ttl...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Exists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Logf("Exists() gotExists = %v", gotExists)
		})
	}
}
