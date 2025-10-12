package kv

import (
	"testing"
)

func TestStorage(t *testing.T) {
	type args[K any, V any] struct {
		key K
		fn  func() (value V, err error)
		ttl []int64
	}
	type testCase[K any, V any] struct {
		name      string
		args      args[K, V]
		wantValue V
		wantErr   bool
	}
	tests := []testCase[int64, string]{
		{
			name: "storage",
			args: args[int64, string]{
				key: 123,
				fn: func() (value string, err error) {
					t.Log("耗时方法")
					value = "abc"
					return
				},
				ttl: []int64{30000},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, err := Storage(tt.args.key, tt.args.fn, tt.args.ttl...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Storage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Logf("Storage() gotValue = %v", gotValue)
		})
	}
}
