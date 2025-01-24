package kv

import (
	"testing"
)

func TestClose(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "close",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Close()
		})
	}
}

func TestDel(t *testing.T) {
	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "delete",
			args: args{
				key: []byte("key"),
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

func TestDrop(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "drop",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Drop(); (err != nil) != tt.wantErr {
				t.Errorf("Drop() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGet(t *testing.T) {
	type args struct {
		key []byte
	}
	tests := []struct {
		name      string
		args      args
		wantValue []byte
		wantErr   bool
	}{
		{
			name: "exists",
			args: args{
				key: []byte("key"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, err := Exists(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Logf("Get() gotValue = %v", gotValue)
		})
	}
}

func TestSet(t *testing.T) {
	type args struct {
		key   []byte
		value []byte
		ttl   int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "set",
			args: args{
				key:   []byte("key"),
				value: []byte("value"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Set(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
