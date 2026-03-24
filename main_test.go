package kv

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if err := Init(); err != nil {
		panic(err)
	}
	code := m.Run()
	if err := db.Close(); err != nil {
		panic(err)
	}
	os.Exit(code)
}
