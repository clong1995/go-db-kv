package kv

import (
	"log"

	"github.com/clong1995/go-config"
	"github.com/dgraph-io/badger/v4"
)

var db *badger.DB

func init() {
	cachePath := config.Value("CACHE PATH")
	var err error
	opt := badger.DefaultOptions(cachePath).WithInMemory(cachePath == "")
	if db, err = badger.Open(opt); err != nil {
		log.Panicln(err)
		return
	}
}
