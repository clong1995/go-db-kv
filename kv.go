package kv

import (
	"github.com/dgraph-io/badger/v4"
	"log"
)

func Del(key []byte) (err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		if err = txn.Delete(key); err != nil {
			log.Println(err)
			return
		}
		return
	}); err != nil {
		log.Println(err)
		return
	}
	return
}

func Close() {
	err := db.Close()
	if err != nil {
		log.Println(err)
	}
}

func Drop() (err error) {
	if err = db.DropAll(); err != nil {
		log.Println(err)
	}
	return
}
