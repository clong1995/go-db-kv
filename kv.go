package kv

import (
	"github.com/clong1995/go-config"
	"github.com/dgraph-io/badger/v4"
	"log"
	"time"
)

var db *badger.DB

func init() {
	cachePath := config.Value("CACHE PATH")
	var err error
	if db, err = badger.Open(badger.DefaultOptions(cachePath).WithInMemory(cachePath == "")); err != nil {
		log.Panicln(err)
		return
	}
}

func Get(key []byte) (value []byte, err error) {
	if err = db.View(func(txn *badger.Txn) (err error) {
		if value, err = get(key, txn); err != nil {
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

func GetTtl(key []byte, ttl int) (value []byte, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		if value, err = get(key, txn); err != nil {
			log.Println(err)
			return
		}
		entry := badger.NewEntry(key, value).WithTTL(time.Duration(ttl) * time.Second)
		if err = txn.SetEntry(entry); err != nil {
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

func get(key []byte, txn *badger.Txn) (value []byte, err error) {
	item, err := txn.Get(key)
	if err != nil {
		log.Println(err)
		return
	}
	if value, err = item.ValueCopy(nil); err != nil {
		log.Println(err)
		return
	}
	return
}

func Set(key, value []byte) (err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		entry := badger.NewEntry(key, value)
		if err = txn.SetEntry(entry); err != nil {
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

func SetTtl(key, value []byte, ttl int) (err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		entry := badger.NewEntry(key, value).WithTTL(time.Duration(ttl) * time.Second)
		if err = txn.SetEntry(entry); err != nil {
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
