package kv

import (
	"encoding/binary"
	"errors"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"log"
	"time"
)

func Set[T any](key string, value T) (err error) {
	bytes, err := serialize[T](value)
	if err != nil {
		log.Println(err)
		return
	}
	if err = db.Update(func(txn *badger.Txn) (err error) {
		if err = txn.Set([]byte(key), bytes); err != nil {
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

func SetTtl[T any](key string, value T, second int) (err error) {
	bytes, err := serialize[T](value)
	if err != nil {
		log.Println(err)
		return
	}
	if err = db.Update(func(txn *badger.Txn) (err error) {
		entry := badger.NewEntry([]byte(key), bytes).WithTTL(time.Duration(second) * time.Second)
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

func Get[T any](key string) (value T, exists bool, err error) {
	if err = db.View(func(txn *badger.Txn) (err error) {
		var bytes []byte
		if bytes, exists, err = get([]byte(key), txn); err != nil {
			log.Println(err)
			return
		}
		if !exists {
			return
		}
		if value, err = deserialize[T](bytes); err != nil {
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

func GetTtl[T any](key string, second int) (value T, exists bool, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var bytes []byte
		k := []byte(key)
		if bytes, exists, err = get(k, txn); err != nil {
			log.Println(err)
			return
		}
		if !exists {
			return
		}

		if value, err = deserialize[T](bytes); err != nil {
			log.Println(err)
			return
		}

		entry := badger.NewEntry(k, bytes).WithTTL(time.Duration(second) * time.Second)
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

func get(key []byte, txn *badger.Txn) (value []byte, exists bool, err error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = nil
		return
	}
	if err != nil {
		log.Println(err)
		return
	}
	exists = true
	if value, err = item.ValueCopy(nil); err != nil {
		log.Println(err)
		return
	}
	return
}

func Del(key string) (err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		if err = txn.Delete([]byte(key)); err != nil {
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

func Storage[T any](key string, fn func() (value T, err error)) (value T, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var exists bool
		var bytes []byte
		k := Key(key)
		if bytes, exists, err = get(k, txn); err != nil {
			log.Println(err)
			return
		}

		if exists {
			if value, err = deserialize[T](bytes); err != nil {
				log.Println(err)
				return
			}
			return
		}

		if value, err = fn(); err != nil {
			log.Println(err)
			return
		}

		if bytes, err = serialize[T](value); err != nil {
			log.Println(err)
			return
		}

		if err = txn.Set(k, bytes); err != nil {
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

func StorageTtl[T any](key string, second int, fn func() (value T, err error)) (value T, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var exists bool
		var bytes []byte
		k := Key(key)
		if bytes, exists, err = get(k, txn); err != nil {
			log.Println(err)
			return
		}

		if exists {
			if value, err = deserialize[T](bytes); err != nil {
				log.Println(err)
				return
			}
		} else {
			if value, err = fn(); err != nil {
				log.Println(err)
				return
			}

			if bytes, err = serialize[T](value); err != nil {
				log.Println(err)
				return
			}
		}

		entry := badger.NewEntry(k, bytes).WithTTL(time.Duration(second) * time.Second)
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

func StorageTtlDiscord[T any](key string, second int, fn func() (value T, err error)) (value T, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var exists bool
		var bytes []byte
		k := Key(key)
		if bytes, exists, err = get(k, txn); err != nil {
			log.Println(err)
			return
		}

		if exists {
			if value, err = deserialize[T](bytes); err != nil {
				log.Println(err)
				return
			}
			return
		}

		if value, err = fn(); err != nil {
			log.Println(err)
			return
		}

		if bytes, err = serialize[T](value); err != nil {
			log.Println(err)
			return
		}

		entry := badger.NewEntry(k, bytes).WithTTL(time.Duration(second) * time.Second)
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

func Key(text string) (buf []byte) {
	n := xxhash.Sum64String(text)
	buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return
}
