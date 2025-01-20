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
	if err = SetTtl[T](key, value, 0); err != nil {
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
		entry := badger.NewEntry([]byte(key), bytes)
		if second > 0 {
			entry.WithTTL(time.Duration(second) * time.Second)
		}
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

func SetKey(key string) (err error) {
	if err = SetKeyTtl(key, 0); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetKeyTtl(key string, second int) (err error) {
	bytes := make([]byte, 0)
	if err = db.Update(func(txn *badger.Txn) (err error) {
		entry := badger.NewEntry([]byte(key), bytes)
		if second > 0 {
			entry.WithTTL(time.Duration(second) * time.Second)
		}
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

func Exists(key string) (exists bool, err error) {
	if err = db.View(func(txn *badger.Txn) (err error) {
		_, err = txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			err = nil
			return
		}
		if err != nil {
			log.Println(err)
			return
		}
		exists = true
		return
	}); err != nil {
		log.Println(err)
		return
	}
	return
}

func ExistsTtl(key string, second int) (exists bool, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		k := []byte(key)
		if _, err = txn.Get(k); errors.Is(err, badger.ErrKeyNotFound) {
			err = nil
			return
		}
		if err != nil {
			log.Println(err)
			return
		}
		bytes := make([]byte, 0)
		entry := badger.NewEntry(k, bytes).WithTTL(time.Duration(second) * time.Second)
		if err = txn.SetEntry(entry); err != nil {
			log.Println(err)
			return
		}
		exists = true
		return
	}); err != nil {
		log.Println(err)
		return
	}
	return
}

func Get[T any](key string) (value T, exists bool, err error) {
	if value, exists, err = GetTtl[T](key, 0); err != nil {
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

		if second > 0 {
			entry := badger.NewEntry(k, bytes).WithTTL(time.Duration(second) * time.Second)
			if err = txn.SetEntry(entry); err != nil {
				log.Println(err)
				return
			}
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
	if value, err = StorageTtl[T](key, fn, 0); err != nil {
		log.Println(err)
		return
	}
	return
}

func StorageTtl[T any](key string, fn func() (value T, err error), second int) (value T, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var exists bool
		var bytes []byte
		k := HashKey(key)
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

		if second > 0 {
			entry := badger.NewEntry(k, bytes).WithTTL(time.Duration(second) * time.Second)
			if err = txn.SetEntry(entry); err != nil {
				log.Println(err)
				return
			}
		}
		return
	}); err != nil {
		log.Println(err)
		return
	}
	return
}

func StorageTtlDiscord[T any](key string, fn func() (value T, err error), second int) (value T, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var exists bool
		var bytes []byte
		k := HashKey(key)
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

func HashKey(text string) (buf []byte) {
	n := xxhash.Sum64String(text)
	buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return
}
