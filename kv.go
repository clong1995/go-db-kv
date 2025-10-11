package kv

import (
	"encoding/binary"
	"errors"
	"log"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
)

// Set 设置值
func Set[K, V any](key K, value V, ttl ...int64) (err error) {
	k, err := serialize[K](key)
	if err != nil {
		log.Println(err)
		return
	}
	var v []byte
	if value != nil {
		if v, err = serialize[V](value); err != nil {
			log.Println(err)
			return
		}
	}

	if err = db.Update(func(txn *badger.Txn) (err error) {
		entry := badger.NewEntry(k, v)
		if ttl != nil && len(ttl) > 0 {
			entry.WithTTL(time.Duration(ttl[0]) * time.Second)

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

// Get 获取值
func Get[K, V any](key K) (value V, exists bool, err error) {
	k, err := serialize[K](key)
	if err != nil {
		log.Println(err)
		return
	}

	exists = true

	if err = db.View(func(txn *badger.Txn) (err error) {
		var item *badger.Item
		if item, err = txn.Get(k); err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				err = nil
				exists = false
				return
			}
			log.Println(err)
			return
		}

		if err = item.Value(func(val []byte) (err error) {
			if value, err = deserialize[V](val); err != nil {
				log.Println(err)
				return
			}
			return
		}); err != nil {
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

// Del 删除
func Del[K any](key K) (err error) {

	k, err := serialize[K](key)
	if err != nil {
		log.Println(err)
		return
	}

	if err = db.Update(func(txn *badger.Txn) (err error) {
		if err = txn.Delete(k); err != nil {
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

// Exists 是否存在
func Exists[K any](key K) (exists bool, err error) {
	k, err := serialize[K](key)
	if err != nil {
		log.Println(err)
		return
	}

	exists = true
	if err = db.View(func(txn *badger.Txn) (err error) {
		if _, err = txn.Get(k); err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				err = nil
				exists = false
				return
			}
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

// Drop 清空
func Drop() (err error) {
	if err = db.DropAll(); err != nil {
		log.Println(err)
		return
	}
	return
}

// Close 关闭
func Close() (err error) {
	if err = db.Close(); err != nil {
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

func IntKey(n int64) (buf []byte) {
	buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n))
	return
}
