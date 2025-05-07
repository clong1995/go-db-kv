package kv

import (
	"encoding/binary"
	"errors"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
	"log"
	"time"
)

func Set[T any](key []byte, value T) (err error) {
	if err = SetTtl[T](key, value, 0); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetTtl[T any](key []byte, value T, millisecond int) (err error) {
	bytes, err := serialize[T](value)
	if err != nil {
		log.Println(err)
		return
	}
	if err = db.Update(func(txn *badger.Txn) (err error) {
		entry := badger.NewEntry(key, bytes)
		if millisecond > 0 {
			entry.WithTTL(time.Duration(millisecond) * time.Millisecond)
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

func SetKey(key []byte) (err error) {
	if err = SetKeyTtl(key, 0); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetKeyTtl(key []byte, millisecond int) (err error) {
	//bytes := make([]byte, 0)
	if err = db.Update(func(txn *badger.Txn) (err error) {
		//entry := badger.NewEntry(key, bytes)
		entry := badger.NewEntry(key, nil)
		if millisecond > 0 {
			entry.WithTTL(time.Duration(millisecond) * time.Millisecond)
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

func Exists(key []byte) (exists bool, err error) {
	if exists, err = ExistsTtl(key, 0); err != nil {
		log.Println(err)
		return
	}
	return
}

// ExistsTtl 检查是否存在并续期
func ExistsTtl(key []byte, millisecond int) (exists bool, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		if _, err = txn.Get(key); errors.Is(err, badger.ErrKeyNotFound) {
			err = nil
			return
		}
		if err != nil {
			log.Println(err)
			return
		}
		//bytes := make([]byte, 0)
		//entry := badger.NewEntry(key, bytes)
		if millisecond > 0 {
			entry := badger.NewEntry(key, nil)
			entry.WithTTL(time.Duration(millisecond) * time.Millisecond)
			if err = txn.SetEntry(entry); err != nil {
				log.Println(err)
				return
			}
		}

		exists = true
		return
	}); err != nil {
		log.Println(err)
		return
	}
	return
}

// ExistsKeySet 检查key是否存在，不存在则并设置
func ExistsKeySet(key []byte) (exists bool, err error) {
	if exists, err = ExistsKeySetTtl(key, 0); err != nil {
		log.Println(err)
		return
	}
	return
}

// ExistsKeySetTtl 检查key是否存在，不存则设置，并续期
func ExistsKeySetTtl(key []byte, millisecond int) (exists bool, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var keyNotFound bool
		if _, err = txn.Get(key); errors.Is(err, badger.ErrKeyNotFound) {
			keyNotFound = true
			err = nil
		}
		if err != nil {
			log.Println(err)
			return
		}

		entry := badger.NewEntry(key, nil)
		if millisecond > 0 {
			entry.WithTTL(time.Duration(millisecond) * time.Millisecond)
		}
		if err = txn.SetEntry(entry); err != nil {
			log.Println(err)
			return
		}

		if !keyNotFound { //存在
			exists = true
		}

		return
	}); err != nil {
		log.Println(err)
		return
	}
	return
}

func Get[T any](key []byte) (value T, exists bool, err error) {
	if value, exists, err = GetTtl[T](key, 0); err != nil {
		log.Println(err)
		return
	}
	return
}

func GetTtl[T any](key []byte, millisecond int) (value T, exists bool, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var bytes []byte
		if bytes, exists, err = get(key, txn); err != nil {
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

		if millisecond > 0 {
			entry := badger.NewEntry(key, nil)
			entry.WithTTL(time.Duration(millisecond) * time.Millisecond)
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

func Del(key []byte) (err error) {
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

func Storage[T any](key []byte, fn func() (value T, err error)) (value T, err error) {
	if value, err = StorageTtl[T](key, fn, 0); err != nil {
		log.Println(err)
		return
	}
	return
}

func StorageTtl[T any](key []byte, fn func() (value T, err error), millisecond int) (value T, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var exists bool
		var bytes []byte
		if bytes, exists, err = get(key, txn); err != nil {
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

		if exists { //存在
			if millisecond > 0 { //且有生存时间
				//则续期
				entry := badger.NewEntry(key, nil)
				entry.WithTTL(time.Duration(millisecond) * time.Millisecond)
				if err = txn.SetEntry(entry); err != nil {
					log.Println(err)
					return
				}
			}
		} else {
			entry := badger.NewEntry(key, bytes)
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

func StorageTtlDiscord[T any](key []byte, fn func() (value T, err error), millisecond int) (value T, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var exists bool
		var bytes []byte
		if bytes, exists, err = get(key, txn); err != nil {
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

		entry := badger.NewEntry(key, bytes).WithTTL(time.Duration(millisecond) * time.Millisecond)
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

func IntKey(n int64) (buf []byte) {
	buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n))
	return
}
