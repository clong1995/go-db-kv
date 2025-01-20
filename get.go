package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"log"
	"time"
)

func Get(key []byte) (value []byte, exist bool, err error) {
	if err = db.View(func(txn *badger.Txn) (err error) {
		if value, exist, err = get(key, txn); err != nil {
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

func GetInt(key string) (value int, exist bool, err error) {
	i, exist, err := GetInt64(key)
	if err != nil {
		log.Println(err)
		return
	}
	value = int(i)
	return
}

func GetInt64(key string) (value int64, exist bool, err error) {
	d, exist, err := Get([]byte(key))
	if err != nil {
		log.Println(err)
		return
	}
	buf := bytes.NewBuffer(d)
	if err = binary.Read(buf, binary.BigEndian, &value); err != nil {
		log.Println(err)
		return
	}
	return
}

func GetString(key string) (value string, exist bool, err error) {
	d, exist, err := Get([]byte(key))
	if err != nil {
		log.Println(err)
		return
	}
	value = string(d)
	return
}

func GetTtl(key []byte, ttl int) (value []byte, exist bool, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		if value, exist, err = get(key, txn); err != nil {
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

func GetIntTtl(key string, ttl int) (value int, exist bool, err error) {
	d, exist, err := GetInt64Ttl(key, ttl)
	if err != nil {
		log.Println(err)
		return
	}
	value = int(d)
	return
}

func GetInt64Ttl(key string, ttl int) (value int64, exist bool, err error) {
	d, exist, err := GetTtl([]byte(key), ttl)
	if err != nil {
		log.Println(err)
		return
	}
	buf := bytes.NewBuffer(d)
	if err = binary.Read(buf, binary.BigEndian, &value); err != nil {
		log.Println(err)
		return
	}
	return
}

func GetStringTtl(key string, ttl int) (value string, exist bool, err error) {
	d, exist, err := GetTtl([]byte(key), ttl)
	if err != nil {
		log.Println(err)
		return
	}
	value = string(d)
	return
}

func get(key []byte, txn *badger.Txn) (value []byte, exist bool, err error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = nil
		exist = false
		return
	}
	if err != nil {
		log.Println(err)
		return
	}
	exist = true
	if value, err = item.ValueCopy(nil); err != nil {
		log.Println(err)
		return
	}
	return
}
