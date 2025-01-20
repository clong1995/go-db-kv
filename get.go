package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"log"
	"time"
)

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

func GetInt(key []byte) (value int, err error) {
	i, err := GetInt64(key)
	if err != nil {
		log.Println(err)
		return
	}
	value = int(i)
	return
}

func GetInt64(key []byte) (value int64, err error) {
	d, err := Get(key)
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

func GetString(key []byte) (value string, err error) {
	d, err := Get(key)
	if err != nil {
		log.Println(err)
		return
	}
	value = string(d)
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

func GetIntTtl(key []byte, ttl int) (value int, err error) {
	d, err := GetInt64Ttl(key, ttl)
	if err != nil {
		log.Println(err)
		return
	}
	value = int(d)
	return
}

func GetInt64Ttl(key []byte, ttl int) (value int64, err error) {
	d, err := GetTtl(key, ttl)
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

func GetStringTtl(key []byte, ttl int) (value string, err error) {
	d, err := GetTtl(key, ttl)
	if err != nil {
		log.Println(err)
		return
	}
	value = string(d)
	return
}

func get(key []byte, txn *badger.Txn) (value []byte, err error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = nil
		return
	}
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
