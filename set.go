package kv

import (
	"bytes"
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"log"
	"time"
)

func Set(key, value []byte) (err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		if err = txn.Set(key, value); err != nil {
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

func SetInt(key []byte, value int) (err error) {
	d := int64(value)
	if err = SetInt64(key, d); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetInt64(key []byte, value int64) (err error) {
	var buf bytes.Buffer
	if err = binary.Write(&buf, binary.BigEndian, value); err != nil {
		log.Println(err)
		return
	}
	if err = Set(key, buf.Bytes()); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetString(key []byte, value string) (err error) {
	err = Set(key, []byte(value))
	if err != nil {
		return err
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

func SetIntTtl(key int, value []byte, ttl int) (err error) {
	d := int64(key)
	if err = SetInt64Ttl(d, value, ttl); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetInt64Ttl(key int64, value []byte, ttl int) (err error) {
	var buf bytes.Buffer
	if err = binary.Write(&buf, binary.BigEndian, key); err != nil {
		log.Println(err)
		return
	}
	if err = SetTtl(buf.Bytes(), value, ttl); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetStringTtl(key string, value []byte, ttl int) (err error) {
	if err = SetTtl([]byte(key), value, ttl); err != nil {
		log.Println(err)
		return
	}
	return
}
