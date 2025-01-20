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

func SetInt(key string, value int) (err error) {
	d := int64(value)
	if err = SetInt64(key, d); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetInt64(key string, value int64) (err error) {
	var buf bytes.Buffer
	if err = binary.Write(&buf, binary.BigEndian, value); err != nil {
		log.Println(err)
		return
	}
	if err = Set([]byte(key), buf.Bytes()); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetString(key string, value string) (err error) {
	if err = Set([]byte(key), []byte(value)); err != nil {
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

func SetIntTtl(key string, value int, ttl int) (err error) {
	d := int64(value)
	if err = SetInt64Ttl(key, d, ttl); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetInt64Ttl(key string, value int64, ttl int) (err error) {
	var buf bytes.Buffer
	if err = binary.Write(&buf, binary.BigEndian, value); err != nil {
		log.Println(err)
		return
	}
	if err = SetTtl([]byte(key), buf.Bytes(), ttl); err != nil {
		log.Println(err)
		return
	}
	return
}

func SetStringTtl(key, value string, ttl int) (err error) {
	if err = SetTtl([]byte(key), []byte(value), ttl); err != nil {
		log.Println(err)
		return
	}
	return
}
