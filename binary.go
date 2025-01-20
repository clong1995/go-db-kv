package kv

import (
	"bytes"
	"encoding/gob"
	"log"
)

func serialize[T any](data T) (b []byte, err error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err = enc.Encode(data); err != nil {
		log.Println(err)
		return
	}
	b = buf.Bytes()
	return
}

func deserialize[T any](data []byte) (result T, err error) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err = dec.Decode(&result); err != nil {
		log.Println(err)
		return
	}
	return
}
