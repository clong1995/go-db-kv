package kv

import (
	"bytes"
	"encoding/gob"
	"log"
)

func serialize[T any](data T) (b []byte, err error) {
	b, ok := any(data).([]byte)
	if ok {
		return
	}

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
	if _, ok := any(result).([]byte); ok {
		return any(data).(T), nil
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err = dec.Decode(&result); err != nil {
		log.Println(err)
		return
	}
	return
}
