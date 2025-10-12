package kv

import (
	"errors"
	"fmt"
	"log"

	"golang.org/x/sync/singleflight"
)

/*
并发安全的存储方法,不会重复写入
用于取值成本高,取值不幂等 的函数缓存
*/

var sf singleflight.Group

// Storage 存在返回不存在存储
func Storage[K, V any](key K, fn func() (value V, err error), ttl ...int64) (value V, err error) {
	value, exists, err := Get[K, V](key, ttl...)
	if err != nil {
		log.Println(err)
		return
	}

	if exists {
		return
	}

	// 使用singleflight执行昂贵操作
	result, err, _ := sf.Do(fmt.Sprintf("%#v", key), func() (value any, err error) {
		if value, exists, err = Get[K, V](key, ttl...); err != nil {
			log.Println(err)
			return
		}

		if exists {
			return
		}

		// 执行耗时操作
		if value, err = fn(); err != nil {
			log.Println(err)
			return
		}

		//存储
		if err = Set[K, V](key, value, ttl...); err != nil {
			log.Println(err)
			return
		}
		return
	})
	if err != nil {
		log.Println(err)
		return
	}
	if result == nil {
		err = errors.New("unexpected nil result")
		log.Println(err)
		return
	}

	value, ok := result.(V)
	if !ok {
		err = errors.New("type assertion failed")
		log.Println(err)
		return
	}
	return

}
