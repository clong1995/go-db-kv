package kv

import (
	"github.com/dgraph-io/badger/v4"
	"log"
	"sync"
	"time"
)

/*
并发安全的存储方法,不会重复写入
用于取值成本高,取值不幂等 的函数缓存
*/

// 保存正在进行的读写操作的锁，每一个key一把锁，用完销毁
var storageMu sync.RWMutex
var keyMutexes = make(map[string]*sync.Mutex)

// 获取key对应的锁
func getKeyMutex(key string) (mu *sync.Mutex) {
	storageMu.RLock()
	mu, ok := keyMutexes[key]
	storageMu.RUnlock()

	if ok {
		return
	}

	storageMu.Lock()
	defer storageMu.Unlock()

	// 再次检查防止竞态
	if mu, ok = keyMutexes[key]; ok {
		return
	}

	// 创建新锁
	mu = &sync.Mutex{}
	keyMutexes[key] = mu
	return
}

func delKeyMutex(key string) {
	storageMu.Lock()
	defer storageMu.Unlock()
	delete(keyMutexes, key)
}

// Storage 存在返回不存在存储
func Storage[T any](key []byte, fn func() (value T, err error)) (value T, err error) {
	if value, err = StorageTtl[T](key, fn, 0); err != nil {
		log.Println(err)
		return
	}
	return
}

// StorageTtl 存在返回不存在存储，并自动续期
func StorageTtl[T any](key []byte, fn func() (value T, err error), millisecond int) (value T, err error) {
	if err = db.Update(func(txn *badger.Txn) (err error) {
		var exists bool
		var bytes []byte
		if bytes, exists, err = get(key, txn); err != nil {
			log.Println(err)
			return
		}

		if exists { // 存在
			if value, err = deserialize[T](bytes); err != nil {
				log.Println(err)
				return
			}
		} else { // 不存在

			// 获取锁
			muKey := string(key)
			mu := getKeyMutex(muKey)
			mu.Lock()
			defer func() {
				mu.Unlock()
				delKeyMutex(muKey)
			}()

			// 再次检查防止竞态
			if bytes, exists, err = get(key, txn); err != nil {
				log.Println(err)
				return
			}
			if exists {
				if value, err = deserialize[T](bytes); err != nil {
					log.Println(err)
					return
				}
				goto ttl
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
		}

	ttl:
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
		} else { //不存在
			entry := badger.NewEntry(key, bytes)
			if millisecond > 0 {
				entry.WithTTL(time.Duration(millisecond) * time.Millisecond)
			}
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

// StorageTtlDiscord 存在返回不存在存储，第一次存储有到期时间
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
		// 不存在

		// 获取锁
		muKey := string(key)
		mu := getKeyMutex(muKey)
		mu.Lock()
		defer func() {
			mu.Unlock()
			delKeyMutex(muKey)
		}()

		// 再次检查防止竞态
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

		// 再次不存在
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
