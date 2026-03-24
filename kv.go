package kv

import (
	"encoding/binary"
	"time"

	"github.com/pkg/errors"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
)

// Set 将键值对存入数据库，可选择性地设置生存时间 (TTL)。
// K, V 是泛型参数，代表任意类型的键和值。
// key: 键。
// value: 值。
// ttl: 可选参数，生命周期，单位毫秒。
func Set[K, V any](key K, value V, ttl ...int64) error {
	// 序列化键。
	k, err := serialize[K](key)
	if err != nil {
		return err
	}
	// 序列化值。
	var v []byte
	if any(value) != nil {
		if v, err = serialize[V](value); err != nil {
			return err
		}
	}

	// 执行数据库更新操作。
	if err = db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(k, v)
		// 如果设置了 TTL，则为条目添加过期时间。
		if ttl != nil && len(ttl) > 0 {
			entry.WithTTL(time.Duration(ttl[0]) * time.Millisecond)
		}
		// 设置条目。
		if err = txn.SetEntry(entry); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Get 从数据库中获取一个值，并可选择性地更新其生存时间 (TTL)。
// K, V 是泛型参数，代表任意类型的键和值。
// key: 键。
// ttl: 可选参数，生命周期，单位毫秒。如果提供，将更新键的 TTL。
// 返回值:
// V: 获取到的值。
// bool: 表示键是否存在。
// error: 操作中发生的任何错误。
func Get[K, V any](key K, ttl ...int64) (V, bool, error) {
	var value V
	// 序列化键。
	k, err := serialize[K](key)
	if err != nil {
		return value, false, err
	}

	exists := true

	// 判断是否需要续期，如果提供了 ttl 参数，则需要读写事务。
	rw := ttl != nil && len(ttl) > 0

	// 定义获取值的核心逻辑。
	getFunc := func(txn *badger.Txn) error {
		var item *badger.Item
		// 尝试从事务中获取条目。
		if item, err = txn.Get(k); err != nil {
			// 如果键不存在，则标记为不存在并返回 nil 错误。
			if errors.Is(err, badger.ErrKeyNotFound) {
				exists = false
				return nil
			}
			return err
		}

		// 获取条目的值。
		if err = item.Value(func(val []byte) error {
			if val == nil {
				return nil
			}
			// 反序列化值。
			if value, err = deserialize[V](val); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	if rw {
		// 如果需要续期，则使用读写事务。
		if err = db.Update(func(txn *badger.Txn) error {
			// 先获取值。
			if err = getFunc(txn); err != nil {
				return err
			}
			// 如果键存在，则更新其 TTL。
			if exists {
				var item *badger.Item
				if item, err = txn.Get(k); err != nil {
					return err
				}
				if err = item.Value(func(val []byte) (err error) {
					entry := badger.NewEntry(k, val).WithTTL(time.Duration(ttl[0]) * time.Millisecond)
					if err = txn.SetEntry(entry); err != nil {
						return
					}
					return
				}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return value, exists, err
		}
	} else {
		// 如果不需要续期，则使用只读事务。
		if err = db.View(func(txn *badger.Txn) error {
			return getFunc(txn)
		}); err != nil {
			return value, exists, err
		}
	}

	return value, exists, err
}

// Del 从数据库中删除一个键。
// K 是泛型参数，代表任意类型的键。
// key: 要删除的键。
func Del[K any](key K) error {
	// 序列化键。
	k, err := serialize[K](key)
	if err != nil {
		return err
	}

	// 执行数据库更新操作以删除键。
	if err = db.Update(func(txn *badger.Txn) (err error) {
		if err = txn.Delete(k); err != nil {
			return err
		}
		return
	}); err != nil {
		return err
	}
	return nil
}

// Exists 检查数据库中是否存在一个键，并可选择性地更新其生存时间 (TTL)。
// K 是泛型参数，代表任意类型的键。
// key: 要检查的键。
// ttl: 可选参数，生命周期，单位毫秒。如果提供，将更新键的 TTL。
// 返回值:
// bool: 表示键是否存在。
// error: 操作中发生的任何错误。
func Exists[K any](key K, ttl ...int64) (bool, error) {
	// 通过调用 Get 函数并忽略值来实现。
	_, exists, err := Get[K, any](key, ttl...)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Drop 清空整个数据库。
func Drop() error {
	if err := db.DropAll(); err != nil {
		return err
	}
	return nil
}

// HashKey 将字符串散列为 []byte，用作数据库的键。
func HashKey(text string) []byte {
	n := xxhash.Sum64String(text)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

// IntKey 将 int64 转换为 []byte，用作数据库的键。
func IntKey(n int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}
