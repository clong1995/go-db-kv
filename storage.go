package kv

import (
	"fmt"

	"github.com/pkg/errors"

	"golang.org/x/sync/singleflight"
)

/*
Storage 函数提供了一个并发安全的缓存机制。
当多个 goroutine 同时请求同一个键时，只有第一个请求会执行昂贵的取值函数 (fn)，
其他请求会等待第一个请求的结果。这可以有效地防止缓存击穿。
它主要用于缓存那些取值成本高或取值不幂等的函数结果。
*/

var sf singleflight.Group

// Storage 是一个泛型函数，用于从缓存中获取或存储数据。
// 如果键存在，则直接返回缓存中的值。
// 如果键不存在，则调用 fn 函数生成值，存入缓存后再返回。
// K, V 是泛型参数，代表任意类型的键和值。
// key: 缓存键。
// fn: 一个函数，当缓存未命中时调用，用于生成值。
// ttl: 可选参数，用于控制生存时间 (TTL)。
//   - ttl[0]: TTL 值，单位毫秒。
//   - ttl[1]: 续期策略。如果为 1，则只在创建时设置 TTL；否则，每次获取时都续期。
//
// 返回值:
// V: 获取或生成的值。
// error: 操作中发生的任何错误。
func Storage[K, V any](key K, fn func() (value V, err error), ttl ...int64) (V, error) {
	// 检查是仅在创建时设置 TTL 还是每次都续期。
	if len(ttl) == 2 && ttl[1] == 1 {
		// 仅在创建时设置 TTL，获取时不续期。
		value, exists, err := Get[K, V](key)
		if err != nil {
			return value, err
		}
		if exists {
			return value, nil
		}
	} else {
		// 每次获取时都续期。
		value, exists, err := Get[K, V](key, ttl...)
		if err != nil {
			return value, err
		}
		if exists {
			return value, nil
		}
	}

	// 使用 singleflight 来确保 fn 函数在同一时间内只对同一个键执行一次。
	// Do 方法的 key 是通过对泛型 key 进行格式化生成的字符串。
	result, err, _ := sf.Do(fmt.Sprintf("%#v", key), func() (any, error) {
		// 在 singleflight 内部再次检查缓存，因为在等待 Do 方法执行期间，
		// 可能已有其他 goroutine 完成了值的计算和存储。
		v, es, err := Get[K, V](key, ttl...)
		if err != nil {
			return nil, err
		}
		if es {
			return v, nil
		}

		// 如果缓存仍然未命中，则执行昂贵的 fn 函数来生成值。
		v, err = fn()
		if err != nil {
			return nil, err
		}

		// 将生成的值存入缓存。
		if err = Set[K, V](key, v, ttl...); err != nil {
			return nil, err
		}
		return v, nil
	})
	var value V
	if err != nil {
		return value, err
	}
	if result == nil {
		return value, nil
	}

	// 将 singleflight 返回的 any 类型结果断言为具体的类型 V。
	value, ok := result.(V)
	if !ok {
		return value, errors.New("type assertion failed")
	}
	return value, nil
}
