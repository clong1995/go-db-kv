package kv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
)

// serialize 函数将任意类型的数据序列化为字节切片。
// 对于 Go 的基础类型（如 int, float, bool, string），它使用 `encoding/binary` 进行高效的二进制编码。
// 对于其他复杂类型（如 struct, slice, map），它回退到使用 `encoding/gob` 进行编码。
// 这种方法旨在为常用类型提供高性能的序列化，同时保持对复杂类型的通用支持。
// T 是泛型参数，代表任意类型的数据。
// data: 需要被序列化的数据。
// 返回值:
// []byte: 序列化后的字节切片。
// error: 序列化过程中发生的任何错误。
func serialize[T any](data T) ([]byte, error) {
	var buf bytes.Buffer
	v := reflect.ValueOf(data)
	k := v.Kind()

	// 如果是指针，解引用以获取实际的值和类型。
	if k == reflect.Ptr {
		if v.IsNil() {
			return nil, nil // nil 指针序列化为 nil
		}
		v = v.Elem()
		k = v.Kind()
	}

	var result []byte
	// 根据数据的具体类型进行不同的序列化处理。
	switch k {
	case reflect.Bool:
		if v.Bool() {
			result = []byte{1}
		} else {
			result = []byte{0}
		}
	case reflect.Int8:
		result = []byte{byte(v.Int())}
	case reflect.Uint8:
		result = []byte{byte(v.Uint())}
	case reflect.Int16:
		result = make([]byte, 2)
		binary.BigEndian.PutUint16(result, uint16(v.Int()))
	case reflect.Uint16:
		result = make([]byte, 2)
		binary.BigEndian.PutUint16(result, uint16(v.Uint()))
	case reflect.Int32:
		result = make([]byte, 4)
		binary.BigEndian.PutUint32(result, uint32(v.Int()))
	case reflect.Uint32:
		result = make([]byte, 4)
		binary.BigEndian.PutUint32(result, uint32(v.Uint()))
	case reflect.Int, reflect.Int64:
		result = make([]byte, 8)
		binary.BigEndian.PutUint64(result, uint64(v.Int()))
	case reflect.Uint, reflect.Uint64:
		result = make([]byte, 8)
		binary.BigEndian.PutUint64(result, v.Uint())
	case reflect.Float32:
		result = make([]byte, 4)
		binary.BigEndian.PutUint32(result, float32Bits(float32(v.Float())))
	case reflect.Float64:
		result = make([]byte, 8)
		binary.BigEndian.PutUint64(result, float64Bits(v.Float()))
	case reflect.String:
		result = []byte(v.String())
	case reflect.Complex64:
		result = make([]byte, 8)
		c := complex64(v.Complex())
		binary.BigEndian.PutUint32(result[0:4], float32Bits(real(c)))
		binary.BigEndian.PutUint32(result[4:8], float32Bits(imag(c)))
	case reflect.Complex128:
		result = make([]byte, 16)
		c := v.Complex()
		binary.BigEndian.PutUint64(result[0:8], float64Bits(real(c)))
		binary.BigEndian.PutUint64(result[8:16], float64Bits(imag(c)))
	default:
		// 对于非基础类型，使用 gob 进行编码。
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(data); err != nil {
			return nil, errors.Wrap(err, "encode error")
		}
		result = buf.Bytes()
	}

	return result, nil
}

// deserialize 函数将字节切片反序列化为指定类型的数据。
// 它与 serialize 函数相对应，对基础类型使用二进制解码，对复杂类型使用 gob 解码。
// T 是泛型参数，代表目标数据类型。
// data: 需要被反序列化的字节切片。
// 返回值:
// T: 反序列化后的数据。
// error: 反序列化过程中发生的任何错误。
func deserialize[T any](data []byte) (T, error) {
	var result T
	if data == nil {
		return result, nil // nil 数据反序列化为零值
	}

	t := reflect.TypeOf(result)
	k := t.Kind()

	// 如果目标类型是指针，则先反序列化为元素类型，然后创建一个新的指针。
	if k == reflect.Ptr {
		elemType := t.Elem()
		elemValue, err := deserializeValue(data, elemType)
		if err != nil {
			return result, err
		}
		ptr := reflect.New(elemType)
		ptr.Elem().Set(elemValue)
		result = ptr.Interface().(T)
		return result, nil
	}

	// 对于非指针类型，直接反序列化。
	value, err := deserializeValue(data, t)
	if err != nil {
		return result, err
	}
	result = value.Interface().(T)
	return result, nil
}

// deserializeValue 是反序列化的核心辅助函数。
// 它根据提供的 reflect.Type 将字节切片解码为 reflect.Value。
func deserializeValue(data []byte, t reflect.Type) (reflect.Value, error) {
	k := t.Kind()
	value := reflect.New(t).Elem()

	// 根据目标类型进行不同的反序列化处理。
	switch k {
	case reflect.Bool:
		if len(data) < 1 {
			return value, errors.New("invalid bool data")
		}
		value.SetBool(data[0] != 0)
	case reflect.Int8:
		if len(data) < 1 {
			return value, errors.New("insufficient data for int8")
		}
		value.SetInt(int64(int8(data[0])))
	case reflect.Uint8:
		if len(data) < 1 {
			return value, errors.New("insufficient data for uint8")
		}
		value.SetUint(uint64(data[0]))
	case reflect.Int16:
		if len(data) < 2 {
			return value, errors.New("insufficient data for int16")
		}
		value.SetInt(int64(int16(binary.BigEndian.Uint16(data))))
	case reflect.Uint16:
		if len(data) < 2 {
			return value, errors.New("insufficient data for uint16")
		}
		value.SetUint(uint64(binary.BigEndian.Uint16(data)))
	case reflect.Int32:
		if len(data) < 4 {
			return value, errors.New("insufficient data for int32")
		}
		value.SetInt(int64(int32(binary.BigEndian.Uint32(data))))
	case reflect.Uint32:
		if len(data) < 4 {
			return value, errors.New("insufficient data for uint32")
		}
		value.SetUint(uint64(binary.BigEndian.Uint32(data)))
	case reflect.Int, reflect.Int64:
		if len(data) < 8 {
			return value, errors.New("insufficient data for int64")
		}
		value.SetInt(int64(binary.BigEndian.Uint64(data)))
	case reflect.Uint, reflect.Uint64:
		if len(data) < 8 {
			return value, errors.New("insufficient data for uint64")
		}
		value.SetUint(binary.BigEndian.Uint64(data))
	case reflect.Float32:
		if len(data) < 4 {
			return value, errors.New("insufficient data for float32")
		}
		value.SetFloat(float64(float32FromBits(binary.BigEndian.Uint32(data))))
	case reflect.Float64:
		if len(data) < 8 {
			return value, errors.New("insufficient data for float64")
		}
		value.SetFloat(float64FromBits(binary.BigEndian.Uint64(data)))
	case reflect.String:
		value.SetString(string(data))
	case reflect.Complex64:
		if len(data) < 8 {
			return value, errors.New("insufficient data for complex64")
		}
		real_ := float32FromBits(binary.BigEndian.Uint32(data[0:4]))
		imag_ := float32FromBits(binary.BigEndian.Uint32(data[4:8]))
		value.SetComplex(complex128(complex(real_, imag_)))
	case reflect.Complex128:
		if len(data) < 16 {
			return value, errors.New("insufficient data for complex128")
		}
		real_ := float64FromBits(binary.BigEndian.Uint64(data[0:8]))
		imag_ := float64FromBits(binary.BigEndian.Uint64(data[8:16]))
		value.SetComplex(complex(real_, imag_))
	default:
		// 对于非基础类型，使用 gob 进行解码。
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(value.Addr().Interface()); err != nil {
			return value, errors.Wrap(err, "decode error")
		}
	}
	return value, nil
}

// float32Bits 使用 unsafe 操作将 float32 的位模式重新解释为 uint32。
func float32Bits(f float32) uint32 {
	return *(*uint32)(unsafe.Pointer(&f))
}

// float64Bits 使用 unsafe 操作将 float64 的位模式重新解释为 uint64。
func float64Bits(f float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&f))
}

// float32FromBits 使用 unsafe 操作将 uint32 的位模式重新解释为 float32。
func float32FromBits(b uint32) float32 {
	return *(*float32)(unsafe.Pointer(&b))
}

// float64FromBits 使用 unsafe 操作将 uint64 的位模式重新解释为 float64。
func float64FromBits(b uint64) float64 {
	return *(*float64)(unsafe.Pointer(&b))
}
