package kv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"log"
	"reflect"
	"unsafe"
)

// serialize 对基础类型使用二进制编码，否则使用 gob。
func serialize[T any](data T) (result []byte, err error) {
	var buf bytes.Buffer
	v := reflect.ValueOf(data)
	k := v.Kind()

	if k == reflect.Ptr {
		if v.IsNil() {
			err = errors.New("cannot serialize nil pointer")
			return
		}
		v = v.Elem()
		k = v.Kind()
	}

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
		enc := gob.NewEncoder(&buf)
		if err = enc.Encode(data); err != nil {
			log.Println(err)
			return
		}
		result = buf.Bytes()
		return
	}

	return
}

// deserialize 对基础类型用二进制解码，否则使用 gob。
func deserialize[T any](data []byte) (result T, err error) {

	if len(data) == 0 {
		err = errors.New("empty data")
		log.Println(err)
		return
	}

	t := reflect.TypeOf(result)
	k := t.Kind()

	if k == reflect.Ptr {
		elemType := t.Elem()
		var elemValue reflect.Value
		if elemValue, err = deserializeValue(data, elemType); err != nil {
			log.Println(err)
			return
		}
		ptr := reflect.New(elemType)
		ptr.Elem().Set(elemValue)
		result = ptr.Interface().(T)
		return
	}

	value, err := deserializeValue(data, t)
	if err != nil {
		log.Println(err)
		return
	}
	result = value.Interface().(T)
	return
}

func deserializeValue(data []byte, t reflect.Type) (value reflect.Value, err error) {

	k := t.Kind()
	value = reflect.New(t).Elem()

	switch k {
	case reflect.Bool:
		if len(data) < 1 {
			err = errors.New("invalid bool data")
			log.Println(err)
			return
		}
		value.SetBool(data[0] != 0)

	case reflect.Int8:
		if len(data) < 1 {
			err = errors.New("insufficient data for int8")
			log.Println(err)
			return
		}
		value.SetInt(int64(int8(data[0])))
	case reflect.Uint8:
		if len(data) < 1 {
			err = errors.New("insufficient data for uint8")
			log.Println(err)
			return
		}
		value.SetUint(uint64(data[0]))

	case reflect.Int16:
		if len(data) < 2 {
			err = errors.New("insufficient data for int16")
			log.Println(err)
			return
		}
		value.SetInt(int64(int16(binary.BigEndian.Uint16(data))))
	case reflect.Uint16:
		if len(data) < 2 {
			err = errors.New("insufficient data for uint16")
			log.Println(err)
			return
		}
		value.SetUint(uint64(binary.BigEndian.Uint16(data)))

	case reflect.Int32:
		if len(data) < 4 {
			err = errors.New("insufficient data for int32")
			log.Println(err)
			return
		}

		value.SetInt(int64(int32(binary.BigEndian.Uint32(data))))
	case reflect.Uint32:
		if len(data) < 4 {
			err = errors.New("insufficient data for uint32")
			log.Println(err)
			return
		}
		value.SetUint(uint64(binary.BigEndian.Uint32(data)))

	case reflect.Int, reflect.Int64:
		if len(data) < 8 {
			err = errors.New("insufficient data for int64")
			log.Println(err)
			return
		}
		value.SetInt(int64(binary.BigEndian.Uint64(data)))
	case reflect.Uint, reflect.Uint64:
		if len(data) < 8 {
			err = errors.New("insufficient data for uint64")
			log.Println(err)
			return
		}
		value.SetUint(binary.BigEndian.Uint64(data))

	case reflect.Float32:
		if len(data) < 4 {
			err = errors.New("insufficient data for float32")
			log.Println(err)
			return
		}
		value.SetFloat(float64(float32FromBits(binary.BigEndian.Uint32(data))))
	case reflect.Float64:
		if len(data) < 8 {
			err = errors.New("insufficient data for float64")
			log.Println(err)
			return
		}
		value.SetFloat(float64FromBits(binary.BigEndian.Uint64(data)))

	case reflect.String:
		value.SetString(string(data))

	case reflect.Complex64:
		if len(data) < 8 {
			err = errors.New("insufficient data for complex64")
			log.Println(err)
			return
		}
		real_ := float32FromBits(binary.BigEndian.Uint32(data[0:4]))
		imag_ := float32FromBits(binary.BigEndian.Uint32(data[4:8]))
		value.SetComplex(complex128(complex(real_, imag_)))
	case reflect.Complex128:
		if len(data) < 16 {
			err = errors.New("insufficient data for complex128")
			log.Println(err)
			return
		}
		real_ := float64FromBits(binary.BigEndian.Uint64(data[0:8]))
		imag_ := float64FromBits(binary.BigEndian.Uint64(data[8:16]))
		value.SetComplex(complex(real_, imag_))

	default:
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		if err = dec.Decode(value.Addr().Interface()); err != nil {
			log.Println(err)
			return
		}
	}
	return
}

func float32Bits(f float32) uint32 {
	return *(*uint32)(unsafe.Pointer(&f))
}

func float64Bits(f float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&f))
}

func float32FromBits(b uint32) float32 {
	return *(*float32)(unsafe.Pointer(&b))
}

func float64FromBits(b uint64) float64 {
	return *(*float64)(unsafe.Pointer(&b))
}
