package kv

// prefix 是用于日志输出的前缀。
const prefix = "kv"

func init() {
	config()
	start()
}
