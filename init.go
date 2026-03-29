package kv

// prefix 是用于日志输出的前缀。
var prefix = "kv"

func init() {
	config()
	start()
}
