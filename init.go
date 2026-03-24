package kv

import (
	"os"
	"path"
	"path/filepath"

	"github.com/clong1995/go-ansi-color"
	"github.com/clong1995/go-config"
	"github.com/dgraph-io/badger/v4"
)

// db 是一个全局的 Badger 数据库连接实例。
var db *badger.DB

// prefix 是用于日志输出的前缀。
var prefix = "kv"

// init 函数在包被导入时自动执行。
// 它负责初始化 Badger 数据库连接。
// 数据库路径可以通过 "CACHE PATH" 配置项来设置。
// 如果没有配置，它默认在当前执行文件的目录下创建一个 ".kv" 文件夹作为数据库路径。
// 如果路径为空字符串，则使用内存模式。
func init() {
	// 从配置中读取缓存路径。
	cachePath, _ := config.Value[string]("CACHE PATH")
	// 如果路径是 "./"，则解析为当前可执行文件的目录。
	if cachePath == "./" {
		exePath, err := os.Executable()
		if err != nil {
			pcolor.PrintFatal(prefix, err.Error())
			return
		}
		cachePath = filepath.Dir(exePath)
		cachePath = path.Join(cachePath, ".kv")
	}
	var err error
	// 设置 Badger 数据库选项。如果 cachePath 为空，则使用内存数据库。
	opt := badger.DefaultOptions(cachePath).WithInMemory(cachePath == "")
	// 禁用 Badger 的默认日志记录器，以避免不必要的输出。
	opt.Logger = nullLogger{}
	// 打开 Badger 数据库。
	if db, err = badger.Open(opt); err != nil {
		pcolor.PrintFatal(prefix, err.Error())
		return
	}
	// 打印连接成功的消息。
	if cachePath == "" {
		pcolor.PrintSucc(prefix, "conn in memory")
	} else {
		pcolor.PrintSucc(prefix, "conn %v", cachePath)
	}
	return
}

// Close 函数用于关闭数据库连接。
// 在程序退出前调用此函数是很好的做法，以确保所有数据都被正确写入磁盘。
func Close() {
	if err := db.Close(); err != nil {
		pcolor.PrintError(prefix, err.Error())
		return
	}
	pcolor.PrintSucc(prefix, "conn closed")
	return
}
