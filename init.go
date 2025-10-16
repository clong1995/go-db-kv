package kv

import (
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/clong1995/go-ansi-color"
	"github.com/clong1995/go-config"
	"github.com/dgraph-io/badger/v4"
)

var db *badger.DB
var prefix = "kv"

func init() {
	cachePath := config.Value("CACHE PATH")
	if cachePath == "./" {
		exePath, err := os.Executable()
		if err != nil {
			log.Println(err)
			return
		}
		cachePath = filepath.Dir(exePath)
		cachePath = path.Join(cachePath, ".kv")
	}
	var err error
	opt := badger.DefaultOptions(cachePath).WithInMemory(cachePath == "")
	opt.Logger = nullLogger{}
	if db, err = badger.Open(opt); err != nil {
		pcolor.PrintFatal(prefix, err.Error())
		return
	}
	if cachePath == "" {
		pcolor.PrintSucc(prefix, "conn in memory")
	} else {
		pcolor.PrintSucc(prefix, "conn %v", cachePath)
	}
}

// Close 关闭
func Close() (err error) {
	if err = db.Close(); err != nil {
		log.Println(err)
		return
	}
	pcolor.PrintSucc(prefix, "conn closed")
	return
}
