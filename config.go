package kv

import conf "github.com/clong1995/go-config"

var cachePath string

func config() {
	cachePath, _ = conf.Value[string]("CACHE PATH")
}
