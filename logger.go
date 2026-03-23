package kv

type nullLogger struct{}

func (nullLogger) Errorf(string, ...interface{})   {}
func (nullLogger) Warningf(string, ...interface{}) {}
func (nullLogger) Infof(string, ...interface{})    {}
func (nullLogger) Debugf(string, ...interface{})   {}
