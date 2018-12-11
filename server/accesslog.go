package server

import (
	"github.com/siddontang/go/log"
)

const (
	accessTimeFormat = "2006/01/02 15:04:05"
)

type accessLog struct {
	l *log.Logger
}

func newAcessLog(baseName string) (*accessLog, error) {
	l := new(accessLog)

	h, err := log.NewRotatingFileHandler(baseName, 1024*1024*30, 2)
	if err != nil {
		return nil, err
	}

	l.l = log.New(h, log.Ltime)

	return l, nil
}

func (l *accessLog) Close() {
	l.l.Close()
}

func (l *accessLog) Log(remoteAddr string, bd int, usedTime int64, request []byte, err error) {

	format := `%s [%d] %q %d [%s]`

	if err == nil {
		l.l.Infof(format, remoteAddr, bd, request, usedTime, "OK")
	} else {
		l.l.Infof(format, remoteAddr, bd, request, usedTime, err.Error())
	}
}
