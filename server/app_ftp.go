package server

import (
	"os"

	"github.com/r0123r/ftp-ledis-driver"
	"github.com/r0123r/ftp-server"
)

func (app *App) serv_ftp(rootPath, User, Pass string, Port int) {
	if _, err := os.Lstat(rootPath); err != nil {
		if app.access != nil {
			app.access.l.Error("Start ftp:", err)
		}
		return
	}
	perm := server.NewSimplePerm("root", "root")
	opt := &server.ServerOpts{
		Port: Port,
		Factory: &ledisdriver.LedisDriverFactory{
			Ldb:      app.Ledis(),
			RootPath: rootPath,
			Perm:     perm,
		},
		Auth: &server.SimpleAuth{
			Name:     User,
			Password: Pass,
		},
		Logger: new(server.StdLogger),
	}

	// start ftp server
	ftpServer := server.NewServer(opt)
	if app.access != nil {
		app.access.l.Info("Start ftp:", ftpServer.Port, " Root:", rootPath)
	}
	if err := ftpServer.ListenAndServe(); err != nil {
		if app.access != nil {
			app.access.l.Error("Start ftp:", err)
		}
		return
	}
}
