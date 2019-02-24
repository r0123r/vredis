package server

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/r0123r/ftpserver/server"
	"github.com/r0123r/vredis/ledis"
)

type LedisDriver struct {
	server.MainDriver
	Ldb       *ledis.Ledis
	BaseDir   string // Base directory from which to serve file
	nbClients int32  // Number of clients
}

func (driver *LedisDriver) GetSettings() (*server.Settings, error) {
	return &server.Settings{
		ListenAddr:  "0.0.0.0:3001",
		IdleTimeout: 900,
		Async:       true,
	}, nil
}

func (driver *LedisDriver) WelcomeUser(cc server.ClientContext) (string, error) {
	nbClients := atomic.AddInt32(&driver.nbClients, 1)
	if nbClients > 10 {
		return "Cannot accept any additional client", fmt.Errorf("too many clients: %d > % d", driver.nbClients, 10)
	}

	// This will remain the official name for now
	return fmt.Sprintf(
			"Welcome on ftpserver, dir:%s, ID:%d, addr:%s, clients:%d",
			driver.BaseDir,
			cc.ID(),
			cc.RemoteAddr(),
			nbClients),
		nil
}

// AuthUser authenticates the user and selects an handling driver
func (driver *LedisDriver) AuthUser(cc server.ClientContext, user, pass string) (server.ClientHandlingDriver, error) {

	if user == "admin" && pass == "admin" {
		db, err := driver.Ldb.Select(0)
		if err != nil {
			return nil, err
		}

		return &LedisClientDriver{RootPath: driver.BaseDir, db: db}, nil
	}

	return nil, fmt.Errorf("could not authenticate you")
}

// UserLeft is called when the user disconnects, even if he never authenticated
func (driver *LedisDriver) UserLeft(cc server.ClientContext) {
	atomic.AddInt32(&driver.nbClients, -1)
}

func (app *App) serv_ftp(rootPath, User, Pass string, Port int) {
	if _, err := os.Lstat(rootPath); err != nil {
		if app.access != nil {
			app.access.l.Error("Start ftp:", err)
		}
		return
	}
	//	perm := server.NewSimplePerm("root", "root")
	//	opt := &server.ServerOpts{
	//		Port: Port,
	//		Factory: &ledisdriver.LedisDriverFactory{
	//			Ldb:      app.Ledis(),
	//			RootPath: rootPath,
	//			Perm:     perm,
	//		},
	//		Auth: &server.SimpleAuth{
	//			Name:     User,
	//			Password: Pass,
	//		},
	//		Logger: new(server.StdLogger),
	//	}
	drv := &LedisDriver{BaseDir: rootPath, Ldb: app.Ledis()}
	// start ftp server
	ftpServer := server.NewFtpServer(drv)
	ftpServer.Logger.SetLevelByName("trace")
	if app.access != nil {
		app.access.l.Info("Start ftp:", ftpServer.Addr(), " Root:", rootPath)
	}
	if err := ftpServer.ListenAndServe(); err != nil {
		if app.access != nil {
			app.access.l.Error("Start ftp:", err)
		}
		return
	}
}
