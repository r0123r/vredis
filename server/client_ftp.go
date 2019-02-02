package server

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/jlaffaye/ftp"
)

type ftpClient struct {
	*ftp.ServerConn
	app *App
}

func newClientFTP(app *App, host, user, pass string) *ftpClient {
	f, err := ftp.Connect(host)
	if err != nil {
		app.access.l.Error(err)
		return nil
	}
	//defer f.Quit()
	if err := f.Login(user, pass); err != nil {
		app.access.l.Error(err)
		return nil
	}
	//	files := Config.Checksum.Docs
	return &ftpClient{f, app}
}
func (f *ftpClient) Download(files ...string) error {
	for _, file := range files {
		r, err := f.Retr(file)
		w, err := os.Create(file)
		if err != nil {
			return fmt.Errorf("file: %s error: %v\n", file, err)
		} else {
			io.Copy(w, r)
		}
		w.Close()
		r.Close()
	}
	return nil
}
func (f *ftpClient) List(dir string) ([]string, error) {
	if err := f.ChangeDir(dir); err != nil {
		return nil, err
	}
	ffiles, _ := f.NameList(".")
	sort.Strings(ffiles)
	return ffiles, nil
}
func (f *ftpClient) UploadDir(sdir string) error {
	dir, err := os.Open(sdir)
	if err != nil {
		return err
	}
	files, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, name := range files {
		if filepath.Ext(name) != ".zip" {
			continue
		}
		f.app.access.l.Info("UploadDir:", name)
		file, err := os.Open(filepath.Join(sdir, name))
		if err != nil {
			f.app.access.l.Error("UploadDir:", err)
			continue
		}
		stat, _ := file.Stat()
		if stat.IsDir() {
			continue
		}

		if err := f.Stor(name, file); err != nil {
			f.app.access.l.Error("UploadDir:", err)
		}
		file.Close()
	}
	return nil
}
