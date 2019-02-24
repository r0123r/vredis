package server

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/r0123r/ftpserver/server"
	"github.com/r0123r/vredis/ledis"
)

// ClientDriver defines a very basic client driver
type LedisClientDriver struct {
	server.ClientHandlingDriver
	RootPath string // Base directory from which to server file
	db       *ledis.DB
	allocate int
}

func (driver *LedisClientDriver) realPath(path string) []byte {
	return []byte(strings.TrimLeft(path, "/"))
}

// ChangeDirectory changes the current working directory
func (driver *LedisClientDriver) ChangeDirectory(cc server.ClientContext, path string) error {
	if path == "/" || path == "" {
		driver.RootPath = path
	} else {
		rpath := driver.realPath(path + "/")
		if ok, _ := driver.db.HKeyExists(rpath); ok != 1 {
			return fmt.Errorf("Not a directory")
		} else {
			driver.RootPath = string(rpath)
		}
	}
	return nil
}

// MakeDirectory creates a directory
func (driver *LedisClientDriver) MakeDirectory(cc server.ClientContext, path string) error {
	rpath := driver.realPath(path + "/")
	if ok, _ := driver.db.HKeyExists(rpath); ok != 1 {
		driver.db.HSet(rpath, []byte("modTime"), ledis.PutInt64(time.Now().Unix()))
	}
	return nil
}
func (driver *LedisClientDriver) GetAtr(rpath []byte, isdir bool) (os.FileInfo, error) {
	buf, _ := driver.db.HGet(rpath, []byte("modTime"))
	i, _ := ledis.Int64(buf, nil)
	modTime := time.Unix(i, 0)
	size := int64(0)
	ind := 0
	if !isdir {
		size, _ = driver.db.StrLen(rpath)
	} else {
		rpath = bytes.TrimRight(rpath, "/")
	}
	ind = bytes.LastIndexByte(rpath, '/')
	ind++

	return &VirtualFileInfo{name: string(rpath[ind:]), isDir: isdir, modTime: modTime, size: size}, nil
}

// ListFiles lists the files of a directory
func (driver *LedisClientDriver) ListFiles(cc server.ClientContext) ([]os.FileInfo, error) {

	files := make([]os.FileInfo, 0)

	cursor := []byte{}
	var f os.FileInfo
	var err error
	//Список каталогов
	dir := string(driver.realPath(cc.Path()))
	if dir != "" {
		dir += "/"
	}
	L := len(dir)
	re := fmt.Sprintf("^%s[^/]*/*$", dir)
	var ents [][]byte
	sep := 0
	fmt.Println("re:", re)
	for {
		ents, _ = driver.db.Scan(ledis.HASH, cursor, 100, false, re)
		for _, cursor = range ents {
			if len(cursor) == L {
				continue
			} else if L > len(cursor) {
				return nil, fmt.Errorf("err:cursor=%q dir=%q", cursor, dir)
			}
			sep = bytes.Count(cursor[L:], []byte("/"))
			if sep == 1 && bytes.HasSuffix(cursor, []byte("/")) {
				f, err = driver.GetAtr(cursor, true)
			} else if sep == 0 {
				f, err = driver.GetAtr(cursor, false)
			} else {
				continue
			}
			if err != nil {
				return nil, err
			}
			files = append(files, f)
		}
		if len(ents) < 100 {
			break
		}
	}

	return files, err
}

// ListFiles lists the files of a directory
func (driver *LedisClientDriver) AsyncListFiles(cc server.ClientContext, cfiles chan<- os.FileInfo) {
	cursor := []byte{}
	var f os.FileInfo
	var err error
	//Список каталогов
	dir := string(driver.realPath(cc.Path()))
	if dir != "" {
		dir += "/"
	}
	L := len(dir)
	re := fmt.Sprintf("^%s[^/]*/*$", dir)
	var ents [][]byte
	sep := 0

	defer func() {
		close(cfiles)
	}()
	for {
		ents, _ = driver.db.Scan(ledis.HASH, cursor, 100, false, re)
		for _, cursor = range ents {
			if len(cursor) == L {
				continue
			} else if L > len(cursor) {
				return
			}
			sep = bytes.Count(cursor[L:], []byte("/"))
			if sep == 1 && bytes.HasSuffix(cursor, []byte("/")) {
				f, err = driver.GetAtr(cursor, true)
			} else if sep == 0 {
				f, err = driver.GetAtr(cursor, false)
			} else {
				continue
			}
			if err != nil {
				return
			}
			cfiles <- f
		}
		if len(ents) < 100 {
			break
		}
	}
}

// GetFileInfo gets some info around a file or a directory
func (driver *LedisClientDriver) GetFileInfo(cc server.ClientContext, path string) (os.FileInfo, error) {
	if path == "/" {
		return &VirtualFileInfo{name: path, isDir: true}, nil
	}
	isdir := false
	rpath := driver.realPath(path)
	ok, err := driver.db.Exists(rpath)
	if err != nil {
		return nil, err
	}
	if ok != 1 {
		if !bytes.HasSuffix(rpath, []byte("/")) {
			rpath = append(rpath, '/')
		}
		ok, _ := driver.db.HKeyExists(rpath)
		if ok != 1 {
			return nil, fmt.Errorf("Not exists:%q", rpath)
		}
		isdir = true
	}
	return driver.GetAtr(rpath, isdir)
}

// CanAllocate gives the approval to allocate some data
func (driver *LedisClientDriver) CanAllocate(cc server.ClientContext, size int) (bool, error) {
	driver.allocate = size
	return true, nil
}

// ChmodFile changes the attributes of the file
func (driver *LedisClientDriver) ChmodFile(cc server.ClientContext, path string, mode os.FileMode) error {
	return nil
}

// DeleteFile deletes a file or a directory
func (driver *LedisClientDriver) DeleteFile(cc server.ClientContext, path string) error {
	rpath := driver.realPath(path)
	driver.db.Del(rpath)
	size, err := driver.db.HClear(rpath)
	if err == nil && size == 0 {
		rpath = append(rpath, '/')
		_, err = driver.db.HClear(rpath)
	}
	return err
}

// RenameFile renames a file or a directory
func (driver *LedisClientDriver) RenameFile(cc server.ClientContext, from, to string) error {
	rp1 := driver.realPath(from)
	rp2 := driver.realPath(to)
	buf, err := driver.db.Get(rp1)
	if err != nil {
		return err
	}
	err = driver.db.Set(rp2, buf)
	if err != nil {
		return err
	}
	val, err := driver.db.HGetAll(rp1)
	if err != nil {
		return err
	}
	err = driver.db.HMset(rp2, val...)
	if err != nil {
		return err
	}
	_, err = driver.db.HClear(rp1)
	_, err = driver.db.Del(rp1)
	return err
}

// OpenFile opens a file in 3 possible modes: read, write, appending write (use appropriate flags)
func (driver *LedisClientDriver) OpenFile(cc server.ClientContext, path string, flag int) (server.FileStream, error) {
	return &LedisVirtualFile{
		rpath:    driver.realPath(path),
		append:   (flag & os.O_APPEND) != 0,
		db:       driver.db,
		allocate: driver.allocate,
	}, nil
}

// The virtual file is an example of how you can implement a purely virtual file
type LedisVirtualFile struct {
	append     bool
	rpath      []byte
	content    []byte // Content of the file
	readOffset int    // Reading offset
	db         *ledis.DB
	allocate   int
}

func (f *LedisVirtualFile) Close() error {
	return nil
}
func (f *LedisVirtualFile) Seek(n int64, w int) (int64, error) {
	return 0, nil
}

func (f *LedisVirtualFile) Read(buffer []byte) (int, error) {
	var err error
	f.content, err = f.db.Get(f.rpath)
	if err != nil {
		return 0, err
	}
	n := copy(buffer, f.content[f.readOffset:])
	f.readOffset += n
	if n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

func (f *LedisVirtualFile) Write(buffer []byte) (int, error) {
	if f.readOffset == 0 {
		if !f.append {
			if err := f.db.Set(f.rpath, []byte{}); err != nil {
				f.db.HClear(f.rpath)
				return 0, err
			}
		}
		f.db.HSet(f.rpath, []byte("modTime"), ledis.PutInt64(time.Now().Unix()))
		f.content = make([]byte, f.allocate)
	}
	size := copy(f.content[f.readOffset:], buffer)
	f.readOffset += size
	if f.readOffset >= f.allocate {
		fmt.Printf("write %q %v\n", f.rpath, f.append)
		_, err := f.db.Append(f.rpath, f.content)
		if err != nil {
			f.db.Del(f.rpath)
			f.db.HClear(f.rpath)
			return 0, err
		}
	}

	return size, nil
}

type VirtualFileInfo struct {
	name    string
	isDir   bool
	modTime time.Time
	size    int64
}

func (f *VirtualFileInfo) Name() string {
	return f.name
}

func (f *VirtualFileInfo) Size() int64 {
	return f.size
}

func (f *VirtualFileInfo) Mode() os.FileMode {
	if f.isDir {
		return os.ModeDir | os.ModePerm
	}
	return os.ModePerm
}

func (f *VirtualFileInfo) ModTime() time.Time {
	return f.modTime
}

func (f *VirtualFileInfo) IsDir() bool {
	return f.isDir
}

func (f *VirtualFileInfo) Sys() interface{} {
	return nil
}
