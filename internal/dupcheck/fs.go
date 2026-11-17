package dupcheck

import (
	"io"
	"os"
	"syscall"
)

// File is an open file being hashed.
type File interface {
	io.Reader
	io.ReaderAt
	Close() error
}

// FS is the filesystem the check reads. Tests substitute it.
type FS interface {
	Lstat(path string) (FileInfo, error)
	Open(path string) (File, error)
	Readlink(path string) (string, error)
}

// OSFS is the real filesystem. Files are opened read-only, and on Linux the
// pages read are dropped from the page cache as reading goes, so hashing
// hundreds of GB does not evict what the NAS keeps cached for its users.
func OSFS() FS { return osFS{} }

type osFS struct{}

func (osFS) Lstat(path string) (FileInfo, error) {
	var st syscall.Stat_t
	if err := syscall.Lstat(path, &st); err != nil {
		return FileInfo{}, &os.PathError{Op: "lstat", Path: path, Err: err}
	}
	return statInfo(&st), nil
}

func (osFS) Readlink(path string) (string, error) { return os.Readlink(path) }

const dropEvery = 8 << 20

func (osFS) Open(path string) (File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}

type osFile struct {
	f     *os.File
	since int64
}

func (o *osFile) count(n int) {
	o.since += int64(n)
	if o.since >= dropEvery {
		o.since = 0
		dropCache(o.f)
	}
}

func (o *osFile) Read(p []byte) (int, error) {
	n, err := o.f.Read(p)
	o.count(n)
	return n, err
}

func (o *osFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := o.f.ReadAt(p, off)
	o.count(n)
	return n, err
}

func (o *osFile) Close() error {
	dropCache(o.f)
	return o.f.Close()
}
