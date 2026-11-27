package deleter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// ErrMountBoundary is reported when a directory step crosses into another
// mounted filesystem or protected snapshot boundary.
var ErrMountBoundary = errors.New("mount point skipped")

// RemoveCallbacks hooks progress reporting, error collection, and mount
// boundary checks into RemoveTarget.
type RemoveCallbacks struct {
	IsMountPoint func(path string, parentDev, dev uint64) bool // nil defaults to dev != parentDev
	OnProgress   func(path string, filesDelta, dirsDelta, freedDBytes, freedABytes int64)
	OnError      func(path string, err error)
}

// RemoveStats summarizes the filesystem effects of removing one Target.
type RemoveStats struct {
	Files      int64
	Dirs       int64
	FreedDSize int64
	FreedASize int64
	Errors     int
	Complete   bool
}

const (
	readdirBatchSize = 512
	maxFailedInDir   = 10000
)

// RemoveTarget deletes one resolved Target on disk using openat/unlinkat with
// O_NOFOLLOW so symlinks are never followed and mount boundaries are never crossed.
func RemoveTarget(ctx context.Context, t Target, cb RemoveCallbacks) RemoveStats {
	var st RemoveStats
	if err := ctx.Err(); err != nil {
		reportErr(&st, cb, t.RawPath, err)
		return st
	}
	if t.Protected || t.VolMount == "" || len(t.RelParts) == 0 {
		reportErr(&st, cb, t.RawPath, errors.New("invalid or protected delete target"))
		return st
	}
	for _, part := range t.RelParts {
		if !validComponent(part) {
			reportErr(&st, cb, t.RawPath, errors.New("invalid path component"))
			return st
		}
	}
	isMount := cb.IsMountPoint
	if isMount == nil {
		isMount = func(_ string, parentDev, dev uint64) bool { return dev != parentDev }
	}

	curFD, err := unix.Open(t.VolMount, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			st.Complete = true
			return st
		}
		reportErr(&st, cb, t.VolMount, err)
		return st
	}
	defer func() {
		if curFD >= 0 {
			unix.Close(curFD)
		}
	}()

	var curStat unix.Stat_t
	if err := unix.Fstat(curFD, &curStat); err != nil {
		reportErr(&st, cb, t.VolMount, err)
		return st
	}
	curDev := uint64(curStat.Dev)
	curPath := t.VolMount

	// Walk intermediate directories with O_NOFOLLOW so swapped symlinks fail with ELOOP.
	for _, part := range t.RelParts[:len(t.RelParts)-1] {
		if err := ctx.Err(); err != nil {
			reportErr(&st, cb, curPath, err)
			return st
		}
		nextPath := joinRaw(curPath, part)
		nextFD, err := unix.Openat(curFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				st.Complete = true
				return st
			}
			reportErr(&st, cb, nextPath, err)
			return st
		}
		var nst unix.Stat_t
		if err := unix.Fstat(nextFD, &nst); err != nil {
			unix.Close(nextFD)
			reportErr(&st, cb, nextPath, err)
			return st
		}
		nextDev := uint64(nst.Dev)
		if isMount(nextPath, curDev, nextDev) {
			unix.Close(nextFD)
			reportErr(&st, cb, nextPath, ErrMountBoundary)
			return st
		}
		unix.Close(curFD)
		curFD = nextFD
		curDev = nextDev
		curPath = nextPath
	}

	leaf := t.RelParts[len(t.RelParts)-1]
	leafPath := joinRaw(curPath, leaf)
	var lst unix.Stat_t
	if err := unix.Fstatat(curFD, leaf, &lst, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			st.Complete = true
			return st
		}
		reportErr(&st, cb, leafPath, err)
		return st
	}

	if lst.Mode&unix.S_IFMT != unix.S_IFDIR {
		if err := unix.Unlinkat(curFD, leaf, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			reportErr(&st, cb, leafPath, err)
			return st
		}
		dbytes := int64(lst.Blocks) * 512
		abytes := lst.Size
		st.Files++
		st.FreedDSize += dbytes
		st.FreedASize += abytes
		if cb.OnProgress != nil {
			cb.OnProgress(leafPath, 1, 0, dbytes, abytes)
		}
		st.Complete = true
		return st
	}

	leafDev := uint64(lst.Dev)
	if isMount(leafPath, curDev, leafDev) {
		reportErr(&st, cb, leafPath, ErrMountBoundary)
		return st
	}

	if removeDir(ctx, curFD, leaf, leafPath, leafDev, lst.Ino, isMount, cb, &st) {
		st.Complete = true
	}
	return st
}

// removeDir recursively removes a directory at (parentFD, name) and then
// unlinks the directory itself. It returns true only if the directory was
// completely removed (or already gone).
func removeDir(
	ctx context.Context,
	parentFD int,
	name string,
	dirPath string,
	expectDev uint64,
	expectIno uint64,
	isMount func(path string, parentDev, dev uint64) bool,
	cb RemoveCallbacks,
	st *RemoveStats,
) bool {
	if err := ctx.Err(); err != nil {
		reportErr(st, cb, dirPath, err)
		return false
	}
	dirFD, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return true
		}
		reportErr(st, cb, dirPath, err)
		return false
	}
	var dst unix.Stat_t
	if err := unix.Fstat(dirFD, &dst); err != nil {
		unix.Close(dirFD)
		reportErr(st, cb, dirPath, err)
		return false
	}
	if uint64(dst.Dev) != expectDev || dst.Ino != expectIno {
		unix.Close(dirFD)
		reportErr(st, cb, dirPath, errors.New("directory changed concurrently"))
		return false
	}

	dirErrs := 0
	failed := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			unix.Close(dirFD)
			reportErr(st, cb, dirPath, err)
			return false
		}
		names, eof, rerr := readDirBatch(dirFD, dirPath, readdirBatchSize+len(failed))
		if rerr != nil {
			unix.Close(dirFD)
			reportErr(st, cb, dirPath, rerr)
			return false
		}
		progressed := false
		for _, child := range names {
			if child == "." || child == ".." || failed[child] {
				continue
			}
			if err := ctx.Err(); err != nil {
				unix.Close(dirFD)
				reportErr(st, cb, dirPath, err)
				return false
			}
			childPath := joinRaw(dirPath, child)
			var cst unix.Stat_t
			if err := unix.Fstatat(dirFD, child, &cst, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				if errors.Is(err, unix.ENOENT) {
					progressed = true
					continue
				}
				reportErr(st, cb, childPath, err)
				failed[child] = true
				dirErrs++
				continue
			}
			if cst.Mode&unix.S_IFMT == unix.S_IFDIR {
				childDev := uint64(cst.Dev)
				if isMount(childPath, expectDev, childDev) {
					reportErr(st, cb, childPath, ErrMountBoundary)
					failed[child] = true
					dirErrs++
					continue
				}
				if removeDir(ctx, dirFD, child, childPath, childDev, cst.Ino, isMount, cb, st) {
					progressed = true
				} else {
					failed[child] = true
					dirErrs++
					if ctx.Err() != nil {
						unix.Close(dirFD)
						return false
					}
				}
				continue
			}
			if err := unix.Unlinkat(dirFD, child, 0); err != nil && !errors.Is(err, unix.ENOENT) {
				reportErr(st, cb, childPath, err)
				failed[child] = true
				dirErrs++
				continue
			}
			dbytes := int64(cst.Blocks) * 512
			abytes := cst.Size
			st.Files++
			st.FreedDSize += dbytes
			st.FreedASize += abytes
			progressed = true
			if cb.OnProgress != nil {
				cb.OnProgress(childPath, 1, 0, dbytes, abytes)
			}
		}
		if len(failed) >= maxFailedInDir {
			reportErr(st, cb, dirPath, fmt.Errorf("too many errors in directory (%d)", len(failed)))
			dirErrs++
			break
		}
		if eof || !progressed {
			break
		}
	}
	unix.Close(dirFD)

	if dirErrs > 0 || ctx.Err() != nil {
		return false
	}
	if err := unix.Unlinkat(parentFD, name, unix.AT_REMOVEDIR); err != nil && !errors.Is(err, unix.ENOENT) {
		reportErr(st, cb, dirPath, err)
		return false
	}
	dbytes := int64(dst.Blocks) * 512
	abytes := dst.Size
	st.Dirs++
	st.FreedDSize += dbytes
	st.FreedASize += abytes
	if cb.OnProgress != nil {
		cb.OnProgress(dirPath, 0, 1, dbytes, abytes)
	}
	return true
}

// readDirBatch dups dirFD, rewinds to offset 0, and reads up to n entry names
// so unlinking entries between passes never triggers filesystem readdir skips.
func readDirBatch(dirFD int, dirPath string, n int) ([]string, bool, error) {
	dupFD, err := unix.FcntlInt(uintptr(dirFD), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, false, err
	}
	if _, err := unix.Seek(dupFD, 0, io.SeekStart); err != nil {
		unix.Close(dupFD)
		return nil, false, err
	}
	f := os.NewFile(uintptr(dupFD), dirPath)
	defer f.Close()
	names, err := f.Readdirnames(n)
	if errors.Is(err, io.EOF) {
		return names, true, nil
	}
	return names, false, err
}

func reportErr(st *RemoveStats, cb RemoveCallbacks, rawPath string, err error) {
	st.Errors++
	if cb.OnError != nil {
		cb.OnError(strings.ToValidUTF8(rawPath, "\uFFFD"), err)
	}
}
