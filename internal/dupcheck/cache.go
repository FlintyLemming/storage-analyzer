package dupcheck

import (
	"database/sql"
	"errors"
	"net/url"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// FileInfo is what the check needs from lstat.
type FileInfo struct {
	Dev, Ino uint64
	Size     int64
	MTime    int64 // seconds, as stored in scan databases
	MTimeNS  int64
	CTimeNS  int64
	Regular  bool
}

// CacheEntry is what is known about one file's content.
type CacheEntry struct {
	Quick Digest  // SHA-256 of the first and last 64KB (whole file when ≤ 128KB)
	Full  *Digest // SHA-256 of the whole file, if computed
}

const (
	cacheFlushRows  = 500
	cacheFlushEvery = 2 * time.Second
)

// Cache remembers file digests across checks in dataDir/dupcache.db. A hit
// requires path, dev, inode, size, mtime and ctime to all match. Writes are
// buffered and committed every few hundred rows or seconds, so an
// interrupted check keeps most of its work. It is safe for concurrent use.
type Cache struct {
	db  *sql.DB
	run int64

	mu        sync.Mutex
	puts      map[string]cacheRow
	touches   map[string]bool
	lastFlush time.Time
}

type cacheRow struct {
	fi FileInfo
	e  CacheEntry
}

const cacheSchema = `
CREATE TABLE IF NOT EXISTS hashes(
  path     BLOB PRIMARY KEY,
  dev      INTEGER NOT NULL,
  ino      INTEGER NOT NULL,
  size     INTEGER NOT NULL,
  mtime_ns INTEGER NOT NULL,
  ctime_ns INTEGER NOT NULL,
  quick    BLOB NOT NULL,
  full     BLOB,
  seen     INTEGER NOT NULL
) WITHOUT ROWID;
`

// OpenCache opens (creating if needed) the cache database at path. Each
// open is one "run" for Touch and Prune.
func OpenCache(path string) (*Cache, error) {
	db, err := sql.Open("sqlite", "file:"+(&url.URL{Path: path}).EscapedPath()+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(cacheSchema); err != nil {
		db.Close()
		return nil, err
	}
	os.Chmod(path, 0o600)
	return &Cache{db: db, run: time.Now().UnixNano(), puts: map[string]cacheRow{}, touches: map[string]bool{}, lastFlush: time.Now()}, nil
}

func matches(fi FileInfo, dev, ino uint64, size, mtimeNS, ctimeNS int64) bool {
	return fi.Dev == dev && fi.Ino == ino && fi.Size == size && fi.MTimeNS == mtimeNS && fi.CTimeNS == ctimeNS
}

// Lookup returns the cached entry for path if fi still matches it.
func (c *Cache) Lookup(path string, fi FileInfo) (CacheEntry, bool) {
	c.mu.Lock()
	if r, ok := c.puts[path]; ok {
		c.mu.Unlock()
		return r.e, r.fi == fi
	}
	c.mu.Unlock()
	var (
		dev, ino               uint64
		size, mtimeNS, ctimeNS int64
		quick, full            []byte
	)
	err := c.db.QueryRow("SELECT dev, ino, size, mtime_ns, ctime_ns, quick, full FROM hashes WHERE path = ?", []byte(path)).
		Scan(&dev, &ino, &size, &mtimeNS, &ctimeNS, &quick, &full)
	if err != nil || !matches(fi, dev, ino, size, mtimeNS, ctimeNS) || len(quick) != len(Digest{}) {
		return CacheEntry{}, false
	}
	var e CacheEntry
	copy(e.Quick[:], quick)
	if len(full) == len(Digest{}) {
		var d Digest
		copy(d[:], full)
		e.Full = &d
	}
	return e, true
}

// Put records what is known about path.
func (c *Cache) Put(path string, fi FileInfo, e CacheEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts[path] = cacheRow{fi, e}
	return c.maybeFlushLocked()
}

// Touch marks path as seen in this run, so Prune keeps it.
func (c *Cache) Touch(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.touches[path] = true
	return c.maybeFlushLocked()
}

func (c *Cache) maybeFlushLocked() error {
	if len(c.puts)+len(c.touches) >= cacheFlushRows || time.Since(c.lastFlush) >= cacheFlushEvery {
		return c.flushLocked()
	}
	return nil
}

// Flush commits buffered writes.
func (c *Cache) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushLocked()
}

func (c *Cache) flushLocked() error {
	c.lastFlush = time.Now()
	if len(c.puts) == 0 && len(c.touches) == 0 {
		return nil
	}
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for p, r := range c.puts {
		var full any
		if r.e.Full != nil {
			full = r.e.Full[:]
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO hashes(path, dev, ino, size, mtime_ns, ctime_ns, quick, full, seen)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, []byte(p), r.fi.Dev, r.fi.Ino, r.fi.Size, r.fi.MTimeNS, r.fi.CTimeNS, r.e.Quick[:], full, c.run); err != nil {
			return err
		}
	}
	for p := range c.touches {
		if _, ok := c.puts[p]; ok {
			continue
		}
		if _, err := tx.Exec("UPDATE hashes SET seen = ? WHERE path = ?", c.run, []byte(p)); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	clear(c.puts)
	clear(c.touches)
	return nil
}

// Prune deletes entries not Put or Touched in this run and returns how many.
func (c *Cache) Prune() (int64, error) {
	if err := c.Flush(); err != nil {
		return 0, err
	}
	r, err := c.db.Exec("DELETE FROM hashes WHERE seen != ?", c.run)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// Close flushes and closes the database.
func (c *Cache) Close() error {
	return errors.Join(c.Flush(), c.db.Close())
}
