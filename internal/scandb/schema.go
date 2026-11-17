// Package scandb stores one scan result as a single SQLite file.
//
// Every entry (volume root, directory, file, ...) is one row. The children of
// a directory always have contiguous ids [FirstChild, FirstChild+NChild), so a
// directory listing is a rowid range scan and no index on parent is needed.
// Volume roots have parent 0 and ids [1, roots].
package scandb

import "errors"

// Entry kinds.
const (
	KindFile    = 0
	KindDir     = 1
	KindSymlink = 2
	KindOther   = 3
	KindMount   = 4 // mount point inside a volume, not descended
)

// Entry flags (bit set).
const (
	FlagError       = 1 // stat/readdir failed
	FlagHardlinkDup = 2 // hard link already counted elsewhere; not in parent totals
	FlagBadName     = 4 // name is not valid UTF-8
)

// MetaRoots is the meta key holding the number of volume roots.
const MetaRoots = "roots"

// ErrNotFound is returned when an entry id does not exist.
var ErrNotFound = errors.New("entry not found")

// Entry is one row of the entries table.
type Entry struct {
	ID, Parent int64
	Name       []byte
	Kind       int
	Flags      int
	DSize      int64 // disk usage (st_blocks*512); directories: subtree total
	ASize      int64 // apparent size (st_size); directories: subtree total
	Items      int64 // directories: number of entries in the subtree
	MTime      int64
	FirstChild int64
	NChild     int64
}

const schemaSQL = `
CREATE TABLE entries(
  id          INTEGER PRIMARY KEY,
  parent      INTEGER NOT NULL,
  name        BLOB    NOT NULL,
  kind        INTEGER NOT NULL,
  flags       INTEGER NOT NULL,
  dsize       INTEGER NOT NULL,
  asize       INTEGER NOT NULL,
  items       INTEGER NOT NULL,
  mtime       INTEGER NOT NULL,
  first_child INTEGER NOT NULL,
  nchild      INTEGER NOT NULL
);
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
`

const entryCols = `id, parent, name, kind, flags, dsize, asize, items, mtime, first_child, nchild`
