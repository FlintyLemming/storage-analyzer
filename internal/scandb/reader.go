package scandb

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
)

// DB is a read-only scan database.
type DB struct {
	db    *sql.DB
	roots int64
}

// ListOpts controls a directory listing.
type ListOpts struct {
	Sort     string // size | name | items | mtime
	Desc     bool
	SizeMode string // disk | apparent
	Offset   int
	Limit    int
}

// Open opens a scan database read-only.
func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", "file:"+(&url.URL{Path: path}).EscapedPath()+"?mode=ro")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA temp_store=MEMORY"); err != nil {
		db.Close()
		return nil, err
	}
	d := &DB{db: db}
	var v string
	if err := db.QueryRow("SELECT value FROM meta WHERE key = ?", MetaRoots).Scan(&v); err != nil {
		db.Close()
		return nil, fmt.Errorf("read roots: %w", err)
	}
	if d.roots, err = strconv.ParseInt(v, 10, 64); err != nil {
		db.Close()
		return nil, err
	}
	return d, nil
}

// Close closes the database.
func (d *DB) Close() error { return d.db.Close() }

func scanEntry(row interface{ Scan(...any) error }) (Entry, error) {
	var e Entry
	err := row.Scan(&e.ID, &e.Parent, &e.Name, &e.Kind, &e.Flags, &e.DSize, &e.ASize, &e.Items, &e.MTime, &e.FirstChild, &e.NChild)
	return e, err
}

// Get returns one entry. id 0 is the synthetic "all volumes" root.
func (d *DB) Get(id int64) (Entry, error) {
	if id == 0 {
		e := Entry{Kind: KindDir, FirstChild: 1, NChild: d.roots}
		err := d.db.QueryRow(`SELECT COALESCE(SUM(dsize),0), COALESCE(SUM(asize),0), COALESCE(SUM(items),0) + COUNT(*), COALESCE(MAX(mtime),0)
			FROM entries WHERE id BETWEEN 1 AND ?`, d.roots).Scan(&e.DSize, &e.ASize, &e.Items, &e.MTime)
		return e, err
	}
	e, err := scanEntry(d.db.QueryRow("SELECT "+entryCols+" FROM entries WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, ErrNotFound
	}
	return e, err
}

// Children lists the children of id, sorted and paginated, plus the total count.
func (d *DB) Children(id int64, o ListOpts) ([]Entry, int, error) {
	parent, err := d.Get(id)
	if err != nil {
		return nil, 0, err
	}
	if parent.NChild == 0 {
		return nil, 0, nil
	}
	sizeCol := "dsize"
	if o.SizeMode == "apparent" {
		sizeCol = "asize"
	}
	col := map[string]string{"size": sizeCol, "name": "name", "items": "items", "mtime": "mtime"}[o.Sort]
	if col == "" {
		col = sizeCol
	}
	dir := "ASC"
	if o.Desc {
		dir = "DESC"
	}
	limit := o.Limit
	if limit <= 0 {
		limit = 500
	}
	q := fmt.Sprintf("SELECT %s FROM entries WHERE id >= ? AND id < ? ORDER BY %s %s, id ASC LIMIT ? OFFSET ?", entryCols, col, dir)
	rows, err := d.db.Query(q, parent.FirstChild, parent.FirstChild+parent.NChild, limit, o.Offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, e)
	}
	return out, int(parent.NChild), rows.Err()
}

// Path returns the chain of entries from the volume root down to id.
// For id 0 it returns an empty slice.
func (d *DB) Path(id int64) ([]Entry, error) {
	var rev []Entry
	for cur := id; cur != 0; {
		if len(rev) > 4096 {
			return nil, errors.New("path too deep or cyclic")
		}
		e, err := d.Get(cur)
		if err != nil {
			return nil, err
		}
		rev = append(rev, e)
		cur = e.Parent
	}
	out := make([]Entry, len(rev))
	for i, e := range rev {
		out[len(rev)-1-i] = e
	}
	return out, nil
}

// Meta returns all meta keys.
func (d *DB) Meta() (map[string]string, error) {
	rows, err := d.db.Query("SELECT key, value FROM meta")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, rows.Err()
}

// Roots returns the number of volume roots (ids 1..Roots()).
func (d *DB) Roots() int64 { return d.roots }

func (d *DB) each(q string, fn func(Entry) error, args ...any) error {
	rows, err := d.db.Query("SELECT "+entryCols+" FROM entries "+q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return rows.Err()
}

// EachDir calls fn for every directory in ascending id order (parents first).
func (d *DB) EachDir(fn func(Entry) error) error {
	return d.each("WHERE kind = ? ORDER BY id", fn, KindDir)
}

// EachDesc calls fn for every entry in descending id order (children first).
func (d *DB) EachDesc(fn func(Entry) error) error {
	return d.each("ORDER BY id DESC", fn)
}

// DupSizeFiles calls fn, in ascending id order, for every regular file that
// is not empty, not a hard-link duplicate, has no error, and shares its
// apparent size with at least one other such file.
func (d *DB) DupSizeFiles(fn func(Entry) error) error {
	const cond = "kind = ? AND flags & ? = 0 AND asize > 0"
	mask := FlagError | FlagHardlinkDup
	return d.each("WHERE "+cond+" AND asize IN (SELECT asize FROM entries WHERE "+cond+
		" GROUP BY asize HAVING COUNT(*) > 1) ORDER BY id", fn, KindFile, mask, KindFile, mask)
}
