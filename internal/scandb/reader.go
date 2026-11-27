package scandb

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// DB is a read-only scan database.
type DB struct {
	db             *sql.DB
	roots          int64
	hasAnnotations bool
	hasDeleted     bool
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
	var one int
	if err := db.QueryRow("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'annotations'").Scan(&one); err == nil {
		d.hasAnnotations = true
	}
	var dc string
	if err := db.QueryRow("SELECT value FROM meta WHERE key = ?", MetaDeletedCount).Scan(&dc); err == nil {
		if n, err := strconv.ParseInt(dc, 10, 64); err == nil && n > 0 {
			d.hasDeleted = true
		}
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
	if err != nil {
		return Entry{}, err
	}
	if e.Flags&FlagDeleted != 0 {
		return Entry{}, ErrNotFound
	}
	return e, nil
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
	total := int(parent.NChild)
	var rows *sql.Rows
	if d.hasDeleted {
		if err := d.db.QueryRow("SELECT COUNT(*) FROM entries WHERE id >= ? AND id < ? AND (flags & ?) = 0",
			parent.FirstChild, parent.FirstChild+parent.NChild, FlagDeleted).Scan(&total); err != nil {
			return nil, 0, err
		}
		if total == 0 {
			return nil, 0, nil
		}
		q := fmt.Sprintf("SELECT %s FROM entries WHERE id >= ? AND id < ? AND (flags & ?) = 0 ORDER BY %s %s, id ASC LIMIT ? OFFSET ?", entryCols, col, dir)
		rows, err = d.db.Query(q, parent.FirstChild, parent.FirstChild+parent.NChild, FlagDeleted, limit, o.Offset)
	} else {
		q := fmt.Sprintf("SELECT %s FROM entries WHERE id >= ? AND id < ? ORDER BY %s %s, id ASC LIMIT ? OFFSET ?", entryCols, col, dir)
		rows, err = d.db.Query(q, parent.FirstChild, parent.FirstChild+parent.NChild, limit, o.Offset)
	}
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
	return out, total, rows.Err()
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

// Annotations returns any persisted annotations for the given entry IDs.
// Legacy scan databases without an annotations table return an empty map.
func (d *DB) Annotations(ids []int64) (map[int64]Annotation, error) {
	out := map[int64]Annotation{}
	if !d.hasAnnotations || len(ids) == 0 {
		return out, nil
	}
	// Deduplicate positive IDs.
	seen := make(map[int64]struct{}, len(ids))
	uniq := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, id)
	}
	const chunkSize = 400
	for start := 0; start < len(uniq); start += chunkSize {
		end := start + chunkSize
		if end > len(uniq) {
			end = len(uniq)
		}
		chunk := uniq[start:end]
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		q := "SELECT id, kind, data FROM annotations WHERE id IN (?" + strings.Repeat(",?", len(chunk)-1) + ")"
		rows, err := d.db.Query(q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var a Annotation
			if err := rows.Scan(&id, &a.Kind, &a.Data); err != nil {
				rows.Close()
				return nil, err
			}
			out[id] = a
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

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

// EachDir calls fn for every non-deleted directory in ascending id order (parents first).
func (d *DB) EachDir(fn func(Entry) error) error {
	return d.each("WHERE kind = ? AND (flags & ?) = 0 ORDER BY id", fn, KindDir, FlagDeleted)
}

// EachDesc calls fn for every non-deleted entry in descending id order (children first).
func (d *DB) EachDesc(fn func(Entry) error) error {
	return d.each("WHERE (flags & ?) = 0 ORDER BY id DESC", fn, FlagDeleted)
}

// DupSizeFiles calls fn, in ascending id order, for every regular file that
// is not empty, not a hard-link duplicate, not deleted, has no error, and
// shares its apparent size with at least one other such file.
func (d *DB) DupSizeFiles(fn func(Entry) error) error {
	const cond = "kind = ? AND flags & ? = 0 AND asize > 0"
	mask := FlagError | FlagHardlinkDup | FlagDeleted
	return d.each("WHERE "+cond+" AND asize IN (SELECT asize FROM entries WHERE "+cond+
		" GROUP BY asize HAVING COUNT(*) > 1) ORDER BY id", fn, KindFile, mask, KindFile, mask)
}
