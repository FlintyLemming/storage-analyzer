package dupcheck

import (
	"bytes"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"slices"
)

// OutMember is one copy as stored in a result database.
type OutMember struct {
	EntryID, ParentID int64  // ids in the scan database
	Path              []byte // "<volume name>/<relative path>", raw bytes
	Covered           bool
}

// OutGroup is one listed group as stored in a result database.
type OutGroup struct {
	ID                                          int64
	Kind                                        int
	Name                                        []byte // last element of the first member's path
	Copies, Files, ASize, DSize, RASize, RDSize int64
	Members                                     []OutMember
}

const resultSchema = `
CREATE TABLE groups(
  id INTEGER PRIMARY KEY, kind INTEGER NOT NULL, name BLOB NOT NULL,
  copies INTEGER NOT NULL, files INTEGER NOT NULL,
  asize INTEGER NOT NULL, dsize INTEGER NOT NULL, rasize INTEGER NOT NULL, rdsize INTEGER NOT NULL,
  rank_d INTEGER NOT NULL, rank_a INTEGER NOT NULL
);
CREATE TABLE members(group_id INTEGER NOT NULL, entry_id INTEGER NOT NULL, parent_id INTEGER NOT NULL,
  path BLOB NOT NULL, covered INTEGER NOT NULL);
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
`

const resultIndexes = `
CREATE INDEX groups_rank_d ON groups(rank_d);
CREATE INDEX groups_rank_a ON groups(rank_a);
CREATE INDEX members_group ON members(group_id, path);
`

// order sorts groups for one size mode: size desc, reclaim desc, first path asc.
func order(gs []OutGroup, apparent bool) []int {
	idx := make([]int, len(gs))
	for i := range idx {
		idx[i] = i
	}
	pick := func(g OutGroup) (int64, int64) {
		if apparent {
			return g.ASize, g.RASize
		}
		return g.DSize, g.RDSize
	}
	slices.SortStableFunc(idx, func(a, b int) int {
		sa, ra := pick(gs[a])
		sb, rb := pick(gs[b])
		if sa != sb {
			return compareInt(sb, sa)
		}
		if ra != rb {
			return compareInt(rb, ra)
		}
		return bytes.Compare(gs[a].Members[0].Path, gs[b].Members[0].Path)
	})
	return idx
}

// WriteResult writes groups to path (via path+".tmp" and a rename).
func WriteResult(path string, groups []OutGroup, meta map[string]string) (err error) {
	for i := range groups {
		g := &groups[i]
		slices.SortFunc(g.Members, func(a, b OutMember) int { return bytes.Compare(a.Path, b.Path) })
		if g.Copies == 0 {
			g.Copies = int64(len(g.Members))
		}
		if g.Name == nil && len(g.Members) > 0 {
			p := g.Members[0].Path
			g.Name = p[bytes.LastIndexByte(p, '/')+1:]
		}
	}
	rankD, rankA := make([]int, len(groups)), make([]int, len(groups))
	for r, i := range order(groups, false) {
		rankD[i] = r
	}
	for r, i := range order(groups, true) {
		rankA[i] = r
	}

	tmp := path + ".tmp"
	os.Remove(tmp)
	db, err := sql.Open("sqlite", "file:"+(&url.URL{Path: tmp}).EscapedPath()+"?_pragma=journal_mode(OFF)&_pragma=synchronous(OFF)")
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	defer func() {
		if cerr := db.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			os.Remove(tmp)
		} else {
			err = os.Rename(tmp, path)
		}
	}()
	if _, err = db.Exec(resultSchema); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	gst, err := tx.Prepare(`INSERT INTO groups(id, kind, name, copies, files, asize, dsize, rasize, rdsize, rank_d, rank_a)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	mst, err := tx.Prepare("INSERT INTO members(group_id, entry_id, parent_id, path, covered) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	for i, g := range groups {
		id := int64(i + 1)
		if _, err = gst.Exec(id, g.Kind, g.Name, g.Copies, g.Files, g.ASize, g.DSize, g.RASize, g.RDSize, rankD[i], rankA[i]); err != nil {
			return err
		}
		for _, m := range g.Members {
			if _, err = mst.Exec(id, m.EntryID, m.ParentID, m.Path, m.Covered); err != nil {
				return err
			}
		}
	}
	for k, v := range meta {
		if _, err = tx.Exec("INSERT INTO meta(key, value) VALUES (?, ?)", k, v); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	_, err = db.Exec(resultIndexes)
	return err
}

// Result is an open, read-only result database.
type Result struct{ db *sql.DB }

// OpenResult opens a result database read-only.
func OpenResult(path string) (*Result, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+(&url.URL{Path: path}).EscapedPath()+"?mode=ro")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &Result{db: db}, nil
}

// Close closes the database.
func (r *Result) Close() error { return r.db.Close() }

// Total returns the number of groups.
func (r *Result) Total() (int, error) {
	var n int
	err := r.db.QueryRow("SELECT COUNT(*) FROM groups").Scan(&n)
	return n, err
}

// Page returns groups in display order for sizeMode ("disk" or "apparent"),
// each with at most maxMembers members (Copies keeps the full count).
func (r *Result) Page(sizeMode string, offset, limit, maxMembers int) ([]OutGroup, error) {
	rank := "rank_d"
	if sizeMode == "apparent" {
		rank = "rank_a"
	}
	rows, err := r.db.Query(`SELECT id, kind, name, copies, files, asize, dsize, rasize, rdsize FROM groups
		ORDER BY `+rank+` LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, err
	}
	var out []OutGroup
	for rows.Next() {
		var g OutGroup
		if err := rows.Scan(&g.ID, &g.Kind, &g.Name, &g.Copies, &g.Files, &g.ASize, &g.DSize, &g.RASize, &g.RDSize); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, g)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		mr, err := r.db.Query("SELECT entry_id, parent_id, path, covered FROM members WHERE group_id = ? ORDER BY path LIMIT ?", out[i].ID, maxMembers)
		if err != nil {
			return nil, err
		}
		for mr.Next() {
			var m OutMember
			if err := mr.Scan(&m.EntryID, &m.ParentID, &m.Path, &m.Covered); err != nil {
				mr.Close()
				return nil, err
			}
			out[i].Members = append(out[i].Members, m)
		}
		mr.Close()
		if err := mr.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Meta returns the meta table.
func (r *Result) Meta() (map[string]string, error) {
	rows, err := r.db.Query("SELECT key, value FROM meta")
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
	return m, errors.Join(rows.Err())
}
