package scandb

import (
	"database/sql"
	"errors"
	"net/url"
	"strconv"
)

// DeletedTotals summarizes the aggregate contribution subtracted from ancestors
// by MarkDeleted.
type DeletedTotals struct {
	Count int
	DSize int64
	ASize int64
	Items int64
}

// MarkDeleted marks each entry in ids (ignoring roots, missing rows, already
// deleted rows, and descendants of deleted entries) with FlagDeleted, subtracts
// their contribution from all ancestor directories up to the volume root, and
// optionally updates the "volumes" meta entry with refreshed statfs data.
//
// The database is kept in rollback (DELETE) journal mode so read-only (?mode=ro)
// readers continue to open it without requiring -wal/-shm sidecar files.
func MarkDeleted(path string, ids []int64, volumesJSON string) (DeletedTotals, error) {
	var tot DeletedTotals
	if len(ids) == 0 && volumesJSON == "" {
		return tot, nil
	}
	dsn := "file:" + (&url.URL{Path: path}).EscapedPath() + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(DELETE)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return tot, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	tx, err := db.Begin()
	if err != nil {
		return tot, err
	}
	defer tx.Rollback()

	getEntry := func(id int64) (Entry, bool, error) {
		e, err := scanEntry(tx.QueryRow("SELECT "+entryCols+" FROM entries WHERE id = ?", id))
		if errors.Is(err, sql.ErrNoRows) {
			return Entry{}, false, nil
		}
		if err != nil {
			return Entry{}, false, err
		}
		return e, true, nil
	}

	// Build unique valid entries and their ancestor chains first so we can fold
	// any descendant whose ancestor is also in ids (or already marked deleted).
	type candidate struct {
		entry     Entry
		ancestors []int64 // parent, grandparent, ..., root
	}
	seen := make(map[int64]bool, len(ids))
	cands := make([]candidate, 0, len(ids))
	for _, id := range ids {
		if id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		e, ok, err := getEntry(id)
		if err != nil {
			return DeletedTotals{}, err
		}
		if !ok || e.Parent == 0 || e.Flags&FlagDeleted != 0 {
			continue
		}
		var ancs []int64
		ancDeleted := false
		for cur := e.Parent; cur != 0; {
			if len(ancs) > 4096 {
				return DeletedTotals{}, errors.New("path too deep or cyclic")
			}
			ae, ok, err := getEntry(cur)
			if err != nil {
				return DeletedTotals{}, err
			}
			if !ok || ae.Flags&FlagDeleted != 0 {
				ancDeleted = true
				break
			}
			ancs = append(ancs, ae.ID)
			cur = ae.Parent
		}
		if ancDeleted {
			continue
		}
		cands = append(cands, candidate{entry: e, ancestors: ancs})
	}

	candSet := make(map[int64]bool, len(cands))
	for _, c := range cands {
		candSet[c.entry.ID] = true
	}

	markStmt, err := tx.Prepare("UPDATE entries SET flags = flags | ? WHERE id = ?")
	if err != nil {
		return DeletedTotals{}, err
	}
	defer markStmt.Close()
	subStmt, err := tx.Prepare(`UPDATE entries
		SET dsize = MAX(0, dsize - ?), asize = MAX(0, asize - ?), items = MAX(0, items - ?)
		WHERE id = ?`)
	if err != nil {
		return DeletedTotals{}, err
	}
	defer subStmt.Close()

	for _, c := range cands {
		covered := false
		for _, ancID := range c.ancestors {
			if candSet[ancID] {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		e := c.entry
		if _, err := markStmt.Exec(FlagDeleted, e.ID); err != nil {
			return DeletedTotals{}, err
		}
		var dD, dA, dI int64
		if e.Kind == KindDir {
			dD, dA, dI = e.DSize, e.ASize, 1+e.Items
		} else {
			dI = 1
			if e.Flags&FlagHardlinkDup == 0 {
				dD, dA = e.DSize, e.ASize
			}
		}
		for _, ancID := range c.ancestors {
			if _, err := subStmt.Exec(dD, dA, dI, ancID); err != nil {
				return DeletedTotals{}, err
			}
		}
		tot.Count++
		tot.DSize += dD
		tot.ASize += dA
		tot.Items += dI
	}

	if tot.Count > 0 {
		var prev int64
		var s string
		if err := tx.QueryRow("SELECT value FROM meta WHERE key = ?", MetaDeletedCount).Scan(&s); err == nil {
			prev, _ = strconv.ParseInt(s, 10, 64)
		}
		if _, err := tx.Exec("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
			MetaDeletedCount, strconv.FormatInt(prev+int64(tot.Count), 10)); err != nil {
			return DeletedTotals{}, err
		}
	}
	if volumesJSON != "" {
		if _, err := tx.Exec("INSERT OR REPLACE INTO meta(key, value) VALUES ('volumes', ?)", volumesJSON); err != nil {
			return DeletedTotals{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return DeletedTotals{}, err
	}
	return tot, nil
}
