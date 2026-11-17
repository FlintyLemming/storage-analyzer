package scandb

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

const batchRows = 16 // modernc binds parameters in O(n^2); small batches are ~15x faster than 1000-row batches

// Writer writes a new scan database. It is not safe for concurrent use.
type Writer struct {
	path  string
	db    *sql.DB
	tx    *sql.Tx
	full  *sql.Stmt // prepared INSERT for a full batch
	buf   []Entry
	meta  map[string]string
	inTx  int
	close bool
}

// Create creates (or replaces) the database at path.
func Create(path string) (*Writer, error) {
	_ = os.Remove(path)
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, p := range []string{
		"PRAGMA journal_mode=OFF",
		"PRAGMA synchronous=OFF",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA cache_size=-32768",
	} {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", p, err)
		}
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		os.Remove(path)
		return nil, err
	}
	w := &Writer{path: path, db: db, meta: map[string]string{}}
	if err := w.begin(); err != nil {
		w.Abort()
		return nil, err
	}
	return w, nil
}

func insertSQL(rows int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO entries(" + entryCols + ") VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?,?,?,?,?,?,?,?,?)")
	}
	return b.String()
}

func (w *Writer) begin() error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	w.tx = tx
	w.full, err = tx.Prepare(insertSQL(batchRows))
	return err
}

// Add buffers one entry; full batches are inserted immediately.
func (w *Writer) Add(e Entry) error {
	w.buf = append(w.buf, e)
	if len(w.buf) >= batchRows {
		return w.flush()
	}
	return nil
}

func (w *Writer) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	args := make([]any, 0, len(w.buf)*11)
	for _, e := range w.buf {
		name := e.Name
		if name == nil {
			name = []byte{}
		}
		args = append(args, e.ID, e.Parent, name, e.Kind, e.Flags, e.DSize, e.ASize, e.Items, e.MTime, e.FirstChild, e.NChild)
	}
	var err error
	if len(w.buf) == batchRows {
		_, err = w.full.Exec(args...)
	} else {
		_, err = w.tx.Exec(insertSQL(len(w.buf)), args...)
	}
	w.buf = w.buf[:0]
	if err != nil {
		return err
	}
	// Commit periodically so the transaction does not grow without bound.
	w.inTx++
	if w.inTx >= 4000 { // ~64k rows per transaction
		w.inTx = 0
		w.full.Close()
		if err := w.tx.Commit(); err != nil {
			return err
		}
		return w.begin()
	}
	return nil
}

// SetMeta records a meta key; meta is written on Close.
func (w *Writer) SetMeta(key, value string) error {
	w.meta[key] = value
	return nil
}

// Close flushes, writes meta and compacts the file (rows arrive out of id
// order, so pages are fragmented; VACUUM INTO rewrites them densely without
// needing the system temp directory).
func (w *Writer) Close() error {
	if err := w.flush(); err != nil {
		w.Abort()
		return err
	}
	for k, v := range w.meta {
		if _, err := w.tx.Exec("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", k, v); err != nil {
			w.Abort()
			return err
		}
	}
	w.full.Close()
	if err := w.tx.Commit(); err != nil {
		w.Abort()
		return err
	}
	w.tx = nil
	compact := w.path + ".compact"
	_ = os.Remove(compact)
	if _, err := w.db.Exec("VACUUM INTO ?", compact); err != nil {
		os.Remove(compact)
		w.Abort()
		return fmt.Errorf("vacuum: %w", err)
	}
	if err := w.db.Close(); err != nil {
		os.Remove(compact)
		os.Remove(w.path)
		return err
	}
	w.close = true
	return os.Rename(compact, w.path)
}

// Abort closes the database and deletes the file.
func (w *Writer) Abort() {
	if w.close {
		return
	}
	w.close = true
	if w.tx != nil {
		w.tx.Rollback()
	}
	w.db.Close()
	os.Remove(w.path)
	os.Remove(w.path + ".compact")
}
