// Package catalog keeps the list of scans and the user settings in
// dataDir/catalog.db.
package catalog

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned for an unknown scan id.
var ErrNotFound = errors.New("scan not found")

// Scan is one row of the scans table.
type Scan struct {
	ID           int64     `json:"id"`
	StartedAt    int64     `json:"startedAt"`
	FinishedAt   int64     `json:"finishedAt"`
	Trigger      string    `json:"trigger"` // manual | schedule
	Status       string    `json:"status"`  // running | done | failed | canceled
	TotalDSize   int64     `json:"totalDSize"`
	TotalASize   int64     `json:"totalASize"`
	Files        int64     `json:"files"`
	Dirs         int64     `json:"dirs"`
	Errors       int64     `json:"errors"`
	DurationS    int64     `json:"durationS"`
	DBBytes      int64     `json:"dbBytes"`
	ErrorMessage string    `json:"errorMessage"`
	Dup          *DupCheck `json:"dup"` // duplicate check of this scan, if any (List only)
}

// DupCheck is the duplicate check of one scan.
type DupCheck struct {
	ScanID       int64  `json:"scanId"`
	Status       string `json:"status"` // running | done | failed | canceled
	StartedAt    int64  `json:"startedAt"`
	FinishedAt   int64  `json:"finishedAt"`
	DurationS    int64  `json:"durationS"`
	Groups       int64  `json:"groups"`
	ReclaimDSize int64  `json:"reclaimDSize"`
	ReclaimASize int64  `json:"reclaimASize"`
	BytesRead    int64  `json:"bytesRead"`
	CacheHits    int64  `json:"cacheHits"`
	Changed      int64  `json:"changed"`
	Errors       int64  `json:"errors"`
	ErrorMessage string `json:"errorMessage"`
}

// Settings are the user-editable settings.
type Settings struct {
	ScheduleMode    string `json:"scheduleMode"`    // off | daily | weekly
	Weekday         int    `json:"weekday"`         // 0 = Sunday
	Time            string `json:"time"`            // "HH:MM"
	Retention       int    `json:"retention"`       // 1–100
	DefaultSizeMode string `json:"defaultSizeMode"` // disk | apparent
	// ScheduleDupCheck runs a duplicate check of each scheduled scan right
	// after it succeeds. Manual scans are not affected.
	ScheduleDupCheck bool `json:"scheduleDupCheck"`
}

// DefaultSettings are used until the user saves settings.
var DefaultSettings = Settings{ScheduleMode: "off", Weekday: 0, Time: "03:00", Retention: 10, DefaultSizeMode: "disk"}

var timeRe = regexp.MustCompile(`^([01]\d|2[0-3]):[0-5]\d$`)

// Validate reports whether s is acceptable.
func (s Settings) Validate() error {
	switch s.ScheduleMode {
	case "off", "daily", "weekly":
	default:
		return fmt.Errorf("invalid schedule mode %q", s.ScheduleMode)
	}
	if s.Weekday < 0 || s.Weekday > 6 {
		return fmt.Errorf("invalid weekday %d", s.Weekday)
	}
	if !timeRe.MatchString(s.Time) {
		return fmt.Errorf("invalid time %q", s.Time)
	}
	if s.Retention < 1 || s.Retention > 100 {
		return fmt.Errorf("retention must be 1–100, got %d", s.Retention)
	}
	if s.DefaultSizeMode != "disk" && s.DefaultSizeMode != "apparent" {
		return fmt.Errorf("invalid size mode %q", s.DefaultSizeMode)
	}
	return nil
}

// Catalog is the open catalog database.
type Catalog struct {
	db      *sql.DB
	dataDir string
}

// ScanPath is where the database of scan id lives.
func ScanPath(dataDir string, id int64) string {
	return filepath.Join(dataDir, "scans", "scan-"+strconv.FormatInt(id, 10)+".db")
}

// DupsPath is where the duplicate check result of scan id lives.
func DupsPath(dataDir string, id int64) string {
	return filepath.Join(dataDir, "scans", "dups-"+strconv.FormatInt(id, 10)+".db")
}

const schema = `
CREATE TABLE IF NOT EXISTS scans(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at INTEGER NOT NULL,
  finished_at INTEGER NOT NULL DEFAULT 0,
  trigger TEXT NOT NULL,
  status TEXT NOT NULL,
  total_dsize INTEGER NOT NULL DEFAULT 0,
  total_asize INTEGER NOT NULL DEFAULT 0,
  files INTEGER NOT NULL DEFAULT 0,
  dirs INTEGER NOT NULL DEFAULT 0,
  errors INTEGER NOT NULL DEFAULT 0,
  duration_s INTEGER NOT NULL DEFAULT 0,
  db_bytes INTEGER NOT NULL DEFAULT 0,
  error_message TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS dupchecks(
  scan_id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  started_at INTEGER NOT NULL DEFAULT 0,
  finished_at INTEGER NOT NULL DEFAULT 0,
  duration_s INTEGER NOT NULL DEFAULT 0,
  ngroups INTEGER NOT NULL DEFAULT 0,
  reclaim_dsize INTEGER NOT NULL DEFAULT 0,
  reclaim_asize INTEGER NOT NULL DEFAULT 0,
  bytes_read INTEGER NOT NULL DEFAULT 0,
  cache_hits INTEGER NOT NULL DEFAULT 0,
  changed INTEGER NOT NULL DEFAULT 0,
  errors INTEGER NOT NULL DEFAULT 0,
  error_message TEXT NOT NULL DEFAULT ''
);
`

// Open opens (creating if needed) dataDir/catalog.db and dataDir/scans.
func Open(dataDir string) (*Catalog, error) {
	// Scan results contain every user's file names: keep them root-only.
	for _, d := range []string{dataDir, filepath.Join(dataDir, "scans")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
		if err := os.Chmod(d, 0o700); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dataDir, "catalog.db")+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Catalog{db: db, dataDir: dataDir}, nil
}

// Close closes the database.
func (c *Catalog) Close() error { return c.db.Close() }

// Begin records a new running scan and returns its id.
func (c *Catalog) Begin(trigger string, now time.Time) (int64, error) {
	r, err := c.db.Exec("INSERT INTO scans(started_at, trigger, status) VALUES (?, ?, 'running')", now.Unix(), trigger)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

// Finish marks s.ID done and stores its results.
func (c *Catalog) Finish(s Scan) error {
	_, err := c.db.Exec(`UPDATE scans SET status='done', finished_at=?, total_dsize=?, total_asize=?, files=?, dirs=?,
		errors=?, duration_s=?, db_bytes=?, error_message='' WHERE id=?`,
		s.FinishedAt, s.TotalDSize, s.TotalASize, s.Files, s.Dirs, s.Errors, s.DurationS, s.DBBytes, s.ID)
	return err
}

// Fail marks a scan failed or canceled.
func (c *Catalog) Fail(id int64, status, msg string, now time.Time) error {
	_, err := c.db.Exec(`UPDATE scans SET status=?, error_message=?, finished_at=?, duration_s=MAX(0, ? - started_at) WHERE id=?`,
		status, msg, now.Unix(), now.Unix(), id)
	return err
}

const scanCols = `id, started_at, finished_at, trigger, status, total_dsize, total_asize, files, dirs, errors, duration_s, db_bytes, error_message`

func scanRow(r interface{ Scan(...any) error }) (Scan, error) {
	var s Scan
	err := r.Scan(&s.ID, &s.StartedAt, &s.FinishedAt, &s.Trigger, &s.Status, &s.TotalDSize, &s.TotalASize,
		&s.Files, &s.Dirs, &s.Errors, &s.DurationS, &s.DBBytes, &s.ErrorMessage)
	return s, err
}

// List returns all scans, newest first.
func (c *Catalog) List() ([]Scan, error) {
	rows, err := c.db.Query("SELECT " + scanCols + " FROM scans ORDER BY started_at DESC, id DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Scan{}
	for rows.Next() {
		s, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	dups, err := c.dupChecks()
	if err != nil {
		return nil, err
	}
	for i := range out {
		if d, ok := dups[out[i].ID]; ok {
			out[i].Dup = &d
		}
	}
	return out, nil
}

// Get returns one scan or ErrNotFound.
func (c *Catalog) Get(id int64) (Scan, error) {
	s, err := scanRow(c.db.QueryRow("SELECT "+scanCols+" FROM scans WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return Scan{}, ErrNotFound
	}
	return s, err
}

// Delete removes the scan row and its database file.
func (c *Catalog) Delete(id int64) error {
	r, err := c.db.Exec("DELETE FROM scans WHERE id=?", id)
	if err != nil {
		return err
	}
	if n, _ := r.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	if _, err := c.db.Exec("DELETE FROM dupchecks WHERE scan_id=?", id); err != nil {
		return err
	}
	for _, p := range []string{ScanPath(c.dataDir, id), DupsPath(c.dataDir, id)} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Prune deletes the oldest done scans beyond keep and returns their ids.
func (c *Catalog) Prune(keep int) ([]int64, error) {
	rows, err := c.db.Query("SELECT id FROM scans WHERE status='done' ORDER BY started_at DESC, id DESC LIMIT -1 OFFSET ?", keep)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if err := c.Delete(id); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// MarkStaleRunning turns scans left in "running" (e.g. after a reboot) into failed.
func (c *Catalog) MarkStaleRunning(now time.Time) (int, error) {
	r, err := c.db.Exec(`UPDATE scans SET status='failed', error_message='interrupted', finished_at=?,
		duration_s=MAX(0, ? - started_at) WHERE status='running'`, now.Unix(), now.Unix())
	if err != nil {
		return 0, err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return 0, err
	}
	r, err = c.db.Exec(`UPDATE dupchecks SET status='failed', error_message='interrupted', finished_at=?,
		duration_s=MAX(0, ? - started_at) WHERE status='running'`, now.Unix(), now.Unix())
	if err != nil {
		return 0, err
	}
	m, err := r.RowsAffected()
	return int(n + m), err
}

// Settings returns the saved settings, or DefaultSettings.
func (c *Catalog) Settings() (Settings, error) {
	s := DefaultSettings
	rows, err := c.db.Query("SELECT key, value FROM settings")
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return s, err
		}
		switch k {
		case "schedule_mode":
			s.ScheduleMode = v
		case "schedule_weekday":
			s.Weekday, _ = strconv.Atoi(v)
		case "schedule_time":
			s.Time = v
		case "retention_count":
			s.Retention, _ = strconv.Atoi(v)
		case "default_size_mode":
			s.DefaultSizeMode = v
		case "schedule_dupcheck":
			s.ScheduleDupCheck = v == "1"
		}
	}
	return s, rows.Err()
}

// SaveSettings validates and stores s.
func (c *Catalog) SaveSettings(s Settings) error {
	if err := s.Validate(); err != nil {
		return err
	}
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for k, v := range map[string]string{
		"schedule_mode":     s.ScheduleMode,
		"schedule_weekday":  strconv.Itoa(s.Weekday),
		"schedule_time":     s.Time,
		"retention_count":   strconv.Itoa(s.Retention),
		"default_size_mode": s.DefaultSizeMode,
		"schedule_dupcheck": boolText(s.ScheduleDupCheck),
	} {
		if _, err := tx.Exec("INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)", k, v); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func boolText(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

const dupCols = `scan_id, status, started_at, finished_at, duration_s, ngroups, reclaim_dsize, reclaim_asize,
	bytes_read, cache_hits, changed, errors, error_message`

func dupRow(r interface{ Scan(...any) error }) (DupCheck, error) {
	var d DupCheck
	err := r.Scan(&d.ScanID, &d.Status, &d.StartedAt, &d.FinishedAt, &d.DurationS, &d.Groups, &d.ReclaimDSize,
		&d.ReclaimASize, &d.BytesRead, &d.CacheHits, &d.Changed, &d.Errors, &d.ErrorMessage)
	return d, err
}

func (c *Catalog) dupChecks() (map[int64]DupCheck, error) {
	rows, err := c.db.Query("SELECT " + dupCols + " FROM dupchecks")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[int64]DupCheck{}
	for rows.Next() {
		d, err := dupRow(rows)
		if err != nil {
			return nil, err
		}
		m[d.ScanID] = d
	}
	return m, rows.Err()
}

// BeginDupCheck records a running duplicate check of scanID, replacing any
// earlier one.
func (c *Catalog) BeginDupCheck(scanID int64, now time.Time) error {
	_, err := c.db.Exec("INSERT OR REPLACE INTO dupchecks(scan_id, status, started_at) VALUES (?, 'running', ?)", scanID, now.Unix())
	return err
}

// FinishDupCheck marks d.ScanID's check done and stores its results.
func (c *Catalog) FinishDupCheck(d DupCheck) error {
	_, err := c.db.Exec(`UPDATE dupchecks SET status='done', finished_at=?, duration_s=?, ngroups=?, reclaim_dsize=?,
		reclaim_asize=?, bytes_read=?, cache_hits=?, changed=?, errors=?, error_message='' WHERE scan_id=?`,
		d.FinishedAt, d.DurationS, d.Groups, d.ReclaimDSize, d.ReclaimASize, d.BytesRead, d.CacheHits, d.Changed, d.Errors, d.ScanID)
	return err
}

// FailDupCheck marks a duplicate check failed or canceled.
func (c *Catalog) FailDupCheck(scanID int64, status, msg string, now time.Time) error {
	_, err := c.db.Exec(`UPDATE dupchecks SET status=?, error_message=?, finished_at=?, duration_s=MAX(0, ? - started_at)
		WHERE scan_id=?`, status, msg, now.Unix(), now.Unix(), scanID)
	return err
}

// GetDupCheck returns the duplicate check of scanID or ErrNotFound.
func (c *Catalog) GetDupCheck(scanID int64) (DupCheck, error) {
	d, err := dupRow(c.db.QueryRow("SELECT "+dupCols+" FROM dupchecks WHERE scan_id=?", scanID))
	if errors.Is(err, sql.ErrNoRows) {
		return DupCheck{}, ErrNotFound
	}
	return d, err
}

// AdjustScanTotals subtracts deleted sizes and entry counts from a finished
// scan's catalog summary, clamping each field at 0.
func (c *Catalog) AdjustScanTotals(id, deltaDSize, deltaASize, deletedFiles, deletedDirs int64) error {
	_, err := c.db.Exec(`UPDATE scans
		SET total_dsize = MAX(0, total_dsize - ?),
		    total_asize = MAX(0, total_asize - ?),
		    files = MAX(0, files - ?),
		    dirs = MAX(0, dirs - ?)
		WHERE id = ?`,
		deltaDSize, deltaASize, deletedFiles, deletedDirs, id)
	return err
}

// UpdateDupSummary updates the cached duplicate group count and reclaimable
// totals for a completed duplicate check after files are deleted.
func (c *Catalog) UpdateDupSummary(scanID, groups, reclaimDSize, reclaimASize int64) error {
	_, err := c.db.Exec(`UPDATE dupchecks
		SET ngroups = ?, reclaim_dsize = ?, reclaim_asize = ?
		WHERE scan_id = ? AND status = 'done'`,
		groups, reclaimDSize, reclaimASize, scanID)
	return err
}
