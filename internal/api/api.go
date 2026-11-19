// Package api serves the JSON API used by the web UI (through QTS CGI).
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"unicode/utf8"

	"storage-analyzer/internal/catalog"
	"storage-analyzer/internal/dupcheck"
	"storage-analyzer/internal/runner"
	"storage-analyzer/internal/scandb"
	"storage-analyzer/internal/schedule"
)

// Config wires the handler to the filesystem and the outside world.
type Config struct {
	DataDir     string
	BinPath     string
	CrontabPath string
	Auth        Authenticator
	Spawn       func(args ...string) error
	RunCmd      func(name string, args ...string) error
}

// Item is one entry as returned to the UI.
type Item struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	BadName     bool   `json:"badName"`
	Kind        int    `json:"kind"`
	DSize       int64  `json:"dsize"`
	ASize       int64  `json:"asize"`
	Items       int64  `json:"items"`
	MTime       int64  `json:"mtime"`
	Flags       int    `json:"flags"`
	HasChildren bool   `json:"hasChildren"`
}

func toItem(e scandb.Entry) Item {
	return Item{
		ID: e.ID, Name: strings.ToValidUTF8(string(e.Name), "�"), BadName: e.Flags&scandb.FlagBadName != 0,
		Kind: e.Kind, DSize: e.DSize, ASize: e.ASize, Items: e.Items, MTime: e.MTime, Flags: e.Flags,
		HasChildren: e.NChild > 0,
	}
}

type handler struct{ cfg Config }

// New returns the API handler. Requests select an operation with ?action=.
func New(cfg Config) http.Handler {
	if cfg.Spawn == nil {
		cfg.Spawn = func(args ...string) error { return spawnDetached(cfg.BinPath, args...) }
	}
	if cfg.RunCmd == nil {
		cfg.RunCmd = func(name string, args ...string) error { return exec.Command(name, args...).Run() }
	}
	if cfg.CrontabPath == "" {
		cfg.CrontabPath = "/etc/config/crontab"
	}
	return &handler{cfg: cfg}
}

// spawnDetached starts bin in its own session so it outlives the CGI request.
func spawnDetached(bin string, args ...string) error {
	cmd := exec.Command(bin, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Dir = "/"
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}

type apiError struct {
	code   int
	key    string
	detail string
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, key, msg string) {
	writeJSON(w, code, map[string]string{"error": key, "message": msg})
}

var mutating = map[string]bool{"scan_start": true, "scan_cancel": true, "scan_delete": true, "dup_start": true}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	action := r.URL.Query().Get("action")
	_, admin, err := h.cfg.Auth.Check(r)
	if err != nil {
		if errors.Is(err, ErrNotLoggedIn) {
			fail(w, 401, "not_logged_in", "not logged in to QTS")
		} else {
			log.Printf("auth check: %v", err)
			fail(w, 502, "auth_unavailable", err.Error())
		}
		return
	}
	if !admin {
		fail(w, 403, "not_admin", "administrator access required")
		return
	}
	isPost := r.Method == http.MethodPost
	if mutating[action] && !isPost {
		fail(w, 405, "method_not_allowed", "POST required")
		return
	}
	if isPost && r.Header.Get("X-SA-Request") != "1" {
		fail(w, 403, "bad_request_origin", "missing X-SA-Request header")
		return
	}
	switch action {
	case "status":
		h.status(w)
	case "scans":
		h.scans(w)
	case "scan_start":
		h.scanStart(w)
	case "scan_cancel":
		h.scanCancel(w)
	case "scan_delete":
		h.scanDelete(w, r)
	case "list":
		h.list(w, r)
	case "dup_start":
		h.dupStart(w, r)
	case "dups":
		h.dups(w, r)
	case "settings":
		h.settings(w, r)
	default:
		fail(w, 400, "unknown_action", "unknown action "+strconv.Quote(action))
	}
}

func (h *handler) internal(w http.ResponseWriter, err error) {
	log.Printf("internal error: %v", err)
	fail(w, 500, "internal", err.Error())
}

func (h *handler) status(w http.ResponseWriter) {
	p, running, err := runner.Running(h.cfg.DataDir)
	if err != nil {
		h.internal(w, err)
		return
	}
	if !running {
		writeJSON(w, 200, map[string]any{"running": false, "progress": nil})
		return
	}
	writeJSON(w, 200, map[string]any{"running": true, "progress": p})
}

func (h *handler) scans(w http.ResponseWriter) {
	c, err := catalog.Open(h.cfg.DataDir)
	if err != nil {
		h.internal(w, err)
		return
	}
	defer c.Close()
	list, err := c.List()
	if err != nil {
		h.internal(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"scans": list})
}

func (h *handler) scanStart(w http.ResponseWriter) {
	if _, running, err := runner.Running(h.cfg.DataDir); err != nil {
		h.internal(w, err)
		return
	} else if running {
		fail(w, 409, "task_running", "another task is already running")
		return
	}
	if err := h.cfg.Spawn("scan", "--trigger", "manual"); err != nil {
		h.internal(w, err)
		return
	}
	writeJSON(w, 202, map[string]any{"started": true})
}

func (h *handler) scanCancel(w http.ResponseWriter) {
	err := runner.Cancel(h.cfg.DataDir)
	if errors.Is(err, runner.ErrNotRunning) {
		fail(w, 404, "not_running", "no scan is running")
		return
	}
	if err != nil {
		h.internal(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"canceled": true})
}

func (h *handler) scanDelete(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.URL.Query().Get("scan"), 10, 64)
	if err != nil {
		fail(w, 400, "bad_scan_id", "invalid scan id")
		return
	}
	if p, running, _ := runner.Running(h.cfg.DataDir); running && p.ScanID == id {
		fail(w, 409, "scan_running", "scan is still running; cancel it first")
		return
	}
	c, err := catalog.Open(h.cfg.DataDir)
	if err != nil {
		h.internal(w, err)
		return
	}
	defer c.Close()
	if err := c.Delete(id); errors.Is(err, catalog.ErrNotFound) {
		fail(w, 404, "scan_not_found", "scan not found")
		return
	} else if err != nil {
		h.internal(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"deleted": id})
}

func atoiDefault(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c, err := catalog.Open(h.cfg.DataDir)
	if err != nil {
		h.internal(w, err)
		return
	}
	defer c.Close()
	var scan catalog.Scan
	if s := q.Get("scan"); s != "" {
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			fail(w, 400, "bad_scan_id", "invalid scan id")
			return
		}
		scan, err = c.Get(id)
		if err != nil || scan.Status != "done" {
			fail(w, 404, "scan_not_found", "scan not found")
			return
		}
	} else {
		list, err := c.List()
		if err != nil {
			h.internal(w, err)
			return
		}
		found := false
		for _, s := range list {
			if s.Status == "done" {
				scan, found = s, true
				break
			}
		}
		if !found {
			fail(w, 404, "no_scans", "no scan results yet")
			return
		}
	}
	dbPath := catalog.ScanPath(h.cfg.DataDir, scan.ID)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fail(w, 404, "scan_not_found", "scan not found")
		return
	}
	db, err := scandb.Open(dbPath)
	if err != nil {
		h.internal(w, err)
		return
	}
	defer db.Close()

	dirID := int64(0)
	if s := q.Get("dir"); s != "" {
		if dirID, err = strconv.ParseInt(s, 10, 64); err != nil {
			fail(w, 400, "bad_dir", "invalid dir")
			return
		}
	}
	dir, err := db.Get(dirID)
	if errors.Is(err, scandb.ErrNotFound) {
		fail(w, 404, "dir_not_found", "directory not found")
		return
	} else if err != nil {
		h.internal(w, err)
		return
	}
	path, err := db.Path(dirID)
	if err != nil {
		h.internal(w, err)
		return
	}
	limit := atoiDefault(q.Get("limit"), 500)
	if limit < 1 || limit > 1000 {
		limit = 500
	}
	opts := scandb.ListOpts{
		Sort: q.Get("sort"), Desc: q.Get("order") != "asc", SizeMode: q.Get("size"),
		Offset: max(0, atoiDefault(q.Get("offset"), 0)), Limit: limit,
	}
	if opts.Sort == "name" && q.Get("order") == "" {
		opts.Desc = false
	}
	children, total, err := db.Children(dirID, opts)
	if err != nil {
		h.internal(w, err)
		return
	}
	resp := map[string]any{"scan": scan, "dir": toItem(dir), "total": total}
	pi := make([]Item, len(path))
	for i, e := range path {
		pi[i] = toItem(e)
	}
	resp["path"] = pi
	items := make([]Item, len(children))
	for i, e := range children {
		items[i] = toItem(e)
	}
	resp["items"] = items
	if dirID == 0 {
		meta, err := db.Meta()
		if err != nil {
			h.internal(w, err)
			return
		}
		resp["volumes"] = json.RawMessage(orEmpty(meta[runner.MetaVolumes]))
		resp["skipped"] = json.RawMessage(orEmpty(meta[runner.MetaSkipped]))
	}
	writeJSON(w, 200, resp)
}

func orEmpty(s string) string {
	if s == "" {
		return "[]"
	}
	return s
}

func (h *handler) settings(w http.ResponseWriter, r *http.Request) {
	c, err := catalog.Open(h.cfg.DataDir)
	if err != nil {
		h.internal(w, err)
		return
	}
	defer c.Close()
	if r.Method != http.MethodPost {
		s, err := c.Settings()
		if err != nil {
			h.internal(w, err)
			return
		}
		writeJSON(w, 200, s)
		return
	}
	var s catalog.Settings
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err := json.Unmarshal(body, &s); err != nil {
		fail(w, 400, "invalid_settings", err.Error())
		return
	}
	if err := c.SaveSettings(s); err != nil {
		fail(w, 400, "invalid_settings", err.Error())
		return
	}
	if err := schedule.Apply(s, h.cfg.BinPath, h.cfg.CrontabPath, h.cfg.RunCmd); err != nil {
		h.internal(w, err)
		return
	}
	writeJSON(w, 200, s)
}

// doneScan returns scan id if it is done and its database exists.
func (h *handler) doneScan(c *catalog.Catalog, idText string) (catalog.Scan, bool) {
	id, err := strconv.ParseInt(idText, 10, 64)
	if err != nil {
		return catalog.Scan{}, false
	}
	s, err := c.Get(id)
	if err != nil || s.Status != "done" {
		return catalog.Scan{}, false
	}
	if _, err := os.Stat(catalog.ScanPath(h.cfg.DataDir, id)); err != nil {
		return catalog.Scan{}, false
	}
	return s, true
}

func (h *handler) dupStart(w http.ResponseWriter, r *http.Request) {
	if _, running, err := runner.Running(h.cfg.DataDir); err != nil {
		h.internal(w, err)
		return
	} else if running {
		fail(w, 409, "task_running", "another task is already running")
		return
	}
	c, err := catalog.Open(h.cfg.DataDir)
	if err != nil {
		h.internal(w, err)
		return
	}
	scan, ok := h.doneScan(c, r.URL.Query().Get("scan"))
	c.Close()
	if !ok {
		fail(w, 404, "scan_not_found", "scan not found")
		return
	}
	if err := h.cfg.Spawn("dupcheck", "--scan", strconv.FormatInt(scan.ID, 10)); err != nil {
		h.internal(w, err)
		return
	}
	writeJSON(w, 202, map[string]any{"started": true})
}

// DupMember is one copy of a duplicate group as returned to the UI.
type DupMember struct {
	EntryID  int64  `json:"entryId"`
	ParentID int64  `json:"parentId"`
	Path     string `json:"path"`
	BadName  bool   `json:"badName"`
	Covered  bool   `json:"covered"`
}

// DupGroup is one duplicate group as returned to the UI.
type DupGroup struct {
	ID      int64       `json:"id"`
	Kind    int         `json:"kind"`
	Name    string      `json:"name"`
	BadName bool        `json:"badName"`
	Copies  int64       `json:"copies"`
	Files   int64       `json:"files"`
	ASize   int64       `json:"asize"`
	DSize   int64       `json:"dsize"`
	RASize  int64       `json:"rasize"`
	RDSize  int64       `json:"rdsize"`
	Members []DupMember `json:"members"`
}

func validName(b []byte) (string, bool) {
	s := string(b)
	return strings.ToValidUTF8(s, "\uFFFD"), utf8.ValidString(s)
}

const maxDupMembers = 100

func (h *handler) dups(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c, err := catalog.Open(h.cfg.DataDir)
	if err != nil {
		h.internal(w, err)
		return
	}
	defer c.Close()
	scan, ok := h.doneScan(c, q.Get("scan"))
	if !ok {
		fail(w, 404, "scan_not_found", "scan not found")
		return
	}
	resp := map[string]any{"scan": scan, "check": nil, "result": nil, "total": 0, "groups": []DupGroup{}}
	if d, err := c.GetDupCheck(scan.ID); err == nil {
		resp["check"] = d
	} else if !errors.Is(err, catalog.ErrNotFound) {
		h.internal(w, err)
		return
	}
	res, err := dupcheck.OpenResult(catalog.DupsPath(h.cfg.DataDir, scan.ID))
	if errors.Is(err, os.ErrNotExist) {
		writeJSON(w, 200, resp)
		return
	} else if err != nil {
		h.internal(w, err)
		return
	}
	defer res.Close()
	meta, err := res.Meta()
	if err != nil {
		h.internal(w, err)
		return
	}
	summary := map[string]int64{}
	for _, k := range []string{"groups", "reclaimDSize", "reclaimASize", "bytesRead", "cacheHits", "changed", "errors", "finishedAt"} {
		summary[k], _ = strconv.ParseInt(meta[k], 10, 64)
	}
	resp["result"] = summary
	total, err := res.Total()
	if err != nil {
		h.internal(w, err)
		return
	}
	limit := atoiDefault(q.Get("limit"), 100)
	if limit < 1 || limit > 500 {
		limit = 100
	}
	page, err := res.Page(q.Get("size"), max(0, atoiDefault(q.Get("offset"), 0)), limit, maxDupMembers)
	if err != nil {
		h.internal(w, err)
		return
	}
	groups := make([]DupGroup, len(page))
	for i, g := range page {
		name, ok := validName(g.Name)
		dg := DupGroup{ID: g.ID, Kind: g.Kind, Name: name, BadName: !ok, Copies: g.Copies, Files: g.Files,
			ASize: g.ASize, DSize: g.DSize, RASize: g.RASize, RDSize: g.RDSize, Members: make([]DupMember, len(g.Members))}
		for j, m := range g.Members {
			p, ok := validName(m.Path)
			dg.Members[j] = DupMember{EntryID: m.EntryID, ParentID: m.ParentID, Path: p, BadName: !ok, Covered: m.Covered}
		}
		groups[i] = dg
	}
	resp["total"], resp["groups"] = total, groups
	writeJSON(w, 200, resp)
}
