// Package api serves the JSON API used by the web UI (through QTS CGI).
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/deleter"
	"github.com/FlintyLemming/storage-analyzer/internal/dupcheck"
	"github.com/FlintyLemming/storage-analyzer/internal/runner"
	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

// Config wires the handler to the filesystem and the outside world.
type Config struct {
	DataDir            string
	BinPath            string
	Auth               Authenticator
	Discover           func() ([]volumes.Volume, []volumes.Skipped, error)
	Spawn              func(args ...string) error   // nil: SpawnDetached(BinPath, args...)
	ApplySchedule      func(catalog.Settings) error // called after settings are saved; nil: nothing to do
	LoginKind          string                       // non-empty: 401 responses carry "auth": LoginKind
	NotLoggedInMessage string                       // optional custom 401 message when LoginKind is empty
	DeleteEnabled      bool
	DeletePolicy       deleter.Policy
}

// Item is one entry as returned to the UI.
type Item struct {
	ID          int64           `json:"id"`
	Name        string          `json:"name"`
	BadName     bool            `json:"badName"`
	Kind        int             `json:"kind"`
	DSize       int64           `json:"dsize"`
	ASize       int64           `json:"asize"`
	Items       int64           `json:"items"`
	MTime       int64           `json:"mtime"`
	Flags       int             `json:"flags"`
	HasChildren bool            `json:"hasChildren"`
	Docker      json.RawMessage `json:"docker,omitempty"`
}

func toItem(e scandb.Entry, anns map[int64]scandb.Annotation) Item {
	it := Item{
		ID: e.ID, Name: strings.ToValidUTF8(string(e.Name), "�"), BadName: e.Flags&scandb.FlagBadName != 0,
		Kind: e.Kind, DSize: e.DSize, ASize: e.ASize, Items: e.Items, MTime: e.MTime, Flags: e.Flags,
		HasChildren: e.NChild > 0,
	}
	if a, ok := anns[e.ID]; ok && a.Kind == "docker" && json.Valid([]byte(a.Data)) {
		it.Docker = json.RawMessage(a.Data)
	}
	return it
}

type handler struct{ cfg Config }

// New returns the API handler. Requests select an operation with ?action=.
func New(cfg Config) http.Handler {
	if cfg.Spawn == nil {
		cfg.Spawn = func(args ...string) error { return SpawnDetached(cfg.BinPath, args...) }
	}
	return &handler{cfg: cfg}
}

// SpawnDetached starts bin in its own session so it outlives the CGI request.
func SpawnDetached(bin string, args ...string) error {
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

var mutating = map[string]bool{
	"scan_start":     true,
	"scan_cancel":    true,
	"scan_delete":    true,
	"dup_start":      true,
	"delete_preview": true,
	"delete_start":   true,
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	action := r.URL.Query().Get("action")
	_, admin, err := h.cfg.Auth.Check(r)
	if err != nil {
		if errors.Is(err, ErrNotLoggedIn) {
			msg := "not logged in to QTS"
			if h.cfg.NotLoggedInMessage != "" {
				msg = h.cfg.NotLoggedInMessage
			} else if h.cfg.LoginKind != "" {
				msg = "not logged in"
			}
			body := map[string]string{"error": "not_logged_in", "message": msg}
			if h.cfg.LoginKind != "" {
				body["auth"] = h.cfg.LoginKind
			}
			writeJSON(w, 401, body)
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
	case "volumes":
		h.volumes(w)
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
	case "delete_preview":
		h.deletePreview(w, r)
	case "delete_start":
		h.deleteStart(w, r)
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
	resp := map[string]any{"running": running, "progress": nil, "deleteEnabled": h.cfg.DeleteEnabled}
	if running {
		resp["progress"] = p
	}
	if last, err := runner.LastDelete(h.cfg.DataDir); err == nil && last != nil {
		resp["lastDelete"] = last
	}
	writeJSON(w, 200, resp)
}

func (h *handler) volumes(w http.ResponseWriter) {
	if h.cfg.Discover == nil {
		h.internal(w, errors.New("volume discovery is not configured"))
		return
	}
	vols, skipped, err := h.cfg.Discover()
	if err != nil {
		h.internal(w, err)
		return
	}
	if skipped == nil {
		skipped = []volumes.Skipped{}
	}
	writeJSON(w, 200, map[string]any{"volumes": runner.StatVolumes(vols), "skipped": skipped})
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
	ids := make([]int64, 0, len(path)+len(children)+1)
	if dirID > 0 {
		ids = append(ids, dirID)
	}
	for _, e := range path {
		ids = append(ids, e.ID)
	}
	for _, e := range children {
		ids = append(ids, e.ID)
	}
	anns, err := db.Annotations(ids)
	if err != nil {
		h.internal(w, err)
		return
	}
	resp := map[string]any{"scan": scan, "dir": toItem(dir, anns), "total": total}
	pi := make([]Item, len(path))
	for i, e := range path {
		pi[i] = toItem(e, anns)
	}
	resp["path"] = pi
	items := make([]Item, len(children))
	for i, e := range children {
		items[i] = toItem(e, anns)
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
	if h.cfg.ApplySchedule != nil {
		if err := h.cfg.ApplySchedule(s); err != nil {
			h.internal(w, err)
			return
		}
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

const maxDeleteEntryIDs = 2000

type deleteReqBody struct {
	ScanID        int64   `json:"scanId"`
	EntryIDs      []int64 `json:"entryIds"`
	ConfirmDocker bool    `json:"confirmDocker"`
}

func (h *handler) parseAndResolveDelete(w http.ResponseWriter, r *http.Request) (deleteReqBody, deleter.ResolveResult, bool) {
	if !h.cfg.DeleteEnabled {
		fail(w, 403, "delete_disabled", "file deletion is disabled")
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil {
		fail(w, 400, "bad_delete_request", "could not read request body")
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	var req deleteReqBody
	if err := json.Unmarshal(body, &req); err != nil {
		fail(w, 400, "bad_delete_request", "invalid JSON: "+err.Error())
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	if req.ScanID <= 0 || len(req.EntryIDs) == 0 || len(req.EntryIDs) > maxDeleteEntryIDs {
		fail(w, 400, "bad_delete_request", fmt.Sprintf("scanId and 1..%d entryIds required", maxDeleteEntryIDs))
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	c, err := catalog.Open(h.cfg.DataDir)
	if err != nil {
		h.internal(w, err)
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	defer c.Close()

	list, err := c.List()
	if err != nil {
		h.internal(w, err)
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	var latestID int64
	foundTarget := false
	for _, s := range list {
		if s.Status != "done" {
			continue
		}
		if latestID == 0 {
			latestID = s.ID
		}
		if s.ID == req.ScanID {
			foundTarget = true
		}
	}
	if !foundTarget {
		fail(w, 404, "scan_not_found", "scan not found")
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	if req.ScanID != latestID {
		fail(w, 409, "not_latest_scan", "deletion is only allowed on the latest scan")
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}

	scanPath := catalog.ScanPath(h.cfg.DataDir, req.ScanID)
	if _, err := os.Stat(scanPath); os.IsNotExist(err) {
		fail(w, 404, "scan_not_found", "scan not found")
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	db, err := scandb.Open(scanPath)
	if err != nil {
		h.internal(w, err)
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	defer db.Close()

	_, roots, err := runner.LoadScanVolumes(db)
	if err != nil {
		h.internal(w, err)
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	resolved, err := deleter.ResolveTargets(db, roots, h.cfg.DeletePolicy, req.EntryIDs)
	if err != nil {
		h.internal(w, err)
		return deleteReqBody{}, deleter.ResolveResult{}, false
	}
	return req, resolved, true
}

func (h *handler) deletePreview(w http.ResponseWriter, r *http.Request) {
	req, resolved, ok := h.parseAndResolveDelete(w, r)
	if !ok {
		return
	}
	writeJSON(w, 200, map[string]any{
		"scanId":        req.ScanID,
		"items":         resolved.Targets,
		"folded":        resolved.Folded,
		"missing":       resolved.Missing,
		"totalDSize":    resolved.TotalDSize,
		"totalASize":    resolved.TotalASize,
		"totalItems":    resolved.TotalItems,
		"hasDockerRisk": resolved.HasDockerRisk,
		"hasProtected":  resolved.HasProtected,
	})
}

func newDeleteJobID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("del-%d-%s", time.Now().Unix(), hex.EncodeToString(b[:]))
}

func (h *handler) deleteStart(w http.ResponseWriter, r *http.Request) {
	if !h.cfg.DeleteEnabled {
		fail(w, 403, "delete_disabled", "file deletion is disabled")
		return
	}
	if _, running, err := runner.Running(h.cfg.DataDir); err != nil {
		h.internal(w, err)
		return
	} else if running {
		fail(w, 409, "task_running", "another task is already running")
		return
	}
	req, resolved, ok := h.parseAndResolveDelete(w, r)
	if !ok {
		return
	}
	if resolved.HasProtected {
		fail(w, 403, "protected_path", "one or more selected paths are protected")
		return
	}
	if len(resolved.Targets) == 0 {
		fail(w, 404, "entry_not_found", "no matching entries found to delete")
		return
	}
	if resolved.HasDockerRisk && !req.ConfirmDocker {
		fail(w, 400, "docker_confirm_required", "confirmation required to delete Docker directories")
		return
	}
	ids := make([]int64, len(resolved.Targets))
	for i, t := range resolved.Targets {
		ids[i] = t.EntryID
	}
	jobID := newDeleteJobID()
	reqPath, err := runner.WriteDeleteRequest(h.cfg.DataDir, runner.DeleteRequest{
		JobID:         jobID,
		ScanID:        req.ScanID,
		EntryIDs:      ids,
		ConfirmDocker: req.ConfirmDocker,
	})
	if err != nil {
		h.internal(w, err)
		return
	}
	if err := h.cfg.Spawn("delete", "--request", reqPath); err != nil {
		os.Remove(reqPath)
		h.internal(w, err)
		return
	}
	writeJSON(w, 202, map[string]any{"started": true, "jobId": jobID, "targets": len(ids)})
}
