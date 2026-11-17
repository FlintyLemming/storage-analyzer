package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"storage-analyzer/internal/catalog"
	"storage-analyzer/internal/dupcheck"
	"storage-analyzer/internal/runner"
	"storage-analyzer/internal/scandb"
)

type fakeAuth struct {
	user  string
	admin bool
	err   error
}

func (f fakeAuth) Check(*http.Request) (string, bool, error) { return f.user, f.admin, f.err }

type env struct {
	t       *testing.T
	data    string
	h       http.Handler
	spawned [][]string
	ran     []string
	cron    string
}

func newEnv(t *testing.T, auth Authenticator) *env {
	e := &env{t: t, data: t.TempDir()}
	e.cron = filepath.Join(t.TempDir(), "crontab")
	os.WriteFile(e.cron, []byte("0 0 * * * /sbin/auto_update\n"), 0o644)
	e.h = New(Config{
		DataDir: e.data, BinPath: "/qpkg/bin/storage-analyzer", CrontabPath: e.cron, Auth: auth,
		Spawn:  func(args ...string) error { e.spawned = append(e.spawned, args); return nil },
		RunCmd: func(name string, args ...string) error { e.ran = append(e.ran, name+" "+strings.Join(args, " ")); return nil },
	})
	return e
}

func admin() Authenticator { return fakeAuth{user: "admin", admin: true} }

// makeScan writes a finished scan: Vol(1) → [big dir(2), small file(3), bad name(4)], big → [f(5)].
func (e *env) makeScan() int64 {
	c, err := catalog.Open(e.data)
	if err != nil {
		e.t.Fatal(err)
	}
	defer c.Close()
	id, _ := c.Begin("manual", time.Unix(1700000000, 0))
	w, err := scandb.Create(catalog.ScanPath(e.data, id))
	if err != nil {
		e.t.Fatal(err)
	}
	for _, en := range []scandb.Entry{
		{ID: 1, Name: []byte("DataVol1"), Kind: scandb.KindDir, DSize: 1111, ASize: 1111, Items: 4, FirstChild: 2, NChild: 3},
		{ID: 2, Parent: 1, Name: []byte("big"), Kind: scandb.KindDir, DSize: 1000, ASize: 1000, Items: 1, FirstChild: 5, NChild: 1},
		{ID: 3, Parent: 1, Name: []byte("small"), DSize: 10, ASize: 10},
		{ID: 4, Parent: 1, Name: []byte("bad\xffname"), DSize: 100, ASize: 100, Flags: scandb.FlagBadName},
		{ID: 5, Parent: 2, Name: []byte("f"), DSize: 1000, ASize: 1000},
	} {
		w.Add(en)
	}
	w.SetMeta(scandb.MetaRoots, "1")
	vj, _ := json.Marshal([]runner.VolumeMeta{{EntryID: 1, Name: "DataVol1", Pool: "2", MountPoint: "/share/CACHEDEV2_DATA", FSTotal: 5000, FSUsed: 2000, FSFree: 3000}})
	w.SetMeta(runner.MetaVolumes, string(vj))
	w.SetMeta(runner.MetaSkipped, "[]")
	if err := w.Close(); err != nil {
		e.t.Fatal(err)
	}
	c.Finish(catalog.Scan{ID: id, FinishedAt: 1700000060, TotalDSize: 1111, Files: 3})
	return id
}

func (e *env) do(method, url string, body string, hdr map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	e.t.Helper()
	req := httptest.NewRequest(method, url, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	e.h.ServeHTTP(rr, req)
	var m map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil && rr.Code != http.StatusNoContent {
		e.t.Fatalf("%s %s: invalid JSON %q: %v", method, url, rr.Body.String(), err)
	}
	return rr, m
}

var post = map[string]string{"X-SA-Request": "1"}

func TestAuthRequired(t *testing.T) {
	e := newEnv(t, fakeAuth{err: ErrNotLoggedIn})
	rr, m := e.do("GET", "/api.cgi?action=status", "", nil)
	if rr.Code != 401 || m["error"] != "not_logged_in" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func TestNonAdminForbidden(t *testing.T) {
	e := newEnv(t, fakeAuth{user: "bob"})
	rr, m := e.do("GET", "/api.cgi?action=scans", "", nil)
	if rr.Code != 403 || m["error"] != "not_admin" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func TestPostNeedsHeader(t *testing.T) {
	e := newEnv(t, admin())
	rr, m := e.do("POST", "/api.cgi?action=scan_start", "", nil)
	if rr.Code != 403 || m["error"] != "bad_request_origin" || len(e.spawned) != 0 {
		t.Fatalf("code=%d body=%v spawned=%v", rr.Code, m, e.spawned)
	}
	rr, _ = e.do("GET", "/api.cgi?action=scan_start", "", post)
	if rr.Code != 405 {
		t.Fatalf("GET scan_start code=%d, want 405", rr.Code)
	}
}

func TestUnknownAction(t *testing.T) {
	e := newEnv(t, admin())
	if rr, m := e.do("GET", "/api.cgi?action=nope", "", nil); rr.Code != 400 || m["error"] != "unknown_action" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func TestQTSAuthParsesXML(t *testing.T) {
	var gotSID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSID = r.URL.Query().Get("sid")
		if gotSID == "good" {
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8" ?><QDocRoot version="1.0"><authPassed><![CDATA[1]]></authPassed><username><![CDATA[admin]]></username><isAdmin><![CDATA[1]]></isAdmin></QDocRoot>`))
			return
		}
		if gotSID == "user" {
			w.Write([]byte(`<QDocRoot><authPassed><![CDATA[1]]></authPassed><username><![CDATA[bob]]></username><isAdmin><![CDATA[0]]></isAdmin></QDocRoot>`))
			return
		}
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8" ?><QDocRoot version="1.0"><authPassed><![CDATA[0]]></authPassed></QDocRoot>`))
	}))
	defer srv.Close()
	a := QTSAuth{URL: srv.URL}

	req := httptest.NewRequest("GET", "/api.cgi", nil)
	req.AddCookie(&http.Cookie{Name: "NAS_SID", Value: "good"})
	if u, adm, err := a.Check(req); err != nil || u != "admin" || !adm {
		t.Fatalf("u=%q admin=%v err=%v", u, adm, err)
	}
	req = httptest.NewRequest("GET", "/api.cgi?sid=user", nil)
	if u, adm, err := a.Check(req); err != nil || u != "bob" || adm {
		t.Fatalf("u=%q admin=%v err=%v", u, adm, err)
	}
	req = httptest.NewRequest("GET", "/api.cgi", nil)
	req.AddCookie(&http.Cookie{Name: "NAS_SID", Value: "expired"})
	if _, _, err := a.Check(req); !errors.Is(err, ErrNotLoggedIn) {
		t.Fatalf("err = %v", err)
	}
	req = httptest.NewRequest("GET", "/api.cgi", nil)
	if _, _, err := a.Check(req); !errors.Is(err, ErrNotLoggedIn) {
		t.Fatalf("no sid: err = %v", err)
	}
}

func TestListRootAndDrill(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	_, m := e.do("GET", "/api.cgi?action=list&scan="+itoa(id)+"&dir=0", "", nil)
	items := m["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["name"] != "DataVol1" {
		t.Fatalf("root items = %v", items)
	}
	vols := m["volumes"].([]any)
	if len(vols) != 1 || vols[0].(map[string]any)["fsUsed"].(float64) != 2000 {
		t.Fatalf("volumes = %v", vols)
	}
	_, m = e.do("GET", "/api.cgi?action=list&scan="+itoa(id)+"&dir=1&sort=size&order=desc", "", nil)
	var names []string
	for _, it := range m["items"].([]any) {
		names = append(names, it.(map[string]any)["name"].(string))
	}
	if strings.Join(names, ",") != "big,bad�name,small" {
		t.Fatalf("names = %v", names)
	}
	if m["total"].(float64) != 3 {
		t.Fatalf("total = %v", m["total"])
	}
	big := m["items"].([]any)[0].(map[string]any)
	if big["hasChildren"] != true || big["kind"].(float64) != scandb.KindDir {
		t.Fatalf("big = %v", big)
	}
	_, m = e.do("GET", "/api.cgi?action=list&scan="+itoa(id)+"&dir=5", "", nil)
	path := m["path"].([]any)
	if len(path) != 3 || path[0].(map[string]any)["name"] != "DataVol1" || path[2].(map[string]any)["name"] != "f" {
		t.Fatalf("path = %v", path)
	}
}

func TestListPaging(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	_, m := e.do("GET", "/api.cgi?action=list&scan="+itoa(id)+"&dir=1&offset=1&limit=1", "", nil)
	if items := m["items"].([]any); len(items) != 1 || items[0].(map[string]any)["id"].(float64) != 4 {
		t.Fatalf("page = %v", items)
	}
}

func TestListJSONBadName(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	rr, m := e.do("GET", "/api.cgi?action=list&scan="+itoa(id)+"&dir=1&sort=name&order=asc", "", nil)
	if !json.Valid(rr.Body.Bytes()) {
		t.Fatal("invalid JSON")
	}
	first := m["items"].([]any)[0].(map[string]any)
	if first["name"] != "bad�name" || first["badName"] != true {
		t.Fatalf("first = %v", first)
	}
}

func TestListDeletedScan(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	if rr, _ := e.do("POST", "/api.cgi?action=scan_delete&scan="+itoa(id), "", post); rr.Code != 200 {
		t.Fatalf("delete code = %d", rr.Code)
	}
	rr, m := e.do("GET", "/api.cgi?action=list&scan="+itoa(id)+"&dir=0", "", nil)
	if rr.Code != 404 || m["error"] != "scan_not_found" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func TestListDefaultsToLatestDone(t *testing.T) {
	e := newEnv(t, admin())
	e.makeScan()
	id2 := e.makeScan()
	_, m := e.do("GET", "/api.cgi?action=list", "", nil)
	if m["scan"].(map[string]any)["id"].(float64) != float64(id2) {
		t.Fatalf("scan = %v", m["scan"])
	}
	e2 := newEnv(t, admin())
	if rr, m := e2.do("GET", "/api.cgi?action=list", "", nil); rr.Code != 404 || m["error"] != "no_scans" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func lockData(t *testing.T, data string) {
	os.MkdirAll(filepath.Join(data, "run"), 0o755)
	f, _ := os.OpenFile(filepath.Join(data, "run", "scan.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
}

func TestScanStartConflict(t *testing.T) {
	e := newEnv(t, admin())
	rr, m := e.do("POST", "/api.cgi?action=scan_start", "", post)
	if rr.Code != 202 || m["started"] != true {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
	if len(e.spawned) != 1 || strings.Join(e.spawned[0], " ") != "scan --trigger manual" {
		t.Fatalf("spawned = %v", e.spawned)
	}
	lockData(t, e.data)
	rr, m = e.do("POST", "/api.cgi?action=scan_start", "", post)
	if rr.Code != 409 || m["error"] != "task_running" || len(e.spawned) != 1 {
		t.Fatalf("code=%d body=%v spawned=%v", rr.Code, m, e.spawned)
	}
}

func TestStatus(t *testing.T) {
	e := newEnv(t, admin())
	_, m := e.do("GET", "/api.cgi?action=status", "", nil)
	if m["running"] != false || m["progress"] != nil {
		t.Fatalf("idle status = %v", m)
	}
	lockData(t, e.data)
	b, _ := json.Marshal(runner.Progress{ScanID: 3, Files: 99})
	os.WriteFile(filepath.Join(e.data, "run", "progress.json"), b, 0o644)
	_, m = e.do("GET", "/api.cgi?action=status", "", nil)
	if m["running"] != true || m["progress"].(map[string]any)["files"].(float64) != 99 {
		t.Fatalf("running status = %v", m)
	}
}

func TestCancelNotRunning(t *testing.T) {
	e := newEnv(t, admin())
	if rr, m := e.do("POST", "/api.cgi?action=scan_cancel", "", post); rr.Code != 404 || m["error"] != "not_running" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func TestDeleteRunningScanRefused(t *testing.T) {
	e := newEnv(t, admin())
	c, _ := catalog.Open(e.data)
	id, _ := c.Begin("manual", time.Now())
	c.Close()
	lockData(t, e.data)
	b, _ := json.Marshal(runner.Progress{ScanID: id})
	os.WriteFile(filepath.Join(e.data, "run", "progress.json"), b, 0o644)
	if rr, m := e.do("POST", "/api.cgi?action=scan_delete&scan="+itoa(id), "", post); rr.Code != 409 || m["error"] != "scan_running" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func TestScansList(t *testing.T) {
	e := newEnv(t, admin())
	e.makeScan()
	_, m := e.do("GET", "/api.cgi?action=scans", "", nil)
	if s := m["scans"].([]any); len(s) != 1 || s[0].(map[string]any)["status"] != "done" {
		t.Fatalf("scans = %v", m)
	}
}

func TestSettingsRoundTrip(t *testing.T) {
	e := newEnv(t, admin())
	_, m := e.do("GET", "/api.cgi?action=settings", "", nil)
	if m["scheduleMode"] != "off" || m["retention"].(float64) != 10 {
		t.Fatalf("defaults = %v", m)
	}
	body := `{"scheduleMode":"weekly","weekday":3,"time":"02:15","retention":5,"defaultSizeMode":"disk"}`
	rr, m := e.do("POST", "/api.cgi?action=settings", body, post)
	if rr.Code != 200 || m["weekday"].(float64) != 3 {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
	cron, _ := os.ReadFile(e.cron)
	if !strings.Contains(string(cron), "15 2 * * 3 /qpkg/bin/storage-analyzer scan --trigger schedule") {
		t.Fatalf("crontab = %s", cron)
	}
	if strings.Join(e.ran, "|") != "crontab "+e.cron+"|/etc/init.d/crond.sh restart" {
		t.Fatalf("ran = %v", e.ran)
	}
	rr, m = e.do("POST", "/api.cgi?action=settings", `{"scheduleMode":"daily","time":"99:00","retention":5,"defaultSizeMode":"disk"}`, post)
	if rr.Code != 400 || m["error"] != "invalid_settings" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestListMissingFile(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	os.Remove(catalog.ScanPath(e.data, id))
	rr, m := e.do("GET", "/api.cgi?action=list&scan="+itoa(id), "", nil)
	if rr.Code != 404 || m["error"] != "scan_not_found" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

// makeDups stores a finished duplicate check of scan id with the given groups.
func (e *env) makeDups(id int64, groups []dupcheck.OutGroup) {
	e.t.Helper()
	c, err := catalog.Open(e.data)
	if err != nil {
		e.t.Fatal(err)
	}
	defer c.Close()
	if err := dupcheck.WriteResult(catalog.DupsPath(e.data, id), groups, map[string]string{"groups": strconv.Itoa(len(groups)), "finishedAt": "1700000100", "changed": "2"}); err != nil {
		e.t.Fatal(err)
	}
	c.BeginDupCheck(id, time.Unix(1700000070, 0))
	c.FinishDupCheck(catalog.DupCheck{ScanID: id, FinishedAt: 1700000100, Groups: int64(len(groups)), ReclaimDSize: 10, Changed: 2})
}

func dupGroup(path string, dsize, asize int64, copies int) dupcheck.OutGroup {
	g := dupcheck.OutGroup{Kind: scandb.KindFile, Files: 1, DSize: dsize, ASize: asize, RDSize: dsize, RASize: asize}
	for i := 0; i < copies; i++ {
		g.Members = append(g.Members, dupcheck.OutMember{EntryID: int64(100 + i), ParentID: 2, Path: []byte(path + "/" + strconv.Itoa(1000+i))})
	}
	return g
}

func TestDupStartSpawns(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	if rr, _ := e.do("GET", "/api.cgi?action=dup_start&scan="+itoa(id), "", nil); rr.Code != 405 {
		t.Fatalf("GET code = %d", rr.Code)
	}
	rr, m := e.do("POST", "/api.cgi?action=dup_start&scan="+itoa(id), "", post)
	if rr.Code != 202 || m["started"] != true {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
	if len(e.spawned) != 1 || strings.Join(e.spawned[0], " ") != "dupcheck --scan "+itoa(id) {
		t.Fatalf("spawned = %v", e.spawned)
	}
}

func TestDupStartConflict(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	lockData(t, e.data)
	rr, m := e.do("POST", "/api.cgi?action=dup_start&scan="+itoa(id), "", post)
	if rr.Code != 409 || m["error"] != "task_running" || len(e.spawned) != 0 {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func TestDupStartMissingScan(t *testing.T) {
	e := newEnv(t, admin())
	rr, m := e.do("POST", "/api.cgi?action=dup_start&scan=999", "", post)
	if rr.Code != 404 || m["error"] != "scan_not_found" || len(e.spawned) != 0 {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func TestDupsNoCheck(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	rr, m := e.do("GET", "/api.cgi?action=dups&scan="+itoa(id), "", nil)
	if rr.Code != 200 || m["check"] != nil || m["result"] != nil || m["total"].(float64) != 0 || len(m["groups"].([]any)) != 0 {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
}

func TestDupsListing(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	e.makeDups(id, []dupcheck.OutGroup{dupGroup("vol/a", 100, 900, 2), dupGroup("vol/b", 200, 50, 3)})
	_, m := e.do("GET", "/api.cgi?action=dups&scan="+itoa(id)+"&size=disk", "", nil)
	gs := m["groups"].([]any)
	if m["total"].(float64) != 2 || len(gs) != 2 {
		t.Fatalf("body = %v", m)
	}
	first := gs[0].(map[string]any)
	if first["dsize"].(float64) != 200 || first["copies"].(float64) != 3 || first["name"] != "1000" {
		t.Fatalf("first (disk) = %v", first)
	}
	mem := first["members"].([]any)[0].(map[string]any)
	if mem["path"] != "vol/b/1000" || mem["entryId"].(float64) != 100 || mem["parentId"].(float64) != 2 || mem["covered"] != false {
		t.Fatalf("member = %v", mem)
	}
	if m["check"].(map[string]any)["status"] != "done" || m["result"].(map[string]any)["changed"].(float64) != 2 ||
		m["result"].(map[string]any)["finishedAt"].(float64) != 1700000100 {
		t.Fatalf("check/result = %v / %v", m["check"], m["result"])
	}
	_, m = e.do("GET", "/api.cgi?action=dups&scan="+itoa(id)+"&size=apparent&limit=1", "", nil)
	gs = m["groups"].([]any)
	if len(gs) != 1 || gs[0].(map[string]any)["asize"].(float64) != 900 {
		t.Fatalf("apparent page = %v", gs)
	}
}

func TestDupsMemberCap(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	e.makeDups(id, []dupcheck.OutGroup{dupGroup("vol/m", 10, 10, 300)})
	_, m := e.do("GET", "/api.cgi?action=dups&scan="+itoa(id), "", nil)
	g := m["groups"].([]any)[0].(map[string]any)
	if len(g["members"].([]any)) != 100 || g["copies"].(float64) != 300 {
		t.Fatalf("members %d copies %v", len(g["members"].([]any)), g["copies"])
	}
}

func TestDupsBadNamePath(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	g := dupGroup("vol/x", 10, 10, 2)
	g.Members[0].Path = []byte("vol/a/b\xff\nc")
	e.makeDups(id, []dupcheck.OutGroup{g})
	rr, m := e.do("GET", "/api.cgi?action=dups&scan="+itoa(id), "", nil)
	mem := m["groups"].([]any)[0].(map[string]any)["members"].([]any)[0].(map[string]any)
	if rr.Code != 200 || mem["badName"] != true || mem["path"] != "vol/a/b\ufffd\nc" {
		t.Fatalf("member = %v", mem)
	}
}

func TestDupsDeletedScan(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	e.makeDups(id, []dupcheck.OutGroup{dupGroup("vol/a", 10, 10, 2)})
	e.do("POST", "/api.cgi?action=scan_delete&scan="+itoa(id), "", post)
	if rr, m := e.do("GET", "/api.cgi?action=dups&scan="+itoa(id), "", nil); rr.Code != 404 || m["error"] != "scan_not_found" {
		t.Fatalf("code=%d body=%v", rr.Code, m)
	}
	if _, err := os.Stat(catalog.DupsPath(e.data, id)); !os.IsNotExist(err) {
		t.Fatalf("dups file left: %v", err)
	}
}

func TestScansIncludeDup(t *testing.T) {
	e := newEnv(t, admin())
	id := e.makeScan()
	e.makeDups(id, []dupcheck.OutGroup{dupGroup("vol/a", 10, 10, 2)})
	_, m := e.do("GET", "/api.cgi?action=scans", "", nil)
	dup := m["scans"].([]any)[0].(map[string]any)["dup"].(map[string]any)
	if dup["status"] != "done" || dup["groups"].(float64) != 1 || dup["reclaimDSize"].(float64) != 10 {
		t.Fatalf("dup = %v", dup)
	}
}
