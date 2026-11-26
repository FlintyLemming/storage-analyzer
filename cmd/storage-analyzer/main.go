// Command storage-analyzer is the single binary of the StorageAnalyzer QPKG.
//
//	storage-analyzer cgi                         serve one CGI request (via web/api.cgi)
//	storage-analyzer scan --trigger manual|schedule
//	storage-analyzer dupcheck --scan <id>
//	storage-analyzer apply-schedule | prune | init-start | init-stop
//	storage-analyzer serve --insecure-no-auth    development only, loopback only
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/cgi"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/api"
	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/dupcheck"
	"github.com/FlintyLemming/storage-analyzer/internal/runner"
	"github.com/FlintyLemming/storage-analyzer/internal/schedule"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

const crontabPath = "/etc/config/crontab"

type paths struct {
	root, bin, data, web string
}

// resolvePaths derives the install root from the executable location
// (<root>/bin/storage-analyzer) unless --root is given.
func resolvePaths(rootFlag string) paths {
	exe, err := os.Executable()
	if err == nil {
		if r, err := filepath.EvalSymlinks(exe); err == nil {
			exe = r
		}
	}
	root := rootFlag
	if root == "" {
		root = filepath.Dir(filepath.Dir(exe))
	}
	root, _ = filepath.Abs(root)
	return paths{root: root, bin: exe, data: filepath.Join(root, "data"), web: filepath.Join(root, "web")}
}

// discover returns the live QTS volumes, or SA_DEV_ROOTS (colon separated)
// when running outside QTS for development.
func discover() ([]volumes.Volume, []volumes.Skipped, error) {
	if dev := os.Getenv("SA_DEV_ROOTS"); dev != "" {
		if _, err := os.Stat("/etc/volume.conf"); os.IsNotExist(err) {
			var vols []volumes.Volume
			for i, p := range strings.Split(dev, ":") {
				vols = append(vols, volumes.Volume{Name: filepath.Base(p), Pool: fmt.Sprint(i + 1), Device: "dev", MountPoint: p})
			}
			return vols, nil, nil
		}
	}
	return volumes.Discover()
}

func runCmd(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %v: %w: %s", name, args, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: storage-analyzer cgi|scan|dupcheck|apply-schedule|prune|init-start|init-stop|serve")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	rootFlag := fs.String("root", "", "install root (default: derived from executable)")
	trigger := fs.String("trigger", "manual", "scan trigger: manual|schedule")
	scanID := fs.Int64("scan", 0, "dupcheck: id of the scan to check")
	addr := fs.String("addr", "127.0.0.1:8099", "serve: listen address (loopback only)")
	webDir := fs.String("web", "", "serve: static web directory (default <root>/web)")
	noAuth := fs.Bool("insecure-no-auth", false, "serve: skip QTS session check (development)")
	fs.Parse(args)
	p := resolvePaths(*rootFlag)
	// SQLite spills sorts to /var/tmp by default, a 64MB tmpfs on QTS. It reads
	// this variable only once, so set it before the first database is opened.
	if err := os.Setenv("SQLITE_TMPDIR", p.data); err != nil {
		log.Fatalf("set SQLite temp directory: %v", err)
	}

	var err error
	switch cmd {
	case "cgi":
		err = cgi.Serve(api.New(api.Config{
			DataDir: p.data, BinPath: p.bin,
			Auth: api.QTSAuth{URL: api.DefaultAuthURL},
			ApplySchedule: func(s catalog.Settings) error {
				return schedule.Apply(s, p.bin, crontabPath, runCmd)
			},
		}))
	case "scan":
		err = scan(p, *trigger)
	case "dupcheck":
		err = dupCheck(p, *scanID)
	case "apply-schedule":
		err = applySchedule(p, nil)
	case "prune":
		err = prune(p)
	case "init-start":
		err = initStart(p)
	case "init-stop":
		err = initStop(p)
	case "serve":
		err = serve(p, *addr, *webDir, *noAuth)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		log.Printf("%s: %v", cmd, err)
		os.Exit(1)
	}
}

func openLog(p paths) {
	dir := filepath.Join(p.data, "logs")
	os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, "scan.log")
	if st, err := os.Stat(path); err == nil && st.Size() > 1<<20 {
		os.Rename(path, path+".1")
	}
	if f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
		log.SetOutput(f)
	}
}

func scan(p paths, trigger string) error {
	if trigger != "manual" && trigger != "schedule" {
		return fmt.Errorf("invalid trigger %q", trigger)
	}
	runner.SetLowPriority()
	openLog(p)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer stop()
	start := time.Now()
	log.Printf("scan (%s) starting, pid %d", trigger, os.Getpid())
	if trigger == "schedule" {
		return scheduledRun(ctx, p, start)
	}
	id, err := runner.Run(ctx, runner.Options{DataDir: p.data, Trigger: trigger, Discover: discover})
	if errors.Is(err, runner.ErrBusy) {
		log.Printf("scan (%s) skipped: another scan or duplicate check is running", trigger)
		return nil
	}
	return logScanResult(id, time.Since(start), err)
}

// scheduledRun is one cron firing: a scan and, if enabled in the settings, a
// duplicate check of it, under one lock.
func scheduledRun(ctx context.Context, p paths, start time.Time) error {
	res, err := runner.RunScheduled(ctx, runner.Options{DataDir: p.data, Discover: discover})
	if errors.Is(err, runner.ErrBusy) {
		log.Printf("scheduled run skipped: another scan or duplicate check is running")
		return nil
	}
	end := res.ScanDone
	if end.IsZero() {
		end = time.Now()
	}
	if err := logScanResult(res.ScanID, end.Sub(start), err); err != nil || !res.Checked {
		return err
	}
	return logDupResult(res.ScanID, time.Since(res.ScanDone), res.Dup, res.DupErr)
}

func logScanResult(id int64, took time.Duration, err error) error {
	took = took.Round(time.Second)
	switch {
	case errors.Is(err, context.Canceled):
		log.Printf("scan %d canceled after %v", id, took)
		return nil
	case err != nil:
		log.Printf("scan %d failed: %v", id, err)
		return err
	}
	log.Printf("scan %d done in %v", id, took)
	return nil
}

func dupCheck(p paths, id int64) error {
	if id <= 0 {
		return fmt.Errorf("dupcheck needs --scan <id>")
	}
	runner.SetLowPriority()
	openLog(p)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer stop()
	start := time.Now()
	log.Printf("dupcheck of scan %d starting, pid %d", id, os.Getpid())
	sum, err := runner.RunDupCheck(ctx, runner.DupOptions{DataDir: p.data, ScanID: id})
	if errors.Is(err, runner.ErrBusy) {
		log.Printf("dupcheck of scan %d skipped: another scan or check is running", id)
		return nil
	}
	return logDupResult(id, time.Since(start), sum, err)
}

func logDupResult(id int64, took time.Duration, sum dupcheck.Summary, err error) error {
	took = took.Round(time.Second)
	switch {
	case errors.Is(err, context.Canceled):
		log.Printf("dupcheck of scan %d canceled after %v", id, took)
		return nil
	case err != nil:
		log.Printf("dupcheck of scan %d failed: %v", id, err)
		return err
	}
	log.Printf("dupcheck of scan %d done in %v: %d groups, reclaimable %d B (disk), read %d B, %d cache hits, %d changed, %d errors",
		id, took, sum.Groups, sum.ReclaimDSize, sum.BytesRead, sum.CacheHits, sum.Changed, sum.Errors)
	return nil
}

func applySchedule(p paths, override *catalog.Settings) error {
	c, err := catalog.Open(p.data)
	if err != nil {
		return err
	}
	defer c.Close()
	s, err := c.Settings()
	if err != nil {
		return err
	}
	if override != nil {
		s = *override
	}
	return schedule.Apply(s, p.bin, crontabPath, runCmd)
}

func prune(p paths) error {
	c, err := catalog.Open(p.data)
	if err != nil {
		return err
	}
	defer c.Close()
	s, err := c.Settings()
	if err != nil {
		return err
	}
	_, err = c.Prune(s.Retention)
	return err
}

func initStart(p paths) error {
	for _, d := range []string{"scans", "run", "logs"} {
		if err := os.MkdirAll(filepath.Join(p.data, d), 0o755); err != nil {
			return err
		}
	}
	if err := runner.CleanupStaleTmp(p.data); err != nil {
		log.Printf("cleanup: %v", err)
	}
	c, err := catalog.Open(p.data)
	if err != nil {
		return err
	}
	if _, running, _ := runner.Running(p.data); !running {
		if _, err := c.MarkStaleRunning(time.Now()); err != nil {
			log.Printf("mark stale: %v", err)
		}
	}
	c.Close()
	return applySchedule(p, nil)
}

func initStop(p paths) error {
	if err := runner.Cancel(p.data); err != nil && !errors.Is(err, runner.ErrNotRunning) {
		log.Printf("cancel: %v", err)
	}
	off := catalog.DefaultSettings
	off.ScheduleMode = "off"
	return applySchedule(p, &off)
}

func serve(p paths, addr, webDir string, noAuth bool) error {
	if !noAuth {
		return errors.New("serve is for development only and requires --insecure-no-auth")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("refusing to listen on non-loopback address %q", host)
	}
	if webDir == "" {
		webDir = p.web
	}
	os.MkdirAll(filepath.Join(p.data, "scans"), 0o755)
	h := api.New(api.Config{
		DataDir: p.data, BinPath: p.bin,
		Auth:  allowAll{},
		Spawn: func(args ...string) error { return devSpawn(p, args...) },
		ApplySchedule: func(s catalog.Settings) error {
			log.Printf("(dev) skip apply schedule %+v", s)
			return nil
		},
	})
	mux := http.NewServeMux()
	mux.Handle("/api.cgi", h)
	mux.Handle("/", http.FileServer(http.Dir(webDir)))
	log.Printf("dev server on http://%s/ (web=%s data=%s)", addr, webDir, p.data)
	return http.ListenAndServe(addr, mux)
}

func devSpawn(p paths, args ...string) error {
	cmd := exec.Command(p.bin, append(args, "--root", p.root)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	go cmd.Wait()
	return nil
}

type allowAll struct{}

func (allowAll) Check(*http.Request) (string, bool, error) { return "dev", true, nil }
