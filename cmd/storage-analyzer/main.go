// Command storage-analyzer is the StorageAnalyzer binary. The platform
// specific commands live in platform_qnap.go (build tag qnap, the QPKG),
// platform_fnos.go (build tag fnos, the fnOS FPK), and platform_generic.go
// (everything else); this file holds the shared ones:
//
//	storage-analyzer scan --trigger manual|schedule
//	storage-analyzer dupcheck --scan <id>
//	storage-analyzer delete --request <path>
//	storage-analyzer prune | version
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/deleter"
	"github.com/FlintyLemming/storage-analyzer/internal/dupcheck"
	"github.com/FlintyLemming/storage-analyzer/internal/runner"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

// version is set with -ldflags "-X main.version=...".
var version = "dev"

// app is what the shared commands need from the platform.
type app struct {
	data         string // data directory
	bin          string // this executable
	discover     func() ([]volumes.Volume, []volumes.Skipped, error)
	mountCheck   func() func(path string, parentDev, dev uint64) bool
	childArgs    []string // appended when spawning this binary again
	allowDelete  bool
	deletePolicy deleter.Policy
}

// exitError makes main exit with code instead of 1.
type exitError struct {
	code int
	err  error
}

func (e exitError) Error() string { return e.err.Error() }
func (e exitError) Unwrap() error { return e.err }

// errUnknownCommand is returned by runPlatform for commands it lacks.
var errUnknownCommand = errors.New("unknown command")

func main() {
	log.SetFlags(log.LstdFlags)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: storage-analyzer "+usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	trigger := fs.String("trigger", "manual", "scan trigger: manual|schedule")
	scanID := fs.Int64("scan", 0, "dupcheck: id of the scan to check")
	delReq := fs.String("request", "", "delete: path to delete request JSON")
	pf := addPlatformFlags(fs)
	fs.Parse(args)
	if cmd == "version" {
		fmt.Println(version)
		return
	}
	err := run(cmd, pf, *trigger, *scanID, *delReq)
	if err != nil {
		log.Printf("%s: %v", cmd, err)
		var ee exitError
		if errors.As(err, &ee) {
			os.Exit(ee.code)
		}
		os.Exit(1)
	}
}

func run(cmd string, pf *platformFlags, trigger string, scanID int64, delReq string) error {
	a, err := newApp(pf)
	if err != nil {
		return err
	}
	// SQLite spills sorts to /var/tmp by default, a 64MB tmpfs on QTS. It reads
	// this variable only once, so set it before the first database is opened.
	if err := os.Setenv("SQLITE_TMPDIR", a.data); err != nil {
		log.Fatalf("set SQLite temp directory: %v", err)
	}
	switch cmd {
	case "scan":
		return scan(a, trigger)
	case "dupcheck":
		return dupCheck(a, scanID)
	case "delete":
		return deleteFiles(a, delReq)
	case "prune":
		return prune(a)
	}
	err = runPlatform(cmd, a, pf)
	if errors.Is(err, errUnknownCommand) {
		return fmt.Errorf("%w %q", errUnknownCommand, cmd)
	}
	return err
}

func openLog(a *app) {
	dir := filepath.Join(a.data, "logs")
	os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, "scan.log")
	if st, err := os.Stat(path); err == nil && st.Size() > 1<<20 {
		os.Rename(path, path+".1")
	}
	if f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
		log.SetOutput(f)
	}
}

func scan(a *app, trigger string) error {
	if trigger != "manual" && trigger != "schedule" {
		return fmt.Errorf("invalid trigger %q", trigger)
	}
	runner.SetLowPriority()
	openLog(a)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer stop()
	start := time.Now()
	log.Printf("scan (%s) starting, pid %d", trigger, os.Getpid())
	if trigger == "schedule" {
		return scheduledRun(ctx, a, start)
	}
	var isMount func(string, uint64, uint64) bool
	if a.mountCheck != nil {
		isMount = a.mountCheck()
	}
	id, err := runner.Run(ctx, runner.Options{DataDir: a.data, Trigger: trigger, Discover: a.discover, IsMountPoint: isMount})
	if errors.Is(err, runner.ErrBusy) {
		log.Printf("scan (%s) skipped: another scan or duplicate check is running", trigger)
		return nil
	}
	return logScanResult(id, time.Since(start), err)
}

// scheduledRun is one cron firing: a scan and, if enabled in the settings, a
// duplicate check of it, under one lock.
func scheduledRun(ctx context.Context, a *app, start time.Time) error {
	var isMount func(string, uint64, uint64) bool
	if a.mountCheck != nil {
		isMount = a.mountCheck()
	}
	res, err := runner.RunScheduled(ctx, runner.Options{DataDir: a.data, Discover: a.discover, IsMountPoint: isMount})
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

func dupCheck(a *app, id int64) error {
	if id <= 0 {
		return fmt.Errorf("dupcheck needs --scan <id>")
	}
	runner.SetLowPriority()
	openLog(a)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer stop()
	start := time.Now()
	log.Printf("dupcheck of scan %d starting, pid %d", id, os.Getpid())
	sum, err := runner.RunDupCheck(ctx, runner.DupOptions{DataDir: a.data, ScanID: id})
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

func prune(a *app) error {
	c, err := catalog.Open(a.data)
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

func deleteFiles(a *app, reqPath string) error {
	if !a.allowDelete {
		return errors.New("file deletion is disabled in configuration")
	}
	if reqPath == "" {
		return errors.New("delete needs --request <path>")
	}
	defer os.Remove(reqPath)
	req, err := runner.ReadDeleteRequest(reqPath)
	if err != nil {
		return err
	}
	runner.SetLowPriority()
	openLog(a)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer stop()
	start := time.Now()
	log.Printf("delete job %s (scan %d, %d ids) starting, pid %d", req.JobID, req.ScanID, len(req.EntryIDs), os.Getpid())
	var isMount func(string, uint64, uint64) bool
	if a.mountCheck != nil {
		isMount = a.mountCheck()
	}
	res, err := runner.RunDelete(ctx, runner.DeleteOptions{
		DataDir:      a.data,
		Request:      req,
		Policy:       a.deletePolicy,
		IsMountPoint: isMount,
	})
	if errors.Is(err, runner.ErrBusy) {
		log.Printf("delete job %s skipped: another task is running", req.JobID)
		return nil
	}
	took := time.Since(start).Round(time.Millisecond)
	if err != nil {
		log.Printf("delete job %s failed in %v: %v", req.JobID, took, err)
		return err
	}
	log.Printf("delete job %s %s in %v: %d/%d targets, %d items, freed %d B (disk), %d failures",
		req.JobID, res.Status, took, res.Succeeded, res.Targets, res.DeletedItems, res.FreedDSize, len(res.Failures))
	return nil
}

// prepareData creates the data directory layout and repairs state a crash
// or reboot left behind.
func prepareData(data string) error {
	for _, d := range []string{"scans", "run", "logs"} {
		if err := os.MkdirAll(filepath.Join(data, d), 0o755); err != nil {
			return err
		}
	}
	if err := runner.CleanupStaleTmp(data); err != nil {
		log.Printf("cleanup: %v", err)
	}
	c, err := catalog.Open(data)
	if err != nil {
		return err
	}
	if _, running, _ := runner.Running(data); !running {
		if _, err := c.MarkStaleRunning(time.Now()); err != nil {
			log.Printf("mark stale: %v", err)
		}
	}
	c.Close()
	return nil
}
