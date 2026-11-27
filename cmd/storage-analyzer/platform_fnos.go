//go:build fnos

package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/api"
	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/config"
	"github.com/FlintyLemming/storage-analyzer/internal/deleter"
	"github.com/FlintyLemming/storage-analyzer/internal/mounts"
	"github.com/FlintyLemming/storage-analyzer/internal/runner"
	"github.com/FlintyLemming/storage-analyzer/internal/scheduler"
	"github.com/FlintyLemming/storage-analyzer/internal/server"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
	"github.com/FlintyLemming/storage-analyzer/web"
)

// fnOS FPK flavor: resident HTTP server listening on the unified gateway Unix
// socket (${TRIM_APPDEST}/app.sock), trusting X-Trim-* headers from
// trim_http_cgi, with an in-process scheduler and embedded frontend.

const (
	usage               = "serve|scan|dupcheck|delete|prune|version"
	defaultFnOSPrefix   = "/app/storage-analyzer"
	defaultFnOSAppEntry = "/var/apps/storage-analyzer"
)

type platformFlags struct {
	data   string
	config string
	socket string
	prefix string
	paths  string
}

func addPlatformFlags(fs *flag.FlagSet) *platformFlags {
	pf := &platformFlags{}
	defaultData := os.Getenv("TRIM_PKGVAR")
	defaultCfg := ""
	if etc := os.Getenv("TRIM_PKGETC"); etc != "" {
		defaultCfg = filepath.Join(etc, "settings.conf")
	}
	defaultSock := ""
	if dest := os.Getenv("TRIM_APPDEST"); dest != "" {
		defaultSock = filepath.Join(dest, "app.sock")
	}
	fs.StringVar(&pf.data, "data", defaultData, "data directory (default: $TRIM_PKGVAR)")
	fs.StringVar(&pf.config, "config", defaultCfg, "settings file (default: $TRIM_PKGETC/settings.conf)")
	fs.StringVar(&pf.socket, "socket", defaultSock, "serve: unified gateway Unix socket path (default: $TRIM_APPDEST/app.sock)")
	fs.StringVar(&pf.prefix, "prefix", defaultFnOSPrefix, "serve: unified gateway URL prefix")
	fs.StringVar(&pf.paths, "paths", "", "comma-separated directories to scan instead of fnOS volume auto-discovery")
	return pf
}

type fnosConfig struct {
	IncludeSystem   bool
	IncludeExternal bool
	AllowDelete     bool
}

func defaultFnOSConfig() fnosConfig {
	return fnosConfig{
		IncludeSystem:   parseBoolEnv("wizard_include_system", false),
		IncludeExternal: parseBoolEnv("wizard_include_external", false),
		AllowDelete:     parseBoolEnv("wizard_allow_delete", true),
	}
}

func parseBoolValue(s string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1", "yes", "on":
		return true, true
	case "false", "0", "no", "off":
		return false, true
	default:
		return false, false
	}
}

func parseBoolEnv(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, valid := parseBoolValue(v); valid {
			return b
		}
	}
	return def
}

func parseFnOSConfig(r io.Reader) (fnosConfig, error) {
	c := fnosConfig{AllowDelete: true}
	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		if n == 1 {
			line = strings.TrimPrefix(line, "\xef\xbb\xbf")
		}
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return fnosConfig{}, fmt.Errorf("settings line %d: expected key = value", n)
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		b, valid := parseBoolValue(v)
		if !valid {
			return fnosConfig{}, fmt.Errorf("settings line %d: %s: expected true or false, got %q", n, k, v)
		}
		switch k {
		case "include_system":
			c.IncludeSystem = b
		case "include_external":
			c.IncludeExternal = b
		case "allow_delete":
			c.AllowDelete = b
		default:
			return fnosConfig{}, fmt.Errorf("settings line %d: unknown key %q", n, k)
		}
	}
	return c, sc.Err()
}

func loadFnOSConfig(path string) (fnosConfig, error) {
	if path == "" {
		return defaultFnOSConfig(), nil
	}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return defaultFnOSConfig(), nil
	}
	if err != nil {
		return fnosConfig{}, err
	}
	defer f.Close()
	c, err := parseFnOSConfig(f)
	if err != nil {
		return fnosConfig{}, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

func newApp(pf *platformFlags) (*app, error) {
	if pf.data == "" {
		return nil, exitError{code: 78, err: errors.New("data directory not set (pass --data or set TRIM_PKGVAR)")}
	}
	absData, err := filepath.Abs(pf.data)
	if err != nil {
		return nil, err
	}
	absConfig := ""
	if pf.config != "" {
		absConfig, err = filepath.Abs(pf.config)
		if err != nil {
			return nil, err
		}
	}
	cfg, err := loadFnOSConfig(absConfig)
	if err != nil {
		return nil, exitError{code: 78, err: err}
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	if r, err := filepath.EvalSymlinks(exe); err == nil {
		exe = r
	}

	childArgs := []string{"--data", absData}
	if absConfig != "" {
		childArgs = append(childArgs, "--config", absConfig)
	}
	var discover func() ([]volumes.Volume, []volumes.Skipped, error)
	if pf.paths != "" {
		var explicit []string
		for _, p := range strings.Split(pf.paths, ",") {
			if p = strings.TrimSpace(p); p != "" {
				explicit = append(explicit, p)
			}
		}
		if len(explicit) == 0 {
			return nil, exitError{code: 78, err: errors.New("--paths is empty")}
		}
		childArgs = append(childArgs, "--paths", pf.paths)
		discover = func() ([]volumes.Volume, []volumes.Skipped, error) {
			return mounts.Discover(config.Config{Paths: explicit})
		}
	} else {
		opts := mounts.FnOSOptions{
			IncludeSystem:   cfg.IncludeSystem,
			IncludeExternal: cfg.IncludeExternal,
		}
		discover = func() ([]volumes.Volume, []volumes.Skipped, error) {
			return mounts.DiscoverFnOS(opts)
		}
	}

	extraProtected := []string{
		defaultFnOSAppEntry,
		absConfig,
		os.Getenv("TRIM_APPDEST"),
		os.Getenv("TRIM_PKGETC"),
		os.Getenv("TRIM_PKGHOME"),
		os.Getenv("TRIM_PKGMETA"),
		os.Getenv("TRIM_PKGTMP"),
	}
	if filepath.Base(filepath.Dir(exe)) == "bin" {
		parent := filepath.Dir(filepath.Dir(exe))
		if filepath.Base(parent) == "storage-analyzer" {
			extraProtected = append(extraProtected, parent)
		}
	}

	return &app{
		data:         absData,
		bin:          exe,
		discover:     discover,
		mountCheck:   mounts.NewBoundaryChecker,
		childArgs:    childArgs,
		allowDelete:  cfg.AllowDelete,
		deletePolicy: deleter.DefaultPolicy("fnos", absData, extraProtected...),
	}, nil
}

func runPlatform(cmd string, a *app, pf *platformFlags) error {
	switch cmd {
	case "serve":
		return serveFnOS(a, pf)
	}
	return errUnknownCommand
}

func serveFnOS(a *app, pf *platformFlags) error {
	if pf.socket == "" {
		return exitError{code: 78, err: errors.New("socket path not set (pass --socket or set TRIM_APPDEST)")}
	}
	if err := prepareData(a.data); err != nil {
		return err
	}
	sockPath, err := filepath.Abs(pf.socket)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		return err
	}
	if err := os.Remove(sockPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove stale socket %s: %w", sockPath, err)
	}
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return err
	}
	// Restrict the socket to its owner (root) so unprivileged local users or
	// other package-user apps cannot dial app.sock directly and forge X-Trim-*
	// gateway identity headers. trim_http_cgi connects as root.
	if err := os.Chmod(sockPath, 0o600); err != nil {
		ln.Close()
		os.Remove(sockPath)
		return fmt.Errorf("chmod socket %s: %w", sockPath, err)
	}
	defer os.Remove(sockPath)

	spawn := func(args ...string) error { return spawnChild(a, args...) }
	apiH := api.New(api.Config{
		DataDir:            a.data,
		BinPath:            a.bin,
		Auth:               api.FnOSAuth{},
		NotLoggedInMessage: "not logged in to fnOS",
		Discover:           a.discover,
		Spawn:              spawn,
		DeleteEnabled:      a.allowDelete,
		DeletePolicy:       a.deletePolicy,
	})
	h := server.Handler(server.Options{
		API:        apiH,
		Static:     web.Files,
		Platform:   "fnos",
		Prefix:     pf.prefix,
		AllowFrame: true,
	})
	srv := server.NewServer("", h)
	log.Printf("listening on unix:%s (prefix=%s, data=%s, allow_delete=%v)", sockPath, pf.prefix, a.data, a.allowDelete)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	sched := &scheduler.Scheduler{
		Settings: func() (catalog.Settings, error) {
			c, err := catalog.Open(a.data)
			if err != nil {
				return catalog.Settings{}, err
			}
			defer c.Close()
			return c.Settings()
		},
		Fire: func() error { return spawn("scan", "--trigger", "schedule") },
	}
	schedDone := make(chan struct{})
	go func() {
		defer close(schedDone)
		sched.Run(ctx)
	}()

	errc := make(chan error, 1)
	go func() {
		errc <- srv.Serve(ln)
	}()

	select {
	case err := <-errc:
		stop()
		<-schedDone
		return err
	case <-ctx.Done():
		stop()
		<-schedDone
	}
	if err := runner.Cancel(a.data); err != nil && !errors.Is(err, runner.ErrNotRunning) {
		log.Printf("cancel running task: %v", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func spawnChild(a *app, args ...string) error {
	_, err := startReaped(a.bin, append(args, a.childArgs...)...)
	return err
}

func startReaped(bin string, args ...string) (pid int, err error) {
	cmd := exec.Command(bin, args...)
	cmd.Dir = "/"
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	go cmd.Wait()
	return cmd.Process.Pid, nil
}
