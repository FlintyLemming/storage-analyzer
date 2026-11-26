//go:build !qnap

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
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"github.com/FlintyLemming/storage-analyzer/internal/api"
	"github.com/FlintyLemming/storage-analyzer/internal/auth"
	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/config"
	"github.com/FlintyLemming/storage-analyzer/internal/mounts"
	"github.com/FlintyLemming/storage-analyzer/internal/runner"
	"github.com/FlintyLemming/storage-analyzer/internal/scheduler"
	"github.com/FlintyLemming/storage-analyzer/internal/server"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
	"github.com/FlintyLemming/storage-analyzer/web"
)

// Generic Linux flavor, configured by a config file. Also what builds on
// macOS for development.

const usage = "scan|dupcheck|prune|version|serve|passwd"

type platformFlags struct {
	config string
	addr   string
	web    string
	noAuth bool
}

func addPlatformFlags(fs *flag.FlagSet) *platformFlags {
	pf := &platformFlags{}
	fs.StringVar(&pf.config, "config", config.DefaultPath, "config file")
	fs.StringVar(&pf.addr, "addr", "", "serve --insecure-no-auth: listen address (loopback only, default 127.0.0.1:8099)")
	fs.StringVar(&pf.web, "web", "", "serve --insecure-no-auth: serve the frontend from this directory instead of the embedded one")
	fs.BoolVar(&pf.noAuth, "insecure-no-auth", false, "serve: skip authentication (development)")
	return pf
}

func newApp(pf *platformFlags) (*app, error) {
	cfg, err := config.Load(pf.config)
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
	return &app{
		data:      cfg.DataDir,
		bin:       exe,
		discover:  func() ([]volumes.Volume, []volumes.Skipped, error) { return mounts.Discover(cfg) },
		childArgs: []string{"--config", pf.config},
	}, nil
}

func runPlatform(cmd string, a *app, pf *platformFlags) error {
	switch cmd {
	case "serve":
		return serve(a, pf)
	case "passwd":
		return passwd(pf)
	}
	return errUnknownCommand
}

// serve runs the web server: production mode by default, or the loopback-only
// development server with --insecure-no-auth.
func serve(a *app, pf *platformFlags) error {
	if pf.noAuth {
		return serveDev(a, pf)
	}
	if pf.addr != "" {
		return errors.New("--addr only applies with --insecure-no-auth; set listen in the config file")
	}
	cfg, err := config.Load(pf.config)
	if err != nil {
		return exitError{code: 78, err: err}
	}
	if err := prepareData(a.data); err != nil {
		return err
	}
	pw := &auth.PasswordFile{Path: cfg.PasswordPath()}
	if _, err := pw.Current(); errors.Is(err, fs.ErrNotExist) {
		return exitError{code: 78, err: errors.New(`no password set: run "sudo storage-analyzer passwd" first`)}
	} else if err != nil {
		return err
	}
	sessions, err := auth.OpenSessions(filepath.Join(a.data, "sessions.db"))
	if err != nil {
		return err
	}
	defer sessions.Close()
	login := auth.NewLogin(pw, sessions, auth.NewLimiter(), cfg.TrustedProxies)

	spawn := func(args ...string) error {
		return api.SpawnDetached(a.bin, append(args, a.childArgs...)...)
	}
	apiH := api.New(api.Config{DataDir: a.data, BinPath: a.bin, Auth: login, Spawn: spawn, LoginKind: "password"})
	srv := server.NewServer(cfg.Listen, server.Handler(server.Options{API: apiH, Login: login, Static: web.Files}))
	useTLS := cfg.TLSCert != ""
	scheme := "http"
	if useTLS {
		scheme = "https"
		srv.TLSConfig = server.TLSConfig(&server.CertReloader{CertFile: cfg.TLSCert, KeyFile: cfg.TLSKey})
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	log.Printf("listening on %s (%s)", ln.Addr(), scheme)
	if !useTLS {
		log.Printf("warning: serving plain HTTP")
	}

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
	go sched.Run(ctx)

	errc := make(chan error, 1)
	go func() {
		if useTLS {
			errc <- srv.ServeTLS(ln, "", "")
		} else {
			errc <- srv.Serve(ln)
		}
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	if err := runner.Cancel(a.data); err != nil && !errors.Is(err, runner.ErrNotRunning) {
		log.Printf("cancel running scan: %v", err)
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

// serveDev is the development server: loopback only, no authentication.
func serveDev(a *app, pf *platformFlags) error {
	addr := pf.addr
	if addr == "" {
		addr = "127.0.0.1:8099"
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("refusing to listen on non-loopback address %q", host)
	}
	var static fs.FS = web.Files
	if pf.web != "" {
		static = os.DirFS(pf.web)
	}
	os.MkdirAll(filepath.Join(a.data, "scans"), 0o755)
	h := api.New(api.Config{
		DataDir: a.data, BinPath: a.bin,
		Auth: allowAll{},
		Spawn: func(args ...string) error {
			return api.SpawnDetached(a.bin, append(args, a.childArgs...)...)
		},
		ApplySchedule: func(s catalog.Settings) error {
			log.Printf("(dev) skip apply schedule %+v", s)
			return nil
		},
	})
	log.Printf("dev server on http://%s/ (data=%s)", addr, a.data)
	return server.NewServer(addr, server.Handler(server.Options{API: h, Static: static})).ListenAndServe()
}

type allowAll struct{}

func (allowAll) Check(*http.Request) (string, bool, error) { return "dev", true, nil }

// passwd sets the web password: prompts twice on a terminal, otherwise reads
// one line from stdin. Prompts go to stderr.
func passwd(pf *platformFlags) error {
	cfg, err := config.Load(pf.config)
	if err != nil {
		return exitError{code: 78, err: err}
	}
	var pw string
	if fd := int(os.Stdin.Fd()); term.IsTerminal(fd) {
		read := func(prompt string) (string, error) {
			fmt.Fprint(os.Stderr, prompt)
			b, err := term.ReadPassword(fd)
			fmt.Fprintln(os.Stderr)
			return string(b), err
		}
		if pw, err = read("New password: "); err != nil {
			return err
		}
		again, err := read("Repeat password: ")
		if err != nil {
			return err
		}
		if pw != again {
			return errors.New("passwords do not match")
		}
	} else {
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && !(errors.Is(err, io.EOF) && line != "") {
			return fmt.Errorf("read password: %w", err)
		}
		pw = strings.TrimRight(line, "\r\n")
	}
	if err := auth.CheckPolicy(pw); err != nil {
		return err
	}
	phc, err := auth.Hash(pw)
	if err != nil {
		return err
	}
	if err := auth.WritePasswordFile(cfg.PasswordPath(), phc); err != nil {
		return err
	}
	fmt.Println("password updated; all sessions are signed out")
	return nil
}
