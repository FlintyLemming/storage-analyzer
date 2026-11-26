//go:build !qnap

package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/FlintyLemming/storage-analyzer/internal/api"
	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/config"
	"github.com/FlintyLemming/storage-analyzer/internal/mounts"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

// Generic Linux flavor, configured by a config file. Also what builds on
// macOS for development.

const usage = "scan|dupcheck|prune|version|serve --insecure-no-auth"

type platformFlags struct {
	config string
	addr   string
	web    string
	noAuth bool
}

func addPlatformFlags(fs *flag.FlagSet) *platformFlags {
	pf := &platformFlags{}
	fs.StringVar(&pf.config, "config", config.DefaultPath, "config file")
	fs.StringVar(&pf.addr, "addr", "127.0.0.1:8099", "serve: listen address (loopback only)")
	fs.StringVar(&pf.web, "web", "", "serve: static web directory")
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
	}
	return errUnknownCommand
}

// serve is the development server: loopback only, no authentication.
func serve(a *app, pf *platformFlags) error {
	if !pf.noAuth {
		return errors.New("serve is for development only and requires --insecure-no-auth")
	}
	host, _, err := net.SplitHostPort(pf.addr)
	if err != nil {
		return err
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("refusing to listen on non-loopback address %q", host)
	}
	webDir := pf.web
	if webDir == "" {
		webDir = "web"
	}
	os.MkdirAll(filepath.Join(a.data, "scans"), 0o755)
	h := api.New(api.Config{
		DataDir: a.data, BinPath: a.bin,
		Auth:  allowAll{},
		Spawn: func(args ...string) error { return devSpawn(a, args...) },
		ApplySchedule: func(s catalog.Settings) error {
			log.Printf("(dev) skip apply schedule %+v", s)
			return nil
		},
	})
	mux := http.NewServeMux()
	mux.Handle("/api.cgi", h)
	mux.Handle("/", http.FileServer(http.Dir(webDir)))
	log.Printf("dev server on http://%s/ (web=%s data=%s)", pf.addr, webDir, a.data)
	return http.ListenAndServe(pf.addr, mux)
}

func devSpawn(a *app, args ...string) error {
	cmd := exec.Command(a.bin, append(args, a.childArgs...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	go cmd.Wait()
	return nil
}

type allowAll struct{}

func (allowAll) Check(*http.Request) (string, bool, error) { return "dev", true, nil }
