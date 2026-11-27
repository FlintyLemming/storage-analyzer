//go:build qnap

package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http/cgi"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/FlintyLemming/storage-analyzer/internal/api"
	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/deleter"
	"github.com/FlintyLemming/storage-analyzer/internal/runner"
	"github.com/FlintyLemming/storage-analyzer/internal/schedule"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

// QNAP QPKG flavor: CGI behind QTS web server, QTS session auth, cron
// schedule in /etc/config/crontab.

const usage = "cgi|scan|dupcheck|delete|apply-schedule|prune|init-start|init-stop|version"

const crontabPath = "/etc/config/crontab"

type platformFlags struct {
	root string
}

func addPlatformFlags(fs *flag.FlagSet) *platformFlags {
	pf := &platformFlags{}
	fs.StringVar(&pf.root, "root", "", "install root (default: derived from executable)")
	return pf
}

// newApp derives the install root from the executable location
// (<root>/bin/storage-analyzer) unless --root is given.
func newApp(pf *platformFlags) (*app, error) {
	exe, err := os.Executable()
	if err == nil {
		if r, err := filepath.EvalSymlinks(exe); err == nil {
			exe = r
		}
	}
	root := pf.root
	if root == "" {
		root = filepath.Dir(filepath.Dir(exe))
	}
	root, _ = filepath.Abs(root)
	dataDir := filepath.Join(root, "data")
	return &app{
		data:         dataDir,
		bin:          exe,
		discover:     volumes.Discover,
		allowDelete:  true,
		deletePolicy: deleter.DefaultPolicy("qnap", dataDir, root),
	}, nil
}

func runPlatform(cmd string, a *app, pf *platformFlags) error {
	switch cmd {
	case "cgi":
		return cgi.Serve(api.New(api.Config{
			DataDir:       a.data,
			BinPath:       a.bin,
			Auth:          api.QTSAuth{URL: api.DefaultAuthURL},
			Discover:      a.discover,
			DeleteEnabled: a.allowDelete,
			DeletePolicy:  a.deletePolicy,
			ApplySchedule: func(s catalog.Settings) error {
				return schedule.Apply(s, a.bin, crontabPath, runCmd)
			},
		}))
	case "apply-schedule":
		return applySchedule(a, nil)
	case "init-start":
		if err := prepareData(a.data); err != nil {
			return err
		}
		return applySchedule(a, nil)
	case "init-stop":
		if err := runner.Cancel(a.data); err != nil && !errors.Is(err, runner.ErrNotRunning) {
			log.Printf("cancel: %v", err)
		}
		off := catalog.DefaultSettings
		off.ScheduleMode = "off"
		return applySchedule(a, &off)
	}
	return errUnknownCommand
}

func runCmd(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %v: %w: %s", name, args, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func applySchedule(a *app, override *catalog.Settings) error {
	c, err := catalog.Open(a.data)
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
	return schedule.Apply(s, a.bin, crontabPath, runCmd)
}
