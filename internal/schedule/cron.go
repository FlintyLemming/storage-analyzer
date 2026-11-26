// Package schedule maintains the StorageAnalyzer line in the QTS crontab.
package schedule

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
)

// Tag marks the line this app owns in /etc/config/crontab.
const Tag = "# StorageAnalyzer"

// Line renders the crontab line for s; ok is false when scheduling is off.
func Line(s catalog.Settings, binPath string) (string, bool) {
	if s.ScheduleMode != "daily" && s.ScheduleMode != "weekly" {
		return "", false
	}
	hh, mm, _ := strings.Cut(s.Time, ":")
	h, _ := strconv.Atoi(hh)
	m, _ := strconv.Atoi(mm)
	dow := "*"
	if s.ScheduleMode == "weekly" {
		dow = strconv.Itoa(s.Weekday)
	}
	return fmt.Sprintf("%d %d * * %s %s scan --trigger schedule >/dev/null 2>&1 %s", m, h, dow, binPath, Tag), true
}

// Rewrite removes every tagged line from crontab and appends line if non-empty.
func Rewrite(crontab, line string) string {
	var b strings.Builder
	for _, l := range strings.Split(strings.TrimSuffix(crontab, "\n"), "\n") {
		if strings.HasSuffix(strings.TrimSpace(l), Tag) {
			continue
		}
		b.WriteString(l)
		b.WriteByte('\n')
	}
	if crontab == "" {
		b.Reset()
	}
	if line != "" {
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// Apply writes the schedule into crontabPath and reloads cron.
func Apply(s catalog.Settings, binPath, crontabPath string, run func(name string, args ...string) error) error {
	old, err := os.ReadFile(crontabPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	line, _ := Line(s, binPath)
	next := Rewrite(string(old), line)
	if next == string(old) {
		return nil
	}
	if err := os.WriteFile(crontabPath, []byte(next), 0o644); err != nil {
		return err
	}
	if err := run("crontab", crontabPath); err != nil {
		return fmt.Errorf("crontab: %w", err)
	}
	if err := run("/etc/init.d/crond.sh", "restart"); err != nil {
		return fmt.Errorf("crond restart: %w", err)
	}
	return nil
}
