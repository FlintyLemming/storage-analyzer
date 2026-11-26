package schedule

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
)

const bin = "/share/CACHEDEV2_DATA/.qpkg/StorageAnalyzer/bin/storage-analyzer"

// A slice of the real /etc/config/crontab from hf-qts.
const deviceCrontab = `# m h dom m dow cmd
0 0 * * * /sbin/auto_update 1>>/dev/null 2>>/dev/null
0-59/20 3 * * * /sbin/adjust_time
0 1 * * * /etc/init.d/flush_memory.sh >/dev/null 2>&1
00 03 * * * sh /share/CACHEDEV1_DATA/.qpkg/MalwareRemover/MalwareRemover.sh scan;#_QSC_:MalwareRemover:malware_remover_schedule:None:d::
0 0 * * * /usr/local/sbin/qsh nc.archive >/dev/null 2>&1 # NotificationCenter
`

func TestLineDaily(t *testing.T) {
	l, ok := Line(catalog.Settings{ScheduleMode: "daily", Time: "03:30"}, "/x")
	if !ok || l != "30 3 * * * /x scan --trigger schedule >/dev/null 2>&1 # StorageAnalyzer" {
		t.Fatalf("line = %q ok=%v", l, ok)
	}
}

func TestLineWeekly(t *testing.T) {
	l, ok := Line(catalog.Settings{ScheduleMode: "weekly", Weekday: 0, Time: "23:05"}, "/x")
	if !ok || l != "5 23 * * 0 /x scan --trigger schedule >/dev/null 2>&1 # StorageAnalyzer" {
		t.Fatalf("line = %q ok=%v", l, ok)
	}
}

func TestLineOff(t *testing.T) {
	if l, ok := Line(catalog.Settings{ScheduleMode: "off", Time: "03:00"}, "/x"); ok || l != "" {
		t.Fatalf("line = %q ok=%v", l, ok)
	}
}

func TestRewriteReplacesOnlyTagged(t *testing.T) {
	old := deviceCrontab + "1 2 * * * /old scan --trigger schedule >/dev/null 2>&1 # StorageAnalyzer\n"
	got := Rewrite(old, "5 4 * * * /new # StorageAnalyzer")
	if got != deviceCrontab+"5 4 * * * /new # StorageAnalyzer\n" {
		t.Fatalf("rewrite =\n%s", got)
	}
	if removed := Rewrite(old, ""); removed != deviceCrontab {
		t.Fatalf("remove =\n%s", removed)
	}
}

func TestRewriteAddsMissingNewline(t *testing.T) {
	got := Rewrite("0 0 * * * /a", "1 1 * * * /b # StorageAnalyzer")
	if got != "0 0 * * * /a\n1 1 * * * /b # StorageAnalyzer\n" {
		t.Fatalf("got %q", got)
	}
}

func TestApplyRunsCrontabAndRestart(t *testing.T) {
	p := filepath.Join(t.TempDir(), "crontab")
	os.WriteFile(p, []byte(deviceCrontab), 0o644)
	var calls []string
	run := func(name string, args ...string) error {
		calls = append(calls, strings.Join(append([]string{name}, args...), " "))
		return nil
	}
	if err := Apply(catalog.Settings{ScheduleMode: "daily", Time: "04:00"}, bin, p, run); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	if !strings.HasSuffix(string(b), "0 4 * * * "+bin+" scan --trigger schedule >/dev/null 2>&1 # StorageAnalyzer\n") {
		t.Fatalf("crontab =\n%s", b)
	}
	want := []string{"crontab " + p, "/etc/init.d/crond.sh restart"}
	if strings.Join(calls, "|") != strings.Join(want, "|") {
		t.Fatalf("calls = %q, want %q", calls, want)
	}
}
