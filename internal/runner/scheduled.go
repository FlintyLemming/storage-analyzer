package runner

import (
	"context"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/dupcheck"
)

// ScheduledRun is what RunScheduled did.
type ScheduledRun struct {
	ScanID   int64
	ScanDone time.Time // when the scan finished (zero if it did not)
	Checked  bool      // a duplicate check of ScanID was started
	Dup      dupcheck.Summary
	DupErr   error
}

// RunScheduled performs what one cron firing asks for: a scan and then, if
// the settings ask for it, a duplicate check of that scan. It takes the lock
// once and holds it across both, so no manual task can slip in between.
//
// A busy lock skips the whole run (ErrBusy). A scan that fails or is
// canceled skips the check. The returned error is the scan's; the check's
// error is in ScheduledRun.DupErr.
func RunScheduled(ctx context.Context, o Options) (ScheduledRun, error) {
	lock, err := tryLock(o.DataDir, 5)
	if err != nil {
		return ScheduledRun{}, err
	}
	defer lock.Close()

	cat, err := catalog.Open(o.DataDir)
	if err != nil {
		return ScheduledRun{}, err
	}
	s, err := cat.Settings()
	cat.Close()
	if err != nil {
		return ScheduledRun{}, err
	}

	if o.Now == nil {
		o.Now = time.Now
	}
	o.Trigger = "schedule"
	o.thenDupCheck = s.ScheduleDupCheck
	var res ScheduledRun
	res.ScanID, err = runScan(ctx, o, lock)
	if err != nil {
		return res, err
	}
	res.ScanDone = o.Now()
	if !s.ScheduleDupCheck || ctx.Err() != nil {
		return res, nil
	}
	res.Checked = true
	res.Dup, res.DupErr = runDupCheck(ctx, DupOptions{
		DataDir: o.DataDir, ScanID: res.ScanID, Now: o.Now, FS: o.FS, Trigger: "schedule",
	}, lock)
	return res, nil
}
