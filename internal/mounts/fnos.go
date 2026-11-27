package mounts

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

// FnOSOptions controls which local filesystems DiscoverFnOS includes.
// By default only fnOS storage spaces (/vol1, /vol2, ...) are scanned.
type FnOSOptions struct {
	IncludeSystem   bool // include "/" (system partition)
	IncludeExternal bool // include external local storage mounts (/vol00/*, /media/*, /mnt/*)
}

var fnosVolRe = regexp.MustCompile(`^/vol([1-9]\d*)$`)

func fnosVolumeIndex(mp string) (int, bool) {
	m := fnosVolRe.FindStringSubmatch(mp)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return n, true
}

func isFnOSExternalMount(mp string) bool {
	clean := filepath.Clean(mp)
	return strings.HasPrefix(clean, "/vol00/") ||
		strings.HasPrefix(clean, "/media/") ||
		strings.HasPrefix(clean, "/mnt/")
}

// SelectFnOS picks fnOS volumes from parsed mountinfo entries.
//
// It first runs Select to filter to local disk filesystems (excluding virtual,
// network, and trimafs filesystems, btrfs .snapshots, and duplicate bind
// mounts), then keeps:
//   - all fnOS storage spaces (/vol1, /vol2, ...), sorted numerically;
//   - "/" when opts.IncludeSystem is true;
//   - external local mounts (/vol00/*, /media/*, /mnt/*) when
//     opts.IncludeExternal is true.
func SelectFnOS(ms []Mount, opts FnOSOptions) []volumes.Volume {
	local := Select(ms, nil)
	var storage, sys, ext []volumes.Volume
	for _, v := range local {
		mp := filepath.Clean(v.MountPoint)
		if _, ok := fnosVolumeIndex(mp); ok {
			storage = append(storage, v)
		} else if mp == "/" && opts.IncludeSystem {
			sys = append(sys, v)
		} else if opts.IncludeExternal && isFnOSExternalMount(mp) {
			ext = append(ext, v)
		}
	}
	sort.Slice(storage, func(i, j int) bool {
		ni, _ := fnosVolumeIndex(storage[i].MountPoint)
		nj, _ := fnosVolumeIndex(storage[j].MountPoint)
		return ni < nj
	})
	sort.Slice(ext, func(i, j int) bool {
		return ext[i].MountPoint < ext[j].MountPoint
	})
	out := make([]volumes.Volume, 0, len(storage)+len(sys)+len(ext))
	out = append(out, storage...)
	out = append(out, sys...)
	out = append(out, ext...)
	return out
}

// DiscoverFnOS discovers scannable volumes on a fnOS host from /proc/self/mountinfo.
func DiscoverFnOS(opts FnOSOptions) ([]volumes.Volume, []volumes.Skipped, error) {
	f, err := os.Open(mountinfoPath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	ms, err := Parse(f)
	if err != nil {
		return nil, nil, err
	}
	return SelectFnOS(ms, opts), nil, nil
}
