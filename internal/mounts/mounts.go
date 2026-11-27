// Package mounts discovers scannable volumes from /proc/self/mountinfo on a
// generic Linux system.
package mounts

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/FlintyLemming/storage-analyzer/internal/config"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

const mountinfoPath = "/proc/self/mountinfo"

// Mount is one mountinfo entry.
type Mount struct {
	ID, Parent int
	Dev        string // "maj:min"
	Root       string
	MountPoint string
	FSType     string
	Source     string
	SubvolID   string // btrfs subvolid=<N> from super options, empty otherwise
	Subvol     string // btrfs subvol=<path> from super options, empty otherwise
}

// LocalTypes are the filesystem types treated as local disks.
var LocalTypes = map[string]bool{
	"ext2": true, "ext3": true, "ext4": true, "xfs": true, "btrfs": true,
	"zfs": true, "f2fs": true, "bcachefs": true, "jfs": true, "reiserfs": true,
	"ntfs": true, "ntfs3": true, "exfat": true, "vfat": true, "fuseblk": true,
}

// Parse reads mountinfo-formatted text. Entries keep file order.
func Parse(r io.Reader) ([]Mount, error) {
	var out []Mount
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		f := strings.Fields(line)
		sep := -1
		for i := 6; i < len(f); i++ {
			if f[i] == "-" {
				sep = i
				break
			}
		}
		if len(f) < 7 || sep < 0 || len(f) < sep+3 {
			return nil, fmt.Errorf("mountinfo line %d: malformed", n)
		}
		id, err1 := strconv.Atoi(f[0])
		parent, err2 := strconv.Atoi(f[1])
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("mountinfo line %d: bad mount id", n)
		}
		fsType := f[sep+1]
		var subvolID, subvol string
		if fsType == "btrfs" && len(f) > sep+3 {
			for _, opt := range strings.Split(f[sep+3], ",") {
				if id, ok := strings.CutPrefix(opt, "subvolid="); ok && id != "" {
					subvolID = id
				} else if sv, ok := strings.CutPrefix(opt, "subvol="); ok && sv != "" {
					subvol = volumes.UnescapeMount(sv)
				}
			}
		}
		out = append(out, Mount{
			ID: id, Parent: parent, Dev: f[2],
			Root:       volumes.UnescapeMount(f[3]),
			MountPoint: volumes.UnescapeMount(f[4]),
			FSType:     fsType,
			Source:     volumes.UnescapeMount(f[sep+2]),
			SubvolID:   subvolID,
			Subvol:     subvol,
		})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// isAncestor reports whether root a equals b or is a path ancestor of b.
func isAncestor(a, b string) bool {
	if a == b || a == "/" {
		return true
	}
	return strings.HasPrefix(b, a+"/")
}

// Select picks the mounts to scan as volumes, deterministically.
func Select(ms []Mount, exclude []string) []volumes.Volume {
	// 1. last mount per mount point wins (topmost).
	last := map[string]int{}
	for i, m := range ms {
		last[m.MountPoint] = i
	}
	groups := map[string][]Mount{}
	for i, m := range ms {
		if last[m.MountPoint] != i {
			continue
		}
		// 2, 3.
		if !LocalTypes[m.FSType] {
			continue
		}
		if m.FSType == "btrfs" && filepath.Base(m.MountPoint) == ".snapshots" {
			continue
		}
		// Distinct btrfs subvolumes share a mountinfo major:minor but have
		// distinct stat(2) st_dev values (so the scanner does not cross into
		// them); group by subvolid so mounted child subvolumes aren't dropped
		// when a parent subvolume (e.g. subvolid=5) is also mounted.
		key := m.Dev
		if m.SubvolID != "" {
			key = m.Dev + "#subvolid=" + m.SubvolID
		}
		groups[key] = append(groups[key], m)
	}

	// 4. drop mounts whose content is already covered by another on the same device.
	var kept []Mount
	for _, g := range groups {
		sort.Slice(g, func(i, j int) bool {
			a, b := g[i], g[j]
			if len(a.Root) != len(b.Root) {
				return len(a.Root) < len(b.Root)
			}
			if len(a.MountPoint) != len(b.MountPoint) {
				return len(a.MountPoint) < len(b.MountPoint)
			}
			return a.MountPoint < b.MountPoint
		})
		var keep []Mount
		for _, m := range g {
			covered := false
			for _, k := range keep {
				if isAncestor(k.Root, m.Root) {
					covered = true
					break
				}
			}
			if !covered {
				keep = append(keep, m)
			}
		}
		kept = append(kept, keep...)
	}

	// 5. exclusions.
	ex := map[string]bool{}
	for _, e := range exclude {
		ex[filepath.Clean(e)] = true
	}
	var out []volumes.Volume
	for _, m := range kept {
		if ex[filepath.Clean(m.MountPoint)] {
			continue
		}
		out = append(out, volumes.Volume{Name: m.MountPoint, MountPoint: m.MountPoint, Device: m.Source, FSType: m.FSType})
	}
	// 6.
	sort.Slice(out, func(i, j int) bool { return out[i].MountPoint < out[j].MountPoint })
	return out
}

// FromPaths builds volumes from explicitly configured paths. Missing or
// non-directory paths are reported as skipped; paths nested inside another
// configured path are dropped with a log message.
func FromPaths(paths []string, ms []Mount) ([]volumes.Volume, []volumes.Skipped) {
	var cleaned []string
	seen := map[string]bool{}
	var skipped []volumes.Skipped
	for _, p := range paths {
		p = filepath.Clean(p)
		if seen[p] {
			continue
		}
		seen[p] = true
		st, err := os.Stat(p)
		if err != nil {
			reason := "read_error"
			if errors.Is(err, fs.ErrNotExist) {
				reason = "not_found"
			}
			skipped = append(skipped, volumes.Skipped{Name: p, Reason: reason})
			continue
		}
		if !st.IsDir() {
			skipped = append(skipped, volumes.Skipped{Name: p, Reason: "not_directory"})
			continue
		}
		cleaned = append(cleaned, p)
	}

	var vols []volumes.Volume
	for _, p := range cleaned {
		inside := ""
		for _, q := range cleaned {
			if q != p && isAncestor(q, p) {
				inside = q
				break
			}
		}
		if inside != "" {
			log.Printf("path %s is inside %s, skipped", p, inside)
			continue
		}
		v := volumes.Volume{Name: p, MountPoint: p}
		best := -1
		for _, m := range ms {
			if isAncestor(m.MountPoint, p) && len(m.MountPoint) >= best {
				best = len(m.MountPoint)
				v.Device, v.FSType = m.Source, m.FSType
			}
		}
		vols = append(vols, v)
	}
	return vols, skipped
}

// Discover finds volumes for the configured paths, or automatically from
// mountinfo when no paths are configured.
func Discover(c config.Config) ([]volumes.Volume, []volumes.Skipped, error) {
	var ms []Mount
	f, err := os.Open(mountinfoPath)
	if err == nil {
		defer f.Close()
		ms, err = Parse(f)
	}
	if c.Paths != nil {
		// Mount info only enriches Device/FSType; ignore failures.
		vs, sk := FromPaths(c.Paths, ms)
		return vs, sk, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return Select(ms, c.Exclude), nil, nil
}
