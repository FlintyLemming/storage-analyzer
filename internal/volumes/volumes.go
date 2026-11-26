// Package volumes discovers the data volumes that live on user-created
// storage pools of a QNAP QTS system.
package volumes

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// Volume is a mounted data volume on a user storage pool.
type Volume struct {
	Name       string `json:"name"`             // volName, e.g. "DataVol1"
	Pool       string `json:"pool"`             // pool id, "" when unknown
	Device     string `json:"device"`           // e.g. "/dev/mapper/cachedev2"
	MountPoint string `json:"mountPoint"`       // e.g. "/share/CACHEDEV2_DATA"
	FSType     string `json:"fsType,omitempty"` // e.g. "ext4"; empty on QNAP
}

// Skipped is a configured volume that will not be scanned.
type Skipped struct {
	Name   string `json:"name"`
	Reason string `json:"reason"` // "not_mounted" | "not_share_volume"
}

// VolConf is one [VOL_n] section of /etc/volume.conf.
type VolConf struct {
	Name        string
	MappingName string
	RaidName    string
}

var shareMount = regexp.MustCompile(`^/share/CACHEDEV\d+_DATA$`)

// ParseVolumeConf parses /etc/volume.conf and returns its [VOL_n] sections in order.
func ParseVolumeConf(r io.Reader) ([]VolConf, error) {
	var out []VolConf
	var cur *VolConf
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section := line[1 : len(line)-1]
			if strings.HasPrefix(section, "VOL_") {
				out = append(out, VolConf{})
				cur = &out[len(out)-1]
			} else {
				cur = nil
			}
			continue
		}
		if cur == nil {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch k {
		case "volName":
			cur.Name = v
		case "mappingName":
			cur.MappingName = v
		case "raidName":
			cur.RaidName = v
		}
	}
	return out, sc.Err()
}

// ParseMounts parses /proc/mounts into device → first mount point.
func ParseMounts(r io.Reader) map[string]string {
	m := map[string]string{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 2 {
			continue
		}
		dev, mp := UnescapeMount(f[0]), UnescapeMount(f[1])
		if _, seen := m[dev]; !seen {
			m[dev] = mp
		}
	}
	return m
}

// UnescapeMount decodes the octal escapes (\040 etc.) used in /proc/mounts.
func UnescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// ParseQcliPools parses the table printed by `qcli_storage` (no arguments).
// The result maps both RAID device (e.g. "/dev/md2") and VolName to pool id.
// The table is fixed-width, so values are cut at the header's column offsets.
func ParseQcliPools(out string) map[string]string {
	pools := map[string]string{}
	lines := strings.Split(out, "\n")
	hdr := -1
	for i, l := range lines {
		if strings.Contains(l, "Pool") && strings.Contains(l, "RAID") && strings.Contains(l, "VolName") {
			hdr = i
			break
		}
	}
	if hdr < 0 {
		return pools
	}
	header := lines[hdr]
	type col struct {
		name  string
		start int
	}
	var cols []col
	for i := 0; i < len(header); {
		if header[i] == ' ' {
			i++
			continue
		}
		j := i
		for j < len(header) && header[j] != ' ' {
			j++
		}
		cols = append(cols, col{header[i:j], i})
		i = j
	}
	cell := func(line, name string) string {
		for k, c := range cols {
			if c.name != name {
				continue
			}
			end := len(line)
			if k+1 < len(cols) && cols[k+1].start < end {
				end = cols[k+1].start
			}
			if c.start >= len(line) {
				return ""
			}
			return strings.TrimSpace(line[c.start:end])
		}
		return ""
	}
	for _, l := range lines[hdr+1:] {
		if strings.TrimSpace(l) == "" {
			continue
		}
		pool := cell(l, "Pool")
		if pool == "" || pool == "--" {
			continue
		}
		if raid := cell(l, "RAID"); strings.HasPrefix(raid, "/dev/") {
			pools[raid] = pool
		}
		for _, v := range strings.Split(cell(l, "VolName"), ",") {
			if v = strings.TrimSpace(v); v != "" && v != "--" {
				pools[v] = pool
			}
		}
	}
	return pools
}

// Resolve combines the parsed sources into the volumes to scan.
func Resolve(vc []VolConf, mounts, pools map[string]string) ([]Volume, []Skipped) {
	var vols []Volume
	var skipped []Skipped
	for _, v := range vc {
		if v.MappingName == "" {
			continue
		}
		mp, ok := mounts[v.MappingName]
		if !ok {
			skipped = append(skipped, Skipped{Name: v.Name, Reason: "not_mounted"})
			continue
		}
		if !shareMount.MatchString(mp) {
			skipped = append(skipped, Skipped{Name: v.Name, Reason: "not_share_volume"})
			continue
		}
		pool := pools[v.RaidName]
		if pool == "" {
			pool = pools[v.Name]
		}
		vols = append(vols, Volume{Name: v.Name, Pool: pool, Device: v.MappingName, MountPoint: mp})
	}
	return vols, skipped
}

// Discover reads the live system files and returns the volumes to scan.
func Discover() ([]Volume, []Skipped, error) {
	vf, err := os.Open("/etc/volume.conf")
	if err != nil {
		return nil, nil, err
	}
	defer vf.Close()
	vc, err := ParseVolumeConf(vf)
	if err != nil {
		return nil, nil, err
	}
	mf, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, nil, err
	}
	defer mf.Close()
	mounts := ParseMounts(mf)
	var pools map[string]string
	if out, err := exec.Command("/sbin/qcli_storage").Output(); err == nil {
		pools = ParseQcliPools(string(out))
	}
	vols, skipped := Resolve(vc, mounts, pools)
	return vols, skipped, nil
}
