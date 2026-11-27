// Package deleter resolves scan-database targets, enforces protected-path
// guardrails, and permanently deletes files and directories without following
// symlinks or crossing filesystem mount boundaries.
package deleter

import (
	"path/filepath"
	"strings"
)

// Protection reason codes returned in Target.ProtectReason.
const (
	ReasonVolumeRoot = "volume_root"
	ReasonMountPoint = "mount_point"
	ReasonSystemPath = "system_path"
	ReasonAppData    = "app_data"
	ReasonBadPath    = "bad_path"
)

// Policy determines which paths may never be deleted.
//
// Paths in AppDirs and SubtreeDeny are subtree-protected: deleting the path
// itself, any descendant inside it, or any ancestor directory containing it is
// forbidden. Paths in ExactDeny cannot be deleted themselves, while entries
// inside them remain deletable.
type Policy struct {
	Platform    string   // "linux" | "qnap"
	AppDirs     []string // Storage Analyzer's own data/config/install directories
	SubtreeDeny []string // OS-critical trees
	ExactDeny   []string // top-level containers whose children may still be deleted
}

// DefaultPolicy builds the built-in path protection policy for platform
// ("linux" or "qnap"), protecting dataDir and any extra app paths (config and
// password files on Linux, install root directory on QNAP).
func DefaultPolicy(platform, dataDir string, extraProtected ...string) Policy {
	p := Policy{
		Platform:  platform,
		ExactDeny: []string{"/"},
	}
	addProtected := func(path string) {
		if path == "" {
			return
		}
		clean := filepath.Clean(path)
		p.AppDirs = appendUnique(p.AppDirs, clean)
		if resolved, err := filepath.EvalSymlinks(clean); err == nil {
			p.AppDirs = appendUnique(p.AppDirs, filepath.Clean(resolved))
		}
	}
	addProtected(dataDir)
	for _, ep := range extraProtected {
		addProtected(ep)
	}

	if platform != "qnap" {
		p.SubtreeDeny = []string{
			"/etc", "/usr", "/boot", "/efi",
			"/bin", "/sbin", "/lib", "/lib64",
			"/proc", "/sys", "/dev", "/run",
		}
		p.ExactDeny = append(p.ExactDeny,
			"/home", "/root", "/tmp",
			"/var", "/var/lib", "/var/log", "/var/cache", "/var/tmp",
			"/opt", "/srv", "/mnt", "/media",
		)
	}
	return p
}

// Check validates absPath at tree depth below its volume root (depth 0 is the
// volume root itself). It returns an empty string if deletion is permitted, or
// a machine-readable Reason* code if blocked.
func (p Policy) Check(absPath string, depth int) string {
	if depth <= 0 {
		return ReasonVolumeRoot
	}
	if absPath == "" || !strings.HasPrefix(absPath, "/") {
		return ReasonBadPath
	}
	clean := filepath.Clean(absPath)
	if clean == "/" {
		return ReasonVolumeRoot
	}
	for _, app := range p.AppDirs {
		if sameOrInside(clean, app) || sameOrInside(app, clean) {
			return ReasonAppData
		}
	}
	for _, sys := range p.SubtreeDeny {
		if sameOrInside(clean, sys) || sameOrInside(sys, clean) {
			return ReasonSystemPath
		}
	}
	for _, ex := range p.ExactDeny {
		if clean == ex {
			return ReasonSystemPath
		}
	}
	if p.Platform == "qnap" && depth == 1 {
		switch filepath.Base(clean) {
		case ".qpkg", "@Recycle", ".system":
			return ReasonSystemPath
		}
	}
	return ""
}

// sameOrInside reports whether child equals parent or lies inside parent.
func sameOrInside(child, parent string) bool {
	if child == parent {
		return true
	}
	if parent == "/" {
		return strings.HasPrefix(child, "/")
	}
	return strings.HasPrefix(child, parent+"/")
}

func appendUnique(ss []string, v string) []string {
	for _, s := range ss {
		if s == v {
			return ss
		}
	}
	return append(ss, v)
}
