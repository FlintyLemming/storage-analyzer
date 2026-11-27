package docker

import (
	"os"
	"path/filepath"
)

// IsTargetName reports whether base is one of the Docker subdirectories this
// package resolves.
func IsTargetName(base string) bool {
	return base == TargetOverlay2 || base == TargetContainers || base == TargetVolumes
}

// DetectRoot reports whether dir looks like a genuine Docker data-root.
// Requiring both a target directory and a Docker metadata fingerprint prevents
// false positives on arbitrary folders named "docker" or "overlay2".
func DetectRoot(dir string) bool {
	if !hasAnyDir(dir, TargetOverlay2, TargetContainers, TargetVolumes) {
		return false
	}
	// Structural fingerprints of a real Docker data-root.
	if exists(filepath.Join(dir, "image", "overlay2", "repositories.json")) ||
		isDir(filepath.Join(dir, "image", "overlay2", "imagedb")) ||
		isDir(filepath.Join(dir, "image", "overlay2", "layerdb")) {
		return true
	}
	if isRegular(filepath.Join(dir, "volumes", "metadata.db")) {
		return true
	}
	if isRegular(filepath.Join(dir, "buildkit", "metadata_v2.db")) ||
		isRegular(filepath.Join(dir, "buildkit", "snapshots.db")) {
		return true
	}
	if hasContainerConfig(filepath.Join(dir, TargetContainers)) {
		return true
	}
	return false
}

// FindRootForTarget checks whether targetPath is an overlay2, containers, or
// volumes directory inside a valid Docker data-root, returning the parent
// Docker root path and target kind.
func FindRootForTarget(targetPath string) (rootPath, targetKind string, ok bool) {
	clean := filepath.Clean(targetPath)
	base := filepath.Base(clean)
	if !IsTargetName(base) {
		return "", "", false
	}
	parent := filepath.Dir(clean)
	if parent == clean || parent == "" {
		return "", "", false
	}
	if !DetectRoot(parent) {
		return "", "", false
	}
	return parent, base, true
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func isDir(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

func isRegular(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.Mode().IsRegular()
}

func hasAnyDir(root string, names ...string) bool {
	for _, n := range names {
		if isDir(filepath.Join(root, n)) {
			return true
		}
	}
	return false
}

func hasContainerConfig(containersDir string) bool {
	entries, err := os.ReadDir(containersDir)
	if err != nil {
		return false
	}
	checked := 0
	for _, e := range entries {
		if !e.IsDir() || !isHex64(e.Name()) {
			continue
		}
		if isRegular(filepath.Join(containersDir, e.Name(), "config.v2.json")) {
			return true
		}
		checked++
		if checked >= 32 {
			break
		}
	}
	return false
}
