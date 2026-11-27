// Package docker resolves on-disk Docker Engine metadata (overlay2 layers,
// containers, volumes, and BuildKit build cache) into self-contained
// annotations persisted inside a scan database.
package docker

import (
	"strings"
	"unicode"
)

// Target directory basenames resolved inside a Docker data-root.
const (
	TargetOverlay2   = "overlay2"
	TargetContainers = "containers"
	TargetVolumes    = "volumes"
)

// Annotation types stored in Annotation.Type.
const (
	TypeImageLayer    = "image_layer"
	TypeContainerRW   = "container_rw"
	TypeContainerInit = "container_init"
	TypeContainer     = "container"
	TypeBuildCache    = "build_cache"
	TypeVolumeAnon    = "volume_anon"
	TypeVolumeNamed   = "volume_named"
	TypeInternal      = "internal"
	TypeOrphan        = "orphan"
	TypeDirOverlay2   = "dir_overlay2"
	TypeDirContainers = "dir_containers"
	TypeDirVolumes    = "dir_volumes"
)

// ComposeInfo captures Docker Compose project/service/volume labels when set.
type ComposeInfo struct {
	Project string `json:"project,omitempty"`
	Service string `json:"service,omitempty"`
	Volume  string `json:"volume,omitempty"`
}

// ImageRef describes one Docker image referencing an overlay2 layer.
type ImageRef struct {
	Name     string `json:"name"`
	ID       string `json:"id,omitempty"`
	Dangling bool   `json:"dangling,omitempty"`
	InUse    bool   `json:"inUse,omitempty"`
}

// ContainerRef describes one Docker container.
type ContainerRef struct {
	Name    string       `json:"name"`
	ID      string       `json:"id"`
	Image   string       `json:"image,omitempty"`
	Status  string       `json:"status,omitempty"`
	Compose *ComposeInfo `json:"compose,omitempty"`
}

// VolumeMount describes one container mounting a Docker volume.
type VolumeMount struct {
	Container   string `json:"container"`
	ContainerID string `json:"containerId,omitempty"`
	Destination string `json:"destination"`
	RW          bool   `json:"rw"`
	Status      string `json:"status,omitempty"`
}

// Annotation is the structured Docker attribution persisted as JSON in scandb.
type Annotation struct {
	Type      string            `json:"type"`
	Label     string            `json:"label,omitempty"`
	Sublabel  string            `json:"sublabel,omitempty"`
	Count     int               `json:"count,omitempty"`
	Status    string            `json:"status,omitempty"`
	CacheType string            `json:"cacheType,omitempty"`
	Compose   *ComposeInfo      `json:"compose,omitempty"`
	Images    []ImageRef        `json:"images,omitempty"`
	Container *ContainerRef     `json:"container,omitempty"`
	Mounts    []VolumeMount     `json:"mounts,omitempty"`
	Unmounted bool              `json:"unmounted,omitempty"`
	Driver    string            `json:"driver,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	CreatedBy string            `json:"createdBy,omitempty"`
	BuildDesc string            `json:"buildDesc,omitempty"`
	Note      string            `json:"note,omitempty"`
	Warnings  []string          `json:"warnings,omitempty"`
}

type sourceStatus int

const (
	sourceOK sourceStatus = iota
	sourceAbsent
	sourceError
)

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < 64; i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func shortID(s string) string {
	s = strings.TrimPrefix(s, "sha256:")
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func normalizeDigestHex(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "layer:")
	s = strings.TrimPrefix(s, "sha256:")
	return strings.TrimSpace(s)
}

func truncateRunes(s string, max int) string {
	s = strings.TrimSpace(s)
	if max <= 0 || len(s) <= max {
		return s
	}
	n := 0
	for i := range s {
		if n == max {
			return strings.TrimSpace(s[:i]) + "…"
		}
		n++
	}
	return s
}

func collapseWhitespace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSpace := false
	for _, r := range strings.TrimSpace(s) {
		if unicode.IsSpace(r) {
			if !inSpace {
				b.WriteByte(' ')
				inSpace = true
			}
			continue
		}
		b.WriteRune(r)
		inSpace = false
	}
	return b.String()
}

func cleanCreatedBy(s string) string {
	s = collapseWhitespace(s)
	for _, prefix := range []string{
		"/bin/sh -c #(nop) ",
		"/bin/sh -c ",
		"cmd /S /C #(nop) ",
		"cmd /S /C ",
	} {
		if rest, ok := strings.CutPrefix(s, prefix); ok {
			s = strings.TrimSpace(rest)
			break
		}
	}
	return truncateRunes(s, 140)
}
