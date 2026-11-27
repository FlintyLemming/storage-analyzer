package docker

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type parsedContainer struct {
	Ref     ContainerRef
	ImageID string // 64-hex image ID without sha256: prefix
}

type containersData struct {
	status        sourceStatus
	byID          map[string]parsedContainer
	missingConfig map[string]bool
	inUseImages   map[string]bool
	volumeMounts  map[string][]VolumeMount
}

type containerConfigV2 struct {
	ID     string `json:"ID"`
	Name   string `json:"Name"`
	Image  string `json:"Image"`
	Config struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Running    bool   `json:"Running"`
		Paused     bool   `json:"Paused"`
		Restarting bool   `json:"Restarting"`
		Dead       bool   `json:"Dead"`
		StartedAt  string `json:"StartedAt"`
	} `json:"State"`
	MountPoints map[string]struct {
		Name        string `json:"Name"`
		Source      string `json:"Source"`
		Destination string `json:"Destination"`
		Driver      string `json:"Driver"`
		Type        string `json:"Type"`
		RW          bool   `json:"RW"`
	} `json:"MountPoints"`
}

func parseContainers(dockerRoot string) containersData {
	out := containersData{
		status:        sourceOK,
		byID:          map[string]parsedContainer{},
		missingConfig: map[string]bool{},
		inUseImages:   map[string]bool{},
		volumeMounts:  map[string][]VolumeMount{},
	}
	dir := filepath.Join(dockerRoot, TargetContainers)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			out.status = sourceAbsent
		} else {
			out.status = sourceError
		}
		return out
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		cid := e.Name()
		cfgPath := filepath.Join(dir, cid, "config.v2.json")
		raw, err := os.ReadFile(cfgPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				if isHex64(cid) {
					out.missingConfig[cid] = true
				}
			} else {
				out.status = sourceError
			}
			continue
		}
		var cfg containerConfigV2
		if err := json.Unmarshal(raw, &cfg); err != nil {
			out.status = sourceError
			continue
		}
		name := strings.TrimPrefix(strings.TrimSpace(cfg.Name), "/")
		if name == "" {
			name = shortID(cid)
		}
		imgHex := normalizeDigestHex(cfg.Image)
		if imgHex != "" {
			out.inUseImages[imgHex] = true
		}
		imgName := strings.TrimSpace(cfg.Config.Image)
		if imgName == "" && imgHex != "" {
			imgName = shortID(imgHex)
		}
		status := deriveContainerStatus(cfg)
		var comp *ComposeInfo
		if proj, svc := cfg.Config.Labels["com.docker.compose.project"], cfg.Config.Labels["com.docker.compose.service"]; proj != "" || svc != "" {
			comp = &ComposeInfo{Project: proj, Service: svc}
		}
		ref := ContainerRef{
			Name:    name,
			ID:      shortID(cid),
			Image:   imgName,
			Status:  status,
			Compose: comp,
		}
		out.byID[cid] = parsedContainer{Ref: ref, ImageID: imgHex}

		for destKey, mp := range cfg.MountPoints {
			if mp.Name == "" || (mp.Type != "" && mp.Type != "volume") {
				continue
			}
			dest := mp.Destination
			if dest == "" {
				dest = destKey
			}
			out.volumeMounts[mp.Name] = append(out.volumeMounts[mp.Name], VolumeMount{
				Container:   name,
				ContainerID: shortID(cid),
				Destination: dest,
				RW:          mp.RW,
				Status:      status,
			})
		}
	}
	for vol := range out.volumeMounts {
		sort.Slice(out.volumeMounts[vol], func(i, j int) bool {
			a, b := out.volumeMounts[vol][i], out.volumeMounts[vol][j]
			if a.Container != b.Container {
				return a.Container < b.Container
			}
			return a.Destination < b.Destination
		})
	}
	return out
}

func deriveContainerStatus(cfg containerConfigV2) string {
	if cfg.State.Running {
		if cfg.State.Paused {
			return "paused"
		}
		if cfg.State.Restarting {
			return "restarting"
		}
		return "running"
	}
	if cfg.State.Dead {
		return "dead"
	}
	if cfg.State.StartedAt == "" || strings.HasPrefix(cfg.State.StartedAt, "0001-01-01") {
		return "created"
	}
	return "exited"
}
