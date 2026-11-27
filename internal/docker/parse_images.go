package docker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type parsedImage struct {
	ID       string // 64-hex without sha256:
	Refs     []string
	Dangling bool
}

type imagesData struct {
	status         sourceStatus
	chainImages    map[string][]parsedImage // chainHex -> images using this layer
	chainCreatedBy map[string]string        // chainHex -> first non-empty Dockerfile created_by
	chainToCacheID map[string]string        // chainHex -> overlay2 cache-id
	cacheIDToChain map[string]string        // overlay2 cache-id -> chainHex
	chainParent    map[string]string        // chainHex -> parent chainHex
	mountIDToCID   map[string]string        // overlay2 mount-id -> containerID
	initIDToCID    map[string]string        // overlay2 init-id -> containerID
	cidParentChain map[string]string        // containerID -> parent chainHex
}

type repositoriesJSON struct {
	Repositories map[string]map[string]string `json:"Repositories"`
}

type imageConfigJSON struct {
	RootFS struct {
		Type    string   `json:"type"`
		DiffIDs []string `json:"diff_ids"`
	} `json:"rootfs"`
	History []struct {
		CreatedBy  string `json:"created_by"`
		EmptyLayer bool   `json:"empty_layer"`
	} `json:"history"`
}

func parseImagesAndLayers(dockerRoot string) imagesData {
	out := imagesData{
		status:         sourceOK,
		chainImages:    map[string][]parsedImage{},
		chainCreatedBy: map[string]string{},
		chainToCacheID: map[string]string{},
		cacheIDToChain: map[string]string{},
		chainParent:    map[string]string{},
		mountIDToCID:   map[string]string{},
		initIDToCID:    map[string]string{},
		cidParentChain: map[string]string{},
	}

	imgRoot := filepath.Join(dockerRoot, "image", "overlay2")
	if _, err := os.Stat(imgRoot); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			out.status = sourceAbsent
		} else {
			out.status = sourceError
		}
		return out
	}

	// 1. repositories.json -> imageID -> preferred human-readable refs.
	refsByImage := parseRepositories(filepath.Join(imgRoot, "repositories.json"), &out.status)

	// 2. imagedb/content/sha256/* -> diff_ids -> chainIDs + history created_by.
	parseImageDB(filepath.Join(imgRoot, "imagedb", "content", "sha256"), refsByImage, &out)

	// 3. layerdb/sha256/* -> chainID -> cache-id & parent.
	parseLayerDB(filepath.Join(imgRoot, "layerdb", "sha256"), &out)

	// 4. layerdb/mounts/* -> containerID -> mount-id, init-id, parent.
	parseLayerMounts(filepath.Join(imgRoot, "layerdb", "mounts"), &out)

	return out
}

func parseRepositories(path string, status *sourceStatus) map[string][]string {
	raw, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			*status = sourceError
		}
		return nil
	}
	var repos repositoriesJSON
	if err := json.Unmarshal(raw, &repos); err != nil {
		*status = sourceError
		return nil
	}
	// Per (imageID, repo): keep tagged refs; keep digest refs only if that repo
	// has no tagged ref for the same imageID.
	type repoRefs struct {
		tags    []string
		digests []string
	}
	byImgRepo := map[string]map[string]*repoRefs{}
	for repo, entries := range repos.Repositories {
		for ref, imgDigest := range entries {
			imgHex := normalizeDigestHex(imgDigest)
			if imgHex == "" {
				continue
			}
			m := byImgRepo[imgHex]
			if m == nil {
				m = map[string]*repoRefs{}
				byImgRepo[imgHex] = m
			}
			rr := m[repo]
			if rr == nil {
				rr = &repoRefs{}
				m[repo] = rr
			}
			if strings.Contains(ref, "@") {
				rr.digests = append(rr.digests, ref)
			} else {
				rr.tags = append(rr.tags, ref)
			}
		}
	}
	out := make(map[string][]string, len(byImgRepo))
	for imgHex, repoMap := range byImgRepo {
		seen := map[string]bool{}
		var list []string
		for _, rr := range repoMap {
			chosen := rr.tags
			if len(chosen) == 0 {
				chosen = rr.digests
			}
			for _, ref := range chosen {
				if !seen[ref] {
					seen[ref] = true
					list = append(list, ref)
				}
			}
		}
		sort.Strings(list)
		out[imgHex] = list
	}
	return out
}

func parseImageDB(dir string, refsByImage map[string][]string, out *imagesData) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			out.status = sourceError
		}
		return
	}
	for _, e := range entries {
		if e.IsDir() || !isHex64(e.Name()) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			out.status = sourceError
			continue
		}
		var cfg imageConfigJSON
		if err := json.Unmarshal(raw, &cfg); err != nil {
			out.status = sourceError
			continue
		}
		if len(cfg.RootFS.DiffIDs) == 0 {
			continue
		}
		refs := refsByImage[e.Name()]
		img := parsedImage{
			ID:       e.Name(),
			Refs:     refs,
			Dangling: len(refs) == 0,
		}
		var nonEmpty []string
		for _, h := range cfg.History {
			if !h.EmptyLayer {
				nonEmpty = append(nonEmpty, h.CreatedBy)
			}
		}
		alignHistory := len(nonEmpty) == len(cfg.RootFS.DiffIDs)

		var curChain string
		for i, diffID := range cfg.RootFS.DiffIDs {
			diffID = strings.TrimSpace(diffID)
			if diffID == "" {
				break
			}
			if i == 0 {
				curChain = diffID
			} else {
				sum := sha256.Sum256([]byte(curChain + " " + diffID))
				curChain = "sha256:" + hex.EncodeToString(sum[:])
			}
			chHex := normalizeDigestHex(curChain)
			out.chainImages[chHex] = append(out.chainImages[chHex], img)
			if alignHistory && nonEmpty[i] != "" {
				if _, exists := out.chainCreatedBy[chHex]; !exists {
					out.chainCreatedBy[chHex] = truncateRunes(nonEmpty[i], 1024)
				}
			}
		}
	}
}

func parseLayerDB(dir string, out *imagesData) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			out.status = sourceError
		}
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		chHex := e.Name()
		ldir := filepath.Join(dir, chHex)
		cidBytes, err := os.ReadFile(filepath.Join(ldir, "cache-id"))
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				out.status = sourceError
			}
			continue
		}
		cacheID := strings.TrimSpace(string(cidBytes))
		if cacheID != "" {
			out.chainToCacheID[chHex] = cacheID
			out.cacheIDToChain[cacheID] = chHex
		}
		if pBytes, err := os.ReadFile(filepath.Join(ldir, "parent")); err == nil {
			if pHex := normalizeDigestHex(string(pBytes)); pHex != "" {
				out.chainParent[chHex] = pHex
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			out.status = sourceError
		}
	}
}

func parseLayerMounts(dir string, out *imagesData) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			out.status = sourceError
		}
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		cid := e.Name()
		mdir := filepath.Join(dir, cid)
		if b, err := os.ReadFile(filepath.Join(mdir, "mount-id")); err == nil {
			if id := strings.TrimSpace(string(b)); id != "" {
				out.mountIDToCID[id] = cid
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			out.status = sourceError
		}
		if b, err := os.ReadFile(filepath.Join(mdir, "init-id")); err == nil {
			if id := strings.TrimSpace(string(b)); id != "" {
				out.initIDToCID[id] = cid
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			out.status = sourceError
		}
		if b, err := os.ReadFile(filepath.Join(mdir, "parent")); err == nil {
			if pHex := normalizeDigestHex(string(b)); pHex != "" {
				out.cidParentChain[cid] = pHex
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			out.status = sourceError
		}
	}
}
