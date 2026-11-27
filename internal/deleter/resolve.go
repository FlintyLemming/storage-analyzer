package deleter

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
)

// VolumeRoot maps a scan volume root entry ID to its display name and OS mount point.
type VolumeRoot struct {
	EntryID    int64
	Name       string
	MountPoint string
}

// Target is one validated, resolved deletion target derived from a scan database.
type Target struct {
	EntryID        int64    `json:"entryId"`
	Kind           int      `json:"kind"`
	Path           string   `json:"path"`        // UTF-8 safe OS path for display
	DisplayPath    string   `json:"displayPath"` // UTF-8 safe volume-prefixed display path
	DSize          int64    `json:"dsize"`
	ASize          int64    `json:"asize"`
	Items          int64    `json:"items"`
	BadName        bool     `json:"badName"`
	DockerRisk     bool     `json:"dockerRisk"`
	DockerType     string   `json:"dockerType,omitempty"`
	Protected      bool     `json:"protected"`
	ProtectReason  string   `json:"protectReason,omitempty"`
	RawPath        string   `json:"-"` // lossless raw byte OS path for syscalls
	RawDisplayPath []byte   `json:"-"` // lossless raw byte "<volName>/<rel>" for dupcheck pruning
	VolMount       string   `json:"-"`
	RelParts       []string `json:"-"`
}

// ResolveResult is the outcome of resolving a set of entry IDs against a scan.
type ResolveResult struct {
	Targets       []Target `json:"items"`
	Folded        int      `json:"folded"`
	Missing       int      `json:"missing"`
	TotalDSize    int64    `json:"totalDSize"`
	TotalASize    int64    `json:"totalASize"`
	TotalItems    int64    `json:"totalItems"`
	HasDockerRisk bool     `json:"hasDockerRisk"`
	HasProtected  bool     `json:"hasProtected"`
}

type dockerAnnType struct {
	Type string `json:"type"`
}

// ResolveTargets resolves entryIDs from db against roots and policy, folding
// descendants covered by an allowed ancestor in the same batch, flagging
// protected targets, and annotating Docker ancestry risk.
func ResolveTargets(db *scandb.DB, roots map[int64]VolumeRoot, policy Policy, entryIDs []int64) (ResolveResult, error) {
	var res ResolveResult
	res.Targets = []Target{}
	if len(entryIDs) == 0 {
		return res, nil
	}

	type rawCand struct {
		target    Target
		pathIDs   []int64 // root .. self
		ancestors []int64 // root .. parent
	}

	seen := make(map[int64]bool, len(entryIDs))
	cands := make([]rawCand, 0, len(entryIDs))
	allowedIDs := make(map[int64]bool, len(entryIDs))

	for _, id := range entryIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		if id <= 0 {
			res.Missing++
			continue
		}
		chain, err := db.Path(id)
		if errors.Is(err, scandb.ErrNotFound) {
			res.Missing++
			continue
		}
		if err != nil {
			return ResolveResult{}, err
		}
		if len(chain) == 0 {
			res.Missing++
			continue
		}
		self := chain[len(chain)-1]
		volEntry := chain[0]
		root, ok := roots[volEntry.ID]
		if !ok {
			root = VolumeRoot{EntryID: volEntry.ID, Name: string(volEntry.Name), MountPoint: string(volEntry.Name)}
		}

		t := Target{
			EntryID: self.ID,
			Kind:    self.Kind,
			DSize:   self.DSize,
			ASize:   self.ASize,
			Items:   self.Items,
			BadName: self.Flags&scandb.FlagBadName != 0,
			VolMount: filepath.Clean(root.MountPoint),
		}
		if self.Kind == scandb.KindDir {
			t.Items = 1 + self.Items
		} else {
			t.Items = 1
		}

		// Build relative components & paths.
		badComp := false
		parts := make([]string, 0, len(chain)-1)
		rawOS := root.MountPoint
		rawDisp := root.Name
		for _, e := range chain[1:] {
			name := string(e.Name)
			if !validComponent(name) {
				badComp = true
			}
			if !utf8.ValidString(name) {
				t.BadName = true
			}
			parts = append(parts, name)
			rawOS = joinRaw(rawOS, name)
			rawDisp = joinRaw(rawDisp, name)
		}
		t.RelParts = parts
		t.RawPath = rawOS
		t.Path = strings.ToValidUTF8(rawOS, "\uFFFD")
		t.RawDisplayPath = []byte(rawDisp)
		t.DisplayPath = strings.ToValidUTF8(rawDisp, "\uFFFD")

		depth := len(chain) - 1
		switch {
		case depth == 0 || self.Parent == 0:
			t.Protected = true
			t.ProtectReason = ReasonVolumeRoot
		case self.Kind == scandb.KindMount:
			t.Protected = true
			t.ProtectReason = ReasonMountPoint
		case badComp || root.MountPoint == "":
			t.Protected = true
			t.ProtectReason = ReasonBadPath
		default:
			if reason := policy.Check(rawOS, depth); reason != "" {
				t.Protected = true
				t.ProtectReason = reason
			}
		}

		pathIDs := make([]int64, len(chain))
		for i, e := range chain {
			pathIDs[i] = e.ID
		}
		ancs := pathIDs[:len(pathIDs)-1]
		cands = append(cands, rawCand{target: t, pathIDs: pathIDs, ancestors: ancs})
		if !t.Protected {
			allowedIDs[t.EntryID] = true
		}
	}

	// Fold descendants covered by an allowed ancestor in the same batch.
	kept := make([]rawCand, 0, len(cands))
	allPathIDs := make([]int64, 0, len(cands)*4)
	for _, c := range cands {
		covered := false
		for _, aid := range c.ancestors {
			if allowedIDs[aid] {
				covered = true
				break
			}
		}
		if covered {
			res.Folded++
			continue
		}
		kept = append(kept, c)
		allPathIDs = append(allPathIDs, c.pathIDs...)
	}

	anns, err := db.Annotations(allPathIDs)
	if err != nil {
		return ResolveResult{}, err
	}

	res.Targets = make([]Target, 0, len(kept))
	for _, c := range kept {
		t := c.target
		// Walk from self up to root so closest Docker annotation wins.
		for i := len(c.pathIDs) - 1; i >= 0; i-- {
			if a, ok := anns[c.pathIDs[i]]; ok && a.Kind == "docker" {
				t.DockerRisk = true
				var dt dockerAnnType
				if json.Unmarshal([]byte(a.Data), &dt) == nil && dt.Type != "" {
					t.DockerType = dt.Type
				}
				break
			}
		}
		if t.Protected {
			res.HasProtected = true
		} else {
			if t.DockerRisk {
				res.HasDockerRisk = true
			}
			res.TotalDSize += t.DSize
			res.TotalASize += t.ASize
			res.TotalItems += t.Items
		}
		res.Targets = append(res.Targets, t)
	}

	sort.Slice(res.Targets, func(i, j int) bool {
		if res.Targets[i].Path != res.Targets[j].Path {
			return res.Targets[i].Path < res.Targets[j].Path
		}
		return res.Targets[i].EntryID < res.Targets[j].EntryID
	})
	return res, nil
}

func validComponent(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.ContainsAny(name, "/\x00")
}

func joinRaw(base, name string) string {
	if base == "" {
		return name
	}
	if strings.HasSuffix(base, "/") {
		return base + name
	}
	return base + "/" + name
}
