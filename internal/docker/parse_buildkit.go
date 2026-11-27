package docker

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

type buildkitRecord struct {
	ID          string
	SnapshotKey string
	ChainHex    string
	RecordType  string
	Description string
}

type buildkitData struct {
	status        sourceStatus
	records       []buildkitRecord
	deletedIDs    map[string]bool
	snapCommitted map[string]string // snapshotKey -> graphdriver overlay2 dir name
	snapChainHex  map[string]string // snapshotKey -> chainHex
	trackedSnaps  map[string]bool   // overlay2 dir names present in snapshots.db
}

type bkMetaValue struct {
	Value json.RawMessage `json:"value,omitempty"`
}

func decodeBKString(raw []byte) string {
	var v bkMetaValue
	if err := json.Unmarshal(raw, &v); err != nil || len(v.Value) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(v.Value, &s); err != nil {
		return ""
	}
	return s
}

func decodeBKBool(raw []byte) bool {
	var v bkMetaValue
	if err := json.Unmarshal(raw, &v); err != nil || len(v.Value) == 0 {
		return false
	}
	var b bool
	if err := json.Unmarshal(v.Value, &b); err != nil {
		return false
	}
	return b
}

func parseBuildKit(dockerRoot, _ string) buildkitData {
	out := buildkitData{
		status:        sourceOK,
		deletedIDs:    map[string]bool{},
		snapCommitted: map[string]string{},
		snapChainHex:  map[string]string{},
		trackedSnaps:  map[string]bool{},
	}
	bkDir := filepath.Join(dockerRoot, "buildkit")
	if _, err := os.Stat(bkDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			out.status = sourceAbsent
		} else {
			out.status = sourceError
		}
		return out
	}

	metaPath := filepath.Join(bkDir, "metadata_v2.db")
	err := viewBoltNoLock(metaPath, func(root *boltBucket) error {
		out.records = out.records[:0]
		clear(out.deletedIDs)

		mainB, err := root.Bucket([]byte("_main"))
		if err != nil || mainB == nil {
			return err
		}
		return mainB.ForEach(func(k, _ []byte, sb *boltBucket) error {
			if sb == nil {
				return nil
			}
			recID := string(k)
			if raw, err := sb.Get([]byte("cache.deleted")); err != nil {
				return err
			} else if len(raw) > 0 && decodeBKBool(raw) {
				out.deletedIDs[recID] = true
				return nil
			}
			rec := buildkitRecord{ID: recID}
			if raw, err := sb.Get([]byte("cache.description")); err != nil {
				return err
			} else if len(raw) > 0 {
				rec.Description = truncateRunes(decodeBKString(raw), 1024)
			}
			if raw, err := sb.Get([]byte("cache.recordType")); err != nil {
				return err
			} else if len(raw) > 0 {
				rec.RecordType = strings.TrimSpace(decodeBKString(raw))
			}
			if raw, err := sb.Get([]byte("cache.snapshot")); err != nil {
				return err
			} else if len(raw) > 0 {
				rec.SnapshotKey = strings.TrimSpace(decodeBKString(raw))
			}
			if raw, err := sb.Get([]byte("cache.chainID")); err != nil {
				return err
			} else if len(raw) > 0 {
				rec.ChainHex = normalizeDigestHex(decodeBKString(raw))
			}
			if rec.SnapshotKey == "" {
				rec.SnapshotKey = recID
			}
			out.records = append(out.records, rec)
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Legacy v1 metadata.db without metadata_v2.db: do not falsely claim orphans.
			if exists(filepath.Join(bkDir, "metadata.db")) {
				out.status = sourceError
				return out
			}
		} else {
			out.status = sourceError
		}
	}

	snapPath := filepath.Join(bkDir, "snapshots.db")
	err = viewBoltNoLock(snapPath, func(root *boltBucket) error {
		clear(out.snapCommitted)
		clear(out.snapChainHex)
		clear(out.trackedSnaps)

		return root.ForEach(func(name, _ []byte, b *boltBucket) error {
			if b == nil {
				return nil
			}
			key := string(name)
			if raw, err := b.Get([]byte("committed")); err != nil {
				return err
			} else if committed := strings.TrimSpace(string(raw)); committed != "" {
				out.snapCommitted[key] = committed
				out.trackedSnaps[committed] = true
			}
			if raw, err := b.Get([]byte("chainid")); err != nil {
				return err
			} else if ch := normalizeDigestHex(string(raw)); ch != "" {
				out.snapChainHex[key] = ch
			}
			isComm, err := b.Get([]byte("iscommitted"))
			if err != nil {
				return err
			}
			parent, err := b.Get([]byte("parent"))
			if err != nil {
				return err
			}
			if isComm != nil || parent != nil {
				out.trackedSnaps[key] = true
			}
			return nil
		})
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		out.status = sourceError
	}

	return out
}
