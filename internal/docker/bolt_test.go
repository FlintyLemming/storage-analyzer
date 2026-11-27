package docker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestViewBoltNoLockReadsExclusivelyLockedMultiLevelAndInlineBuckets(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "live.db")

	// Open and hold an exclusive bbolt lock, mimicking a running dockerd.
	live, err := bolt.Open(dbPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()

	largeVal := strings.Repeat("x", 20000) // forces multi-page overflow leaf (> 4KB)
	if err := live.Update(func(tx *bolt.Tx) error {
		// 1. Inline sub-bucket + non-inline sub-bucket under _main, plus many keys to force branch pages.
		mainB, err := tx.CreateBucketIfNotExists([]byte("_main"))
		if err != nil {
			return err
		}
		for i := 0; i < 400; i++ {
			id := fmt.Sprintf("rec%022d", i)
			sb, err := mainB.CreateBucketIfNotExists([]byte(id))
			if err != nil {
				return err
			}
			if err := sb.Put([]byte("cache.description"), []byte(fmt.Sprintf(`{"value":"step %d"}`, i))); err != nil {
				return err
			}
			if i == 0 {
				if err := sb.Put([]byte("big"), []byte(largeVal)); err != nil {
					return err
				}
			}
		}
		// 2. Separate top-level bucket to prove we can skip huge sibling buckets.
		extB, err := tx.CreateBucketIfNotExists([]byte("_external"))
		if err != nil {
			return err
		}
		return extB.Put([]byte("blob"), []byte(strings.Repeat("z", 50000)))
	}); err != nil {
		t.Fatal(err)
	}

	var count int
	var gotFirstDesc, gotBig string
	err = viewBoltNoLock(dbPath, func(root *boltBucket) error {
		count = 0
		gotFirstDesc, gotBig = "", ""
		mainB, err := root.Bucket([]byte("_main"))
		if err != nil {
			return err
		}
		if mainB == nil {
			t.Fatal("missing _main bucket")
		}
		return mainB.ForEach(func(k, _ []byte, sub *boltBucket) error {
			if sub == nil {
				return nil
			}
			count++
			if string(k) == "rec0000000000000000000000" {
				v, err := sub.Get([]byte("cache.description"))
				if err != nil {
					return err
				}
				gotFirstDesc = string(v)
				b, err := sub.Get([]byte("big"))
				if err != nil {
					return err
				}
				gotBig = string(b)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("viewBoltNoLock failed: %v", err)
	}
	if count != 400 {
		t.Fatalf("count = %d, want 400", count)
	}
	if gotFirstDesc != `{"value":"step 0"}` {
		t.Fatalf("gotFirstDesc = %q", gotFirstDesc)
	}
	if gotBig != largeVal {
		t.Fatalf("gotBig len = %d, want %d", len(gotBig), len(largeVal))
	}
}

func TestViewBoltNoLockRejectsCorruptAndMissing(t *testing.T) {
	dir := t.TempDir()
	corrupt := filepath.Join(dir, "corrupt.db")
	if err := os.WriteFile(corrupt, []byte("not a bolt database"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := viewBoltNoLock(corrupt, func(*boltBucket) error { return nil }); err == nil {
		t.Fatal("expected error for corrupt file, got nil")
	}
	err := viewBoltNoLock(filepath.Join(dir, "missing.db"), func(*boltBucket) error { return nil })
	if !os.IsNotExist(err) {
		t.Fatalf("err = %v, want os.ErrNotExist", err)
	}
}
