package docker

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type volumeMeta struct {
	Name    string            `json:"Name"`
	Driver  string            `json:"Driver"`
	Labels  map[string]string `json:"Labels"`
	Options map[string]string `json:"Options"`
}

type volumesData struct {
	status sourceStatus
	byName map[string]volumeMeta
}

func parseVolumesDB(dockerRoot, _ string) volumesData {
	out := volumesData{
		status: sourceOK,
		byName: map[string]volumeMeta{},
	}
	dbPath := filepath.Join(dockerRoot, TargetVolumes, "metadata.db")
	err := viewBoltNoLock(dbPath, func(root *boltBucket) error {
		clear(out.byName)
		out.status = sourceOK

		b, err := root.Bucket([]byte("volumes"))
		if err != nil || b == nil {
			return err
		}
		return b.ForEach(func(k, v []byte, sub *boltBucket) error {
			if sub != nil || len(v) == 0 {
				return nil
			}
			var vm volumeMeta
			if err := json.Unmarshal(v, &vm); err != nil {
				out.status = sourceError
				return nil
			}
			name := vm.Name
			if name == "" {
				name = string(k)
			}
			out.byName[name] = vm
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			out.status = sourceAbsent
		} else {
			out.status = sourceError
		}
	}
	return out
}
