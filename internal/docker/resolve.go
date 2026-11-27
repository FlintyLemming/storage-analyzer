package docker

import (
	"sort"
	"strings"
)

const maxEmbeddedImages = 50

// RootResult holds resolved annotations for one Docker data-root.
type RootResult struct {
	overlay2   map[string]*Annotation
	containers map[string]*Annotation
	missingCfg map[string]bool
	volumes    map[string]volumeMeta
	volMounts  map[string][]VolumeMount
	warnings   map[string][]string
	contOK     bool
}

// ResolveRoot reads on-disk metadata from dockerRoot once and builds lookup
// tables for overlay2/, containers/, and volumes/. tmpDir is used for brief
// lock-free BoltDB snapshot copies.
func ResolveRoot(dockerRoot, tmpDir string) (*RootResult, error) {
	conts := parseContainers(dockerRoot)
	imgs := parseImagesAndLayers(dockerRoot)
	bk := parseBuildKit(dockerRoot, tmpDir)
	vols := parseVolumesDB(dockerRoot, tmpDir)

	res := &RootResult{
		overlay2:   map[string]*Annotation{},
		containers: map[string]*Annotation{},
		missingCfg: conts.missingConfig,
		volumes:    vols.byName,
		volMounts:  conts.volumeMounts,
		warnings:   map[string][]string{},
		contOK:     conts.status != sourceError,
	}

	// Build warnings per target directory.
	var ovWarn []string
	if imgs.status == sourceError {
		ovWarn = append(ovWarn, "images")
	}
	if conts.status == sourceError {
		ovWarn = append(ovWarn, "containers")
	}
	if bk.status == sourceError {
		ovWarn = append(ovWarn, "buildkit")
	}
	if len(ovWarn) > 0 {
		res.warnings[TargetOverlay2] = ovWarn
	}
	if conts.status == sourceError {
		res.warnings[TargetContainers] = []string{"containers"}
	}
	var volWarn []string
	if vols.status == sourceError {
		volWarn = append(volWarn, "volumes")
	}
	if conts.status == sourceError {
		volWarn = append(volWarn, "containers")
	}
	if len(volWarn) > 0 {
		res.warnings[TargetVolumes] = volWarn
	}

	// 1. Containers directory entries.
	for cid, pc := range conts.byID {
		ref := pc.Ref
		res.containers[cid] = &Annotation{
			Type:      TypeContainer,
			Label:     ref.Name,
			Sublabel:  ref.Image,
			Status:    ref.Status,
			Compose:   ref.Compose,
			Container: &ref,
		}
	}

	// 2. Container writable & init layers in overlay2/.
	attachContainerLayer := func(dirID, cid, kind string) {
		var ref ContainerRef
		var note string
		if pc, ok := conts.byID[cid]; ok {
			ref = pc.Ref
		} else {
			ref = ContainerRef{Name: shortID(cid), ID: shortID(cid)}
			note = "missing_config"
		}
		res.overlay2[dirID] = &Annotation{
			Type:      kind,
			Label:     ref.Name,
			Sublabel:  ref.Image,
			Status:    ref.Status,
			Compose:   ref.Compose,
			Container: &ref,
			Note:      note,
		}
	}
	for mountID, cid := range imgs.mountIDToCID {
		attachContainerLayer(mountID, cid, TypeContainerRW)
	}
	for initID, cid := range imgs.initIDToCID {
		attachContainerLayer(initID, cid, TypeContainerInit)
	}

	// 3. Walk container mount parent chains upward through layerdb so layers of
	// containers whose image was force-removed are never misclassified as orphan.
	chainContainers := map[string][]parsedContainer{}
	for cid, startChain := range imgs.cidParentChain {
		pc, ok := conts.byID[cid]
		if !ok {
			pc = parsedContainer{Ref: ContainerRef{Name: shortID(cid), ID: shortID(cid), Image: shortID(cid)}}
		}
		seen := map[string]bool{}
		for ch := startChain; ch != "" && !seen[ch]; ch = imgs.chainParent[ch] {
			seen[ch] = true
			chainContainers[ch] = append(chainContainers[ch], pc)
		}
	}

	// 4. Image layers in overlay2/ (both 64-hex pulled and 25-char BuildKit-built).
	for cacheID, chHex := range imgs.cacheIDToChain {
		if _, exists := res.overlay2[cacheID]; exists {
			continue
		}
		refs := collectImageRefs(imgs.chainImages[chHex], conts.inUseImages)
		if len(refs) == 0 {
			// Check if any live container's mount parent chain uses this layer.
			for _, pc := range chainContainers[chHex] {
				name := pc.Ref.Image
				if name == "" {
					name = pc.Ref.Name
				}
				refs = append(refs, ImageRef{
					Name:     name,
					ID:       shortID(pc.ImageID),
					Dangling: true,
					InUse:    true,
				})
			}
			refs = dedupeAndSortImages(refs)
		}
		if len(refs) == 0 {
			continue
		}
		total := len(refs)
		embedded := refs
		if len(embedded) > maxEmbeddedImages {
			embedded = embedded[:maxEmbeddedImages]
		}
		label := embedded[0].Name
		if embedded[0].Dangling && embedded[0].Name == "<none>" && embedded[0].ID != "" {
			label = "<none> (" + embedded[0].ID + ")"
		}
		createdBy := imgs.chainCreatedBy[chHex]
		res.overlay2[cacheID] = &Annotation{
			Type:      TypeImageLayer,
			Label:     label,
			Sublabel:  cleanCreatedBy(createdBy),
			Count:     total,
			Images:    embedded,
			CreatedBy: createdBy,
		}
	}

	// 5. BuildKit build cache records (metadata_v2.db + snapshots.db).
	for _, rec := range bk.records {
		targets := resolveBuildKitTargets(rec, bk, imgs.chainToCacheID)
		for _, dirID := range targets {
			if existing, ok := res.overlay2[dirID]; ok {
				// Keep primary image/container identity; enrich with build description if useful.
				if existing.Type == TypeImageLayer && existing.BuildDesc == "" && rec.Description != "" {
					existing.BuildDesc = rec.Description
				}
				continue
			}
			res.overlay2[dirID] = makeBuildCacheAnnotation(rec.RecordType, rec.Description)
		}
	}
	// Any snapshot tracked in snapshots.db without a metadata_v2.db record is still
	// BuildKit cache (not orphan), unless explicitly marked deleted.
	for snapID := range bk.trackedSnaps {
		if bk.deletedIDs[snapID] {
			continue
		}
		if _, exists := res.overlay2[snapID]; exists {
			continue
		}
		res.overlay2[snapID] = makeBuildCacheAnnotation("", "")
	}

	return res, nil
}

func resolveBuildKitTargets(rec buildkitRecord, bk buildkitData, chainToCacheID map[string]string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(id string) {
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	if gdID := bk.snapCommitted[rec.SnapshotKey]; gdID != "" {
		add(gdID)
	}
	if chHex := bk.snapChainHex[rec.SnapshotKey]; chHex != "" {
		if cacheID := chainToCacheID[chHex]; cacheID != "" {
			add(cacheID)
		}
	}
	if rec.ChainHex != "" {
		if cacheID := chainToCacheID[rec.ChainHex]; cacheID != "" {
			add(cacheID)
		}
	}
	if strings.HasPrefix(rec.SnapshotKey, "sha256:") || strings.HasPrefix(rec.SnapshotKey, "layer:sha256:") {
		if cacheID := chainToCacheID[normalizeDigestHex(rec.SnapshotKey)]; cacheID != "" {
			add(cacheID)
		}
	}
	if len(out) == 0 && rec.SnapshotKey != "" {
		add(rec.SnapshotKey)
	}
	return out
}

func makeBuildCacheAnnotation(cacheType, desc string) *Annotation {
	if cacheType == "" {
		cacheType = "regular"
	}
	label := cleanCreatedBy(desc)
	return &Annotation{
		Type:      TypeBuildCache,
		Label:     label,
		CacheType: cacheType,
		BuildDesc: desc,
	}
}

func collectImageRefs(imgs []parsedImage, inUseImages map[string]bool) []ImageRef {
	if len(imgs) == 0 {
		return nil
	}
	var refs []ImageRef
	for _, img := range imgs {
		inUse := inUseImages[img.ID]
		sid := shortID(img.ID)
		if len(img.Refs) == 0 {
			refs = append(refs, ImageRef{Name: "<none>", ID: sid, Dangling: true, InUse: inUse})
			continue
		}
		for _, r := range img.Refs {
			refs = append(refs, ImageRef{Name: r, ID: sid, Dangling: false, InUse: inUse})
		}
	}
	return dedupeAndSortImages(refs)
}

func dedupeAndSortImages(refs []ImageRef) []ImageRef {
	type key struct {
		name, id string
		dangling bool
	}
	byKey := map[key]ImageRef{}
	for _, r := range refs {
		k := key{r.Name, r.ID, r.Dangling}
		if prev, ok := byKey[k]; ok {
			if r.InUse {
				prev.InUse = true
				byKey[k] = prev
			}
			continue
		}
		byKey[k] = r
	}
	out := make([]ImageRef, 0, len(byKey))
	for _, r := range byKey {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		// Tagged before dangling.
		if a.Dangling != b.Dangling {
			return !a.Dangling
		}
		// In-use before unused.
		if a.InUse != b.InUse {
			return a.InUse
		}
		// Named tags before digest-only refs.
		aDig, bDig := strings.Contains(a.Name, "@"), strings.Contains(b.Name, "@")
		if aDig != bDig {
			return !aDig
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.ID < b.ID
	})
	return out
}

// Warnings returns metadata warning codes recorded for targetKind.
func (r *RootResult) Warnings(targetKind string) []string {
	return r.warnings[targetKind]
}

// DirAnnotation returns the annotation to attach to the target directory itself
// (overlay2, containers, or volumes).
func (r *RootResult) DirAnnotation(targetKind string) *Annotation {
	var t string
	switch targetKind {
	case TargetOverlay2:
		t = TypeDirOverlay2
	case TargetContainers:
		t = TypeDirContainers
	case TargetVolumes:
		t = TypeDirVolumes
	default:
		return nil
	}
	return &Annotation{
		Type:     t,
		Warnings: r.warnings[targetKind],
	}
}

// EntryAnnotation returns the resolved annotation for one direct child of
// overlay2/, containers/, or volumes/, or nil if it should remain unlabeled.
func (r *RootResult) EntryAnnotation(targetKind, name string, isDir bool) *Annotation {
	switch targetKind {
	case TargetOverlay2:
		return r.overlay2Entry(name, isDir)
	case TargetContainers:
		return r.containerEntry(name, isDir)
	case TargetVolumes:
		return r.volumeEntry(name, isDir)
	default:
		return nil
	}
}

func (r *RootResult) overlay2Entry(name string, isDir bool) *Annotation {
	if name == "l" || name == "backingFsBlockDev" {
		return &Annotation{Type: TypeInternal, Note: name}
	}
	if ann, ok := r.overlay2[name]; ok {
		return ann
	}
	if !isDir {
		return nil
	}
	// Anti-misclassification rule: only label unmatched dirs as orphan when all
	// overlay2 metadata sources parsed without errors.
	if len(r.warnings[TargetOverlay2]) > 0 {
		return nil
	}
	return &Annotation{Type: TypeOrphan}
}

func (r *RootResult) containerEntry(name string, isDir bool) *Annotation {
	if ann, ok := r.containers[name]; ok {
		return ann
	}
	if isDir && r.missingCfg[name] && len(r.warnings[TargetContainers]) == 0 {
		return &Annotation{Type: TypeOrphan, Note: "missing_config"}
	}
	return nil
}

func (r *RootResult) volumeEntry(name string, isDir bool) *Annotation {
	if name == "metadata.db" || name == "backingFsBlockDev" {
		return &Annotation{Type: TypeInternal, Note: name}
	}
	if !isDir {
		return nil
	}
	vm, hasMeta := r.volumes[name]
	mounts := r.volMounts[name]

	_, hasAnonLabel := vm.Labels["com.docker.volume.anonymous"]
	isAnon := hasAnonLabel || (isHex64(name) && vm.Labels["com.docker.compose.volume"] == "")

	kind := TypeVolumeNamed
	if isAnon {
		kind = TypeVolumeAnon
	}

	var comp *ComposeInfo
	if proj, vol := vm.Labels["com.docker.compose.project"], vm.Labels["com.docker.compose.volume"]; proj != "" || vol != "" {
		comp = &ComposeInfo{Project: proj, Volume: vol}
	}

	var label, sublabel string
	if len(mounts) > 0 {
		label = mounts[0].Container
		sublabel = mounts[0].Destination
	} else if comp != nil && comp.Project != "" {
		label = comp.Project
		if comp.Volume != "" {
			sublabel = comp.Volume
		}
	}

	var labels map[string]string
	if hasMeta && len(vm.Labels) > 0 {
		labels = vm.Labels
	}
	driver := vm.Driver
	if driver == "" && hasMeta {
		driver = "local"
	}

	return &Annotation{
		Type:      kind,
		Label:     label,
		Sublabel:  sublabel,
		Count:     len(mounts),
		Compose:   comp,
		Mounts:    mounts,
		Unmounted: len(mounts) == 0 && r.contOK,
		Driver:    driver,
		Labels:    labels,
	}
}
