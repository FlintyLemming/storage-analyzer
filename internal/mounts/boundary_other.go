//go:build !linux

package mounts

func defaultBtrfsProber() btrfsProber { return btrfsProber{} }
