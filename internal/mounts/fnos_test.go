package mounts

import (
	"reflect"
	"testing"
)

func TestSelectFnOS(t *testing.T) {
	ms := load(t, "fnos")

	t.Run("default storage spaces only in numeric order", func(t *testing.T) {
		got := mps(SelectFnOS(ms, FnOSOptions{}))
		want := []string{"/vol1", "/vol2", "/vol10"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("include system", func(t *testing.T) {
		got := mps(SelectFnOS(ms, FnOSOptions{IncludeSystem: true}))
		want := []string{"/vol1", "/vol2", "/vol10", "/"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("include external", func(t *testing.T) {
		got := mps(SelectFnOS(ms, FnOSOptions{IncludeExternal: true}))
		want := []string{"/vol1", "/vol2", "/vol10", "/media/extdisk", "/vol00/USB_Backup"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("include system and external excludes boot efi and trimafs and cifs", func(t *testing.T) {
		got := mps(SelectFnOS(ms, FnOSOptions{IncludeSystem: true, IncludeExternal: true}))
		want := []string{"/vol1", "/vol2", "/vol10", "/", "/media/extdisk", "/vol00/USB_Backup"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}
