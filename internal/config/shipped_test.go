package config

import (
	"reflect"
	"testing"
)

func TestShippedConfigParses(t *testing.T) {
	c, err := Load("../../packaging/linux/config")
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8099" {
		t.Fatalf("Listen = %q", c.Listen)
	}
	wantExclude := []string{
		"/var/lib/containerd",
		"/var/lib/docker",
		"/var/lib/containers",
		"/var/lib/etcd",
		"/var/lib/kubelet",
	}
	if !reflect.DeepEqual(c.Exclude, wantExclude) {
		t.Fatalf("Exclude = %v, want %v", c.Exclude, wantExclude)
	}
	if !c.AllowDelete {
		t.Fatalf("AllowDelete = false, want true")
	}
}
