package config

import "testing"

func TestShippedConfigParses(t *testing.T) {
	c, err := Load("../../packaging/linux/config")
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8099" {
		t.Fatalf("Listen = %q", c.Listen)
	}
}
