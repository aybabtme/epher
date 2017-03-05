package ephertest

import "testing"

func Start(t *testing.T, n int) {
	sd := ServiceDiscovery(t)

	rc, err := sd.Discover()
	if err != nil {
		t.Fatal(err)
	}

	// create N nodes

}
