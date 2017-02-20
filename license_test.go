package epher

import (
	"os"
	"path/filepath"
	"testing"

	license "github.com/ryanuber/go-license"
)

func TestCheckLicenses(t *testing.T) {

	whitelist := map[string]struct{}{
		license.LicenseMIT:       struct{}{},
		license.LicenseApache20:  struct{}{},
		license.LicenseMPL20:     struct{}{},
		license.LicenseUnlicense: struct{}{},
	}
	blacklist := map[string]struct{}{
		license.LicenseISC:    struct{}{},
		license.LicenseGPL20:  struct{}{},
		license.LicenseGPL30:  struct{}{},
		license.LicenseLGPL21: struct{}{},
		license.LicenseLGPL30: struct{}{},
		license.LicenseAGPL30: struct{}{},
		license.LicenseCDDL10: struct{}{},
		license.LicenseEPL10:  struct{}{},
	}

	walkfn := func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			return nil
		}

		lc, err := license.NewFromDir(path)
		if err != nil && err.Error() == license.ErrNoLicenseFile {
			return nil
		}
		if err != nil {
			t.Fatalf("PROBLEM with %v: %v", path, err)
			return nil
		}

		if _, ok := whitelist[lc.Type]; ok {
			t.Logf("OK! %v in %v", lc.Type, path)
			return nil
		}
		if _, ok := blacklist[lc.Type]; ok {
			t.Errorf("PROBLEM! %v in %v", lc.Type, path)
			return nil
		}
		return nil
	}

	err := filepath.Walk(".", walkfn)
	if err != nil {
		t.Fatal(err)
	}
}
