package updater

import "testing"

func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		current, latest string
		want            Staleness
	}{
		{"1.2.3", "1.2.3", StalenessUpToDate}, {"2.0.0", "1.9.9", StalenessUpToDate},
		{"1.2.3", "1.2.4", StalenessPatch}, {"1.2.9", "1.3.0", StalenessMinor},
		{"1.9.9", "2.0.0", StalenessMajor}, {"v1.0.0", "v1.1.0", StalenessMinor},
		{"bad", "1.0.0", StalenessUnknown}, {"1.0.0", "bad", StalenessUnknown},
		{"1!2.0", "2!2.0", StalenessUnknown}, {"1.0.0-rc.1", "1.0.0", StalenessUnknown},
		{"1.0.0-rc.1", "1.0.0-rc.2", StalenessUnknown}, {"1.0.0+build1", "1.0.0+build2", StalenessUpToDate},
	} {
		t.Run(tc.current+"/"+tc.latest, func(t *testing.T) {
			if got := Classify(tc.current, tc.latest); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
