// Package updater classifies direct dependency version gaps.
package updater

import ms "github.com/Masterminds/semver/v3"

type Staleness string

const (
	StalenessUpToDate Staleness = "up_to_date"
	StalenessPatch    Staleness = "patch"
	StalenessMinor    Staleness = "minor"
	StalenessMajor    Staleness = "major"
	StalenessUnknown  Staleness = "unknown"
)

// Classify returns unknown for versions outside semver or a prerelease-only gap.
func Classify(current, latest string) Staleness {
	c, err := ms.NewVersion(current)
	if err != nil {
		return StalenessUnknown
	}
	l, err := ms.NewVersion(latest)
	if err != nil {
		return StalenessUnknown
	}
	if !l.GreaterThan(c) {
		return StalenessUpToDate
	}
	if l.Major() > c.Major() {
		return StalenessMajor
	}
	if l.Minor() > c.Minor() {
		return StalenessMinor
	}
	if l.Patch() > c.Patch() {
		return StalenessPatch
	}
	return StalenessUnknown
}
