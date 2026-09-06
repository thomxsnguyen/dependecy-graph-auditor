// Package depfile parses package.json files and extracts the dependency map.
// It is the seed step for the auditor — the entry point reads a package.json
// and uses this package to produce the initial set of jobs to enqueue.
package depfile

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

// Dependency represents a single entry from a dependency file.
type Dependency struct {
	Name         string
	VersionRange string // raw range from the file, e.g., "^4.18.0"
	Indirect     bool   // Go requirements marked // indirect; retained for audit seeds
}

// Manifest is the package metadata needed to seed an audit.
type Manifest struct {
	Name         string
	Dependencies []Dependency
}

// packageJSON is the subset of package.json fields we care about.
type packageJSON struct {
	Name            string            `json:"name"`
	Dependencies    map[string]string `json:"dependencies"`
	DevDependencies map[string]string `json:"devDependencies"`
}

// ParsePackageJSON parses a package.json stream. Both "dependencies" and
// "devDependencies" are included when includeDevDeps is true.
func ParsePackageJSON(reader io.Reader, includeDevDeps bool) (Manifest, error) {
	var pkg packageJSON
	if err := json.NewDecoder(reader).Decode(&pkg); err != nil {
		return Manifest{}, fmt.Errorf("depfile: decode package.json: %w", err)
	}

	dependencies := make([]Dependency, 0, len(pkg.Dependencies)+len(pkg.DevDependencies))
	for name, versionRange := range pkg.Dependencies {
		dependencies = append(dependencies, Dependency{Name: name, VersionRange: versionRange})
	}
	if includeDevDeps {
		for name, versionRange := range pkg.DevDependencies {
			dependencies = append(dependencies, Dependency{Name: name, VersionRange: versionRange})
		}
	}
	sort.Slice(dependencies, func(i, j int) bool {
		if dependencies[i].Name == dependencies[j].Name {
			return dependencies[i].VersionRange < dependencies[j].VersionRange
		}
		return dependencies[i].Name < dependencies[j].Name
	})

	return Manifest{Name: pkg.Name, Dependencies: dependencies}, nil
}

// ParsePackageJSONFile opens and parses a local package.json file.
//
// Set includeDevDeps to false to restrict the result to production dependencies
// only (the "dependencies" key).
func ParsePackageJSONFile(path string, includeDevDeps bool) (Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("depfile: open %s: %w", path, err)
	}
	defer f.Close()

	manifest, err := ParsePackageJSON(f, includeDevDeps)
	if err != nil {
		return Manifest{}, fmt.Errorf("depfile: parse %s: %w", path, err)
	}
	return manifest, nil
}
