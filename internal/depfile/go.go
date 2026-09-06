package depfile

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	modsemver "golang.org/x/mod/semver"
)

// GoManifest is the root module metadata needed by Go module selection.
// Manifest supplies the shared audit seed while GoVersion preserves the root
// go directive for later graph-pruning decisions.
type GoManifest struct {
	Manifest
	GoVersion string
}

// ParseGoMod strictly parses a root go.mod without invoking the Go command or
// evaluating any module code.
func ParseGoMod(reader io.Reader) (GoManifest, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return GoManifest{}, fmt.Errorf("depfile: read go.mod: %w", err)
	}

	file, err := modfile.Parse("go.mod", data, preserveGoVersion)
	if err != nil {
		return GoManifest{}, fmt.Errorf("depfile: parse go.mod: %w", err)
	}
	if file.Module == nil || strings.TrimSpace(file.Module.Mod.Path) == "" {
		return GoManifest{}, fmt.Errorf("depfile: go.mod requires a module directive")
	}
	modulePath := file.Module.Mod.Path
	if err := module.CheckPath(modulePath); err != nil {
		return GoManifest{}, fmt.Errorf("depfile: go.mod module %q: %w", modulePath, err)
	}
	if file.Go == nil || strings.TrimSpace(file.Go.Version) == "" {
		return GoManifest{}, fmt.Errorf("depfile: go.mod requires a go directive")
	}
	if len(file.Replace) != 0 {
		return GoManifest{}, fmt.Errorf("depfile: go.mod replace directives are not supported")
	}
	if len(file.Exclude) != 0 {
		return GoManifest{}, fmt.Errorf("depfile: go.mod exclude directives are not supported")
	}

	dependencies := make([]Dependency, 0, len(file.Require))
	for _, requirement := range file.Require {
		path := requirement.Mod.Path
		version := requirement.Mod.Version
		if err := module.Check(path, version); err != nil {
			return GoManifest{}, fmt.Errorf("depfile: go.mod requirement %s@%s: %w", path, version, err)
		}
		if module.CanonicalVersion(version) != version {
			return GoManifest{}, fmt.Errorf("depfile: go.mod requirement %s@%s: version must be canonical", path, version)
		}
		if module.IsPseudoVersion(version) {
			if _, err := module.PseudoVersionTime(version); err != nil {
				return GoManifest{}, fmt.Errorf("depfile: go.mod requirement %s@%s: invalid pseudo-version: %w", path, version, err)
			}
			if _, err := module.PseudoVersionBase(version); err != nil {
				return GoManifest{}, fmt.Errorf("depfile: go.mod requirement %s@%s: invalid pseudo-version: %w", path, version, err)
			}
		}
		dependencies = append(dependencies, Dependency{Name: path, VersionRange: version, Indirect: requirement.Indirect})
	}
	sort.SliceStable(dependencies, func(i, j int) bool {
		if dependencies[i].Name != dependencies[j].Name {
			return dependencies[i].Name < dependencies[j].Name
		}
		comparison := modsemver.Compare(dependencies[i].VersionRange, dependencies[j].VersionRange)
		if comparison != 0 {
			return comparison < 0
		}
		return dependencies[i].VersionRange < dependencies[j].VersionRange
	})

	return GoManifest{
		Manifest:  Manifest{Name: modulePath, Dependencies: dependencies},
		GoVersion: file.Go.Version,
	}, nil
}

func preserveGoVersion(_ string, version string) (string, error) {
	return version, nil
}
