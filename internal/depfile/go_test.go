package depfile_test

import (
	"strings"
	"testing"

	"github.com/thomxsnguyen/mini-distributed-job-api/internal/depfile"
)

func TestParseGoModRootMetadataAndRequirements(t *testing.T) {
	manifest, err := depfile.ParseGoMod(strings.NewReader(`
module example.com/service

go 1.23

require github.com/zeta/module v1.10.0
require (
	golang.org/x/sync v0.16.0 // indirect
	github.com/zeta/module v1.9.0
)
`))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Name != "example.com/service" {
		t.Fatalf("name: got %q", manifest.Name)
	}
	if manifest.GoVersion != "1.23" {
		t.Fatalf("Go version: got %q", manifest.GoVersion)
	}
	want := []depfile.Dependency{
		{Name: "github.com/zeta/module", VersionRange: "v1.9.0"},
		{Name: "github.com/zeta/module", VersionRange: "v1.10.0"},
		{Name: "golang.org/x/sync", VersionRange: "v0.16.0", Indirect: true},
	}
	if len(manifest.Dependencies) != len(want) {
		t.Fatalf("dependencies: got %+v, want %+v", manifest.Dependencies, want)
	}
	for index := range want {
		if manifest.Dependencies[index] != want[index] {
			t.Fatalf("dependency %d: got %+v, want %+v", index, manifest.Dependencies[index], want[index])
		}
	}
}

func TestParseGoModAcceptsSupportedExactVersions(t *testing.T) {
	manifest, err := depfile.ParseGoMod(strings.NewReader(`
module example.com/service
go 1.23
require (
	example.com/legacy v2.3.4+incompatible
	example.com/pseudo v0.0.0-20240102150405-abcdefabcdef
	example.com/module/v2 v2.1.0
)
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Dependencies) != 3 {
		t.Fatalf("dependencies: got %+v", manifest.Dependencies)
	}
}

func TestParseGoModAllowsEmptyRequirements(t *testing.T) {
	manifest, err := depfile.ParseGoMod(strings.NewReader("module example.com/empty\n\ngo 1.23\n"))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Name != "example.com/empty" || manifest.GoVersion != "1.23" || len(manifest.Dependencies) != 0 {
		t.Fatalf("manifest: %+v", manifest)
	}
}

func TestParseGoModIgnoresNonRequirementDirectives(t *testing.T) {
	manifest, err := depfile.ParseGoMod(strings.NewReader(`
module example.com/service
go 1.23
toolchain go1.23.4
godebug default=go1.23
retract v1.0
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Dependencies) != 0 {
		t.Fatalf("dependencies: got %+v", manifest.Dependencies)
	}
}

func TestParseGoModRejectsInvalidRootContracts(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{name: "malformed syntax", content: "module (\n", wantErr: "parse go.mod"},
		{name: "missing module", content: "go 1.23\n", wantErr: "requires a module directive"},
		{name: "invalid module path", content: "module service\ngo 1.23\n", wantErr: "malformed module path"},
		{name: "missing Go directive", content: "module example.com/service\n", wantErr: "requires a go directive"},
		{name: "invalid Go directive", content: "module example.com/service\ngo latest\n", wantErr: "invalid go version"},
		{name: "npm range", content: "module example.com/service\ngo 1.23\nrequire example.com/a ^1.2.3\n", wantErr: "version"},
		{name: "noncanonical version", content: "module example.com/service\ngo 1.23\nrequire example.com/a v1.2\n", wantErr: "version must be canonical"},
		{name: "major mismatch", content: "module example.com/service\ngo 1.23\nrequire example.com/a/v2 v1.2.3\n", wantErr: "should be v2"},
		{name: "invalid pseudo timestamp", content: "module example.com/service\ngo 1.23\nrequire example.com/a v0.0.0-20241302150405-abcdefabcdef\n", wantErr: "invalid pseudo-version"},
		{name: "replace", content: "module example.com/service\ngo 1.23\nreplace example.com/a => ../a\n", wantErr: "replace directives are not supported"},
		{name: "remote replace", content: "module example.com/service\ngo 1.23\nreplace example.com/a v1.0.0 => example.com/b v1.0.0\n", wantErr: "replace directives are not supported"},
		{name: "exclude", content: "module example.com/service\ngo 1.23\nexclude example.com/a v1.0.0\n", wantErr: "exclude directives are not supported"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := depfile.ParseGoMod(strings.NewReader(tt.content))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error: got %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}
