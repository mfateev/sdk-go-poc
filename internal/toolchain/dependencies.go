package toolchain

import (
	"fmt"
	"path/filepath"
	"strings"
)

const ProtobufModule = "google.golang.org/protobuf"
const ProtobufVersion = "v1.36.11"

const TemporalAPIModule = "go.temporal.io/api"
const TemporalAPIVersion = "v1.63.6"

const TemporalSDKModule = "go.temporal.io/sdk"

// TemporalSDKSource supports official SDK revisions and development checkouts.
// The caller must also verify the owning module is TemporalSDKModule. Replacing
// it with another module does not grant that module SDK metadata privileges.
func TemporalSDKSource(replacementPath, replacementVersion string) bool {
	if replacementPath == "" {
		return replacementVersion == ""
	}
	return replacementVersion == "" && (filepath.IsAbs(replacementPath) ||
		strings.HasPrefix(replacementPath, "./") || strings.HasPrefix(replacementPath, "../") ||
		replacementPath == "." || replacementPath == "..")
}

const OTelTraceModule = "go.opentelemetry.io/otel/trace"
const OTelVersion = "v1.44.0"

func validateDependencies(packages []*Package) error {
	for _, p := range packages {
		for _, trusted := range []struct{ path, version string }{
			{ProtobufModule, ProtobufVersion}, {TemporalAPIModule, TemporalAPIVersion}, {TemporalSDKModule, ""}, {OTelTraceModule, OTelVersion},
		} {
			if p.ImportPath != trusted.path && !strings.HasPrefix(p.ImportPath, trusted.path+"/") {
				continue
			}
			if trusted.path == TemporalSDKModule && strings.HasPrefix(p.ImportPath, trusted.path+"/contrib/") {
				continue
			}
			valid := p.Module != nil && p.Module.Path == trusted.path && p.Module.Version == trusted.version && p.Module.Replace == nil
			if trusted.path == TemporalSDKModule && p.Module != nil && p.Module.Path == trusted.path {
				var path, version string
				if p.Module.Replace != nil {
					path, version = p.Module.Replace.Path, p.Module.Replace.Version
				}
				valid = TemporalSDKSource(path, version)
			}
			if !valid || strings.Contains(filepath.ToSlash(p.Dir), "/vendor/") {
				if trusted.path == TemporalSDKModule {
					return fmt.Errorf("SDK metadata services require %s as the owning module, using an official revision or filesystem checkout; replacement modules, nested modules, and vendored sources are unsupported", trusted.path)
				}
				return fmt.Errorf("metadata services require %s@%s without a replacement, nested module, or vendored source; audit the service manifest before upgrading", trusted.path, trusted.version)
			}
		}
	}
	return nil
}
