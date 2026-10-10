package worker

import (
	"isolate"
	"runtime"
	"runtime/debug"

	"github.com/mfateev/sdk-go-poc/internal/integrationcontract"
	bindings "go.temporal.io/sdk/internalbindings"
)

// IntegrationInfo identifies the contracts and module revisions in this binary.
// Source revisions diagnose deployments; they are not workflow replay inputs.
type IntegrationInfo struct {
	Runtime       isolate.Contract `json:"runtime"`
	Protocol      uint32           `json:"protocol"`
	SDKBindings   uint32           `json:"sdk_bindings"`
	Toolchain     string           `json:"toolchain"`
	SDKModule     string           `json:"sdk_module"`
	SDKVersion    string           `json:"sdk_version"`
	SDKChecksum   string           `json:"sdk_checksum,omitempty"`
	BuildRevision string           `json:"build_revision,omitempty"`
	BuildModified bool             `json:"build_modified"`
}

// GetIntegrationInfo is a host diagnostic. It does not create an isolate or use
// Temporal history. Static program identities are available from Handle.Name.
func GetIntegrationInfo() IntegrationInfo {
	info := IntegrationInfo{Runtime: isolate.CurrentContract(), Protocol: integrationcontract.ProtocolVersion, SDKBindings: bindings.IntegrationVersion, Toolchain: runtime.Version()}
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, module := range build.Deps {
			if module.Path != "go.temporal.io/sdk" {
				continue
			}
			if module.Replace != nil {
				module = module.Replace
			}
			info.SDKModule, info.SDKVersion, info.SDKChecksum = module.Path, module.Version, module.Sum
		}
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				info.BuildRevision = setting.Value
			case "vcs.modified":
				info.BuildModified = setting.Value == "true"
			}
		}
	}
	return info
}
