package temporalbridge

import (
	"fmt"
	"isolate"

	"github.com/mfateev/sdk-go-poc/internal/integrationcontract"
	bindings "go.temporal.io/sdk/internalbindings"
)

func validateSDKBindings() error {
	if bindings.IntegrationVersion != 1 {
		return fmt.Errorf("isolate integration: incompatible SDK binding version %d, requires 1", bindings.IntegrationVersion)
	}
	return nil
}

func (d *definition) validateStartupContract(command *isolate.Command) {
	body, err := integrationcontract.Unpack(command.Payload)
	if err == nil && len(body) != 0 {
		err = fmt.Errorf("isolate integration: unexpected startup request body")
	}
	if err != nil {
		d.failTask(err)
	}
}
