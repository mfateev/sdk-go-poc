package temporalbridge

import (
	"isolate"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

// FailureConverterOptions snapshots compiler-created metadata and copied config.
type FailureConverterOptions struct {
	Factory isolate.Handle
	Config  []byte
}

func (d *definition) hostFailureConverter(dc converter.DataConverter, scopes ...converter.SerializationContext) converter.FailureConverter {
	if d.failures.Factory.Name() != "" {
		var fc converter.FailureConverter
		if root, ok := d.env.(interface {
			GetRootFailureConverter() converter.FailureConverter
		}); ok {
			fc = root.GetRootFailureConverter()
		} else {
			fc = d.env.GetFailureConverter()
		}
		if fc != nil {
			info := d.env.WorkflowInfo()
			scope := converter.SerializationContext(converter.WorkflowSerializationContext{Namespace: info.Namespace, WorkflowID: info.WorkflowExecution.ID})
			if len(scopes) != 0 {
				scope = scopes[0]
			}
			return converter.WithFailureConverterSerializationContext(fc, scope)
		}
	}
	return temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc})
}

func (d *definition) activityFailureConverter(dc converter.DataConverter, name, queue string, local bool) converter.FailureConverter {
	info := d.env.WorkflowInfo()
	return d.hostFailureConverter(dc, converter.ActivitySerializationContext{Namespace: info.Namespace, WorkflowID: info.WorkflowExecution.ID,
		WorkflowType: info.WorkflowType.Name, ActivityType: name, TaskQueue: queue, IsLocal: local})
}

func (d *definition) encodeInboundFailure(cause error, dc converter.DataConverter, converters ...converter.FailureConverter) ([]byte, error) {
	fc := d.hostFailureConverter(dc)
	if len(converters) != 0 {
		fc = converters[0]
	}
	return encodeInboundFailure(cause, dc, fc)
}

func (d *definition) decodeOutboundFailure(raw []byte, dc converter.DataConverter) (error, error) {
	return decodeOutboundFailure(raw, dc, d.hostFailureConverter(dc))
}
