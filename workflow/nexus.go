package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"strings"

	"go.temporal.io/sdk/converter"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type NexusOperationOptions = goWorkflow.NexusOperationOptions
type NexusOperationExecution = goWorkflow.NexusOperationExecution
type NexusOperationCancellationType = goWorkflow.NexusOperationCancellationType

const (
	NexusOperationCancellationTypeUnspecified   = goWorkflow.NexusOperationCancellationTypeUnspecified
	NexusOperationCancellationTypeAbandon       = goWorkflow.NexusOperationCancellationTypeAbandon
	NexusOperationCancellationTypeTryCancel     = goWorkflow.NexusOperationCancellationTypeTryCancel
	NexusOperationCancellationTypeWaitRequested = goWorkflow.NexusOperationCancellationTypeWaitRequested
	NexusOperationCancellationTypeWaitCompleted = goWorkflow.NexusOperationCancellationTypeWaitCompleted
)

type NexusOperationFuture interface {
	Future
	GetNexusOperationExecution() Future
}
type nexusFuture struct {
	*activityFuture
	execution *activityFuture
}

func (f *nexusFuture) GetNexusOperationExecution() Future { return f.execution }

type NexusClient interface {
	Endpoint() string
	Service() string
	ExecuteOperation(context.Context, any, any, NexusOperationOptions) NexusOperationFuture
}
type nexusClient struct{ endpoint, service string }

func NewNexusClient(endpoint, service string) NexusClient {
	if endpoint == "" || service == "" || strings.HasPrefix(endpoint, "__temporal_") || strings.HasPrefix(service, "__temporal_") {
		panic("workflow: empty or reserved Nexus endpoint/service")
	}
	return nexusClient{endpoint, service}
}
func (c nexusClient) Endpoint() string { return c.endpoint }
func (c nexusClient) Service() string  { return c.service }

type NexusRequest struct {
	ID                           uint64
	Endpoint, Service, Operation string
	Payloads                     []byte
	Options                      NexusOperationOptions
}

func (c nexusClient) ExecuteOperation(ctx context.Context, operation, input any, options NexusOperationOptions) NexusOperationFuture {
	assertWritable()
	f := &nexusFuture{activityFuture: newOperationFuture(), execution: newOperationFuture()}
	f.execution.dataConverter = converter.GetDefaultDataConverter()
	fail := func(err error) NexusOperationFuture {
		failOperation(f.activityFuture, err)
		failOperation(f.execution, err)
		return f
	}
	if ctx == nil {
		return fail(errors.New("workflow: nil context"))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	name, ok := operation.(string)
	if !ok {
		op, valid := operation.(interface {
			Name() string
			InputType() reflect.Type
		})
		if !valid {
			return fail(errors.New("workflow: Nexus operation must be a name or operation reference"))
		}
		name = op.Name()
		if t := reflect.TypeOf(input); t != nil && !t.AssignableTo(op.InputType()) {
			return fail(fmt.Errorf("workflow: Nexus input %v does not match %v", t, op.InputType()))
		}
	}
	if name == "" || options.ScheduleToCloseTimeout < 0 || options.ScheduleToStartTimeout < 0 || options.StartToCloseTimeout < 0 {
		return fail(errors.New("workflow: invalid Nexus name or timeout"))
	}
	if options.CancellationType == NexusOperationCancellationTypeUnspecified {
		options.CancellationType = NexusOperationCancellationTypeWaitCompleted
	}
	if options.CancellationType < NexusOperationCancellationTypeAbandon || options.CancellationType > NexusOperationCancellationTypeWaitCompleted {
		return fail(errors.New("workflow: invalid Nexus cancellation policy"))
	}
	payloads, err := encodeActivityArgs([]any{input})
	if err != nil {
		return fail(err)
	}
	id := nextCallID.Add(1)
	p, err := json.Marshal(NexusRequest{ID: id, Endpoint: c.endpoint, Service: c.service, Operation: name, Payloads: payloads, Options: options})
	if err != nil {
		return fail(err)
	}
	if _, err = isolate.Call(OpScheduleNexus, p); err != nil {
		return fail(err)
	}
	stop := context.AfterFunc(ctx, func() { p, _ := json.Marshal(id); _, _ = isolate.Call(OpCancelNexus, p) })
	go func() { defer stop(); awaitOperation(f.activityFuture, OpAwaitNexus, id, ctx) }()
	go awaitOperation(f.execution, OpAwaitNexusExecution, id, ctx)
	return f
}
