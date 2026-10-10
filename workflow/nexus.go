package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"strings"

	"github.com/nexus-rpc/sdk-go/nexus"
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
	Header                       nexus.Header
}

func (c nexusClient) ExecuteOperation(ctx context.Context, operation, input any, options NexusOperationOptions) NexusOperationFuture {
	assertWritable()
	if ctx != nil {
		if out := currentOutbound(ctx); out != nil {
			return out.ExecuteNexusOperation(ctx, ExecuteNexusOperationInput{Client: c, Operation: operation, Input: input, Options: options, NexusHeader: make(nexus.Header)})
		}
	}
	return c.executeOperation(ctx, operation, input, options, nil)
}

func executeNexusOperation(ctx context.Context, input ExecuteNexusOperationInput) NexusOperationFuture {
	c, ok := input.Client.(nexusClient)
	if !ok {
		err := errors.New("workflow: interceptor Nexus client must be created with NewNexusClient")
		return &nexusFuture{activityFuture: failOperation(newOperationFuture(), err), execution: failOperation(newOperationFuture(), err)}
	}
	return c.executeOperation(ctx, input.Operation, input.Input, input.Options, input.NexusHeader)
}

func requestCancelNexusOperation(ctx context.Context, input RequestCancelNexusOperationInput) {
	assertWritable()
	p, _ := json.Marshal(input.seq)
	_, _ = isolate.Call(OpCancelNexus, p)
}

func (c nexusClient) executeOperation(ctx context.Context, operation, input any, options NexusOperationOptions, header nexus.Header) NexusOperationFuture {
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
	p, err := json.Marshal(NexusRequest{ID: id, Endpoint: c.endpoint, Service: c.service, Operation: name, Payloads: payloads, Options: options, Header: header})
	if err != nil {
		return fail(err)
	}
	if _, err = isolate.Call(OpScheduleNexus, p); err != nil {
		return fail(err)
	}
	stop := context.AfterFunc(ctx, func() {
		in := RequestCancelNexusOperationInput{Client: c, Operation: operation, seq: id}
		if out := currentOutbound(ctx); out != nil {
			out.RequestCancelNexusOperation(ctx, in)
		} else {
			requestCancelNexusOperation(ctx, in)
		}
	})
	go func() { defer stop(); awaitOperation(f.activityFuture, OpAwaitNexus, id, ctx) }()
	go awaitOperation(f.execution, OpAwaitNexusExecution, id, ctx)
	return f
}
