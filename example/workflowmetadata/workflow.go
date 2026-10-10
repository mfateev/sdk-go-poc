// Package workflowmetadata exercises the remaining workflow interceptor API hooks.
package workflowmetadata

import (
	"context"
	"errors"
	"fmt"
	"isolate"
	"reflect"
	"strings"
	"time"

	"github.com/mfateev/sdk-go-poc/interceptor"
	"github.com/mfateev/sdk-go-poc/workflow"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
)

const Precise int64 = 9007199254740993

var calls = make(map[string]int)

//go:isolate
func NewInterceptors(_ []byte) ([]interceptor.WorkerInterceptor, error) {
	return []interceptor.WorkerInterceptor{&workerInterceptor{}}, nil
}

type workerInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (*workerInterceptor) InterceptWorkflow(_ context.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &inbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}

type inbound struct {
	interceptor.WorkflowInboundInterceptorBase
	out *outbound
}

func (i *inbound) Init(next interceptor.WorkflowOutboundInterceptor) error {
	i.out = &outbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: next}}
	return i.Next.Init(i.out)
}

type outbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	reads int
}

func (o *outbound) GetTypedSearchAttributes(ctx context.Context) temporal.SearchAttributes {
	if isolate.IsReadOnly() {
		o.reads++
	} else {
		calls["GetTypedSearchAttributes"]++
	}
	return o.Next.GetTypedSearchAttributes(ctx)
}

func (o *outbound) UpsertSearchAttributes(ctx context.Context, attributes map[string]any) error {
	if isolate.IsReadOnly() {
		o.reads++
	} else {
		calls["UpsertSearchAttributes"]++
	}
	return o.Next.UpsertSearchAttributes(ctx, attributes)
}

func (o *outbound) UpsertTypedSearchAttributes(ctx context.Context, attributes ...temporal.SearchAttributeUpdate) error {
	if isolate.IsReadOnly() {
		o.reads++
	} else {
		calls["UpsertTypedSearchAttributes"]++
	}
	return o.Next.UpsertTypedSearchAttributes(ctx, attributes...)
}

func (o *outbound) UpsertMemo(ctx context.Context, memo map[string]any) error {
	if isolate.IsReadOnly() {
		o.reads++
	} else {
		calls["UpsertMemo"]++
	}
	return o.Next.UpsertMemo(ctx, memo)
}

func (o *outbound) GetSignalChannelWithOptions(ctx context.Context, name string, options workflow.SignalChannelOptions) <-chan workflow.SignalResult {
	if isolate.IsReadOnly() {
		o.reads++
	} else {
		calls["GetSignalChannelWithOptions"]++
	}
	return o.Next.GetSignalChannelWithOptions(ctx, name, options)
}

func (o *outbound) HasLastCompletionResult(ctx context.Context) bool {
	if isolate.IsReadOnly() {
		o.reads++
	} else {
		calls["HasLastCompletionResult"]++
	}
	return o.Next.HasLastCompletionResult(ctx)
}

func (o *outbound) GetLastCompletionResult(ctx context.Context, values ...any) error {
	if isolate.IsReadOnly() {
		o.reads++
	} else {
		calls["GetLastCompletionResult"]++
	}
	return o.Next.GetLastCompletionResult(ctx, values...)
}

func (o *outbound) GetLastError(ctx context.Context) error {
	if isolate.IsReadOnly() {
		o.reads++
	} else {
		calls["GetLastError"]++
	}
	return o.Next.GetLastError(ctx)
}

func checkReads(ctx context.Context, previous string) error {
	if workflow.HasLastCompletionResult(ctx) != (previous != "absent") {
		return fmt.Errorf("previous result presence lost")
	}
	switch previous {
	case "absent":
		if !errors.Is(workflow.GetLastCompletionResult(ctx, new(int64)), temporal.ErrNoData) {
			return fmt.Errorf("missing ErrNoData")
		}
	case "empty":
		if err := workflow.GetLastCompletionResult(ctx); err != nil {
			return err
		}
	case "values":
		var number int64
		var text string
		if err := workflow.GetLastCompletionResult(ctx, &number, &text); err != nil {
			return err
		}
		if number != Precise || text != "previous" {
			return fmt.Errorf("previous result changed")
		}
	}
	cause := workflow.GetLastError(ctx)
	if previous == "values" {
		var application *temporal.ApplicationError
		if !errors.As(cause, &application) || application.Type() != "Previous" || !application.NonRetryable() {
			return fmt.Errorf("previous failure changed: %v", cause)
		}
		var number int64
		if err := application.Details(&number); err != nil || number != Precise {
			return fmt.Errorf("previous failure details changed: %v", err)
		}
	} else if cause != nil {
		return fmt.Errorf("unexpected previous failure: %v", cause)
	}
	attributes := workflow.GetTypedSearchAttributes(ctx)
	if number, ok := attributes.GetInt64(temporal.NewSearchAttributeKeyInt64("CustomIntField")); !ok || number != Precise {
		return fmt.Errorf("typed integer lost: %v", number)
	}
	if attributes.ContainsKey(temporal.NewSearchAttributeKeyKeyword("CustomKeywordField")) {
		return fmt.Errorf("unset keyword retained")
	}
	return nil
}

//go:isolate
func Workflow(ctx context.Context, previous string, memo bool) (string, error) {
	if err := workflow.UpsertSearchAttributes(ctx, map[string]any{}); err == nil {
		return "", fmt.Errorf("empty search attributes accepted")
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx); err == nil {
		return "", fmt.Errorf("empty typed search attributes accepted")
	}
	if err := workflow.UpsertMemo(ctx, map[string]any{}); err == nil {
		return "", fmt.Errorf("empty memo accepted")
	}
	if err := workflow.UpsertSearchAttributes(ctx, map[string]any{"TemporalChangeVersion": 1}); err == nil {
		return "", fmt.Errorf("reserved attribute accepted")
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx, temporal.NewSearchAttributeKeyKeyword("TemporalChangeVersion").ValueSet("x")); err == nil {
		return "", fmt.Errorf("reserved typed attribute accepted")
	}
	if err := workflow.UpsertSearchAttributes(ctx, map[string]any{"CustomBoolField": true}); err != nil {
		return "", err
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx, temporal.NewSearchAttributeKeyInt64("CustomIntField").ValueSet(Precise), temporal.NewSearchAttributeKeyTime("CustomDatetimeField").ValueSet(time.Unix(1700000000, 123456789).UTC()), temporal.NewSearchAttributeKeyKeyword("CustomKeywordField").ValueSet("temporary")); err != nil {
		return "", err
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx, temporal.NewSearchAttributeKeyKeyword("CustomKeywordField").ValueUnset()); err != nil {
		return "", err
	}
	if memo {
		if err := workflow.UpsertMemo(ctx, map[string]any{"number": Precise, "remove": "temporary"}); err != nil {
			return "", err
		}
		if err := workflow.UpsertMemo(ctx, map[string]any{"remove": nil}); err != nil {
			return "", err
		}
	}
	if err := checkKeyOrdering(); err != nil {
		return "", err
	}
	if err := checkReads(ctx, previous); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandlerWithOptions(ctx, "metadata-state", func() (string, error) { return "ok", checkReads(ctx, previous) }, workflow.QueryHandlerOptions{Description: "Reads metadata without changing state"}); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "bad-metadata", func() (string, error) { return "bad", workflow.UpsertMemo(ctx, map[string]any{"forbidden": true}) }); err != nil {
		return "", err
	}
	if err := workflow.SetQueryHandler(ctx, "bad-key", func() (string, error) {
		values := map[temporal.SearchAttributeKey]int{customKey{}: 1}
		for range values {
		}
		return "bad", nil
	}); err != nil {
		return "", err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, "check", func(context.Context) error { return nil }, workflow.UpdateHandlerOptions{Validator: func() error { return checkReads(ctx, previous) }, Description: "Validate metadata reads"}); err != nil {
		return "", err
	}

	finish := workflow.GetSignalChannelWithOptions(ctx, "finish", workflow.SignalChannelOptions{Description: "Complete the metadata check"})
	if finish != workflow.GetSignalChannelWithOptions(ctx, "finish", workflow.SignalChannelOptions{Description: "ignored"}) || finish != workflow.GetSignalChannel(ctx, "finish") {
		return "", fmt.Errorf("signal channel identity changed")
	}
	for _, name := range []string{"GetTypedSearchAttributes", "UpsertSearchAttributes", "UpsertTypedSearchAttributes", "UpsertMemo", "GetSignalChannelWithOptions", "HasLastCompletionResult", "GetLastCompletionResult", "GetLastError"} {
		if calls[name] == 0 {
			return "", fmt.Errorf("missing interceptor hook %s", name)
		}
	}
	signal := <-finish
	if signal.Err != nil {
		return "", signal.Err
	}
	return "ok", nil
}

// SDK keys contain reflect.Type pointers. Verify canonical order independently
// of those process-specific addresses, including collisions in attribute names.
func checkKeyOrdering() error {
	values := make(map[temporal.SearchAttributeKey]int)
	for i := 255; i >= 0; i-- {
		name := fmt.Sprintf("key-%03d", i)
		values[temporal.NewSearchAttributeKeyString(name)] = i
		values[temporal.NewSearchAttributeKeyInt64(name)] = i
	}
	values[temporal.NewSearchAttributeKeyKeyword("same")] = 2
	values[temporal.NewSearchAttributeKeyKeywordList("same")] = 7
	values[temporal.NewSearchAttributeKeyTime("same")] = 6
	values[temporal.NewSearchAttributeKeyBool("same")] = 5
	values[temporal.NewSearchAttributeKeyFloat64("same")] = 4
	var lastName string
	var lastKind int32
	count := 0
	for key, value := range values {
		name, kind := key.GetName(), int32(key.GetValueType())
		if name < lastName || name == lastName && kind <= lastKind {
			return fmt.Errorf("noncanonical SDK key order: %s/%d after %s/%d", name, kind, lastName, lastKind)
		}
		if strings.HasPrefix(name, "key-") && fmt.Sprintf("key-%03d", value) != name {
			return fmt.Errorf("SDK map value changed")
		}
		lastName, lastKind = name, kind
		count++
	}
	if count != 517 {
		return fmt.Errorf("lost SDK keys: %d", count)
	}
	return nil
}

type customKey struct{}

func (customKey) GetName() string                        { return "custom" }
func (customKey) GetValueType() enumspb.IndexedValueType { return enumspb.INDEXED_VALUE_TYPE_TEXT }
func (customKey) GetReflectType() reflect.Type           { return reflect.TypeFor[string]() }

func (i *inbound) HandleQuery(ctx context.Context, input *interceptor.HandleQueryInput) (any, error) {
	result, err := i.Next.HandleQuery(ctx, input)
	if err == nil && input.QueryType == "metadata-state" && i.out.reads != 4 {
		return nil, fmt.Errorf("query getters did not share the fresh inbound/outbound chain: %d", i.out.reads)
	}
	return result, err
}

func (i *inbound) ValidateUpdate(ctx context.Context, input *interceptor.UpdateInput) error {
	err := i.Next.ValidateUpdate(ctx, input)
	wantReads := 4
	if workflow.IsReplaying(ctx) {
		// Temporal accepts recorded updates without invoking their validator.
		wantReads = 0
	}
	if err == nil && input.Name == "check" && i.out.reads != wantReads {
		return fmt.Errorf("validator getters did not share the scratch chain: %d", i.out.reads)
	}
	return err
}
