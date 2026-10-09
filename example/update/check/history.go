package main

import (
	"fmt"
	"isolate"
	"os"

	"github.com/mfateev/sdk-go-poc/example/update"
	"github.com/mfateev/sdk-go-poc/temporalbridge"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	sdkwf "go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

func checkHistory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
	if err != nil {
		return err
	}
	if err := replayCheckedHistory(history); err != nil {
		return err
	}
	corrupt := proto.Clone(history).(*historypb.History)
	changed := false
	for _, event := range corrupt.Events {
		if c := event.GetWorkflowExecutionUpdateCompletedEventAttributes(); c != nil && c.Outcome.GetSuccess() != nil {
			c.Outcome.GetSuccess().Payloads[0].Data = []byte("0")
			changed = true
			break
		}
	}
	if !changed {
		return fmt.Errorf("update fixture has no successful update")
	}
	if err := replayCheckedHistory(corrupt); err == nil {
		return fmt.Errorf("corrupted update outcome passed replay audit")
	}
	fmt.Println("saved update acceptance/completion/cancellation history replay and outcome audit passed")
	return nil
}

func replayCheckedHistory(history *historypb.History) error {
	h, ok := isolate.LookupFunction(update.UpdateWorkflow)
	if !ok {
		return fmt.Errorf("missing update workflow")
	}
	audit := &replayAudit{expected: make(map[string]*updatepb.Outcome), seen: make(map[string]bool)}
	for _, event := range history.Events {
		if c := event.GetWorkflowExecutionUpdateCompletedEventAttributes(); c != nil {
			audit.expected[c.Meta.UpdateId] = c.Outcome
		}
	}
	r := sdkworker.NewWorkflowReplayer()
	r.RegisterWorkflowWithOptions(auditFactory{Factory: temporalbridge.Factory{Function: h}, audit: audit}, sdkwf.RegisterOptions{Name: "UpdatedAlias"})
	if err := r.ReplayWorkflowHistory(nil, history); err != nil {
		return err
	}
	if audit.err != nil {
		return audit.err
	}
	if len(audit.seen) != len(audit.expected) {
		return fmt.Errorf("replay completed %d/%d updates", len(audit.seen), len(audit.expected))
	}
	completion := history.Events[len(history.Events)-1].GetWorkflowExecutionCompletedEventAttributes()
	if completion == nil {
		return fmt.Errorf("update fixture is not completed")
	}
	var expected, actual int64
	if err := converter.GetDefaultDataConverter().FromPayloads(completion.Result, &expected); err != nil {
		return err
	}
	if err := r.(interface{ GetWorkflowResult(string, any) error }).GetWorkflowResult("", &actual); err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("update replay result=%d, history=%d", actual, expected)
	}
	return nil
}

type replayAudit struct {
	expected map[string]*updatepb.Outcome
	seen     map[string]bool
	err      error
}
type auditFactory struct {
	temporalbridge.Factory
	audit *replayAudit
}

func (f auditFactory) NewWorkflowDefinition() bindings.WorkflowDefinition {
	return &auditDefinition{WorkflowDefinition: f.Factory.NewWorkflowDefinition(), audit: f.audit}
}

type auditDefinition struct {
	bindings.WorkflowDefinition
	audit *replayAudit
}

func (d *auditDefinition) Execute(env bindings.WorkflowEnvironment, header *commonpb.Header, input *commonpb.Payloads) {
	d.WorkflowDefinition.Execute(&auditEnvironment{WorkflowEnvironment: env, audit: d.audit}, header, input)
}

type auditEnvironment struct {
	bindings.WorkflowEnvironment
	audit *replayAudit
}

func (e *auditEnvironment) RegisterUpdateHandler(handler func(string, string, *commonpb.Payloads, *commonpb.Header, bindings.UpdateCallbacks)) {
	e.WorkflowEnvironment.RegisterUpdateHandler(func(name, id string, args *commonpb.Payloads, header *commonpb.Header, c bindings.UpdateCallbacks) {
		handler(name, id, args, header, &auditCallbacks{UpdateCallbacks: c, id: id, audit: e.audit})
	})
}

type auditCallbacks struct {
	bindings.UpdateCallbacks
	id    string
	audit *replayAudit
}

func (c *auditCallbacks) Complete(value any, cause error) {
	actual := new(updatepb.Outcome)
	if cause != nil {
		fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{})
		actual.Value = &updatepb.Outcome_Failure{Failure: fc.ErrorToFailure(cause)}
	} else {
		payloads, err := converter.GetDefaultDataConverter().ToPayloads(value)
		if err != nil {
			c.audit.err = err
		}
		actual.Value = &updatepb.Outcome_Success{Success: payloads}
	}
	if expected, ok := c.audit.expected[c.id]; !ok || c.audit.seen[c.id] || !proto.Equal(actual, expected) {
		c.audit.err = fmt.Errorf("update %s replay outcome differs from saved history", c.id)
	}
	c.audit.seen[c.id] = true
	c.UpdateCallbacks.Complete(value, cause)
}
