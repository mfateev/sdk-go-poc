package temporalbridge

import (
	"errors"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	goWorkflow "go.temporal.io/sdk/workflow"
	"isolate"
	"testing"
)

type effectEnvironment struct {
	bindings.WorkflowEnvironment
	completes int
}

func (e *effectEnvironment) Complete(*commonpb.Payloads, error) { e.completes++ }
func TestEffectFailsTaskWithoutCompletingExecution(t *testing.T) {
	env := &effectEnvironment{}
	d := &definition{env: env}
	effect := &isolate.EffectError{Operation: "os.ReadFile", Stack: "workflow stack"}
	func() {
		defer func() {
			if got := recover(); got != effect {
				t.Errorf("panic = %v", got)
			}
		}()
		d.fail(fmt.Errorf("isolate returned: %w", effect))
	}()
	if env.completes != 0 || !d.completed || d.StackTrace() != effect.Stack {
		t.Fatalf("complete calls=%d completed=%t stack=%s", env.completes, d.completed, d.StackTrace())
	}
}
func TestOrdinaryFailureStillCompletesExecution(t *testing.T) {
	env := &effectEnvironment{}
	d := &definition{env: env}
	d.fail(errors.New("ordinary failure"))
	if env.completes != 1 {
		t.Fatalf("complete calls=%d", env.completes)
	}
}

type logEnvironment struct {
	bindings.WorkflowEnvironment
	replay bool
	logger log.Logger
}

func (e *logEnvironment) IsReplaying() bool { return e.replay }
func (e *logEnvironment) WorkflowInfo() *goWorkflow.Info {
	return &goWorkflow.Info{WorkflowExecution: goWorkflow.Execution{ID: "workflow-id", RunID: "run-id"}, WorkflowType: goWorkflow.Type{Name: "Work"}}
}
func (e *logEnvironment) GetLogger() log.Logger { return e.logger }

type recordingLogger struct{ records, warnings []string }

func (l *recordingLogger) Debug(string, ...any)      {}
func (l *recordingLogger) Info(msg string, _ ...any) { l.records = append(l.records, msg) }
func (l *recordingLogger) Warn(msg string, _ ...any) { l.warnings = append(l.warnings, msg) }
func (l *recordingLogger) Error(string, ...any)      {}
func TestLoggingUsesHostMetadataWithoutReply(t *testing.T) {
	env := &logEnvironment{}
	var got []LogEvent
	var handler LogHandler
	d := &definition{env: env, resolveLogHandler: func() LogHandler { return handler }}
	// Configuration is read for every record, including after registration.
	handler = func(e LogEvent) { got = append(got, e) }
	for _, replay := range []bool{false, true} {
		env.replay = replay
		d.handleWrite(&isolate.Message{Op: isolate.LogOp, Payload: append([]byte{0}, []byte("message")...)})
		if len(d.immediate) != 0 || len(d.pending) != 0 {
			t.Fatal("logging produced a reply")
		}
	}
	if len(got) != 2 || got[0].Replay || !got[1].Replay || got[0].WorkflowID != "workflow-id" || got[0].RunID != "run-id" || got[0].WorkflowType != "Work" || got[0].Message != "message" {
		t.Fatalf("events: %+v", got)
	}
	handler = nil
	logger := &recordingLogger{}
	env.logger = logger
	d.writeLog(isolate.LogRecord{Source: "fmt", Message: "default"})
	if len(logger.records) != 1 || logger.records[0] != "default" {
		t.Fatalf("default logger: %+v", logger.records)
	}
}

func TestLoggingFailuresRemainHostDiagnostics(t *testing.T) {
	logger := &recordingLogger{}
	env := &logEnvironment{logger: logger}
	d := &definition{env: env, resolveLogHandler: func() LogHandler {
		return func(LogEvent) { panic("backend unavailable") }
	}}
	d.handleWrite(&isolate.Message{Op: isolate.LogOp, Payload: []byte{0, 'x'}})
	d.handleWrite(&isolate.Message{Op: isolate.LogOp, Payload: nil})
	d.handleWrite(&isolate.Message{Op: 999})
	if len(logger.warnings) != 3 || d.completed || d.closed || len(d.immediate) != 0 || len(d.pending) != 0 {
		t.Fatalf("observations changed workflow state: warnings=%v completed=%v closed=%v", logger.warnings, d.completed, d.closed)
	}
}
