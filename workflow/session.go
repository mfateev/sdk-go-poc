package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"isolate"
	"time"

	"go.temporal.io/sdk/temporal"
	goWorkflow "go.temporal.io/sdk/workflow"
)

type SessionOptions = goWorkflow.SessionOptions
type SessionState = goWorkflow.SessionState

// The SDK exports these enum values as process-owned variables. Keep the pinned
// wire values as constants so isolate initialization never reads those globals.
const (
	SessionStateOpen SessionState = iota
	SessionStateFailed
	SessionStateClosed
)

var ErrSessionFailed = errors.New("session has failed")

const sessionCreationActivity = "internalSessionCreationActivity"
const sessionCompletionActivity = "internalSessionCompletionActivity"

type sessionKey struct{}

// SessionInfo has the SDK's public fields; its native context and queue belong
// to this isolate. IDs are deterministic execution/sequence IDs rather than UUIDs.
type SessionInfo struct {
	SessionID         string
	HostName          string
	SessionState      SessionState
	taskqueue         string
	cancel            context.CancelFunc
	completionContext context.Context
}

func GetSessionInfo(ctx context.Context) *SessionInfo {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(sessionKey{}).(*SessionInfo)
	return s
}
func (s *SessionInfo) GetRecreateToken() []byte {
	p, err := json.Marshal(struct{ Taskqueue string }{s.taskqueue})
	if err != nil {
		panic(err)
	}
	return p
}
func CreateSession(ctx context.Context, options *SessionOptions) (context.Context, error) {
	if ctx == nil {
		return nil, errors.New("workflow: nil context")
	}
	return createSession(ctx, GetActivityOptions(ctx).TaskQueue+"__internal_session_creation", options)
}
func RecreateSession(ctx context.Context, token []byte, options *SessionOptions) (context.Context, error) {
	var p struct{ Taskqueue string }
	if err := json.Unmarshal(token, &p); err != nil {
		return nil, err
	}
	if p.Taskqueue == "" {
		return nil, errors.New("workflow: invalid recreate token")
	}
	return createSession(ctx, p.Taskqueue, options)
}
func createSession(ctx context.Context, queue string, options *SessionOptions) (context.Context, error) {
	assertWritable()
	if ctx == nil {
		return nil, errors.New("workflow: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options == nil || options.CreationTimeout <= 0 || options.ExecutionTimeout <= 0 || options.HeartbeatTimeout < 0 {
		return nil, errors.New("workflow: session creation/execution timeouts must be positive")
	}
	if s := GetSessionInfo(ctx); s != nil && s.SessionState == SessionStateOpen {
		return nil, errors.New("found existing open session in the context")
	}
	id, err := isolate.Call(OpSessionID, nil)
	if err != nil {
		return nil, err
	}
	s := &SessionInfo{SessionID: string(id), SessionState: SessionStateOpen}
	s.completionContext = context.WithValue(ctx, sessionKey{}, s)
	sessionCtx, cancel := context.WithCancel(s.completionContext)
	s.cancel = cancel
	heartbeat := options.HeartbeatTimeout
	if heartbeat == 0 {
		heartbeat = 20 * time.Second
	}
	creationCtx := WithActivityOptions(sessionCtx, ActivityOptions{TaskQueue: queue, ScheduleToStartTimeout: options.CreationTimeout, StartToCloseTimeout: options.ExecutionTimeout, HeartbeatTimeout: heartbeat, RetryPolicy: &RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 1.1, MaximumInterval: 10 * time.Second, NonRetryableErrorTypes: []string{"TemporalTimeout:StartToClose", "TemporalTimeout:Heartbeat"}}})
	// Session responses are structs, while the public signal channel currently
	// exposes []byte. Decode the response directly under the receiving owner.
	type response struct{ Taskqueue, HostName, ResourceID string }
	type received struct {
		response
		err error
	}
	incoming := make(chan received, 1)
	go func() {
		p, err := call(sessionCtx, OpSignal, id)
		var signal Signal
		var value response
		if err == nil {
			err = json.Unmarshal(p, &signal)
		}
		if err == nil {
			payloads, decodeErr := decodePayloads(signal.Payloads)
			err = decodeErr
			if err == nil {
				err = checkArgumentCount(payloads, 1)
			}
			if err == nil {
				err = currentDataConverter().FromPayloads(payloads, &value)
			}
		}
		incoming <- received{value, err}
	}()
	creation := ExecuteActivity(creationCtx, sessionCreationActivity, s.SessionID)
	select {
	case r := <-incoming:
		if r.err != nil || r.Taskqueue == "" {
			cancel()
			if r.err == nil {
				r.err = errors.New("workflow: session response has no queue")
			}
			return nil, r.err
		}
		s.taskqueue, s.HostName = r.Taskqueue, r.HostName
	case result := <-creation.ToChannel():
		cancel()
		if result.Err == nil {
			return nil, errors.New("workflow: session creation completed without response")
		}
		return nil, result.Err
	}
	p, _ := json.Marshal(s)
	if _, err := isolate.Call(OpAddSession, p); err != nil {
		cancel()
		return nil, err
	}
	go func() {
		err := creation.Get(creationCtx, nil)
		if err != nil && !temporal.IsCanceledError(err) && !errors.Is(err, context.Canceled) && s.SessionState == SessionStateOpen {
			s.SessionState = SessionStateFailed
			removeSession(s.SessionID)
			cancel()
		}
	}()
	return sessionCtx, nil
}

func CompleteSession(ctx context.Context) {
	assertWritable()
	s := GetSessionInfo(ctx)
	if s == nil || s.SessionState != SessionStateOpen {
		return
	}
	s.cancel()
	completionCtx := WithActivityOptions(s.completionContext, ActivityOptions{ScheduleToStartTimeout: 3 * time.Second, StartToCloseTimeout: 3 * time.Second})
	// Completion stays on the resource queue while the session is open.
	_ = ExecuteActivity(completionCtx, sessionCompletionActivity, s.SessionID).Get(completionCtx, nil)
	s.SessionState = SessionStateClosed
	removeSession(s.SessionID)
}
func removeSession(id string) {
	if _, err := isolate.Call(OpRemoveSession, []byte(id)); err != nil {
		panic(err)
	}
}
